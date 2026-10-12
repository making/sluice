package am.ik.sluice.spike.h2demux;

import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.Http2CodecUtil;
import io.netty.handler.codec.http2.Http2ConnectionAdapter;
import io.netty.handler.codec.http2.Http2Error;
import io.netty.handler.codec.http2.Http2Exception;
import io.netty.handler.codec.http2.Http2FrameAdapter;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2Settings;
import io.netty.handler.codec.http2.Http2Stream;
import org.jspecify.annotations.Nullable;

/**
 * The h2 demux on netty's codec: the client connection is a netty HTTP/2 server endpoint,
 * every leg a netty HTTP/2 client endpoint over a {@link Transport} (a tunnel
 * {@code VirtualConnection}); this class only bridges streams between them. Each hop is a
 * complete HTTP/2 connection of its own -- framing, HPACK, stream states, SETTINGS, PING,
 * GOAWAY and both flow controllers are netty's -- and a stream keeps its id on its leg
 * (each leg opens a subsequence of the client's ids, which netty accepts: skipped ids are
 * implicitly closed, RFC 9113 5.1.1).
 * <p>
 * Flow control across the bridge: DATA read on one hop is written on the other, and the
 * reading hop's {@code consumeBytes} -- which is what makes netty send WINDOW_UPDATE --
 * runs only once the writing hop's remote flow controller actually wrote the frame. So
 * what the relay holds per stream is bounded by the window it announced to the sender,
 * and a slow receiver backs the sender up hop by hop. Connection windows are granted back
 * on arrival (auto refill), so one stalled stream never holds the connection window the
 * other streams need.
 */
public final class NettyDemuxRelay implements AutoCloseable {

	/** Opens the transport of a new leg for the authority. */
	@FunctionalInterface
	public interface LegDialer {

		Transport dial(String authority);

	}

	private final Spec spec;

	private final Loop loop = new Loop("relay");

	private final Hop client;

	/** Alive legs by authority; with the loop's lock. */
	private final Map<String, Leg> legs = new HashMap<>();

	/** Bridged streams by id; with the loop's lock. */
	private final Map<Integer, Exchange> exchanges = new HashMap<>();

	private final List<String> dialed = new ArrayList<>();

	private final Map<Integer, AtomicLong> fromClient = new ConcurrentHashMap<>();

	private final Map<Integer, AtomicLong> fromUpstream = new ConcurrentHashMap<>();

	private NettyDemuxRelay(Socket socket, Spec spec) {
		this.spec = spec;
		Http2Settings settings = new Http2Settings().maxConcurrentStreams(100).initialWindowSize(spec.clientWindow);
		this.client = this.loop.call(() -> {
			Hop hop = Hop.builder()
				.name("relay-client")
				.loop(this.loop)
				.transport(Transport.of(socket))
				.writabilityFeedback(spec.writabilityFeedback)
				.handler(Http2Handlers.spec()
					.server(true)
					.listener(new ClientListener())
					.settings(settings)
					.autoRefillConnectionWindow(spec.autoRefillConnectionWindow)
					.frameLogger(spec.timeline == null ? null : spec.timeline.logger("relay-client"))
					.build())
				.onInactive(this::clientClosedLocked)
				.build();
			hop.connection().addListener(new Http2ConnectionAdapter() {

				@Override
				public void onStreamClosed(Http2Stream stream) {
					clientStreamClosedLocked(stream.id());
				}

			});
			return hop;
		});
		this.client.start();
	}

	/** Relays the accepted client socket. */
	public static NettyDemuxRelay start(Socket socket, Spec spec) {
		return new NettyDemuxRelay(socket, spec.validated());
	}

	public static Spec spec() {
		return new Spec();
	}

	/** The client's DATA payload the relay read on the stream. */
	public long fromClient(int streamId) {
		return counter(this.fromClient, streamId).get();
	}

	/** The upstream's DATA payload the relay read on the stream. */
	public long fromUpstream(int streamId) {
		return counter(this.fromUpstream, streamId).get();
	}

	public int clientActiveStreams() {
		return this.loop.call(() -> this.client.connection().numActiveStreams());
	}

	/** Active streams of the authority's leg; -1 when it has none alive. */
	public int legActiveStreams(String authority) {
		return this.loop.call(() -> {
			Leg leg = this.legs.get(authority);
			return leg == null ? -1 : leg.hop.connection().numActiveStreams();
		});
	}

	/** The authorities legs were dialed for, in order. */
	public List<String> dialed() {
		return this.loop.call(() -> List.copyOf(this.dialed));
	}

	public int exchanges() {
		return this.loop.call(this.exchanges::size);
	}

	/** Whether the client hop's writer queue ever turned the channel unwritable. */
	public boolean clientEverUnwritable() {
		return this.client.everUnwritable();
	}

	/** Exceptions that reached the end of any hop's pipeline. */
	public List<Throwable> failures() {
		return this.loop.call(() -> {
			List<Throwable> failures = new ArrayList<>(this.client.failuresLocked());
			this.legs.values().forEach(leg -> failures.addAll(leg.hop.failuresLocked()));
			return failures;
		});
	}

	@Override
	public void close() {
		this.loop.run(() -> {
			this.client.closeLocked();
			List.copyOf(this.legs.values()).forEach(leg -> leg.hop.closeLocked());
		});
		this.loop.close();
	}

	private static AtomicLong counter(Map<Integer, AtomicLong> counters, int streamId) {
		return counters.computeIfAbsent(streamId, id -> new AtomicLong());
	}

	private Leg legFor(String authority) {
		Leg leg = this.legs.get(authority);
		if (leg != null && leg.hop.open()) {
			return leg;
		}
		Leg opened = new Leg(authority, Objects.requireNonNull(this.spec.dialer).dial(authority));
		this.legs.put(authority, opened);
		this.dialed.add(authority);
		opened.hop.start();
		return opened;
	}

	private void clientClosedLocked() {
		// HTTP/2 has no half-closed connection: the client is gone, and so are the legs
		List.copyOf(this.legs.values()).forEach(leg -> leg.hop.closeLocked());
	}

	/**
	 * The client's side of a stream closed: the exchange ends. A response the upstream
	 * has not ended -- the client's connection went away, say -- is cancelled on the leg.
	 */
	private void clientStreamClosedLocked(int streamId) {
		Exchange exchange = this.exchanges.remove(streamId);
		if (exchange == null || exchange.done) {
			return;
		}
		exchange.done = true;
		if (!exchange.responseEnded) {
			exchange.leg.hop.encoder()
				.writeRstStream(exchange.leg.ctx(), streamId, Http2Error.CANCEL.code(),
						exchange.leg.ctx().newPromise());
		}
	}

	/** The relay's send window towards the client's connection. */
	public int clientConnectionWindow() {
		return this.loop
			.call(() -> this.client.encoder().flowController().windowSize(this.client.connection().connectionStream()));
	}

	/** Returns the client's DATA to its windows once the leg wrote it. */
	private void consumeClient(int streamId, int bytes) {
		consume(this.client, streamId, bytes);
	}

	private static void consume(Hop hop, int streamId, int bytes) {
		Http2Stream stream = hop.connection().stream(streamId);
		if (stream == null || bytes == 0) {
			return; // closed: netty returned its unconsumed bytes to the connection
		}
		try {
			hop.localFlow().consumeBytes(stream, bytes);
		}
		catch (Http2Exception e) {
			hop.closeLocked();
		}
	}

	private void resetClient(int streamId, long errorCode) {
		this.client.encoder().writeRstStream(this.client.ctx(), streamId, errorCode, this.client.ctx().newPromise());
	}

	/**
	 * A stream its leg lost without the upstream's END_STREAM or reset (the leg failed or
	 * closed): a {@code 502} while no response head was relayed, else
	 * {@code INTERNAL_ERROR} -- the error scope the relay keeps today.
	 */
	private void lostLocked(Exchange exchange) {
		exchange.done = true;
		this.exchanges.remove(exchange.id);
		if (exchange.responseBegun) {
			resetClient(exchange.id, Http2Error.INTERNAL_ERROR.code());
			return;
		}
		Http2Headers badGateway = new DefaultHttp2Headers().status("502");
		this.client.encoder()
			.writeHeaders(this.client.ctx(), exchange.id, badGateway, 0, true, this.client.ctx().newPromise());
		Http2Stream stream = this.client.connection().stream(exchange.id);
		if (stream != null && stream.state().remoteSideOpen()) {
			resetClient(exchange.id, Http2Error.NO_ERROR.code());
		}
	}

	/** One bridged stream; with the loop's lock. */
	private static final class Exchange {

		final int id;

		final Leg leg;

		boolean responseBegun;

		boolean responseEnded;

		/** Reset by either side, or lost: nothing more is bridged. */
		boolean done;

		Exchange(int id, Leg leg) {
			this.id = id;
			this.leg = leg;
		}

	}

	/** One leg: a netty HTTP/2 client endpoint towards one authority's upstream. */
	private final class Leg {

		final String authority;

		final Hop hop;

		/** The upstream's first SETTINGS has not arrived. */
		boolean awaitingSettings = true;

		Leg(String authority, Transport transport) {
			this.authority = authority;
			Http2Settings settings = new Http2Settings().pushEnabled(false)
				.initialWindowSize(NettyDemuxRelay.this.spec.legWindow);
			this.hop = Hop.builder()
				.name("relay-leg-" + authority)
				.loop(NettyDemuxRelay.this.loop)
				.transport(transport)
				.writabilityFeedback(NettyDemuxRelay.this.spec.writabilityFeedback)
				.handler(Http2Handlers.spec()
					.server(false)
					.listener(new LegListener(this))
					.settings(settings)
					.autoRefillConnectionWindow(NettyDemuxRelay.this.spec.autoRefillConnectionWindow)
					.bufferStreams(true)
					.frameLogger(NettyDemuxRelay.this.spec.timeline == null ? null
							: NettyDemuxRelay.this.spec.timeline.logger("relay-leg-" + authority))
					.build())
				.onInactive(() -> NettyDemuxRelay.this.legs.remove(authority, this))
				.build();
			// one stream until the upstream's SETTINGS tell its limit, as the relay does
			// today (RFC 9113 5.1.2 leaves the limit open until then). Applied through
			// the
			// encoder: StreamBufferingEncoder keeps a limit of its own (100 until the
			// peer's SETTINGS) and reads the connection's only in remoteSettings.
			try {
				this.hop.encoder().remoteSettings(new Http2Settings().maxConcurrentStreams(1));
			}
			catch (Http2Exception e) {
				throw new IllegalStateException(e);
			}
			this.hop.connection().addListener(new Http2ConnectionAdapter() {

				@Override
				public void onStreamClosed(Http2Stream stream) {
					Exchange exchange = NettyDemuxRelay.this.exchanges.get(stream.id());
					if (exchange != null && exchange.leg == Leg.this && !exchange.done && !exchange.responseEnded) {
						lostLocked(exchange);
					}
				}

			});
		}

		ChannelHandlerContext ctx() {
			return this.hop.ctx();
		}

	}

	/** The client hop's frames: request heads, request DATA and resets go to the legs. */
	private final class ClientListener extends Http2FrameAdapter {

		@Override
		public void onHeadersRead(ChannelHandlerContext ctx, int streamId, Http2Headers headers, int streamDependency,
				short weight, boolean exclusive, int padding, boolean endStream) {
			onHeadersRead(ctx, streamId, headers, padding, endStream);
		}

		@Override
		public void onHeadersRead(ChannelHandlerContext ctx, int streamId, Http2Headers headers, int padding,
				boolean endStream) {
			Exchange exchange = NettyDemuxRelay.this.exchanges.get(streamId);
			if (exchange != null) {
				if (!exchange.done) {
					// trailers
					exchange.leg.hop.encoder()
						.writeHeaders(exchange.leg.ctx(), streamId, headers, 0, endStream,
								exchange.leg.ctx().newPromise());
				}
				return;
			}
			Leg leg = legFor(String.valueOf(headers.authority()));
			Exchange opened = new Exchange(streamId, leg);
			NettyDemuxRelay.this.exchanges.put(streamId, opened);
			ChannelPromise promise = leg.ctx().newPromise();
			promise.addListener(future -> {
				if (!future.isSuccess() && !opened.done) {
					// the leg could not open it (closed, or its upstream's GOAWAY): it
					// never reached the upstream, safe to retry (RFC 9113 8.7)
					opened.done = true;
					NettyDemuxRelay.this.exchanges.remove(streamId);
					resetClient(streamId, Http2Error.REFUSED_STREAM.code());
				}
			});
			leg.hop.encoder().writeHeaders(leg.ctx(), streamId, headers, 0, endStream, promise);
		}

		@Override
		public int onDataRead(ChannelHandlerContext ctx, int streamId, ByteBuf data, int padding, boolean endStream) {
			int length = data.readableBytes();
			counter(NettyDemuxRelay.this.fromClient, streamId).addAndGet(length);
			Exchange exchange = NettyDemuxRelay.this.exchanges.get(streamId);
			if (exchange == null || exchange.done) {
				return length + padding;
			}
			ChannelPromise promise = exchange.leg.ctx().newPromise();
			boolean onWrite = NettyDemuxRelay.this.spec.consumeOnWrite;
			if (onWrite) {
				// written or failed (the leg's stream reset): the bytes left the relay
				promise.addListener(future -> consumeClient(streamId, length));
			}
			exchange.leg.hop.encoder()
				.writeData(exchange.leg.ctx(), streamId, data.retainedSlice(), 0, endStream, promise);
			return onWrite ? padding : length + padding;
		}

		@Override
		public void onRstStreamRead(ChannelHandlerContext ctx, int streamId, long errorCode) {
			Exchange exchange = NettyDemuxRelay.this.exchanges.remove(streamId);
			if (exchange == null || exchange.done) {
				return;
			}
			exchange.done = true;
			exchange.leg.hop.encoder()
				.writeRstStream(exchange.leg.ctx(), streamId, errorCode, exchange.leg.ctx().newPromise());
		}

	}

	/** A leg hop's frames: response heads, response DATA and resets go to the client. */
	private final class LegListener extends Http2FrameAdapter {

		private final Leg leg;

		LegListener(Leg leg) {
			this.leg = leg;
		}

		@Override
		public void onSettingsRead(ChannelHandlerContext ctx, Http2Settings settings) throws Http2Exception {
			if (this.leg.awaitingSettings) {
				this.leg.awaitingSettings = false;
				if (settings.maxConcurrentStreams() == null) {
					// no limit announced: unlimited (RFC 9113 6.5.2); applied through the
					// encoder so the buffered streams open
					this.leg.hop.encoder().remoteSettings(new Http2Settings().maxConcurrentStreams(Integer.MAX_VALUE));
				}
			}
		}

		@Override
		public void onHeadersRead(ChannelHandlerContext ctx, int streamId, Http2Headers headers, int streamDependency,
				short weight, boolean exclusive, int padding, boolean endStream) {
			onHeadersRead(ctx, streamId, headers, padding, endStream);
		}

		@Override
		public void onHeadersRead(ChannelHandlerContext ctx, int streamId, Http2Headers headers, int padding,
				boolean endStream) {
			Exchange exchange = exchange(streamId);
			if (exchange == null) {
				return;
			}
			CharSequence status = headers.status();
			if (status == null || status.length() == 0 || status.charAt(0) != '1') {
				exchange.responseBegun = true;
			}
			if (endStream) {
				exchange.responseEnded = true;
			}
			NettyDemuxRelay.this.client.encoder()
				.writeHeaders(NettyDemuxRelay.this.client.ctx(), streamId, headers, 0, endStream,
						NettyDemuxRelay.this.client.ctx().newPromise());
		}

		@Override
		public int onDataRead(ChannelHandlerContext ctx, int streamId, ByteBuf data, int padding, boolean endStream) {
			int length = data.readableBytes();
			counter(NettyDemuxRelay.this.fromUpstream, streamId).addAndGet(length);
			Exchange exchange = exchange(streamId);
			if (exchange == null) {
				return length + padding;
			}
			if (endStream) {
				exchange.responseEnded = true;
			}
			ChannelPromise promise = NettyDemuxRelay.this.client.ctx().newPromise();
			boolean onWrite = NettyDemuxRelay.this.spec.consumeOnWrite;
			if (onWrite) {
				promise.addListener(future -> consume(this.leg.hop, streamId, length));
			}
			NettyDemuxRelay.this.client.encoder()
				.writeData(NettyDemuxRelay.this.client.ctx(), streamId, data.retainedSlice(), 0, endStream, promise);
			return onWrite ? padding : length + padding;
		}

		@Override
		public void onRstStreamRead(ChannelHandlerContext ctx, int streamId, long errorCode) {
			Exchange exchange = exchange(streamId);
			if (exchange == null) {
				return;
			}
			exchange.done = true;
			NettyDemuxRelay.this.exchanges.remove(streamId);
			resetClient(streamId, errorCode);
		}

		private @Nullable Exchange exchange(int streamId) {
			Exchange exchange = NettyDemuxRelay.this.exchanges.get(streamId);
			return exchange == null || exchange.leg != this.leg || exchange.done ? null : exchange;
		}

	}

	public static final class Spec {

		private boolean consumeOnWrite = true;

		private boolean autoRefillConnectionWindow = true;

		private boolean writabilityFeedback = true;

		private int clientWindow = Http2CodecUtil.DEFAULT_WINDOW_SIZE;

		private int legWindow = Http2CodecUtil.DEFAULT_WINDOW_SIZE;

		private @Nullable LegDialer dialer;

		private @Nullable Timeline timeline;

		private Spec() {
		}

		/** Records every frame of every hop. */
		public Spec timeline(Timeline timeline) {
			this.timeline = timeline;
			return this;
		}

		/**
		 * {@code true} (the design): DATA is consumed -- its window granted back -- once
		 * the other hop wrote it; {@code false}: on arrival, the negative control.
		 */
		public Spec consumeOnWrite(boolean consumeOnWrite) {
			this.consumeOnWrite = consumeOnWrite;
			return this;
		}

		public Spec autoRefillConnectionWindow(boolean autoRefillConnectionWindow) {
			this.autoRefillConnectionWindow = autoRefillConnectionWindow;
			return this;
		}

		public Spec writabilityFeedback(boolean writabilityFeedback) {
			this.writabilityFeedback = writabilityFeedback;
			return this;
		}

		/** The stream window the relay announces to the client. */
		public Spec clientWindow(int clientWindow) {
			this.clientWindow = clientWindow;
			return this;
		}

		/** The stream window the relay announces to every upstream. */
		public Spec legWindow(int legWindow) {
			this.legWindow = legWindow;
			return this;
		}

		public Spec dialer(LegDialer dialer) {
			this.dialer = dialer;
			return this;
		}

		Spec validated() {
			Objects.requireNonNull(this.dialer, "dialer is required");
			return this;
		}

	}

}
