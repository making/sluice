package am.ik.sluice.server.proxy;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.SequenceInputStream;
import java.net.InetAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import am.ik.sluice.server.proxy.hop.Hop;
import am.ik.sluice.server.proxy.hop.Loop;
import am.ik.sluice.server.route.Router;
import am.ik.sluice.server.tunnel.SessionRegistry;
import am.ik.sluice.server.tunnel.TunnelSession;
import am.ik.sluice.tunnel.DuplexPipe;
import am.ik.sluice.tunnel.VirtualConnection;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http2.Http2ConnectionAdapter;
import io.netty.handler.codec.http2.Http2ConnectionHandlerBuilder;
import io.netty.handler.codec.http2.Http2Error;
import io.netty.handler.codec.http2.Http2Exception;
import io.netty.handler.codec.http2.Http2FrameAdapter;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2Settings;
import io.netty.handler.codec.http2.Http2Stream;
import io.netty.util.AsciiString;

/**
 * Relays one plaintext or TLS-terminated HTTP/2 client connection over per-route legs on
 * netty's HTTP/2 codec: the client connection is a server endpoint ({@link Hop}), each
 * route's leg -- a virtual connection of its tunnel session -- a client endpoint, all of
 * them on one {@link Loop}, so every channel operation of the relay runs under its lock.
 * <p>
 * The connection head {@link DataProxyServer} read (the preface up to the first header
 * block) is fed to the client endpoint first, the socket after it, so netty's codec
 * validates the first stream like any other; the first stream takes the route
 * {@code DataProxyServer} resolved for the connection without a second lookup (a lookup
 * advances the load balancing state). Every later request head is routed by its
 * {@code :authority} (else {@code host}) and access-checked on its own: a refused stream
 * gets the no-route ({@code 503}) or forbidden ({@code 403}) page written through the
 * client endpoint's flow controller, and the connection goes on. A stream of a
 * {@code force-http1} route is answered {@code 421} without a body, then reset with
 * {@code NO_ERROR} while its request is still being sent; it opens no leg. A head that is
 * no request (no {@code :method}, no {@code :path} but for a CONNECT, a CONNECT without
 * {@code :authority}) is reset with {@code PROTOCOL_ERROR}. A CONNECT stream is routed by
 * its {@code :authority} and logged with it as its path.
 * <p>
 * Streams of one route share the route's leg and keep their stream ids on it; with
 * {@code rewrite-host} the head's {@code :authority} becomes the route's address. DATA
 * read on one hop is written on the other as a retained slice and consumed (its window
 * granted back) once written, so what the relay holds per stream is bounded by the window
 * it announced. Resets pass either way with their code. A stream lost to its leg -- the
 * leg closed, or its stream closed without the response's end -- is reset with
 * {@code INTERNAL_ERROR}, one the leg never took ({@code REFUSED_STREAM}); a stream the
 * client ends without its request's end resets its upstream stream with {@code CANCEL}. A
 * leg whose channel closed or that received a GOAWAY takes no new stream: the route's
 * next stream opens a new leg.
 * <p>
 * netty's codec answers protocol errors itself; a header block that does not decode ends
 * the connection with a GOAWAY {@code COMPRESSION_ERROR}, logged as a request line naming
 * neither route, method nor path. The client endpoint's end -- EOF, error, the codec
 * closing it -- closes every leg. Once every hop's threads ended and the frames the legs
 * queued left the tunnel sender, the relay finishes: the connection is logged closed,
 * {@code onFinish} runs, then {@code release}.
 */
final class NettyHttp2DemuxRelay {

	private static final Logger log = LoggerFactory.getLogger(NettyHttp2DemuxRelay.class);

	/** The concurrent streams the relay advertises to the client. */
	static final int MAX_CONCURRENT_STREAMS = 100;

	/** The header list size limit of both faces, mirroring the connection head's. */
	static final int MAX_HEADER_LIST_SIZE = 64 * 1024;

	/** How long a hop's threads may take to end once its channel closed. */
	private static final Duration HOP_DONE_TIMEOUT = Duration.ofSeconds(30);

	private static final String HTTP_2 = "2";

	private final DuplexPipe pipe;

	private final byte[] seed;

	/** The stream of the connection head, routed to the initial route; -1 = none. */
	private int seedStreamId;

	private final Router router;

	private final SessionRegistry sessions;

	private final AccessControl accessControl;

	private final ErrorResponse errorResponse;

	private final AccessLogger.Connection access;

	private final H2DemuxMeters meters;

	private final Router.Route initialRoute;

	private final TunnelSession initialSession;

	private final @Nullable InetAddress peer;

	private final ConnectionLimits.KeepAliveDeadline deadline;

	private final Runnable onFinish;

	private final Runnable release;

	private final Loop loop;

	/** The client endpoint; set once in {@link #start()}. */
	private @Nullable Hop client;

	/** The leg taking new streams, by route; with the loop's lock. */
	private final Map<Router.Route, Leg> legs = new HashMap<>();

	/** Every leg opened; with the loop's lock. */
	private final List<Leg> opened = new ArrayList<>();

	/**
	 * The streams relayed to a leg, by id, until closed for the client; with the lock.
	 */
	private final Map<Integer, Exchange> exchanges = new HashMap<>();

	/** The highest stream id the client opened; with the loop's lock. */
	private int highestStreamId;

	/** The client hop, plus each leg until its queued frames left; zero = finished. */
	private final AtomicInteger pending = new AtomicInteger(1);

	private final AtomicBoolean finished = new AtomicBoolean();

	private NettyHttp2DemuxRelay(Builder builder) {
		this.pipe = Objects.requireNonNull(builder.pipe, "pipe is required");
		ConnectionHeadParser.Head head = Objects.requireNonNull(builder.head, "head is required");
		this.seed = head.bytes();
		this.seedStreamId = head.h2() == null ? -1 : head.h2().streamId();
		this.router = Objects.requireNonNull(builder.router, "router is required");
		this.sessions = Objects.requireNonNull(builder.sessions, "sessions is required");
		this.accessControl = Objects.requireNonNull(builder.accessControl, "accessControl is required");
		this.errorResponse = Objects.requireNonNull(builder.errorResponse, "errorResponse is required");
		this.access = Objects.requireNonNull(builder.access, "access is required");
		this.meters = Objects.requireNonNull(builder.meters, "meters is required");
		this.initialRoute = Objects.requireNonNull(builder.initialRoute, "initialRoute is required");
		this.initialSession = Objects.requireNonNull(builder.initialSession, "initialSession is required");
		this.peer = builder.peer;
		this.deadline = Objects.requireNonNull(builder.deadline, "deadline is required");
		this.onFinish = Objects.requireNonNull(builder.onFinish, "onFinish is required");
		this.release = Objects.requireNonNull(builder.release, "release is required");
		this.loop = new Loop("sluice-h2n",
				(source, cause) -> log.warn("h2 demux {}: {}", source, cause.toString(), cause));
	}

	static Builder builder() {
		return new Builder();
	}

	/**
	 * Starts the client endpoint over the connection head and the socket; the legs open
	 * as streams need them.
	 */
	void start() {
		Hop started;
		try {
			started = this.loop.call(() -> {
				Hop hop = Hop.builder()
					.name("sluice-h2n-client")
					.loop(this.loop)
					.pipe(seeded(this.pipe, this.seed))
					.server(true)
					.listener(ClientListener::new)
					.codec(codec -> configure(codec, new Http2Settings().maxConcurrentStreams(MAX_CONCURRENT_STREAMS)))
					.onInactive(this::clientClosedLocked)
					.build();
				this.client = hop;
				hop.addConnectionListener(new ClientStreams());
				return hop;
			});
		}
		catch (RuntimeException | Error e) {
			this.loop.close();
			throw e;
		}
		watch(started, this::settle);
	}

	/**
	 * Both faces' codec: the settings plus the header list limit (netty's default is
	 * 8KiB), and header blocks encoded as never-indexed literals, so every block the
	 * relay writes decodes on its own whatever the peer's decoder saw before -- as the
	 * legacy relay's table-less encoding does.
	 */
	private static void configure(Http2ConnectionHandlerBuilder codec, Http2Settings settings) {
		codec.initialSettings(settings.maxHeaderListSize(MAX_HEADER_LIST_SIZE))
			.headerSensitivityDetector((name, value) -> true);
	}

	private Hop client() {
		return Objects.requireNonNull(this.client, "the relay is not started");
	}

	/**
	 * The client pipe reading the connection head first, then the socket; writes,
	 * half-close and close stay the pipe's.
	 */
	private static DuplexPipe seeded(DuplexPipe pipe, byte[] seed) {
		InputStream source = new SequenceInputStream(new ByteArrayInputStream(seed), pipe.source());
		return new DuplexPipe() {

			@Override
			public InputStream source() {
				return source;
			}

			@Override
			public OutputStream sink() {
				return pipe.sink();
			}

			@Override
			public void shutdownOutput() throws Exception {
				pipe.shutdownOutput();
			}

			@Override
			public void close() throws Exception {
				pipe.close();
			}

		};
	}

	/** Routes a new stream's head; with the loop's lock. */
	private void routeLocked(int streamId, Http2Headers headers, boolean endStream) {
		boolean seedStream = streamId == this.seedStreamId;
		this.seedStreamId = -1;
		ConnectionHeadParser.Head.@Nullable Request request = ConnectionHeadParser.http2RequestOf(headers);
		if (request == null) {
			// malformed: a stream error, never forwarded (RFC 9113 8.1.1)
			CharSequence method = headers.method();
			this.access.request("-", method == null ? "-" : method.toString(), "-", HTTP_2);
			resetClientLocked(streamId, Http2Error.PROTOCOL_ERROR);
			return;
		}
		Router.Route route;
		TunnelSession session;
		if (seedStream) {
			// resolved with the connection; the lookup must not run twice
			route = this.initialRoute;
			session = this.initialSession;
		}
		else {
			String host = hostOf(headers);
			Optional<Router.Route> found = this.router.lookup(host);
			TunnelSession serving = found.flatMap(candidate -> this.sessions.find(candidate.clientId())).orElse(null);
			if (found.isEmpty() || serving == null) {
				refuseLocked(streamId, request, this.errorResponse.noRoutePage(host, request));
				return;
			}
			if (!this.accessControl.allowed(found.get(), this.peer,
					headerOf(headers, ConnectionHeadParser.Head.FORWARDED_FOR),
					headerOf(headers, ConnectionHeadParser.Head.FORWARDED))) {
				refuseLocked(streamId, request, this.errorResponse.forbiddenPage(host, request));
				return;
			}
			route = found.get();
			session = serving;
		}
		if (route.forceHttp1()) {
			misdirectLocked(streamId, route, request, endStream);
			return;
		}
		Leg leg = legLocked(route, session);
		if (leg == null) {
			resetClientLocked(streamId, Http2Error.REFUSED_STREAM);
			return;
		}
		if (route.rewriteHost()) {
			headers.authority(AsciiString.of(route.address()));
		}
		Exchange exchange = new Exchange(streamId, leg);
		exchange.requestEnded = endStream;
		this.exchanges.put(streamId, exchange);
		leg.routeMeters.trackedStreams().increment();
		Hop hop = leg.hop();
		hop.encoder().writeHeaders(hop.ctx(), streamId, headers, 0, endStream, hop.newPromise(future -> {
			if (!future.isSuccess()) {
				// the leg never took the stream: safe for the client to retry
				loseLocked(exchange, Http2Error.REFUSED_STREAM);
			}
		}));
		this.access.request(route.routeTag(), request.method(), request.path(), HTTP_2);
	}

	/** Answers a refused stream with the page; with the loop's lock. */
	private void refuseLocked(int streamId, ConnectionHeadParser.Head.Request request, ErrorResponse.StreamPage page) {
		this.access.request("-", request.method(), request.path(), HTTP_2);
		Hop hop = client();
		byte[] body = page.body();
		hop.encoder().writeHeaders(hop.ctx(), streamId, page.headers(), 0, body.length == 0, hop.ctx().newPromise());
		if (body.length > 0) {
			// through the flow controller: held to the client's windows
			hop.encoder().writeData(hop.ctx(), streamId, Unpooled.wrappedBuffer(body), 0, true, hop.ctx().newPromise());
		}
	}

	/**
	 * Answers a stream of a route reached over HTTP/1.1 only {@code 421}; a request still
	 * being sent is reset with {@code NO_ERROR} once the answer is written (RFC 9113
	 * 8.1). With the loop's lock.
	 */
	private void misdirectLocked(int streamId, Router.Route route, ConnectionHeadParser.Head.Request request,
			boolean endStream) {
		this.meters.misdirectedForHttp1();
		this.access.misdirected(route.routeTag(), request);
		Hop hop = client();
		hop.encoder()
			.writeHeaders(hop.ctx(), streamId, this.errorResponse.misdirectedHeaders(), 0, true,
					endStream ? hop.ctx().newPromise() : hop.newPromise(future -> {
						if (future.isSuccess()) {
							resetClientLocked(streamId, Http2Error.NO_ERROR);
						}
					}));
	}

	/**
	 * The leg taking the route's new streams: the open one, else a new one; null when
	 * none could be opened. With the loop's lock.
	 */
	private @Nullable Leg legLocked(Router.Route route, TunnelSession session) {
		Leg leg = this.legs.get(route);
		if (leg != null && leg.takesStreamsLocked()) {
			return leg;
		}
		VirtualConnection connection = session.open(route.address());
		Leg opening = new Leg(this.meters.route(route.routeTag()));
		try {
			Hop hop = Hop.builder()
				.name("sluice-h2n-leg-" + connection.connectionId())
				.loop(this.loop)
				.pipe(DuplexPipe.of(connection))
				.listener(built -> new LegListener(opening))
				.codec(codec -> configure(codec, new Http2Settings().pushEnabled(false)))
				.onInactive(() -> this.legs.remove(route, opening))
				.build();
			opening.hop = hop;
			hop.addConnectionListener(new LegStreams(opening));
		}
		catch (RuntimeException e) {
			log.warn("h2 demux: the leg of route {} did not open: {}", route.routeTag(), e.toString(), e);
			connection.close();
			session.remove(connection.connectionId());
			return null;
		}
		if (this.opened.isEmpty()) {
			this.access.connectionId(connection.connectionId());
		}
		this.opened.add(opening);
		this.legs.put(route, opening);
		this.pending.incrementAndGet();
		watch(opening.hop(), () -> {
			try {
				connection.close();
				session.remove(connection.connectionId());
			}
			finally {
				// the connection's slot covers what the leg still queues for the tunnel
				connection.afterQueued(this::settle);
			}
		});
		return opening;
	}

	/** The client endpoint closed: so do the legs; with the loop's lock. */
	private void clientClosedLocked() {
		for (Leg leg : this.opened) {
			leg.hop().abortLocked();
		}
	}

	/**
	 * Resets the exchange's client stream with the code unless it was reset or answered
	 * already: the leg lost it. With the loop's lock.
	 */
	private void loseLocked(Exchange exchange, Http2Error error) {
		if (exchange.reset || exchange.responseEnded) {
			return;
		}
		exchange.reset = true;
		resetClientLocked(exchange.id, error);
	}

	private void resetClientLocked(int streamId, Http2Error error) {
		reset(client(), streamId, error.code());
	}

	/** Resets the hop's stream unless it is closed or gone; with the loop's lock. */
	private static void reset(Hop hop, int streamId, long errorCode) {
		Http2Stream stream = hop.connection().stream(streamId);
		if (stream == null || stream.state() == Http2Stream.State.CLOSED) {
			// netty would write a reset of an unknown stream all the same
			return;
		}
		hop.encoder().writeRstStream(hop.ctx(), streamId, errorCode, hop.ctx().newPromise());
	}

	/** Returns DATA to the reading hop's window once the other hop wrote it. */
	private static void consume(Hop hop, int streamId, int bytes) {
		Http2Stream stream = hop.connection().stream(streamId);
		if (stream == null || bytes == 0) {
			return; // closed: netty returned its unconsumed bytes to the connection
		}
		try {
			hop.localFlow().consumeBytes(stream, bytes);
		}
		catch (Http2Exception e) {
			throw new IllegalStateException(e);
		}
	}

	/** Writes DATA read on one hop to the other; the bytes consumed at once (padding). */
	private static int forward(Hop from, Hop to, int streamId, ByteBuf data, int padding, boolean endStream) {
		int length = data.readableBytes();
		to.encoder()
			.writeData(to.ctx(), streamId, data.retainedSlice(), 0, endStream,
					to.newPromise(future -> consume(from, streamId, length)));
		return padding;
	}

	/**
	 * Waits on a virtual thread until the hop's threads ended, then runs {@code done},
	 * which settles the hop's share of {@link #pending}.
	 */
	private void watch(Hop hop, Runnable done) {
		Thread.ofVirtual().name(hop.name() + "-done").start(() -> {
			try {
				if (!hop.awaitDone(HOP_DONE_TIMEOUT)) {
					log.warn("h2 demux: {} did not end within {}", hop.name(), HOP_DONE_TIMEOUT);
				}
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			finally {
				done.run();
			}
		});
	}

	/** One of {@link #pending} settled; the last finishes the relay. */
	private void settle() {
		if (this.pending.decrementAndGet() == 0 && this.finished.compareAndSet(false, true)) {
			this.deadline.disarm();
			this.loop.close();
			this.access.close();
			try {
				this.onFinish.run();
			}
			finally {
				this.release.run();
			}
		}
	}

	private static @Nullable String hostOf(Http2Headers headers) {
		CharSequence authority = headers.authority();
		if (authority != null && !authority.isEmpty()) {
			return authority.toString();
		}
		CharSequence host = headers.get(AsciiString.cached("host"));
		return host == null || host.isEmpty() ? null : host.toString();
	}

	private static @Nullable String headerOf(Http2Headers headers, String name) {
		CharSequence value = headers.get(AsciiString.of(name));
		return value == null || value.isEmpty() ? null : value.toString();
	}

	/** One route's leg: the hop over its virtual connection. */
	private static final class Leg {

		final H2DemuxMeters.Route routeMeters;

		/** Set once the hop is built. */
		@Nullable Hop hop;

		Leg(H2DemuxMeters.Route routeMeters) {
			this.routeMeters = routeMeters;
		}

		Hop hop() {
			return Objects.requireNonNull(this.hop, "the leg's hop is not built");
		}

		/** Whether a new stream may go to the leg; with the loop's lock. */
		boolean takesStreamsLocked() {
			Hop hop = hop();
			return hop.open() && !hop.connection().goAwayReceived() && !hop.connection().goAwaySent();
		}

	}

	/** A client stream relayed to a leg; with the loop's lock. */
	private static final class Exchange {

		final int id;

		final Leg leg;

		/** The client's END_STREAM was forwarded to the leg. */
		boolean requestEnded;

		/** The upstream's END_STREAM was forwarded to the client. */
		boolean responseEnded;

		/** A reset was sent or relayed on either side. */
		boolean reset;

		Exchange(int id, Leg leg) {
			this.id = id;
			this.leg = leg;
		}

	}

	/** The client's frames: new heads are routed; the rest goes to the stream's leg. */
	private final class ClientListener extends Http2FrameAdapter {

		private final Hop hop;

		ClientListener(Hop hop) {
			this.hop = hop;
		}

		@Override
		public void onHeadersRead(ChannelHandlerContext ctx, int streamId, Http2Headers headers, int streamDependency,
				short weight, boolean exclusive, int padding, boolean endStream) {
			onHeadersRead(ctx, streamId, headers, padding, endStream);
		}

		@Override
		public void onHeadersRead(ChannelHandlerContext ctx, int streamId, Http2Headers headers, int padding,
				boolean endStream) {
			Exchange exchange = NettyHttp2DemuxRelay.this.exchanges.get(streamId);
			if (exchange != null) {
				// trailers
				if (!exchange.reset) {
					exchange.requestEnded |= endStream;
					Hop leg = exchange.leg.hop();
					leg.encoder().writeHeaders(leg.ctx(), streamId, headers, 0, endStream, leg.ctx().newPromise());
				}
				return;
			}
			if (streamId <= NettyHttp2DemuxRelay.this.highestStreamId) {
				return; // a later block of a stream the relay answered itself
			}
			NettyHttp2DemuxRelay.this.highestStreamId = streamId;
			routeLocked(streamId, headers, endStream);
		}

		@Override
		public int onDataRead(ChannelHandlerContext ctx, int streamId, ByteBuf data, int padding, boolean endStream) {
			Exchange exchange = NettyHttp2DemuxRelay.this.exchanges.get(streamId);
			if (exchange == null || exchange.reset) {
				return data.readableBytes() + padding;
			}
			exchange.requestEnded |= endStream;
			return forward(this.hop, exchange.leg.hop(), streamId, data, padding, endStream);
		}

		@Override
		public void onRstStreamRead(ChannelHandlerContext ctx, int streamId, long errorCode) {
			Exchange exchange = NettyHttp2DemuxRelay.this.exchanges.get(streamId);
			if (exchange != null && !exchange.reset) {
				exchange.reset = true;
				reset(exchange.leg.hop(), streamId, errorCode);
			}
		}

	}

	/**
	 * The client's stream lifecycle: a stream closed without its request's end cancels
	 * its upstream stream; a GOAWAY {@code COMPRESSION_ERROR} is logged.
	 */
	private final class ClientStreams extends Http2ConnectionAdapter {

		@Override
		public void onStreamClosed(Http2Stream stream) {
			Exchange exchange = NettyHttp2DemuxRelay.this.exchanges.remove(stream.id());
			if (exchange == null) {
				return;
			}
			exchange.leg.routeMeters.trackedStreams().decrement();
			if (!exchange.requestEnded && !exchange.reset) {
				exchange.reset = true;
				reset(exchange.leg.hop(), exchange.id, Http2Error.CANCEL.code());
			}
		}

		@Override
		public void onGoAwaySent(int lastStreamId, long errorCode, ByteBuf debugData) {
			if (errorCode == Http2Error.COMPRESSION_ERROR.code()) {
				// a header block that did not decode: nothing of it is known
				NettyHttp2DemuxRelay.this.access.request("-", "-", "-", HTTP_2);
			}
		}

	}

	/** A leg's frames: heads, DATA and resets go to the client. */
	private final class LegListener extends Http2FrameAdapter {

		private final Leg leg;

		LegListener(Leg leg) {
			this.leg = leg;
		}

		private @Nullable Exchange exchange(int streamId) {
			Exchange exchange = NettyHttp2DemuxRelay.this.exchanges.get(streamId);
			return exchange == null || exchange.leg != this.leg || exchange.reset ? null : exchange;
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
			exchange.responseEnded |= endStream;
			Hop client = client();
			client.encoder().writeHeaders(client.ctx(), streamId, headers, 0, endStream, client.ctx().newPromise());
		}

		@Override
		public int onDataRead(ChannelHandlerContext ctx, int streamId, ByteBuf data, int padding, boolean endStream) {
			Exchange exchange = exchange(streamId);
			if (exchange == null) {
				return data.readableBytes() + padding;
			}
			exchange.responseEnded |= endStream;
			return forward(this.leg.hop(), client(), streamId, data, padding, endStream);
		}

		@Override
		public void onRstStreamRead(ChannelHandlerContext ctx, int streamId, long errorCode) {
			Exchange exchange = exchange(streamId);
			if (exchange != null) {
				exchange.reset = true;
				resetClientLocked(streamId, errorCode);
			}
		}

		private void resetClientLocked(int streamId, long errorCode) {
			reset(client(), streamId, errorCode);
		}

	}

	/**
	 * A leg's stream lifecycle: a stream closed without the response's end -- the leg
	 * closed under it, say -- is lost for the client.
	 */
	private final class LegStreams extends Http2ConnectionAdapter {

		private final Leg leg;

		LegStreams(Leg leg) {
			this.leg = leg;
		}

		@Override
		public void onStreamClosed(Http2Stream stream) {
			Exchange exchange = NettyHttp2DemuxRelay.this.exchanges.get(stream.id());
			if (exchange != null && exchange.leg == this.leg) {
				loseLocked(exchange, Http2Error.INTERNAL_ERROR);
			}
		}

	}

	static final class Builder {

		private @Nullable DuplexPipe pipe;

		private ConnectionHeadParser.@Nullable Head head;

		private @Nullable Router router;

		private @Nullable SessionRegistry sessions;

		private @Nullable AccessControl accessControl;

		private @Nullable ErrorResponse errorResponse;

		private AccessLogger.@Nullable Connection access;

		private @Nullable H2DemuxMeters meters;

		private Router.@Nullable Route initialRoute;

		private @Nullable TunnelSession initialSession;

		private @Nullable InetAddress peer;

		private ConnectionLimits.@Nullable KeepAliveDeadline deadline;

		private @Nullable Runnable onFinish;

		private @Nullable Runnable release;

		private Builder() {
		}

		/** The client connection's pipe, past the connection head. */
		Builder pipe(DuplexPipe pipe) {
			this.pipe = pipe;
			return this;
		}

		/** The connection head read from the pipe, fed to the codec first. */
		Builder head(ConnectionHeadParser.Head head) {
			this.head = head;
			return this;
		}

		Builder router(Router router) {
			this.router = router;
			return this;
		}

		Builder sessions(SessionRegistry sessions) {
			this.sessions = sessions;
			return this;
		}

		Builder accessControl(AccessControl accessControl) {
			this.accessControl = accessControl;
			return this;
		}

		Builder errorResponse(ErrorResponse errorResponse) {
			this.errorResponse = errorResponse;
			return this;
		}

		Builder access(AccessLogger.Connection access) {
			this.access = access;
			return this;
		}

		Builder meters(H2DemuxMeters meters) {
			this.meters = meters;
			return this;
		}

		/** The route and session resolved for the connection head's stream. */
		Builder initialRoute(Router.Route initialRoute, TunnelSession initialSession) {
			this.initialRoute = initialRoute;
			this.initialSession = initialSession;
			return this;
		}

		/** The address judged by the access control; null when unknown. */
		Builder peer(@Nullable InetAddress peer) {
			this.peer = peer;
			return this;
		}

		/** The connection's deadline, disarmed once the relay finished. */
		Builder deadline(ConnectionLimits.KeepAliveDeadline deadline) {
			this.deadline = deadline;
			return this;
		}

		/** Runs once the relay finished. */
		Builder onFinish(Runnable onFinish) {
			this.onFinish = onFinish;
			return this;
		}

		/** Frees the connection's slot; runs after {@code onFinish}. */
		Builder release(Runnable release) {
			this.release = release;
			return this;
		}

		NettyHttp2DemuxRelay build() {
			return new NettyHttp2DemuxRelay(this);
		}

	}

}
