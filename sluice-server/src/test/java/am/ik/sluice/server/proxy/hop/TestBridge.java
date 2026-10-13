package am.ik.sluice.server.proxy.hop;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import am.ik.sluice.tunnel.DuplexPipe;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http2.Http2ConnectionAdapter;
import io.netty.handler.codec.http2.Http2Error;
import io.netty.handler.codec.http2.Http2Exception;
import io.netty.handler.codec.http2.Http2FrameAdapter;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2Settings;
import io.netty.handler.codec.http2.Http2Stream;

/**
 * The smallest relay over hops, as the h2 demux bridges them: the client connection is a
 * server hop, each authority's leg a client hop on the same loop, and streams keep their
 * ids. Forwarded DATA is a retained slice of the reading hop's inbound buffer, written on
 * the other hop; the reading hop consumes it (grants its window back) once the other hop
 * wrote it, so what the bridge holds per stream is bounded by the window it announced.
 */
final class TestBridge implements AutoCloseable {

	private final RecordingSink sink = new RecordingSink();

	private final Loop loop = new Loop("relay", this.sink);

	private final Function<String, DuplexPipe> dialer;

	private final Hop client;

	/** Open legs by authority; with the lock. */
	private final Map<String, Hop> legs = new HashMap<>();

	/** Every leg ever opened; with the lock. */
	private final List<Hop> opened = new ArrayList<>();

	/** The leg of each bridged stream; with the lock. */
	private final Map<Integer, Hop> streams = new HashMap<>();

	private final Map<Integer, AtomicLong> fromClient = new ConcurrentHashMap<>();

	private final Map<Integer, AtomicLong> fromUpstream = new ConcurrentHashMap<>();

	TestBridge(DuplexPipe clientPipe, Function<String, DuplexPipe> dialer) {
		this.dialer = dialer;
		this.client = this.loop.call(() -> {
			Hop hop = Hop.builder()
				.name("relay-client")
				.loop(this.loop)
				.pipe(clientPipe)
				.server(true)
				.listener(ClientListener::new)
				.codec(codec -> codec.initialSettings(new Http2Settings().maxConcurrentStreams(100)))
				.onInactive(() -> List.copyOf(this.legs.values()).forEach(Hop::abortLocked))
				.build();
			hop.addConnectionListener(new Http2ConnectionAdapter() {

				@Override
				public void onStreamClosed(Http2Stream stream) {
					Hop leg = TestBridge.this.streams.remove(stream.id());
					if (leg == null) {
						return;
					}
					Http2Stream legStream = leg.connection().stream(stream.id());
					if (legStream != null && legStream.state() != Http2Stream.State.CLOSED) {
						leg.encoder()
							.writeRstStream(leg.ctx(), stream.id(), Http2Error.CANCEL.code(), leg.ctx().newPromise());
					}
				}

			});
			return hop;
		});
	}

	Hop client() {
		return this.client;
	}

	Loop loop() {
		return this.loop;
	}

	/** The client's DATA payload the bridge read on the stream. */
	long fromClient(int streamId) {
		return counter(this.fromClient, streamId).get();
	}

	/** The upstream's DATA payload the bridge read on the stream. */
	long fromUpstream(int streamId) {
		return counter(this.fromUpstream, streamId).get();
	}

	/** The relay's send window towards the client's connection. */
	int clientConnectionWindow() {
		return this.loop
			.call(() -> this.client.encoder().flowController().windowSize(this.client.connection().connectionStream()));
	}

	List<RecordingSink.Failure> failures() {
		return this.sink.failures();
	}

	List<Hop> hops() {
		return this.loop.call(() -> {
			List<Hop> hops = new ArrayList<>(this.opened);
			hops.add(this.client);
			return hops;
		});
	}

	@Override
	public void close() {
		List<Hop> hops = hops();
		this.loop.close();
		Fixture.awaitDone(hops);
	}

	private static AtomicLong counter(Map<Integer, AtomicLong> counters, int streamId) {
		return counters.computeIfAbsent(streamId, id -> new AtomicLong());
	}

	private Hop legFor(String authority) {
		Hop leg = this.legs.get(authority);
		if (leg != null && leg.open()) {
			return leg;
		}
		Hop dialed = Hop.builder()
			.name("relay-leg-" + authority)
			.loop(this.loop)
			.pipe(this.dialer.apply(authority))
			.listener(LegListener::new)
			.codec(codec -> codec.initialSettings(new Http2Settings().pushEnabled(false)))
			.build();
		this.legs.put(authority, dialed);
		this.opened.add(dialed);
		return dialed;
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

	/** Writes DATA read on one hop to the other. */
	private static int forward(Hop from, Hop to, int streamId, ByteBuf data, int padding, boolean endStream) {
		int length = data.readableBytes();
		to.encoder()
			.writeData(to.ctx(), streamId, data.retainedSlice(), 0, endStream,
					to.newPromise(future -> consume(from, streamId, length)));
		return padding;
	}

	/** The client's frames: heads, DATA and resets go to the stream's leg. */
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
			Hop leg = TestBridge.this.streams.get(streamId);
			if (leg == null) {
				leg = legFor(String.valueOf(headers.authority()));
				TestBridge.this.streams.put(streamId, leg);
			}
			leg.encoder().writeHeaders(leg.ctx(), streamId, headers, 0, endStream, leg.ctx().newPromise());
		}

		@Override
		public int onDataRead(ChannelHandlerContext ctx, int streamId, ByteBuf data, int padding, boolean endStream) {
			counter(TestBridge.this.fromClient, streamId).addAndGet(data.readableBytes());
			Hop leg = TestBridge.this.streams.get(streamId);
			if (leg == null) {
				return data.readableBytes() + padding;
			}
			return forward(this.hop, leg, streamId, data, padding, endStream);
		}

		@Override
		public void onRstStreamRead(ChannelHandlerContext ctx, int streamId, long errorCode) {
			Hop leg = TestBridge.this.streams.remove(streamId);
			if (leg != null) {
				leg.encoder().writeRstStream(leg.ctx(), streamId, errorCode, leg.ctx().newPromise());
			}
		}

	}

	/** A leg's frames: heads, DATA and resets go to the client. */
	private final class LegListener extends Http2FrameAdapter {

		private final Hop hop;

		LegListener(Hop hop) {
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
			Hop client = TestBridge.this.client;
			client.encoder().writeHeaders(client.ctx(), streamId, headers, 0, endStream, client.ctx().newPromise());
		}

		@Override
		public int onDataRead(ChannelHandlerContext ctx, int streamId, ByteBuf data, int padding, boolean endStream) {
			counter(TestBridge.this.fromUpstream, streamId).addAndGet(data.readableBytes());
			return forward(this.hop, TestBridge.this.client, streamId, data, padding, endStream);
		}

		@Override
		public void onRstStreamRead(ChannelHandlerContext ctx, int streamId, long errorCode) {
			Hop client = TestBridge.this.client;
			client.encoder().writeRstStream(client.ctx(), streamId, errorCode, client.ctx().newPromise());
		}

	}

}
