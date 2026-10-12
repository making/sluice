package am.ik.sluice.spike.h2demux;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.CRC32;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.Http2Error;
import io.netty.handler.codec.http2.Http2Exception;
import io.netty.handler.codec.http2.Http2FrameAdapter;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2Settings;
import io.netty.handler.codec.http2.Http2Stream;

/**
 * A netty HTTP/2 server at the far end of a {@link TunnelPair}, one connection per leg.
 * Paths: {@code /echo} answers with what it read; {@code /download/N} sends N bytes;
 * {@code /slow} answers after 200ms; {@code /reset} sends a head and 1KiB, then resets
 * with {@code ENHANCE_YOUR_CALM}; {@code /hang} never answers; {@code /malformed} writes
 * a SETTINGS frame on its stream, which RFC 9113 6.5 makes a connection error.
 */
final class TestUpstream implements AutoCloseable {

	static final int TUNNEL_WINDOW = 64 * 1024;

	private final String name;

	private final Http2Settings settings;

	private final Loop loop;

	private final List<Hop> connections = new CopyOnWriteArrayList<>();

	private final List<Integer> streamIds = new CopyOnWriteArrayList<>();

	private final Map<Integer, Long> resets = new ConcurrentHashMap<>();

	private final Map<Integer, AtomicLong> received = new ConcurrentHashMap<>();

	private final AtomicLong goAwayCode = new AtomicLong(-1);

	private final AtomicInteger maxActive = new AtomicInteger();

	private volatile boolean holdRequests;

	/** Request DATA read and not consumed, by connection and stream; with the lock. */
	private final Map<Hop, Map<Integer, Integer>> held = new HashMap<>();

	TestUpstream(String name, Http2Settings settings) {
		this.name = name;
		this.settings = settings;
		this.loop = new Loop("upstream-" + name);
	}

	static TestUpstream named(String name) {
		return new TestUpstream(name, new Http2Settings());
	}

	/** Takes the relay's leg: serves the far end of the pair. */
	Transport accept(TunnelPair pair, Timeline timeline) {
		AtomicReference<Hop> self = new AtomicReference<>();
		Hop hop = this.loop.call(() -> {
			Hop built = Hop.builder()
				.name("upstream-" + this.name + "-" + this.connections.size())
				.loop(this.loop)
				.transport(Transport.of(pair.remote()))
				.handler(Http2Handlers.spec()
					.server(true)
					.listener(new Listener(self))
					.settings(this.settings)
					.frameLogger(timeline.logger("upstream-" + this.name))
					.build())
				.build();
			self.set(built);
			return built;
		});
		this.connections.add(hop);
		hop.start();
		return Transport.of(pair.local());
	}

	/** Leaves request DATA unconsumed: the upstream's windows close. */
	void holdRequests() {
		this.holdRequests = true;
	}

	/** Consumes what was held and stops holding. */
	void release() {
		this.loop.run(() -> {
			this.holdRequests = false;
			this.held.forEach((hop, streams) -> streams.forEach((id, bytes) -> {
				Http2Stream stream = hop.connection().stream(id);
				if (stream != null) {
					try {
						hop.localFlow().consumeBytes(stream, bytes);
					}
					catch (Http2Exception e) {
						throw new IllegalStateException(e);
					}
				}
			}));
			this.held.clear();
		});
	}

	List<Integer> streamIds() {
		return List.copyOf(this.streamIds);
	}

	Map<Integer, Long> resets() {
		return Map.copyOf(this.resets);
	}

	long received(int streamId) {
		AtomicLong bytes = this.received.get(streamId);
		return bytes == null ? 0 : bytes.get();
	}

	long goAwayCode() {
		return this.goAwayCode.get();
	}

	int maxActive() {
		return this.maxActive.get();
	}

	int connections() {
		return this.connections.size();
	}

	@Override
	public void close() {
		this.loop.run(() -> this.connections.forEach(Hop::closeLocked));
		this.loop.close();
	}

	static byte[] pattern(int length) {
		byte[] bytes = new byte[length];
		for (int i = 0; i < length; i++) {
			bytes[i] = (byte) (i % 251);
		}
		return bytes;
	}

	static String crc(byte[] bytes) {
		CRC32 crc = new CRC32();
		crc.update(bytes);
		return Long.toHexString(crc.getValue());
	}

	/** One request as it arrives; with the lock. */
	private static final class Request {

		final String authority;

		final String path;

		final CRC32 crc = new CRC32();

		long bytes;

		Request(String authority, String path) {
			this.authority = authority;
			this.path = path;
		}

	}

	private final class Listener extends Http2FrameAdapter {

		private final AtomicReference<Hop> self;

		private final Map<Integer, Request> requests = new HashMap<>();

		Listener(AtomicReference<Hop> self) {
			this.self = self;
		}

		private Hop hop() {
			return this.self.get();
		}

		@Override
		public void onHeadersRead(ChannelHandlerContext ctx, int streamId, Http2Headers headers, int streamDependency,
				short weight, boolean exclusive, int padding, boolean endStream) {
			onHeadersRead(ctx, streamId, headers, padding, endStream);
		}

		@Override
		public void onHeadersRead(ChannelHandlerContext ctx, int streamId, Http2Headers headers, int padding,
				boolean endStream) {
			Request request = new Request(String.valueOf(headers.authority()), String.valueOf(headers.path()));
			this.requests.put(streamId, request);
			TestUpstream.this.streamIds.add(streamId);
			TestUpstream.this.maxActive.accumulateAndGet(hop().connection().numActiveStreams(), Math::max);
			if (request.path.equals("/malformed")) {
				// SETTINGS on a stream: a connection error PROTOCOL_ERROR (RFC 9113 6.5)
				hop().injectRawLocked(new byte[] { 0, 0, 0, 0x4, 0, 0, 0, 0, (byte) streamId });
				return;
			}
			if (request.path.equals("/reset")) {
				Http2Headers head = new DefaultHttp2Headers().status("200");
				hop().encoder().writeHeaders(ctx, streamId, head, 0, false, ctx.newPromise());
				hop().encoder()
					.writeData(ctx, streamId, Unpooled.wrappedBuffer(pattern(1024)), 0, false, ctx.newPromise());
				hop().encoder().writeRstStream(ctx, streamId, Http2Error.ENHANCE_YOUR_CALM.code(), ctx.newPromise());
				return;
			}
			if (endStream) {
				respond(ctx, streamId, request);
			}
		}

		@Override
		public int onDataRead(ChannelHandlerContext ctx, int streamId, ByteBuf data, int padding, boolean endStream) {
			int length = data.readableBytes();
			TestUpstream.this.received.computeIfAbsent(streamId, id -> new AtomicLong()).addAndGet(length);
			Request request = this.requests.get(streamId);
			if (request != null) {
				byte[] bytes = new byte[length];
				data.getBytes(data.readerIndex(), bytes);
				request.crc.update(bytes);
				request.bytes += length;
				if (endStream) {
					respond(ctx, streamId, request);
				}
			}
			if (TestUpstream.this.holdRequests) {
				TestUpstream.this.held.computeIfAbsent(hop(), hop -> new HashMap<>())
					.merge(streamId, length, Integer::sum);
				return padding;
			}
			return length + padding;
		}

		@Override
		public void onRstStreamRead(ChannelHandlerContext ctx, int streamId, long errorCode) {
			TestUpstream.this.resets.put(streamId, errorCode);
		}

		@Override
		public void onGoAwayRead(ChannelHandlerContext ctx, int lastStreamId, long errorCode, ByteBuf debugData) {
			TestUpstream.this.goAwayCode.set(errorCode);
		}

		private void respond(ChannelHandlerContext ctx, int streamId, Request request) {
			if (request.path.equals("/hang")) {
				return;
			}
			if (request.path.equals("/slow")) {
				Thread.ofVirtual().start(() -> {
					try {
						Thread.sleep(200);
					}
					catch (InterruptedException e) {
						Thread.currentThread().interrupt();
					}
					TestUpstream.this.loop.run(() -> echo(ctx, streamId, request));
				});
				return;
			}
			if (request.path.startsWith("/download/")) {
				int length = Integer.parseInt(request.path.substring("/download/".length()));
				hop().encoder()
					.writeHeaders(ctx, streamId, new DefaultHttp2Headers().status("200"), 0, false, ctx.newPromise());
				hop().encoder()
					.writeData(ctx, streamId, Unpooled.wrappedBuffer(pattern(length)), 0, true, ctx.newPromise());
				return;
			}
			echo(ctx, streamId, request);
		}

		private void echo(ChannelHandlerContext ctx, int streamId, Request request) {
			String body = "upstream=%s authority=%s stream=%d bytes=%d crc=%s".formatted(TestUpstream.this.name,
					request.authority, streamId, request.bytes, Long.toHexString(request.crc.getValue()));
			hop().encoder()
				.writeHeaders(ctx, streamId, new DefaultHttp2Headers().status("200"), 0, false, ctx.newPromise());
			hop().encoder()
				.writeData(ctx, streamId, Unpooled.copiedBuffer(body, StandardCharsets.UTF_8), 0, true,
						ctx.newPromise());
		}

	}

}
