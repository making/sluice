package am.ik.sluice.server.proxy.hop;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;

import am.ik.sluice.tunnel.DuplexPipe;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.Http2Exception;
import io.netty.handler.codec.http2.Http2FrameAdapter;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2Stream;

/**
 * A netty HTTP/2 server at the far end of a {@link TunnelPair}, one hop per leg. Paths:
 * {@code /echo} answers with the CRC of what it read; {@code /download/N/SEED} sends
 * {@link Payloads#of N bytes of SEED}; {@code /hang} never answers.
 */
final class TestUpstream implements AutoCloseable {

	private final String name;

	private final RecordingSink sink = new RecordingSink();

	private final Loop loop;

	private final List<Hop> connections = new CopyOnWriteArrayList<>();

	private final Map<Integer, AtomicLong> received = new ConcurrentHashMap<>();

	private volatile boolean holdRequests;

	/** Request DATA read and not consumed, by connection and stream; with the lock. */
	private final Map<Hop, Map<Integer, Integer>> held = new HashMap<>();

	TestUpstream(String name) {
		this.name = name;
		this.loop = new Loop("upstream-" + name, this.sink);
	}

	/** Serves the far end of the pair; returns the relay's end. */
	DuplexPipe accept(TunnelPair pair) {
		Hop hop = this.loop.call(() -> Hop.builder()
			.name("upstream-" + this.name + "-" + this.connections.size())
			.loop(this.loop)
			.pipe(DuplexPipe.of(pair.remote()))
			.server(true)
			.listener(Listener::new)
			.build());
		this.connections.add(hop);
		return DuplexPipe.of(pair.local());
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

	/** The request DATA payload read on the stream. */
	long received(int streamId) {
		AtomicLong bytes = this.received.get(streamId);
		return bytes == null ? 0 : bytes.get();
	}

	List<RecordingSink.Failure> failures() {
		return this.sink.failures();
	}

	List<Hop> connections() {
		return List.copyOf(this.connections);
	}

	/** Closes every connection and waits for their threads. */
	@Override
	public void close() {
		this.loop.close();
		Fixture.awaitDone(this.connections);
	}

	/** One request as it arrives; with the lock. */
	private static final class Request {

		final String path;

		final CRC32 crc = new CRC32();

		long bytes;

		Request(String path) {
			this.path = path;
		}

	}

	private final class Listener extends Http2FrameAdapter {

		private final Hop hop;

		private final Map<Integer, Request> requests = new HashMap<>();

		Listener(Hop hop) {
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
			if (this.requests.containsKey(streamId)) {
				return; // trailers
			}
			Request request = new Request(String.valueOf(headers.path()));
			this.requests.put(streamId, request);
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
				request.crc.update(data.nioBuffer());
				request.bytes += length;
				if (endStream) {
					respond(ctx, streamId, request);
				}
			}
			if (TestUpstream.this.holdRequests) {
				TestUpstream.this.held.computeIfAbsent(this.hop, hop -> new HashMap<>())
					.merge(streamId, length, Integer::sum);
				return padding;
			}
			return length + padding;
		}

		private void respond(ChannelHandlerContext ctx, int streamId, Request request) {
			if (request.path.equals("/hang")) {
				return;
			}
			ByteBuf body;
			if (request.path.startsWith("/download/")) {
				String[] parts = request.path.split("/");
				body = Unpooled.wrappedBuffer(Payloads.of(Long.parseLong(parts[3]), Integer.parseInt(parts[2])));
			}
			else {
				String text = "upstream=%s stream=%d bytes=%d crc=%s".formatted(TestUpstream.this.name, streamId,
						request.bytes, Long.toHexString(request.crc.getValue()));
				body = Unpooled.copiedBuffer(text, StandardCharsets.UTF_8);
			}
			this.hop.encoder()
				.writeHeaders(ctx, streamId, new DefaultHttp2Headers().status("200"), 0, false, ctx.newPromise());
			this.hop.encoder().writeData(ctx, streamId, body, 0, true, ctx.newPromise());
		}

	}

}
