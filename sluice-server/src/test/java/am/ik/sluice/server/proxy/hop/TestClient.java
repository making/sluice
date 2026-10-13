package am.ik.sluice.server.proxy.hop;

import java.io.ByteArrayOutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import am.ik.sluice.tunnel.DuplexPipe;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.Http2Exception;
import io.netty.handler.codec.http2.Http2FrameAdapter;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2Settings;

/**
 * A netty HTTP/2 client over a real loopback socket to the relay; its reads can be held
 * ({@link #pauseReading}), a client that stops reading its socket.
 */
final class TestClient implements AutoCloseable {

	private final RecordingSink sink = new RecordingSink();

	private final Loop loop = new Loop("client", this.sink);

	private final GatedPipe pipe;

	private final Hop hop;

	private final Map<Integer, Exchange> exchanges = new ConcurrentHashMap<>();

	/** The GOAWAY the relay sent, if any. */
	private volatile String goAway = "none";

	/** The id the next request opens; with the lock. */
	private int nextStreamId = 1;

	TestClient(Socket socket, Http2Settings settings) {
		this.pipe = new GatedPipe(DuplexPipe.of(socket));
		this.hop = this.loop.call(() -> Hop.builder()
			.name("client")
			.loop(this.loop)
			.pipe(this.pipe)
			.listener(hop -> new Listener())
			.codec(codec -> codec.initialSettings(settings))
			.onInactive(this::closed)
			.build());
	}

	/** A complete response, or the reset that ended it. */
	static final class Response {

		private String status = "";

		private final ByteArrayOutputStream body = new ByteArrayOutputStream();

		private long resetCode = -1;

		String status() {
			return this.status;
		}

		byte[] body() {
			return this.body.toByteArray();
		}

		String text() {
			return this.body.toString(StandardCharsets.UTF_8);
		}

		long resetCode() {
			return this.resetCode;
		}

		@Override
		public String toString() {
			return "status=%s reset=%d body=%d".formatted(this.status, this.resetCode, this.body.size());
		}

	}

	/** A request sent: its stream id and its response. */
	record Sent(int streamId, CompletableFuture<Response> response) {
	}

	private static final class Exchange {

		final Response response = new Response();

		final CompletableFuture<Response> done = new CompletableFuture<>();

		final AtomicLong received = new AtomicLong();

	}

	/** Sends a request with its body (none: a GET), ending the stream. */
	Sent send(String authority, String path, byte[] body) {
		return this.loop.call(() -> {
			int id = this.nextStreamId;
			this.nextStreamId += 2;
			Exchange exchange = new Exchange();
			this.exchanges.put(id, exchange);
			boolean headOnly = body.length == 0;
			Http2Headers headers = new DefaultHttp2Headers().method(headOnly ? "GET" : "POST")
				.scheme("http")
				.authority(authority)
				.path(path);
			ChannelHandlerContext ctx = this.hop.ctx();
			this.hop.encoder().writeHeaders(ctx, id, headers, 0, headOnly, ctx.newPromise());
			if (!headOnly) {
				this.hop.encoder().writeData(ctx, id, Unpooled.wrappedBuffer(body), 0, true, ctx.newPromise());
			}
			return new Sent(id, exchange.done);
		});
	}

	/** Grants the relay more connection window at once (WINDOW_UPDATE on stream 0). */
	void growConnectionWindow(int delta) {
		this.loop.run(() -> {
			try {
				this.hop.localFlow().incrementWindowSize(this.hop.connection().connectionStream(), delta);
			}
			catch (Http2Exception e) {
				throw new IllegalStateException(e);
			}
		});
	}

	void pauseReading() {
		this.pipe.pause();
	}

	void resumeReading() {
		this.pipe.resume();
	}

	/** The response DATA payload read on the stream. */
	long received(int streamId) {
		Exchange exchange = this.exchanges.get(streamId);
		return exchange == null ? 0 : exchange.received.get();
	}

	List<RecordingSink.Failure> failures() {
		return this.sink.failures();
	}

	Hop hop() {
		return this.hop;
	}

	/** The connection closed: what has no response by now gets none. */
	private void closed() {
		IllegalStateException closed = new IllegalStateException("the connection closed, GOAWAY " + this.goAway);
		this.exchanges.values().forEach(exchange -> exchange.done.completeExceptionally(closed));
	}

	@Override
	public void close() {
		this.loop.close();
		Fixture.awaitDone(List.of(this.hop));
	}

	private final class Listener extends Http2FrameAdapter {

		@Override
		public void onHeadersRead(ChannelHandlerContext ctx, int streamId, Http2Headers headers, int streamDependency,
				short weight, boolean exclusive, int padding, boolean endStream) {
			onHeadersRead(ctx, streamId, headers, padding, endStream);
		}

		@Override
		public void onHeadersRead(ChannelHandlerContext ctx, int streamId, Http2Headers headers, int padding,
				boolean endStream) {
			Exchange exchange = TestClient.this.exchanges.get(streamId);
			if (exchange == null) {
				return;
			}
			if (headers.status() != null) {
				exchange.response.status = headers.status().toString();
			}
			if (endStream) {
				exchange.done.complete(exchange.response);
			}
		}

		@Override
		public int onDataRead(ChannelHandlerContext ctx, int streamId, ByteBuf data, int padding, boolean endStream) {
			int length = data.readableBytes();
			Exchange exchange = TestClient.this.exchanges.get(streamId);
			if (exchange != null) {
				byte[] bytes = new byte[length];
				data.getBytes(data.readerIndex(), bytes);
				exchange.response.body.writeBytes(bytes);
				exchange.received.addAndGet(length);
				if (endStream) {
					exchange.done.complete(exchange.response);
				}
			}
			return length + padding;
		}

		@Override
		public void onRstStreamRead(ChannelHandlerContext ctx, int streamId, long errorCode) {
			Exchange exchange = TestClient.this.exchanges.get(streamId);
			if (exchange != null) {
				exchange.response.resetCode = errorCode;
				exchange.done.complete(exchange.response);
			}
		}

		@Override
		public void onGoAwayRead(ChannelHandlerContext ctx, int lastStreamId, long errorCode, ByteBuf debugData) {
			TestClient.this.goAway = "last=%d code=%d %s".formatted(lastStreamId, errorCode,
					debugData.toString(StandardCharsets.UTF_8));
		}

	}

}
