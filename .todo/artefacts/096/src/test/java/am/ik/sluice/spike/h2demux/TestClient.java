package am.ik.sluice.spike.h2demux;

import java.io.ByteArrayOutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.Http2Exception;
import io.netty.handler.codec.http2.Http2FrameAdapter;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2Settings;
import io.netty.handler.codec.http2.Http2Stream;

/**
 * A netty HTTP/2 client over a real loopback socket to the relay. Its connection window
 * is granted back on arrival; its stream windows only as the test lets it consume.
 */
final class TestClient implements AutoCloseable {

	private final Loop loop = new Loop("client");

	private final Hop hop;

	private final Map<Integer, Exchange> exchanges = new ConcurrentHashMap<>();

	private final AtomicLong goAwayCode = new AtomicLong(-1);

	private volatile boolean holdResponses;

	/** The id the next request opens; with the lock. */
	private int nextStreamId = 1;

	/** Response DATA read and not consumed, by stream; with the lock. */
	private final Map<Integer, Integer> held = new HashMap<>();

	TestClient(Socket socket, Http2Settings settings, Timeline timeline) {
		this.hop = this.loop.call(() -> Hop.builder()
			.name("client")
			.loop(this.loop)
			.transport(Transport.of(socket))
			.handler(Http2Handlers.spec()
				.server(false)
				.listener(new Listener())
				.settings(settings)
				.autoRefillConnectionWindow(true)
				.build())
			.build());
		this.hop.start();
	}

	record Request(String authority, String path) {
	}

	/** A request sent: its stream id and its response. */
	record Sent(int streamId, CompletableFuture<Response> response) {
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

	private static final class Exchange {

		final Response response = new Response();

		final CompletableFuture<Response> done = new CompletableFuture<>();

		final AtomicLong received = new AtomicLong();

	}

	/** Sends the request with its body, ending the stream. */
	Sent send(Request request, byte[] body) {
		return open(request, body, true);
	}

	/** Sends the head and the body, leaving the stream open. */
	Sent sendOpen(Request request, byte[] body) {
		return open(request, body, false);
	}

	private Sent open(Request request, byte[] body, boolean end) {
		return this.loop.call(() -> {
			int id = this.nextStreamId;
			this.nextStreamId += 2;
			Exchange exchange = new Exchange();
			this.exchanges.put(id, exchange);
			Http2Headers headers = new DefaultHttp2Headers().method(body.length == 0 && end ? "GET" : "POST")
				.scheme("http")
				.authority(request.authority())
				.path(request.path());
			ChannelHandlerContext ctx = this.hop.ctx();
			boolean headOnly = body.length == 0 && end;
			this.hop.encoder().writeHeaders(ctx, id, headers, 0, headOnly, ctx.newPromise());
			if (!headOnly) {
				this.hop.encoder().writeData(ctx, id, Unpooled.wrappedBuffer(body), 0, end, ctx.newPromise());
			}
			return new Sent(id, exchange.done);
		});
	}

	void reset(int streamId, long errorCode) {
		this.loop.run(() -> this.hop.encoder()
			.writeRstStream(this.hop.ctx(), streamId, errorCode, this.hop.ctx().newPromise()));
	}

	/** Leaves response DATA unconsumed: the client's stream windows close. */
	void holdResponses() {
		this.holdResponses = true;
	}

	void releaseResponses() {
		this.loop.run(() -> {
			this.holdResponses = false;
			this.held.forEach((id, bytes) -> {
				Http2Stream stream = this.hop.connection().stream(id);
				if (stream != null) {
					try {
						this.hop.localFlow().consumeBytes(stream, bytes);
					}
					catch (Http2Exception e) {
						throw new IllegalStateException(e);
					}
				}
			});
			this.held.clear();
		});
	}

	/** Grants the relay more connection window at once (a WINDOW_UPDATE on stream 0). */
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
		this.hop.pauseReading();
	}

	void resumeReading() {
		this.hop.resumeReading();
	}

	/** The response DATA payload read on the stream. */
	long received(int streamId) {
		Exchange exchange = this.exchanges.get(streamId);
		return exchange == null ? 0 : exchange.received.get();
	}

	long goAwayCode() {
		return this.goAwayCode.get();
	}

	boolean open() {
		return this.loop.call(this.hop::open);
	}

	@Override
	public void close() {
		this.loop.run(this.hop::closeLocked);
		this.loop.close();
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
				exchange.received.addAndGet(length);
				byte[] bytes = new byte[length];
				data.getBytes(data.readerIndex(), bytes);
				exchange.response.body.writeBytes(bytes);
				if (endStream) {
					exchange.done.complete(exchange.response);
				}
			}
			if (TestClient.this.holdResponses) {
				TestClient.this.held.merge(streamId, length, Integer::sum);
				return padding;
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
			TestClient.this.goAwayCode.set(errorCode);
		}

	}

}
