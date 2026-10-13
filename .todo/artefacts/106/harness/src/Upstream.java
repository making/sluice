import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http2.DefaultHttp2DataFrame;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.DefaultHttp2HeadersFrame;
import io.netty.handler.codec.http2.Http2DataFrame;
import io.netty.handler.codec.http2.Http2FrameCodecBuilder;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2HeadersFrame;
import io.netty.handler.codec.http2.Http2MultiplexHandler;
import io.netty.handler.codec.http2.Http2Settings;
import io.netty.handler.codec.http2.Http2StreamChannel;

/**
 * Load harness upstream. Port h1 speaks HTTP/1.1 only, port h2 speaks h2c prior knowledge
 * only, port ctl answers the byte tallies ({@code GET /tally}, {@code GET /reset}).
 * Paths: {@code /small} (2-byte body, browser-like response headers), {@code /bytes?n=N}
 * (N bytes, written as the peer's window / writability allows), {@code /sink} (consumes
 * the request body, answers its length), {@code /hold} (response head, never ends).
 */
public final class Upstream {

	static final FrameTally.Totals H2 = new FrameTally.Totals();

	static final FrameTally.Totals H1 = new FrameTally.Totals();

	static final ByteBuf CHUNK;

	static {
		byte[] bytes = new byte[16384];
		for (int i = 0; i < bytes.length; i++) {
			bytes[i] = (byte) ('a' + i % 26);
		}
		CHUNK = Unpooled.unreleasableBuffer(Unpooled.directBuffer(bytes.length).writeBytes(bytes));
	}

	static final byte[] SMALL = "ok".getBytes(StandardCharsets.US_ASCII);

	public static void main(String[] args) throws Exception {
		int h1Port = Integer.parseInt(args[0]);
		int h2Port = Integer.parseInt(args[1]);
		int ctlPort = Integer.parseInt(args[2]);
		int window = args.length > 3 ? Integer.parseInt(args[3]) : 65535;
		EventLoopGroup boss = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
		EventLoopGroup workers = new MultiThreadIoEventLoopGroup(4, NioIoHandler.newFactory());
		FrameTally h2Tally = new FrameTally(H2, true, true);
		FrameTally h1Tally = new FrameTally(H1, false, true);
		new ServerBootstrap().group(boss, workers)
			.channel(NioServerSocketChannel.class)
			.childOption(ChannelOption.TCP_NODELAY, true)
			.childHandler(new ChannelInitializer<SocketChannel>() {
				@Override
				protected void initChannel(SocketChannel ch) {
					ch.pipeline().addLast(h2Tally.forConnection());
					ch.pipeline()
						.addLast(Http2FrameCodecBuilder.forServer()
							.initialSettings(Http2Settings.defaultSettings().initialWindowSize(window))
							.build());
					ch.pipeline().addLast(new Http2MultiplexHandler(new ChannelInitializer<Http2StreamChannel>() {
						@Override
						protected void initChannel(Http2StreamChannel sc) {
							sc.pipeline().addLast(new H2Stream());
						}
					}));
				}
			})
			.bind(h2Port)
			.sync();
		new ServerBootstrap().group(boss, workers)
			.channel(NioServerSocketChannel.class)
			.childOption(ChannelOption.TCP_NODELAY, true)
			.childHandler(new ChannelInitializer<SocketChannel>() {
				@Override
				protected void initChannel(SocketChannel ch) {
					ch.pipeline().addLast(h1Tally.forConnection());
					ch.pipeline().addLast(new HttpServerCodec());
					ch.pipeline().addLast(new H1Handler(false));
				}
			})
			.bind(h1Port)
			.sync();
		new ServerBootstrap().group(boss, workers)
			.channel(NioServerSocketChannel.class)
			.childHandler(new ChannelInitializer<SocketChannel>() {
				@Override
				protected void initChannel(SocketChannel ch) {
					ch.pipeline().addLast(new HttpServerCodec());
					ch.pipeline().addLast(new H1Handler(true));
				}
			})
			.bind(ctlPort)
			.sync();
		System.out.println("upstream ready h1=" + h1Port + " h2=" + h2Port + " ctl=" + ctlPort + " window=" + window);
	}

	static String date() {
		return DateTimeFormatter.RFC_1123_DATE_TIME.format(ZonedDateTime.now(java.time.ZoneOffset.UTC));
	}

	static long sizeOf(String path) {
		int q = path.indexOf("n=");
		return q < 0 ? 0 : Long.parseLong(path.substring(q + 2));
	}

	static final class H2Stream extends ChannelInboundHandlerAdapter {

		private String path = "";

		private long received;

		private long remaining;

		private boolean pumping;

		@Override
		public void channelRead(ChannelHandlerContext ctx, Object msg) {
			if (msg instanceof Http2HeadersFrame frame) {
				this.path = String.valueOf(frame.headers().path());
				if (frame.isEndStream()) {
					respond(ctx);
				}
			}
			else if (msg instanceof Http2DataFrame frame) {
				this.received += frame.content().readableBytes();
				boolean end = frame.isEndStream();
				frame.release();
				if (end) {
					respond(ctx);
				}
			}
			else {
				io.netty.util.ReferenceCountUtil.release(msg);
			}
		}

		private Http2Headers head() {
			return new DefaultHttp2Headers().status("200")
				.set("content-type", "text/plain;charset=utf-8")
				.set("date", date())
				.set("server", "sluice-harness/1.0")
				.set("cache-control", "no-cache, no-store, must-revalidate")
				.set("vary", "Accept-Encoding")
				.set("x-request-id", UUID.randomUUID().toString());
		}

		private void respond(ChannelHandlerContext ctx) {
			if (this.path.startsWith("/small")) {
				ctx.write(new DefaultHttp2HeadersFrame(head().setInt("content-length", SMALL.length)));
				ctx.writeAndFlush(new DefaultHttp2DataFrame(Unpooled.wrappedBuffer(SMALL), true));
			}
			else if (this.path.startsWith("/sink")) {
				byte[] body = Long.toString(this.received).getBytes(StandardCharsets.US_ASCII);
				ctx.write(new DefaultHttp2HeadersFrame(head().setInt("content-length", body.length)));
				ctx.writeAndFlush(new DefaultHttp2DataFrame(Unpooled.wrappedBuffer(body), true));
			}
			else if (this.path.startsWith("/hold")) {
				ctx.writeAndFlush(new DefaultHttp2HeadersFrame(head()));
			}
			else if (this.path.startsWith("/bytes")) {
				this.remaining = sizeOf(this.path);
				ctx.write(new DefaultHttp2HeadersFrame(head().setLong("content-length", this.remaining),
						this.remaining == 0));
				this.pumping = true;
				pump(ctx);
			}
			else {
				ctx.writeAndFlush(new DefaultHttp2HeadersFrame(new DefaultHttp2Headers().status("404"), true));
			}
		}

		private void pump(ChannelHandlerContext ctx) {
			while (this.remaining > 0 && ctx.channel().isWritable()) {
				int n = (int) Math.min(CHUNK.capacity(), this.remaining);
				this.remaining -= n;
				ctx.write(new DefaultHttp2DataFrame(CHUNK.retainedSlice(0, n), this.remaining == 0));
			}
			ctx.flush();
		}

		@Override
		public void channelWritabilityChanged(ChannelHandlerContext ctx) {
			if (this.pumping) {
				ctx.executor().execute(() -> pump(ctx));
			}
			ctx.fireChannelWritabilityChanged();
		}

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
			ctx.close();
		}

	}

	static final class H1Handler extends ChannelInboundHandlerAdapter {

		private final boolean control;

		private String path = "";

		private long received;

		private long remaining;

		H1Handler(boolean control) {
			this.control = control;
		}

		@Override
		public void channelRead(ChannelHandlerContext ctx, Object msg) {
			if (msg instanceof HttpRequest request) {
				this.path = request.uri();
				this.received = 0;
				if (request.decoderResult().isFailure()) {
					System.err.println("decode failure " + request.decoderResult().cause());
				}
			}
			if (msg instanceof HttpContent content) {
				this.received += content.content().readableBytes();
				boolean last = content instanceof LastHttpContent;
				content.release();
				if (last) {
					respond(ctx);
				}
			}
		}

		private HttpResponse head(long length) {
			HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
			response.headers()
				.set("content-type", "text/plain;charset=utf-8")
				.set("date", date())
				.set("server", "sluice-harness/1.0")
				.set("cache-control", "no-cache, no-store, must-revalidate")
				.set("vary", "Accept-Encoding")
				.set("x-request-id", UUID.randomUUID().toString());
			if (length >= 0) {
				response.headers().set(HttpHeaderNames.CONTENT_LENGTH, length);
			}
			return response;
		}

		private void respond(ChannelHandlerContext ctx) {
			if (this.control) {
				String body;
				if (this.path.startsWith("/reset")) {
					for (FrameTally.Totals t : new FrameTally.Totals[] { H1, H2 }) {
						for (int i = 0; i < 12; i++) {
							t.in.set(i, 0);
							t.out.set(i, 0);
						}
					}
					body = "{}";
				}
				else {
					body = "{\"h1\":" + H1.json() + ",\"h2\":" + H2.json() + "}";
				}
				FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK,
						Unpooled.copiedBuffer(body, StandardCharsets.US_ASCII));
				response.headers().set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());
				ctx.writeAndFlush(response);
				return;
			}
			if (this.path.startsWith("/small")) {
				ctx.write(head(SMALL.length));
				ctx.writeAndFlush(new DefaultLastHttpContent(Unpooled.wrappedBuffer(SMALL)));
			}
			else if (this.path.startsWith("/sink")) {
				byte[] body = Long.toString(this.received).getBytes(StandardCharsets.US_ASCII);
				ctx.write(head(body.length));
				ctx.writeAndFlush(new DefaultLastHttpContent(Unpooled.wrappedBuffer(body)));
			}
			else if (this.path.startsWith("/hold")) {
				ctx.writeAndFlush(head(-1));
			}
			else if (this.path.startsWith("/bytes")) {
				this.remaining = sizeOf(this.path);
				ctx.write(head(this.remaining));
				pump(ctx);
			}
			else {
				System.err.println("404 path=" + this.path);
				FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
						HttpResponseStatus.NOT_FOUND);
				response.headers().set(HttpHeaderNames.CONTENT_LENGTH, 0);
				ctx.writeAndFlush(response);
			}
		}

		private void pump(ChannelHandlerContext ctx) {
			while (this.remaining > 0 && ctx.channel().isWritable()) {
				int n = (int) Math.min(CHUNK.capacity(), this.remaining);
				this.remaining -= n;
				ByteBuf slice = CHUNK.retainedSlice(0, n);
				ctx.write(this.remaining == 0 ? new DefaultLastHttpContent(slice) : new DefaultHttpContent(slice));
			}
			ctx.flush();
		}

		@Override
		public void channelWritabilityChanged(ChannelHandlerContext ctx) {
			if (this.remaining > 0) {
				ctx.executor().execute(() -> pump(ctx));
			}
			ctx.fireChannelWritabilityChanged();
		}

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
			ctx.close();
		}

	}

}
