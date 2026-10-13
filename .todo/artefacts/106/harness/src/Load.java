import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http2.DefaultHttp2DataFrame;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.DefaultHttp2HeadersFrame;
import io.netty.handler.codec.http2.DefaultHttp2WindowUpdateFrame;
import io.netty.handler.codec.http2.Http2DataFrame;
import io.netty.handler.codec.http2.Http2FrameCodecBuilder;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2HeadersFrame;
import io.netty.handler.codec.http2.Http2MultiplexHandler;
import io.netty.handler.codec.http2.Http2ResetFrame;
import io.netty.handler.codec.http2.Http2Settings;
import io.netty.handler.codec.http2.Http2StreamChannel;
import io.netty.handler.codec.http2.Http2StreamChannelBootstrap;
import io.netty.util.ReferenceCountUtil;

/**
 * Load client for the harness: h2 (prior knowledge, S concurrent streams per connection)
 * or h1 (one request in flight per connection), C connections, warmup then a measured
 * window. Prints one JSON line. {@code --hold} opens C x S held streams and waits.
 */
public final class Load {

	static String host = "127.0.0.1";

	static int port;

	static String authority;

	static boolean h2 = true;

	static int connections = 1;

	static int streams = 1;

	static String path = "/small";

	static long upload = -1;

	static int warmup = 3;

	static int duration = 10;

	static int threads = 8;

	static int streamWindow = 65535;

	static int connWindow = 65535;

	static boolean hold;

	static int holdSeconds = 30;

	static volatile boolean running = true;

	static volatile boolean recording;

	static final AtomicLong bodyIn = new AtomicLong();

	static final AtomicLong bodyOut = new AtomicLong();

	static final AtomicLong completed = new AtomicLong();

	static final AtomicLong errors = new AtomicLong();

	static final AtomicLong resets = new AtomicLong();

	static final AtomicLong heads = new AtomicLong();

	static final Map<String, AtomicLong> errorKinds = new HashMap<>();

	static final FrameTally.Totals TALLY = new FrameTally.Totals();

	static final List<Conn> conns = new ArrayList<>();

	static final ByteBuf CHUNK;

	static {
		byte[] bytes = new byte[16384];
		Arrays.fill(bytes, (byte) 'u');
		CHUNK = Unpooled.unreleasableBuffer(Unpooled.directBuffer(bytes.length).writeBytes(bytes));
	}

	public static void main(String[] args) throws Exception {
		for (int i = 0; i < args.length; i += 2) {
			String v = args[i + 1];
			switch (args[i]) {
				case "--host" -> host = v;
				case "--port" -> port = Integer.parseInt(v);
				case "--authority" -> authority = v;
				case "--proto" -> h2 = v.equals("h2");
				case "--connections" -> connections = Integer.parseInt(v);
				case "--streams" -> streams = Integer.parseInt(v);
				case "--path" -> path = v;
				case "--upload" -> upload = Long.parseLong(v);
				case "--warmup" -> warmup = Integer.parseInt(v);
				case "--duration" -> duration = Integer.parseInt(v);
				case "--threads" -> threads = Integer.parseInt(v);
				case "--stream-window" -> streamWindow = Integer.parseInt(v);
				case "--conn-window" -> connWindow = Integer.parseInt(v);
				case "--hold" -> {
					hold = true;
					holdSeconds = Integer.parseInt(v);
				}
				default -> throw new IllegalArgumentException(args[i]);
			}
		}
		if (!h2) {
			streams = 1;
		}
		EventLoopGroup group = new MultiThreadIoEventLoopGroup(threads, NioIoHandler.newFactory());
		FrameTally tally = new FrameTally(TALLY, h2, false);
		Bootstrap bootstrap = new Bootstrap().group(group)
			.channel(NioSocketChannel.class)
			.option(ChannelOption.TCP_NODELAY, true)
			.handler(new ChannelInitializer<SocketChannel>() {
				@Override
				protected void initChannel(SocketChannel ch) {
					ch.pipeline().addLast(tally.forConnection());
					if (h2) {
						ch.pipeline()
							.addLast(Http2FrameCodecBuilder.forClient()
								.initialSettings(Http2Settings.defaultSettings().initialWindowSize(streamWindow))
								.build());
						ch.pipeline().addLast(new Http2MultiplexHandler(new ChannelInboundHandlerAdapter()));
					}
					else {
						ch.pipeline().addLast(new HttpClientCodec());
					}
				}
			});
		for (int i = 0; i < connections; i++) {
			Channel ch = bootstrap.connect(host, port).sync().channel();
			Conn conn = new Conn(ch);
			conns.add(conn);
			if (h2 && connWindow > 65535) {
				ch.writeAndFlush(new DefaultHttp2WindowUpdateFrame(connWindow - 65535));
			}
			if (!h2) {
				ch.pipeline().addLast(new H1Exchange(conn));
			}
		}
		if (hold) {
			runHold();
			group.shutdownGracefully(0, 1, TimeUnit.SECONDS);
			return;
		}
		for (Conn conn : conns) {
			for (int s = 0; s < streams; s++) {
				conn.next();
			}
		}
		Thread.sleep(warmup * 1000L);
		long in0 = bodyIn.get();
		long out0 = bodyOut.get();
		long done0 = completed.get();
		System.out.println("MARK start");
		System.out.flush();
		long t0 = System.nanoTime();
		recording = true;
		Thread.sleep(duration * 1000L);
		recording = false;
		long t1 = System.nanoTime();
		System.out.println("MARK stop");
		System.out.flush();
		long in = bodyIn.get() - in0;
		long out = bodyOut.get() - out0;
		long done = completed.get() - done0;
		running = false;
		double secs = (t1 - t0) / 1e9;
		long[] all = merge();
		Arrays.sort(all);
		StringBuilder sb = new StringBuilder("{");
		sb.append("\"proto\":\"").append(h2 ? "h2" : "h1").append('"');
		sb.append(",\"connections\":").append(connections).append(",\"streams\":").append(streams);
		sb.append(",\"path\":\"").append(path).append('"').append(",\"upload\":").append(upload);
		sb.append(",\"secs\":").append(String.format("%.3f", secs));
		sb.append(",\"requests\":").append(done).append(",\"rps\":").append(String.format("%.1f", done / secs));
		sb.append(",\"in_MBps\":").append(String.format("%.1f", in / secs / 1048576));
		sb.append(",\"out_MBps\":").append(String.format("%.1f", out / secs / 1048576));
		sb.append(",\"lat_n\":").append(all.length);
		for (double p : new double[] { 50, 90, 99, 99.9 }) {
			sb.append(",\"p").append(String.valueOf(p).replace(".0", "").replace('.', '_')).append("_ms\":");
			sb.append(String.format("%.3f", pct(all, p) / 1e6));
		}
		sb.append(",\"max_ms\":").append(String.format("%.3f", all.length == 0 ? 0 : all[all.length - 1] / 1e6));
		sb.append(",\"errors\":").append(errors.get()).append(",\"resets\":").append(resets.get());
		synchronized (errorKinds) {
			sb.append(",\"error_kinds\":\"").append(errorKinds.toString().replace('"', '\'')).append('"');
		}
		sb.append(",\"tally\":").append(TALLY.json()).append('}');
		System.out.println(sb);
		for (Conn conn : conns) {
			conn.ch.close();
		}
		group.shutdownGracefully(0, 1, TimeUnit.SECONDS).await(5, TimeUnit.SECONDS);
		System.exit(0);
	}

	static void runHold() throws Exception {
		int total = connections * streams;
		for (Conn conn : conns) {
			for (int s = 0; s < streams; s++) {
				conn.next();
			}
		}
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
		while (heads.get() < total && System.nanoTime() < deadline) {
			Thread.sleep(50);
		}
		System.out.println("HOLDING heads=" + heads.get() + " of " + total + " errors=" + errors.get() + " "
				+ errorKinds);
		System.out.flush();
		Thread.sleep(holdSeconds * 1000L);
		running = false;
		for (Conn conn : conns) {
			conn.ch.close();
		}
		System.out.println("RELEASED");
		System.exit(0);
	}

	static void error(String kind) {
		errors.incrementAndGet();
		synchronized (errorKinds) {
			errorKinds.computeIfAbsent(kind, k -> new AtomicLong()).incrementAndGet();
		}
	}

	static double pct(long[] sorted, double p) {
		if (sorted.length == 0) {
			return 0;
		}
		int i = (int) Math.ceil(p / 100.0 * sorted.length) - 1;
		return sorted[Math.max(0, Math.min(sorted.length - 1, i))];
	}

	static long[] merge() {
		int n = 0;
		for (Conn c : conns) {
			synchronized (c) {
				n += c.latCount;
			}
		}
		long[] all = new long[n];
		int k = 0;
		for (Conn c : conns) {
			synchronized (c) {
				System.arraycopy(c.lat, 0, all, k, Math.min(c.latCount, n - k));
				k += c.latCount;
			}
		}
		return all;
	}

	static Http2Headers requestHeaders() {
		Http2Headers h = new DefaultHttp2Headers().method(upload >= 0 ? "POST" : "GET")
			.scheme("http")
			.authority(authority)
			.path(path);
		browserHeaders((name, value) -> h.add(name, value));
		if (upload >= 0) {
			h.setLong("content-length", upload);
		}
		return h;
	}

	interface Adder {

		void add(String name, String value);

	}

	/** What a browser sends on a same-origin fetch, to make the header bytes realistic. */
	static void browserHeaders(Adder adder) {
		adder.add("user-agent",
				"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36");
		adder.add("accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8");
		adder.add("accept-encoding", "gzip, deflate, br, zstd");
		adder.add("accept-language", "en-US,en;q=0.9,ja;q=0.8");
		adder.add("cookie", "session=6f1c2a9e8b7d4c3f0a1e2d3c4b5a69788f7e6d5c4b3a2918; theme=dark; _ga=GA1.1.1234567890.1700000000");
		adder.add("sec-fetch-mode", "navigate");
		adder.add("sec-fetch-site", "same-origin");
	}

	/** Checks a response body: /bytes?n=N has N bytes, /sink echoes the upload length. */
	static final class BodyCheck {

		private long got;

		private final StringBuilder text = new StringBuilder();

		void add(ByteBuf content) {
			this.got += content.readableBytes();
			if (upload >= 0 && this.text.length() < 32) {
				this.text.append(content.toString(java.nio.charset.StandardCharsets.US_ASCII));
			}
		}

		void verify() {
			if (path.startsWith("/bytes")) {
				long n = Long.parseLong(path.substring(path.indexOf("n=") + 2));
				if (this.got != n) {
					error("short-body");
				}
			}
			else if (upload >= 0 && !this.text.toString().equals(Long.toString(upload))) {
				error("sink-mismatch");
			}
			this.got = 0;
			this.text.setLength(0);
		}

	}

	static final class Conn {

		final Channel ch;

		long[] lat = new long[1024];

		int latCount;

		Conn(Channel ch) {
			this.ch = ch;
		}

		synchronized void record(long started, long nanos) {
			if (!recording || started < 0) {
				return;
			}
			if (this.latCount == this.lat.length) {
				this.lat = Arrays.copyOf(this.lat, this.lat.length * 2);
			}
			this.lat[this.latCount++] = nanos;
		}

		void next() {
			if (!running || !this.ch.isActive()) {
				return;
			}
			if (h2) {
				new Http2StreamChannelBootstrap(this.ch).handler(new H2Exchange(this)).open().addListener(f -> {
					if (!f.isSuccess()) {
						error("open:" + f.cause().getClass().getSimpleName());
					}
				});
			}
			else {
				// on the channel's loop: the writability callback pumps there too
				H1Exchange exchange = this.ch.pipeline().get(H1Exchange.class);
				this.ch.eventLoop().execute(exchange::start);
			}
		}

	}

	static final class H2Exchange extends ChannelInboundHandlerAdapter {

		private final Conn conn;

		private long started;

		private boolean recordable;

		private long remaining;

		private final BodyCheck check = new BodyCheck();

		H2Exchange(Conn conn) {
			this.conn = conn;
		}

		@Override
		public void channelActive(ChannelHandlerContext ctx) {
			this.started = System.nanoTime();
			this.recordable = recording;
			boolean body = upload >= 0;
			ctx.write(new DefaultHttp2HeadersFrame(requestHeaders(), !body || upload == 0));
			if (body) {
				this.remaining = upload;
				pump(ctx);
			}
			else {
				ctx.flush();
			}
		}

		private void pump(ChannelHandlerContext ctx) {
			while (this.remaining > 0 && ctx.channel().isWritable()) {
				int n = (int) Math.min(CHUNK.capacity(), this.remaining);
				this.remaining -= n;
				if (recording) {
					bodyOut.addAndGet(n);
				}
				ctx.write(new DefaultHttp2DataFrame(CHUNK.retainedSlice(0, n), this.remaining == 0));
			}
			ctx.flush();
		}

		@Override
		public void channelWritabilityChanged(ChannelHandlerContext ctx) {
			if (this.remaining > 0) {
				ctx.executor().execute(() -> pump(ctx));
			}
		}

		@Override
		public void channelRead(ChannelHandlerContext ctx, Object msg) {
			try {
				if (msg instanceof Http2HeadersFrame frame) {
					CharSequence status = frame.headers().status();
					if (status != null && !"200".contentEquals(status) && frame.headers().status().charAt(0) != '1') {
						error("status:" + status);
					}
					if (status != null) {
						heads.incrementAndGet();
					}
					if (frame.isEndStream()) {
						finish(ctx);
					}
				}
				else if (msg instanceof Http2DataFrame frame) {
					if (recording) {
						bodyIn.addAndGet(frame.content().readableBytes());
					}
					this.check.add(frame.content());
					if (frame.isEndStream()) {
						finish(ctx);
					}
				}
				else if (msg instanceof Http2ResetFrame frame) {
					resets.incrementAndGet();
					error("rst:" + frame.errorCode());
					ctx.close();
					this.conn.next();
				}
			}
			finally {
				ReferenceCountUtil.release(msg);
			}
		}

		private void finish(ChannelHandlerContext ctx) {
			this.check.verify();
			long now = System.nanoTime();
			completed.incrementAndGet();
			this.conn.record(this.recordable ? this.started : -1, now - this.started);
			ctx.close();
			this.conn.next();
		}

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
			error("exc:" + cause.getClass().getSimpleName());
			ctx.close();
		}

	}

	static final class H1Exchange extends ChannelInboundHandlerAdapter {

		private final Conn conn;

		private ChannelHandlerContext ctx;

		private long started;

		private boolean recordable;

		private long remaining;

		private final BodyCheck check = new BodyCheck();

		H1Exchange(Conn conn) {
			this.conn = conn;
		}

		@Override
		public void handlerAdded(ChannelHandlerContext ctx) {
			this.ctx = ctx;
		}

		void start() {
			this.started = System.nanoTime();
			this.recordable = recording;
			HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
					upload >= 0 ? HttpMethod.POST : HttpMethod.GET, path);
			request.headers().set(HttpHeaderNames.HOST, authority);
			browserHeaders((name, value) -> request.headers().add(name, value));
			request.headers().set(HttpHeaderNames.CONTENT_LENGTH, Math.max(0, upload));
			this.ctx.write(request);
			this.remaining = Math.max(0, upload);
			pump();
		}

		private void pump() {
			while (this.remaining > 0 && this.ctx.channel().isWritable()) {
				int n = (int) Math.min(CHUNK.capacity(), this.remaining);
				this.remaining -= n;
				if (recording) {
					bodyOut.addAndGet(n);
				}
				ByteBuf slice = CHUNK.retainedSlice(0, n);
				this.ctx.write(this.remaining == 0 ? new DefaultLastHttpContent(slice) : new DefaultHttpContent(slice));
			}
			if (upload <= 0) {
				this.ctx.write(LastHttpContent.EMPTY_LAST_CONTENT);
			}
			this.ctx.flush();
		}

		@Override
		public void channelWritabilityChanged(ChannelHandlerContext ctx) {
			if (this.remaining > 0) {
				ctx.executor().execute(this::pump);
			}
			ctx.fireChannelWritabilityChanged();
		}

		@Override
		public void channelRead(ChannelHandlerContext ctx, Object msg) {
			try {
				if (msg instanceof HttpResponse response) {
					if (response.status().code() != 200) {
						error("status:" + response.status().code());
					}
					heads.incrementAndGet();
				}
				if (msg instanceof HttpContent content) {
					if (recording) {
						bodyIn.addAndGet(content.content().readableBytes());
					}
					this.check.add(content.content());
					if (content instanceof LastHttpContent) {
						this.check.verify();
						long now = System.nanoTime();
						completed.incrementAndGet();
						this.conn.record(this.recordable ? this.started : -1, now - this.started);
						this.conn.next();
					}
				}
			}
			finally {
				ReferenceCountUtil.release(msg);
			}
		}

		@Override
		public void channelInactive(ChannelHandlerContext ctx) {
			if (running) {
				error("h1:closed");
			}
		}

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
			error("exc:" + cause.getClass().getSimpleName());
			ctx.close();
		}

	}

}
