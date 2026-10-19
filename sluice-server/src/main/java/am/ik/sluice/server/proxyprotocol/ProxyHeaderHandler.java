package am.ik.sluice.server.proxyprotocol;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.ScheduledFuture;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import am.ik.sluice.server.tunnel.LogThrottle;

/**
 * Consumes the PROXY protocol header a front end may prepend to a connection, before any
 * handler behind it sees a byte. The header is parsed by {@link ProxyProtocolParser}, so
 * v1 / v2, headerless connections and malformed headers are treated as on the data plane;
 * the header's source lands in the channel attribute {@link #SOURCE}. Until the head is
 * decided, the inbound bytes and the user events are held back: gRPC starts its protocol
 * negotiation on a user event and reads the channel's remote address then, so it sees the
 * header's source. A malformed header, or a head undecided by the deadline, closes the
 * connection. Once decided, the handler releases what it held and leaves the pipeline.
 * <p>
 * <b>Native image.</b> netty reflects on the public methods of every channel handler;
 * this one is registered in {@code reflect-config.json}.
 */
final class ProxyHeaderHandler extends ChannelInboundHandlerAdapter {

	/** The source of the connection's PROXY header; unset without one. */
	static final AttributeKey<InetSocketAddress> SOURCE = AttributeKey.valueOf(ProxyHeaderHandler.class, "source");

	private static final Logger log = LoggerFactory.getLogger(ProxyHeaderHandler.class);

	private static final LogThrottle failureLog = new LogThrottle(Duration.ofMinutes(1), System::nanoTime);

	private final Duration deadline;

	private final List<Object> heldEvents = new ArrayList<>();

	private @Nullable ByteBuf head;

	private @Nullable ScheduledFuture<?> timeout;

	ProxyHeaderHandler(Duration deadline) {
		this.deadline = deadline;
	}

	@Override
	public void handlerAdded(ChannelHandlerContext ctx) {
		this.timeout = ctx.executor()
			.schedule(() -> fail(ctx, "no PROXY protocol decision within " + this.deadline), this.deadline.toNanos(),
					TimeUnit.NANOSECONDS);
	}

	@Override
	public void handlerRemoved(ChannelHandlerContext ctx) {
		this.release();
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) {
		if (!(msg instanceof ByteBuf bytes)) {
			ctx.fireChannelRead(msg);
			return;
		}
		ByteBuf held = this.head;
		if (held == null) {
			held = ctx.alloc().buffer(bytes.readableBytes());
			this.head = held;
		}
		held.writeBytes(bytes);
		bytes.release();
		this.decide(ctx, held);
	}

	@Override
	public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
		this.heldEvents.add(evt);
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) {
		this.release();
		ctx.fireChannelInactive();
	}

	private void decide(ChannelHandlerContext ctx, ByteBuf held) {
		ExhaustibleInput input = new ExhaustibleInput(ByteBufUtil.getBytes(held));
		ProxyProtocolParser.Header header;
		try {
			header = new ProxyProtocolParser().parse(input).header();
		}
		catch (ExhaustibleInput.Exhausted e) {
			// the bytes so far are a prefix of a header: wait for more
			return;
		}
		catch (IOException e) {
			this.fail(ctx, e.getMessage());
			return;
		}
		if (header != null && header.source() != null) {
			ctx.channel().attr(SOURCE).set(new InetSocketAddress(header.source(), header.sourcePort()));
		}
		// a headerless stream is pushed back whole; a header is read exactly to its end
		held.skipBytes(header == null ? 0 : input.position());
		this.head = null;
		List<Object> events = List.copyOf(this.heldEvents);
		this.heldEvents.clear();
		// the events first: they set up the handlers the bytes are meant for
		events.forEach(ctx::fireUserEventTriggered);
		if (held.isReadable()) {
			ctx.fireChannelRead(held);
		}
		else {
			held.release();
		}
		ctx.pipeline().remove(this);
	}

	private void fail(ChannelHandlerContext ctx, @Nullable String reason) {
		long suppressed = failureLog.tryAcquire();
		if (suppressed >= 0) {
			log.warn("closing control plane connection from {}: {}{}", ctx.channel().remoteAddress(), reason,
					suppressed == 0 ? "" : " (%d more closed since the last report)".formatted(suppressed));
		}
		ctx.close();
	}

	private void release() {
		if (this.timeout != null) {
			this.timeout.cancel(false);
			this.timeout = null;
		}
		if (this.head != null) {
			this.head.release();
			this.head = null;
		}
		this.heldEvents.forEach(ReferenceCountUtil::release);
		this.heldEvents.clear();
	}

	/**
	 * The bytes received so far, as a stream that signals its end with {@link Exhausted}
	 * rather than an EOF: the parser then stops where it would block on a socket.
	 */
	private static final class ExhaustibleInput extends InputStream {

		private final byte[] bytes;

		private int position;

		ExhaustibleInput(byte[] bytes) {
			this.bytes = bytes;
		}

		int position() {
			return this.position;
		}

		@Override
		public int read() throws IOException {
			if (this.position == this.bytes.length) {
				throw Exhausted.INSTANCE;
			}
			return this.bytes[this.position++] & 0xff;
		}

		@Override
		public int read(byte[] buffer, int offset, int length) throws IOException {
			if (length == 0) {
				return 0;
			}
			if (this.position == this.bytes.length) {
				throw Exhausted.INSTANCE;
			}
			int count = Math.min(length, this.bytes.length - this.position);
			System.arraycopy(this.bytes, this.position, buffer, offset, count);
			this.position += count;
			return count;
		}

		private static final class Exhausted extends IOException {

			static final Exhausted INSTANCE = new Exhausted();

			private Exhausted() {
				super("more input needed");
			}

			@Override
			public synchronized Throwable fillInStackTrace() {
				// a control flow signal, thrown once per partial head
				return this;
			}

		}

	}

}
