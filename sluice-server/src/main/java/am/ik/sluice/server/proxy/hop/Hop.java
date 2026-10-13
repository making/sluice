package am.ik.sluice.server.proxy.hop;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;

import am.ik.sluice.tunnel.DuplexPipe;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http2.DefaultHttp2Connection;
import io.netty.handler.codec.http2.DefaultHttp2LocalFlowController;
import io.netty.handler.codec.http2.Http2Connection;
import io.netty.handler.codec.http2.Http2ConnectionEncoder;
import io.netty.handler.codec.http2.Http2ConnectionHandler;
import io.netty.handler.codec.http2.Http2ConnectionHandlerBuilder;
import io.netty.handler.codec.http2.Http2FrameListener;
import io.netty.handler.codec.http2.Http2LocalFlowController;
import io.netty.util.ReferenceCountUtil;
import org.jspecify.annotations.Nullable;

/**
 * One HTTP/2 endpoint: netty's {@link Http2ConnectionHandler} in an
 * {@link EmbeddedChannel}, driven over a blocking {@link DuplexPipe} (the client's
 * socket, or a leg's tunnel connection) by two virtual threads of its own. The reader
 * feeds what it reads into the channel; the writer writes what the channel wrote. Both
 * enter the channel only through the hop's {@link Loop}, and the writer blocks on the
 * pipe outside it. Building a hop starts both threads.
 * <p>
 * <b>No ByteBuf crosses the lock.</b> The reader hands the channel a copy made under the
 * lock, and what the channel writes is copied to the heap under the lock (in
 * {@code doWrite}, see {@link HopChannel}) before the writer sees it. netty's buffers
 * assume one thread at a time: a writer reading the channel's ByteBufs outside the lock
 * (frame headers, forwarded DATA that is a slice of another hop's inbound buffer) while
 * the loop's threads allocate and append garbles the bytes on the wire with netty 4.2's
 * default adaptive allocator ({@code HopStressTest}).
 * <p>
 * <b>Writability.</b> The writer queue feeds back into netty as channel writability: past
 * the high water mark of queued bytes a user-defined writability bit of the channel turns
 * unwritable, so the remote flow controller stops writing DATA; at the low water mark it
 * turns writable again. Without it the channel drains to the queue at every flush and
 * stays writable, so everything the peer's windows allow piles up in the queue.
 * <p>
 * <b>Reading pauses</b> while the queue holds more than the read pause bytes. What the
 * flow controller holds back is DATA only; the frames netty answers on its own (PING and
 * SETTINGS acks, resets) would otherwise grow the queue without bound towards a peer that
 * sends without reading. Netty's own limit on queued control frames counts unfinished
 * writes, and here every write finishes once copied to the queue.
 * <p>
 * <b>Failures.</b> Exceptions reaching the end of the pipeline, exceptions of the frame
 * listener, of connection listeners added through {@link #addConnectionListener} and of
 * promise listeners given to {@link #newPromise(ChannelFutureListener)}, tasks failing in
 * the channel, the pipe failing in the reader or the writer, and pipeline operations
 * without the loop's lock all go to the loop's exception sink, and the hop closes. A
 * write without the lock is dropped: released, its promise failed.
 * <p>
 * <b>Closing.</b> The peer's EOF, a broken pipe and {@link #abortLocked()} close the
 * channel at once; {@link #closeLocked()} is netty's graceful close (GOAWAY, then close
 * once the streams ended or the handler's graceful shutdown timeout ran out). Once the
 * channel closed, the writer writes what the channel wrote, half-closes the pipe and
 * waits for the peer's EOF; at the close grace after the channel closed the pipe is
 * closed whatever the writer does (a peer not reading).
 * <p>
 * <b>Native image.</b> netty reflects on the public methods of every channel handler
 * ({@code ChannelHandlerMask}): each handler class here is registered with
 * {@code queryAllPublicMethods} in {@code META-INF/native-image/.../reflect-config.json}.
 */
public final class Hop {

	/** The default high water mark of the writer queue. */
	public static final int DEFAULT_HIGH_WATER = 64 * 1024;

	/** The default low water mark of the writer queue. */
	public static final int DEFAULT_LOW_WATER = 32 * 1024;

	/** The default queued bytes past which the reader pauses. */
	public static final int DEFAULT_READ_PAUSE_BYTES = 1024 * 1024;

	/** The default time the pipe stays open after the channel closed. */
	public static final Duration DEFAULT_CLOSE_GRACE = Duration.ofSeconds(1);

	private static final int READ_BUFFER = 16 * 1024;

	private static final int WRITE_BUFFER = 64 * 1024;

	/** The user-defined writability bit the writer queue drives. */
	private static final int QUEUE_WRITABILITY = 1;

	private final String name;

	private final Loop loop;

	private final DuplexPipe pipe;

	private final int highWater;

	private final int lowWater;

	private final int readPauseBytes;

	private final Duration closeGrace;

	private final @Nullable Runnable onInactive;

	private final ReentrantLock queueLock = new ReentrantLock();

	private final Condition queueChanged = this.queueLock.newCondition();

	/** What the channel wrote and the writer has not written yet; with queueLock. */
	private final ArrayDeque<byte[]> queue = new ArrayDeque<>();

	/** The bytes of {@link #queue}; with queueLock. */
	private long queuedBytes;

	/** The writability bit is cleared; written with queueLock (and the loop's lock). */
	private volatile boolean unwritable;

	/** The channel closed: the writer ends once the queue is written; with queueLock. */
	private boolean finishing;

	/** The writer ended: what the channel writes now is dropped; with queueLock. */
	private boolean writerGone;

	/** Messages the channel wrote; with the loop's lock. */
	private long writes;

	/** The channel closed and the close grace started; with the loop's lock. */
	private boolean finished;

	/** When the pipe is closed at the latest, in {@link System#nanoTime()}. */
	private volatile long closeDeadline;

	/** The earliest scheduled task of the channel, or {@code Long.MAX_VALUE}. */
	private long nextScheduled = Long.MAX_VALUE;

	/** A failure was reported: the channel closes at the next settle. */
	private volatile boolean poisoned;

	private final AtomicLong bytesRead = new AtomicLong();

	private final AtomicLong bytesWritten = new AtomicLong();

	private final CountDownLatch readerDone = new CountDownLatch(1);

	private final CountDownLatch writerDone = new CountDownLatch(1);

	private final Http2ConnectionHandler handler;

	private final HopChannel channel;

	private final ChannelHandlerContext ctx;

	private final ChannelHandlerContext guardCtx;

	private Hop(Builder builder) {
		this.name = Objects.requireNonNull(builder.name, "name is required");
		this.loop = Objects.requireNonNull(builder.loop, "loop is required");
		this.pipe = Objects.requireNonNull(builder.pipe, "pipe is required");
		Function<Hop, Http2FrameListener> listener = Objects.requireNonNull(builder.listener, "listener is required");
		this.highWater = builder.highWater;
		this.lowWater = builder.lowWater;
		this.readPauseBytes = builder.readPauseBytes;
		this.closeGrace = builder.closeGrace;
		this.onInactive = builder.onInactive;
		if (this.lowWater < 0 || this.lowWater > this.highWater) {
			throw new IllegalArgumentException("0 <= lowWater <= highWater is required");
		}
		if (!this.loop.heldByCurrentThread()) {
			throw new IllegalStateException("a hop is built inside Loop#run");
		}
		DefaultHttp2Connection connection = new DefaultHttp2Connection(builder.server);
		connection.local()
			.flowController(new DefaultHttp2LocalFlowController(connection,
					DefaultHttp2LocalFlowController.DEFAULT_WINDOW_UPDATE_RATIO, builder.autoRefillConnectionWindow));
		Http2ConnectionHandlerBuilder handlerBuilder = new Http2ConnectionHandlerBuilder();
		builder.codec.accept(handlerBuilder);
		this.handler = handlerBuilder.connection(connection)
			.frameListener(new ReportingFrameListener(listener.apply(this), this::fail))
			.build();
		LockGuard guard = new LockGuard();
		this.channel = new HopChannel(guard, this.handler, new Tail());
		try {
			// registering activates the channel: the handler writes its preface
			this.channel.register();
		}
		catch (Exception e) {
			throw new IllegalStateException(this.name + ": the channel did not register", e);
		}
		this.ctx = Objects.requireNonNull(this.channel.pipeline().context(this.handler));
		this.guardCtx = Objects.requireNonNull(this.channel.pipeline().context(guard));
		this.loop.registerLocked(this);
		Thread.ofVirtual().name(this.name + "-read").start(this::read);
		Thread.ofVirtual().name(this.name + "-write").start(this::write);
	}

	public static Builder builder() {
		return new Builder();
	}

	public String name() {
		return this.name;
	}

	public Loop loop() {
		return this.loop;
	}

	/** The handler's context: netty's encoder and flow controllers take it. */
	public ChannelHandlerContext ctx() {
		return this.ctx;
	}

	public Http2ConnectionHandler handler() {
		return this.handler;
	}

	public Http2Connection connection() {
		return this.handler.connection();
	}

	public Http2ConnectionEncoder encoder() {
		return this.handler.encoder();
	}

	public Http2LocalFlowController localFlow() {
		return this.handler.connection().local().flowController();
	}

	/**
	 * A new promise of the channel whose listener's exceptions go to the hop's failure
	 * handling (netty's promises only log them).
	 */
	public ChannelPromise newPromise(ChannelFutureListener listener) {
		ChannelPromise promise = this.ctx.newPromise();
		promise.addListener((ChannelFutureListener) future -> {
			try {
				listener.operationComplete(future);
			}
			catch (Exception | Error e) {
				fail(e);
			}
		});
		return promise;
	}

	/**
	 * Adds a listener to the connection whose exceptions go to the hop's failure handling
	 * (netty's connection only logs them); with the loop's lock.
	 */
	public void addConnectionListener(Http2Connection.Listener listener) {
		requireLock();
		connection().addListener(new ReportingConnectionListener(listener, this::fail));
	}

	/** Whether the channel is open. */
	public boolean open() {
		return this.channel.isOpen();
	}

	/**
	 * Whether the writer queue lets the channel write DATA: {@code false} from the high
	 * water mark until the queue drained to the low water mark.
	 */
	public boolean writable() {
		return !this.unwritable;
	}

	/** The bytes the channel wrote that the writer has not written yet. */
	public long queuedBytes() {
		this.queueLock.lock();
		try {
			return this.queuedBytes;
		}
		finally {
			this.queueLock.unlock();
		}
	}

	/** The bytes read from the pipe. */
	public long bytesRead() {
		return this.bytesRead.get();
	}

	/** The bytes written to the pipe. */
	public long bytesWritten() {
		return this.bytesWritten.get();
	}

	/** netty's graceful close (see the class comment); with the loop's lock. */
	public void closeLocked() {
		requireLock();
		if (this.channel.isOpen()) {
			// through the pipeline, not EmbeddedChannel#close: that one cancels every
			// scheduled task of the channel, the handler's graceful close timer too, and
			// keeps cancelling what is scheduled later (netty 4.2.19)
			this.channel.pipeline().close();
		}
	}

	/** Closes the channel now, sending nothing more; with the loop's lock. */
	public void abortLocked() {
		requireLock();
		if (this.channel.isOpen()) {
			// from the guard's context: past the handler, whose close is graceful
			this.guardCtx.close();
		}
	}

	/** Waits until the reader and the writer ended and the pipe is closed. */
	public boolean awaitDone(Duration timeout) throws InterruptedException {
		long deadline = System.nanoTime() + timeout.toNanos();
		return this.readerDone.await(timeout.toNanos(), TimeUnit.NANOSECONDS)
				&& this.writerDone.await(deadline - System.nanoTime(), TimeUnit.NANOSECONDS);
	}

	@Override
	public String toString() {
		return this.name;
	}

	/**
	 * Runs pending tasks, flushes (what the channel writes moves to the queue), closes a
	 * poisoned channel and starts the close grace of a closed one; whether anything
	 * moved. With the loop's lock.
	 */
	boolean settleLocked() {
		if (this.finished) {
			return false;
		}
		long writesBefore = this.writes;
		if (this.poisoned) {
			abortLocked();
		}
		try {
			this.channel.runPendingTasks();
			this.nextScheduled = scheduledAt(this.channel.runScheduledPendingTasks());
			// what the tasks threw, recorded by the channel
			this.channel.checkException();
			if (this.channel.isOpen()) {
				this.channel.flush();
			}
		}
		catch (Throwable e) {
			fail(e);
			abortLocked();
		}
		if (!this.channel.isOpen()) {
			finishLocked();
			return true;
		}
		return this.writes != writesBefore || this.channel.hasPendingTasks();
	}

	/** Whether the channel closed and the hop left the loop's settling; with the lock. */
	boolean finishedLocked() {
		return this.finished;
	}

	/** The earliest scheduled task of the channel; with the loop's lock. */
	long nextScheduledLocked() {
		return this.nextScheduled;
	}

	private static long scheduledAt(long delayNanos) {
		return delayNanos < 0 ? Long.MAX_VALUE : System.nanoTime() + delayNanos;
	}

	private void requireLock() {
		if (!this.loop.heldByCurrentThread()) {
			throw new IllegalStateException(this.name + ": the loop's lock is required");
		}
	}

	/**
	 * Reports a failure and closes the hop at the next settle: the current one with the
	 * loop's lock, else one the ticker runs now.
	 */
	private void fail(Throwable cause) {
		this.poisoned = true;
		if (this.loop.heldByCurrentThread()) {
			this.loop.report(this.name, cause);
		}
		else {
			this.loop.reportLater(this.name, cause);
		}
	}

	/** Copies what the channel wrote to the queue; in {@code doWrite}, with the lock. */
	private void enqueueLocked(Object message) {
		if (!(message instanceof ByteBuf buffer)) {
			ReferenceCountUtil.release(message);
			fail(new IllegalStateException(this.name + ": unexpected outbound message " + message.getClass()));
			return;
		}
		byte[] bytes;
		try {
			bytes = ByteBufUtil.getBytes(buffer);
		}
		finally {
			// doWrite retained it for us
			buffer.release();
		}
		this.writes++;
		if (bytes.length == 0) {
			return;
		}
		boolean turnUnwritable = false;
		this.queueLock.lock();
		try {
			if (this.writerGone) {
				return;
			}
			this.queue.addLast(bytes);
			this.queuedBytes += bytes.length;
			if (!this.unwritable && this.queuedBytes > this.highWater) {
				this.unwritable = true;
				turnUnwritable = true;
			}
			this.queueChanged.signalAll();
		}
		finally {
			this.queueLock.unlock();
		}
		if (turnUnwritable) {
			// fires channelWritabilityChanged later, as a task of the channel
			setQueueWritability(false);
		}
	}

	/** Turns the channel writable once the queue drained to the low water mark. */
	private void updateWritabilityLocked() {
		boolean turnWritable = false;
		this.queueLock.lock();
		try {
			if (this.unwritable && this.queuedBytes <= this.lowWater) {
				this.unwritable = false;
				turnWritable = true;
			}
		}
		finally {
			this.queueLock.unlock();
		}
		if (turnWritable) {
			setQueueWritability(true);
		}
	}

	private void setQueueWritability(boolean writable) {
		ChannelOutboundBuffer outbound = this.channel.unsafe().outboundBuffer();
		if (outbound != null) {
			outbound.setUserDefinedWritability(QUEUE_WRITABILITY, writable);
		}
	}

	private void finishLocked() {
		if (this.finished) {
			return;
		}
		this.finished = true;
		this.closeDeadline = System.nanoTime() + this.closeGrace.toNanos();
		this.queueLock.lock();
		try {
			this.finishing = true;
			this.queueChanged.signalAll();
		}
		finally {
			this.queueLock.unlock();
		}
		// a writer blocked on a peer that does not read ends when the pipe closes
		Thread.ofVirtual().name(this.name + "-close").start(() -> {
			try {
				if (!this.writerDone.await(this.closeGrace.toNanos(), TimeUnit.NANOSECONDS)) {
					closePipe();
				}
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				closePipe();
			}
		});
	}

	/** Feeds bytes read into the channel; with the loop's lock. */
	private void feedLocked(byte[] bytes, int length) {
		if (!this.channel.isOpen()) {
			return;
		}
		// copied under the lock: the reader's array is reused after this call
		this.channel.writeInbound(Unpooled.copiedBuffer(bytes, 0, length));
	}

	private void read() {
		try {
			InputStream in = this.pipe.source();
			byte[] buffer = new byte[READ_BUFFER];
			while (awaitReadable()) {
				int n = in.read(buffer);
				if (n < 0) {
					break;
				}
				this.bytesRead.addAndGet(n);
				// processed before the next read: the pipe's own flow control (TCP, the
				// tunnel's credit) backs the peer up meanwhile
				this.loop.run(() -> feedLocked(buffer, n));
			}
		}
		catch (IOException e) {
			// the pipe failed or was closed: ends as on EOF
		}
		catch (RuntimeException | Error e) {
			this.loop.run(() -> fail(e));
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		finally {
			try {
				// HTTP/2 has no half-closed connection: the peer's EOF ends it
				this.loop.run(this::abortLocked);
			}
			finally {
				this.readerDone.countDown();
			}
		}
	}

	/** Waits while the queue holds more than the read pause bytes. */
	private boolean awaitReadable() throws InterruptedException {
		this.queueLock.lock();
		try {
			while (this.queuedBytes > this.readPauseBytes && !this.finishing && !this.writerGone) {
				this.queueChanged.await();
			}
			return true;
		}
		finally {
			this.queueLock.unlock();
		}
	}

	private void write() {
		try {
			OutputStream out = new BufferedOutputStream(this.pipe.sink(), WRITE_BUFFER);
			List<byte[]> batch = new ArrayList<>();
			while (takeBatch(batch)) {
				long written = 0;
				for (byte[] bytes : batch) {
					out.write(bytes);
					written += bytes.length;
				}
				out.flush();
				batch.clear();
				this.bytesWritten.addAndGet(written);
				if (written(written)) {
					// the controller resumes on the writability change
					this.loop.run(this::updateWritabilityLocked);
				}
			}
			// the channel closed and everything it wrote is out: half-close, then give
			// the peer until the close grace to read it and close its side
			try {
				this.pipe.shutdownOutput();
			}
			catch (Exception e) {
				// closed already, or no half-close (TLS): the close below ends it
			}
			this.readerDone.await(this.closeDeadline - System.nanoTime(), TimeUnit.NANOSECONDS);
		}
		catch (IOException e) {
			this.loop.run(this::abortLocked);
		}
		catch (RuntimeException | Error e) {
			this.loop.run(() -> {
				fail(e);
				abortLocked();
			});
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		finally {
			this.queueLock.lock();
			try {
				this.writerGone = true;
				this.queue.clear();
				this.queuedBytes = 0;
				this.queueChanged.signalAll();
			}
			finally {
				this.queueLock.unlock();
			}
			closePipe();
			this.writerDone.countDown();
		}
	}

	/** Moves the queue to the batch; {@code false} once the queue ended. */
	private boolean takeBatch(List<byte[]> batch) throws InterruptedException {
		this.queueLock.lock();
		try {
			while (this.queue.isEmpty() && !this.finishing) {
				this.queueChanged.await();
			}
			batch.addAll(this.queue);
			this.queue.clear();
			return !batch.isEmpty();
		}
		finally {
			this.queueLock.unlock();
		}
	}

	/** Accounts written bytes; whether the channel should turn writable. */
	private boolean written(long bytes) {
		this.queueLock.lock();
		try {
			this.queuedBytes -= bytes;
			this.queueChanged.signalAll();
			return this.unwritable && this.queuedBytes <= this.lowWater;
		}
		finally {
			this.queueLock.unlock();
		}
	}

	private void closePipe() {
		try {
			this.pipe.close();
		}
		catch (Exception e) {
			// closed already
		}
	}

	/**
	 * The channel: what it writes is copied to the writer queue in {@code doWrite}, which
	 * runs only on a flush with the loop's lock. Relies on {@code EmbeddedChannel} as of
	 * netty 4.2.19: {@code doWrite} retains each message for
	 * {@code handleOutboundMessage} and completes its promise right after; a task's
	 * exception is recorded for {@code checkException}, an error is not caught;
	 * {@code close()} cancels the scheduled tasks.
	 */
	private final class HopChannel extends EmbeddedChannel {

		HopChannel(io.netty.channel.ChannelHandler... handlers) {
			super(false, false, handlers);
		}

		@Override
		protected void handleOutboundMessage(Object msg) {
			enqueueLocked(msg);
		}

		@Override
		protected void handleInboundMessage(Object msg) {
			ReferenceCountUtil.release(msg);
		}

	}

	/**
	 * First in the pipeline: every inbound event passes it first and every outbound
	 * operation last. One without the loop's lock is dropped and reported.
	 */
	final class LockGuard extends ChannelDuplexHandler {

		@Override
		public void channelRead(ChannelHandlerContext ctx, Object msg) {
			if (held("channelRead")) {
				ctx.fireChannelRead(msg);
			}
			else {
				ReferenceCountUtil.release(msg);
			}
		}

		@Override
		public void channelReadComplete(ChannelHandlerContext ctx) {
			if (held("channelReadComplete")) {
				ctx.fireChannelReadComplete();
			}
		}

		@Override
		public void channelWritabilityChanged(ChannelHandlerContext ctx) {
			if (held("channelWritabilityChanged")) {
				ctx.fireChannelWritabilityChanged();
			}
		}

		@Override
		public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
			if (held("userEventTriggered")) {
				ctx.fireUserEventTriggered(evt);
			}
			else {
				ReferenceCountUtil.release(evt);
			}
		}

		@Override
		public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
			if (held("write")) {
				ctx.write(msg, promise);
			}
			else {
				ReferenceCountUtil.release(msg);
				promise.tryFailure(new IllegalStateException(Hop.this.name + ": write without the loop's lock"));
			}
		}

		@Override
		public void flush(ChannelHandlerContext ctx) {
			if (held("flush")) {
				ctx.flush();
			}
		}

		@Override
		public void read(ChannelHandlerContext ctx) {
			if (held("read")) {
				ctx.read();
			}
		}

		@Override
		public void close(ChannelHandlerContext ctx, ChannelPromise promise) {
			if (held("close")) {
				ctx.close(promise);
			}
			else {
				promise.tryFailure(new IllegalStateException(Hop.this.name + ": close without the loop's lock"));
			}
		}

		private boolean held(String operation) {
			if (Hop.this.loop.heldByCurrentThread()) {
				return true;
			}
			fail(new IllegalStateException(Hop.this.name + ": " + operation + " without the loop's lock"));
			return false;
		}

	}

	/**
	 * Last in the pipeline: the exception sink, so the channel never records an exception
	 * to rethrow into a driving thread.
	 */
	final class Tail extends ChannelInboundHandlerAdapter {

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
			fail(cause);
		}

		@Override
		public void channelRead(ChannelHandlerContext ctx, Object msg) {
			// the handler consumes every byte; nothing should arrive here
			ReferenceCountUtil.release(msg);
		}

		@Override
		public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
			ReferenceCountUtil.release(evt);
		}

		@Override
		public void channelInactive(ChannelHandlerContext ctx) {
			Runnable onInactive = Hop.this.onInactive;
			if (onInactive == null) {
				return;
			}
			try {
				onInactive.run();
			}
			catch (RuntimeException | Error e) {
				fail(e);
			}
		}

	}

	public static final class Builder {

		private @Nullable String name;

		private @Nullable Loop loop;

		private @Nullable DuplexPipe pipe;

		private boolean server;

		private @Nullable Function<Hop, Http2FrameListener> listener;

		private boolean autoRefillConnectionWindow = true;

		private Consumer<Http2ConnectionHandlerBuilder> codec = handlerBuilder -> {
		};

		private @Nullable Runnable onInactive;

		private int highWater = DEFAULT_HIGH_WATER;

		private int lowWater = DEFAULT_LOW_WATER;

		private int readPauseBytes = DEFAULT_READ_PAUSE_BYTES;

		private Duration closeGrace = DEFAULT_CLOSE_GRACE;

		private Builder() {
		}

		/** Names the hop and its threads. */
		public Builder name(String name) {
			this.name = name;
			return this;
		}

		public Builder loop(Loop loop) {
			this.loop = loop;
			return this;
		}

		/** The pipe to the peer; the hop closes it once done. */
		public Builder pipe(DuplexPipe pipe) {
			this.pipe = pipe;
			return this;
		}

		/** The server role (the client connection) or the client role (a leg). */
		public Builder server(boolean server) {
			this.server = server;
			return this;
		}

		/**
		 * The frame listener, made from the hop being built: the hop's handler does not
		 * exist yet when the function runs, so it keeps the hop for later.
		 */
		public Builder listener(Function<Hop, Http2FrameListener> listener) {
			this.listener = listener;
			return this;
		}

		/**
		 * Grants the connection window back as DATA arrives (netty's auto refill), so
		 * only the stream windows bound what the hop holds and one held stream never
		 * starves the others; {@code true} by default.
		 */
		public Builder autoRefillConnectionWindow(boolean autoRefillConnectionWindow) {
			this.autoRefillConnectionWindow = autoRefillConnectionWindow;
			return this;
		}

		/**
		 * Configures netty's handler builder: initial settings, frame logger, header
		 * validation, stream buffering, the graceful shutdown timeout. The hop sets the
		 * connection and the frame listener after it, so it must not set them, nor the
		 * role or {@code codec(...)}.
		 */
		public Builder codec(Consumer<Http2ConnectionHandlerBuilder> codec) {
			this.codec = codec;
			return this;
		}

		/** Runs with the loop's lock once the channel closed. */
		public Builder onInactive(Runnable onInactive) {
			this.onInactive = onInactive;
			return this;
		}

		/** The writer queue's water marks for the channel's writability. */
		public Builder waterMarks(int lowWater, int highWater) {
			this.lowWater = lowWater;
			this.highWater = highWater;
			return this;
		}

		/** Queued bytes past which the reader pauses. */
		public Builder readPauseBytes(int readPauseBytes) {
			this.readPauseBytes = readPauseBytes;
			return this;
		}

		/** How long the pipe stays open after the channel closed, at most. */
		public Builder closeGrace(Duration closeGrace) {
			this.closeGrace = closeGrace;
			return this;
		}

		/** Builds the hop and starts its reader and writer; inside {@link Loop#run}. */
		public Hop build() {
			return new Hop(this);
		}

	}

}
