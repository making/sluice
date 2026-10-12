package am.ik.sluice.spike.h2demux;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http2.Http2Connection;
import io.netty.handler.codec.http2.Http2ConnectionEncoder;
import io.netty.handler.codec.http2.Http2ConnectionHandler;
import io.netty.handler.codec.http2.Http2LocalFlowController;
import org.jspecify.annotations.Nullable;

/**
 * One HTTP/2 endpoint -- an {@link Http2ConnectionHandler} in an {@link EmbeddedChannel}
 * -- driven over a blocking {@link Transport} by threads of our own, the way the relay
 * runs today: a reader thread feeds what it reads into the channel, a writer thread
 * writes what the channel wrote to the transport. Both enter the channel only through the
 * {@link Loop}; the writer blocks on the transport outside it.
 * <p>
 * The writer queue feeds back into netty as channel writability: past {@link #HIGH_WATER}
 * queued bytes the channel turns unwritable (a user-defined writability bit of its
 * outbound buffer), so the remote flow controller stops pulling DATA from the streams;
 * below {@link #LOW_WATER} it turns writable again and the controller resumes. Without
 * that bit the EmbeddedChannel's outbound buffer drains at every flush, so the channel is
 * always writable and everything the peer's windows allow piles up in the queue.
 * <p>
 * No ByteBuf crosses the lock: what the channel wrote is copied to the heap under the
 * lock, then written by the writer thread. A ByteBuf the channel wrote can be a slice of
 * another hop's cumulation buffer (forwarded DATA), and reading it on another thread
 * while the owning hop appends to that buffer misplaces the append (netty buffers share a
 * cached internal NIO buffer; {@code ByteBufCrossThreadProbeTest}).
 */
public final class Hop {

	/**
	 * {@code -Dspike.writerReadsByteBufs=true}: hands the channel's ByteBufs to the
	 * writer thread as they are -- the zero-copy variant that corrupts payloads (see
	 * {@code ByteBufCrossThreadProbeTest}); for reproducing only.
	 */
	static final boolean WRITER_READS_BYTEBUFS = Boolean.getBoolean("spike.writerReadsByteBufs");

	static final int HIGH_WATER = 64 * 1024;

	static final int LOW_WATER = 32 * 1024;

	private static final int READ_BUFFER = 16 * 1024;

	/** The user-defined writability bit the writer queue drives. */
	private static final int QUEUE_WRITABILITY = 1;

	private static final long CLOSE_GRACE_MILLIS = 1000;

	private final String name;

	private final Loop loop;

	private final Transport transport;

	private final Http2ConnectionHandler handler;

	private final boolean writabilityFeedback;

	private final EmbeddedChannel channel;

	private final ChannelHandlerContext ctx;

	private final List<Throwable> failures = new ArrayList<>();

	private final ReentrantLock queueLock = new ReentrantLock();

	private final Condition queueChanged = this.queueLock.newCondition();

	/** What the channel wrote and the writer has not written yet; with queueLock. */
	private final ArrayDeque<ByteBuf> queue = new ArrayDeque<>();

	/** The bytes of {@link #queue}; with queueLock. */
	private long queuedBytes;

	/** The channel closed: the writer ends once the queue is written; with queueLock. */
	private boolean finishing;

	/** The reader holds off reading; with queueLock. */
	private boolean paused;

	/** The queue bit is cleared; with the loop's lock. */
	private boolean unwritable;

	/** The bit was cleared at least once; for tests. */
	private volatile boolean everUnwritable;

	private final AtomicLong bytesRead = new AtomicLong();

	private final AtomicLong bytesWritten = new AtomicLong();

	private final CountDownLatch readerDone = new CountDownLatch(1);

	private final CountDownLatch writerDone = new CountDownLatch(1);

	private Hop(Builder builder) {
		this.name = Objects.requireNonNull(builder.name, "name is required");
		this.loop = Objects.requireNonNull(builder.loop, "loop is required");
		this.transport = Objects.requireNonNull(builder.transport, "transport is required");
		this.handler = Objects.requireNonNull(builder.handler, "handler is required");
		this.writabilityFeedback = builder.writabilityFeedback;
		if (!this.loop.heldByCurrentThread()) {
			throw new IllegalStateException("a hop is built inside Loop#run");
		}
		Runnable onInactive = builder.onInactive;
		this.channel = new EmbeddedChannel(this.handler, new ChannelInboundHandlerAdapter() {

			@Override
			public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
				// recorded here so the EmbeddedChannel never rethrows it into a driver
				// thread
				Hop.this.failures.add(cause);
			}

			@Override
			public void channelInactive(ChannelHandlerContext ctx) {
				if (onInactive != null) {
					onInactive.run();
				}
			}

		});
		this.ctx = Objects.requireNonNull(this.channel.pipeline().context(this.handler));
		this.loop.register(this);
	}

	public static Builder builder() {
		return new Builder();
	}

	/** Starts the reader and the writer. */
	public void start() {
		Thread.ofVirtual().name(this.name + "-read").start(this::pumpReader);
		Thread.ofVirtual().name(this.name + "-write").start(this::pumpWriter);
	}

	public String name() {
		return this.name;
	}

	public ChannelHandlerContext ctx() {
		return this.ctx;
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

	public boolean open() {
		return this.channel.isOpen();
	}

	/** Closes the channel, as a peer's EOF does; with the loop's lock. */
	public void closeLocked() {
		this.channel.close();
	}

	public long bytesRead() {
		return this.bytesRead.get();
	}

	public long bytesWritten() {
		return this.bytesWritten.get();
	}

	public boolean everUnwritable() {
		return this.everUnwritable;
	}

	/** Exceptions that reached the end of the pipeline; with the loop's lock. */
	public List<Throwable> failuresLocked() {
		return List.copyOf(this.failures);
	}

	/** Holds the reader off the transport: its peer's writes back up. */
	public void pauseReading() {
		this.queueLock.lock();
		try {
			this.paused = true;
		}
		finally {
			this.queueLock.unlock();
		}
	}

	public void resumeReading() {
		this.queueLock.lock();
		try {
			this.paused = false;
			this.queueChanged.signalAll();
		}
		finally {
			this.queueLock.unlock();
		}
	}

	/**
	 * Queues raw bytes behind what the channel wrote, bypassing netty: a test peer
	 * writing a frame netty would never write. With the loop's lock.
	 */
	public void injectRawLocked(byte[] bytes) {
		settleLocked();
		enqueue(Unpooled.wrappedBuffer(bytes));
	}

	/** Waits until both threads ended and the transport is closed. */
	public boolean awaitDone(long timeout, TimeUnit unit) throws InterruptedException {
		return this.readerDone.await(timeout, unit) && this.writerDone.await(timeout, unit);
	}

	/**
	 * Runs pending tasks, flushes, and moves what the channel wrote to the writer queue;
	 * whether anything moved. With the loop's lock.
	 */
	boolean settleLocked() {
		this.channel.runPendingTasks();
		if (this.channel.isOpen()) {
			this.channel.flush();
		}
		boolean moved = false;
		for (Object message = this.channel.readOutbound(); message != null; message = this.channel.readOutbound()) {
			ByteBuf written = (ByteBuf) message;
			if (!WRITER_READS_BYTEBUFS) {
				// no ByteBuf leaves the lock: the writer gets a heap copy of the bytes
				ByteBuf copy = Unpooled.wrappedBuffer(ByteBufUtil.getBytes(written));
				written.release();
				written = copy;
			}
			enqueue(written);
			moved = true;
		}
		updateWritabilityLocked();
		if (!this.channel.isOpen()) {
			finish();
		}
		return moved;
	}

	void runScheduledLocked() {
		if (this.channel.isOpen()) {
			this.channel.runScheduledPendingTasks();
		}
	}

	private void enqueue(ByteBuf buffer) {
		this.queueLock.lock();
		try {
			if (this.finishing && this.queue.isEmpty() && this.writerDone.getCount() == 0) {
				buffer.release();
				return;
			}
			this.queue.addLast(buffer);
			this.queuedBytes += buffer.readableBytes();
			this.queueChanged.signalAll();
		}
		finally {
			this.queueLock.unlock();
		}
	}

	private void finish() {
		this.queueLock.lock();
		try {
			this.finishing = true;
			this.queueChanged.signalAll();
		}
		finally {
			this.queueLock.unlock();
		}
	}

	private long queuedBytes() {
		this.queueLock.lock();
		try {
			return this.queuedBytes;
		}
		finally {
			this.queueLock.unlock();
		}
	}

	private void updateWritabilityLocked() {
		if (!this.writabilityFeedback || !this.channel.isOpen()) {
			return;
		}
		long queued = queuedBytes();
		if (!this.unwritable && queued > HIGH_WATER) {
			this.unwritable = true;
			this.everUnwritable = true;
			this.channel.unsafe().outboundBuffer().setUserDefinedWritability(QUEUE_WRITABILITY, false);
		}
		else if (this.unwritable && queued <= LOW_WATER) {
			this.unwritable = false;
			this.channel.unsafe().outboundBuffer().setUserDefinedWritability(QUEUE_WRITABILITY, true);
		}
	}

	private void pumpReader() {
		try {
			InputStream in = this.transport.in();
			byte[] buffer = new byte[READ_BUFFER];
			while (true) {
				awaitUnpaused();
				int n = in.read(buffer);
				if (n < 0) {
					break;
				}
				this.bytesRead.addAndGet(n);
				ByteBuf bytes = Unpooled.copiedBuffer(buffer, 0, n);
				// processed before the next read: the transport's own flow control backs
				// up the peer while the channel works
				this.loop.run(() -> {
					if (this.channel.isOpen()) {
						this.channel.writeInbound(bytes);
					}
					else {
						bytes.release();
					}
				});
			}
		}
		catch (IOException | RuntimeException e) {
			// the transport failed or closed: the channel ends as on EOF
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		finally {
			this.loop.run(() -> {
				if (this.channel.isOpen()) {
					this.channel.close();
				}
			});
			this.readerDone.countDown();
		}
	}

	private void awaitUnpaused() throws InterruptedException {
		this.queueLock.lock();
		try {
			while (this.paused) {
				this.queueChanged.await();
			}
		}
		finally {
			this.queueLock.unlock();
		}
	}

	private void pumpWriter() {
		OutputStream out = this.transport.out();
		try {
			while (true) {
				List<ByteBuf> batch = new ArrayList<>();
				boolean wasAboveLow;
				this.queueLock.lock();
				try {
					while (this.queue.isEmpty() && !this.finishing) {
						this.queueChanged.await();
					}
					if (this.queue.isEmpty()) {
						break;
					}
					wasAboveLow = this.queuedBytes > LOW_WATER;
					while (!this.queue.isEmpty()) {
						batch.add(this.queue.pollFirst());
					}
				}
				finally {
					this.queueLock.unlock();
				}
				long written = 0;
				try {
					for (ByteBuf buffer : batch) {
						int length = buffer.readableBytes();
						buffer.getBytes(buffer.readerIndex(), out, length);
						written += length;
					}
					out.flush();
				}
				finally {
					batch.forEach(ByteBuf::release);
				}
				this.bytesWritten.addAndGet(written);
				boolean wake;
				this.queueLock.lock();
				try {
					this.queuedBytes -= written;
					wake = wasAboveLow && this.queuedBytes <= LOW_WATER;
				}
				finally {
					this.queueLock.unlock();
				}
				if (wake) {
					// the controller resumes on the writability change
					this.loop.run(this::updateWritabilityLocked);
				}
			}
			// the channel closed and everything it wrote is out: half-close, give the
			// peer a moment to read it and close its side, then close
			this.transport.shutdownOutput();
			this.readerDone.await(CLOSE_GRACE_MILLIS, TimeUnit.MILLISECONDS);
		}
		catch (IOException | RuntimeException e) {
			this.loop.run(() -> {
				if (this.channel.isOpen()) {
					this.channel.close();
				}
			});
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		finally {
			this.transport.close();
			releaseQueued();
			this.writerDone.countDown();
		}
	}

	private void releaseQueued() {
		this.queueLock.lock();
		try {
			for (ByteBuf buffer = this.queue.pollFirst(); buffer != null; buffer = this.queue.pollFirst()) {
				buffer.release();
			}
			this.queuedBytes = 0;
			this.finishing = true;
		}
		finally {
			this.queueLock.unlock();
		}
	}

	public static final class Builder {

		private @Nullable String name;

		private @Nullable Loop loop;

		private @Nullable Transport transport;

		private @Nullable Http2ConnectionHandler handler;

		private boolean writabilityFeedback = true;

		private @Nullable Runnable onInactive;

		private Builder() {
		}

		public Builder name(String name) {
			this.name = name;
			return this;
		}

		public Builder loop(Loop loop) {
			this.loop = loop;
			return this;
		}

		public Builder transport(Transport transport) {
			this.transport = transport;
			return this;
		}

		public Builder handler(Http2ConnectionHandler handler) {
			this.handler = handler;
			return this;
		}

		public Builder writabilityFeedback(boolean writabilityFeedback) {
			this.writabilityFeedback = writabilityFeedback;
			return this;
		}

		/** Runs, with the loop's lock, once the channel went inactive. */
		public Builder onInactive(Runnable onInactive) {
			this.onInactive = onInactive;
			return this;
		}

		/** Builds the hop; inside {@link Loop#run}. */
		public Hop build() {
			return new Hop(this);
		}

	}

}
