package am.ik.sluice.tunnel;

import org.jspecify.annotations.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * One multiplexed virtual TCP connection over the tunnel stream (the port of
 * remotedialer's connection.go).
 * <p>
 * The inbound direction is an unbounded-number-of-writers / single-reader queue; close
 * and failure markers are never delayed by the DATA bound. The outbound direction
 * forwards to the session's {@link FrameWriter}. CLOSE is honored as a half-close so the
 * opposite direction may keep flowing (required for HTTP keep-alive and half-duplex
 * protocols).
 * <p>
 * A connection runs in one of two flow control modes, fixed at construction:
 * <ul>
 * <li><b>Windowed</b> ({@link #builder}): credit-based per-connection flow control. The
 * remote may have at most the receive window of DATA payload in flight beyond what this
 * side granted back; {@link #acceptData(byte[])} never blocks, and payload past the
 * window fails only this connection ({@code flow control window exceeded}). The reader
 * grants the bytes it consumed once they reach half the window. The writer sends only
 * against the credit the remote granted and waits for more on its own thread; the credit
 * lock is never held while the frame writer is called, so a grant on the receive path
 * never waits for a blocked writer. Closing while the remote may still send tells it to
 * stop with an ERROR, since a remote waiting for credit would otherwise wait
 * forever.</li>
 * <li><b>Legacy</b> ({@link #VirtualConnection(long, FrameWriter)}): for peers without
 * per-connection flow control. {@link #acceptData(byte[])} blocks while 128 chunks are
 * buffered, propagating backpressure to the whole gRPC stream.</li>
 * </ul>
 */
public final class VirtualConnection implements AutoCloseable {

	/**
	 * The per-connection receive window both sides announce: 1MiB, grpc-java's default
	 * HTTP/2 stream window.
	 */
	public static final int DEFAULT_WINDOW = 1024 * 1024;

	/** The ERROR message sent for payload beyond the granted window. */
	public static final String WINDOW_EXCEEDED = "flow control window exceeded";

	private static final int QUEUE_CAPACITY = 128; // x 64KiB chunks

	private sealed interface Inbound permits Chunk, Eof, Failure {

	}

	private record Chunk(byte[] data) implements Inbound {
	}

	private record Eof() implements Inbound {
	}

	private record Failure(String message) implements Inbound {
	}

	private final long connectionId;

	private final FrameWriter frameWriter;

	/** The receive window announced to the remote; 0 in legacy mode. */
	private final int receiveWindow;

	private final LinkedBlockingDeque<Inbound> inbound = new LinkedBlockingDeque<>();

	/**
	 * Legacy mode: bounds the buffered DATA chunks; released by {@link Source#read}.
	 * Markers are not counted, so remote close/failure can never be delayed by the data
	 * bound.
	 */
	private final Semaphore inboundPermits = new Semaphore(QUEUE_CAPACITY);

	/** Windowed mode: payload received minus payload granted back. */
	private final AtomicLong outstanding = new AtomicLong();

	private final Source source = new Source();

	private final Sink sink = new Sink();

	private final ReentrantLock writeLock = new ReentrantLock();

	/** Signals the writer waiting for credit: grant, close, remote failure. */
	private final Condition creditChanged = this.writeLock.newCondition();

	/** Windowed mode: payload the remote still accepts; guarded by {@link #writeLock}. */
	private long sendCredit;

	private volatile boolean closed;

	private volatile boolean readClosed;

	/** A marker of each kind is queued at most once; reset when the queue is cleared. */
	private final AtomicBoolean eofPending = new AtomicBoolean();

	private final AtomicBoolean failurePending = new AtomicBoolean();

	/** This side sent an ERROR for the connection; at most one is sent. */
	private final AtomicBoolean errorSent = new AtomicBoolean();

	private volatile boolean writeClosed;

	/** The remote sent CLOSE or ERROR: no more DATA arrives from it. */
	private volatile boolean remoteDone;

	private volatile @Nullable String failure;

	/** A legacy-mode connection: no per-connection flow control. */
	public VirtualConnection(long connectionId, FrameWriter frameWriter) {
		this(connectionId, frameWriter, 0, 0);
	}

	private VirtualConnection(long connectionId, FrameWriter frameWriter, int receiveWindow, long sendCredit) {
		this.connectionId = connectionId;
		this.frameWriter = Objects.requireNonNull(frameWriter, "frameWriter is required");
		this.receiveWindow = receiveWindow;
		this.sendCredit = sendCredit;
	}

	/** Builds a windowed connection. */
	public static Builder builder(long connectionId, FrameWriter frameWriter) {
		return new Builder(connectionId, frameWriter);
	}

	public static final class Builder {

		private final long connectionId;

		private final FrameWriter frameWriter;

		private int receiveWindow = DEFAULT_WINDOW;

		private long sendCredit = DEFAULT_WINDOW;

		private Builder(long connectionId, FrameWriter frameWriter) {
			this.connectionId = connectionId;
			this.frameWriter = Objects.requireNonNull(frameWriter, "frameWriter is required");
		}

		/**
		 * The receive window this side announced and the initial send credit, the
		 * remote's announced window (both positive).
		 */
		public Builder window(int receiveWindow, long sendCredit) {
			if (receiveWindow <= 0 || sendCredit <= 0) {
				throw new IllegalArgumentException(
						"windows must be positive: receive=%d send=%d".formatted(receiveWindow, sendCredit));
			}
			this.receiveWindow = receiveWindow;
			this.sendCredit = sendCredit;
			return this;
		}

		public VirtualConnection build() {
			return new VirtualConnection(this.connectionId, this.frameWriter, this.receiveWindow, this.sendCredit);
		}

	}

	public long connectionId() {
		return this.connectionId;
	}

	/** Whether the connection runs per-connection credit flow control. */
	public boolean windowed() {
		return this.receiveWindow > 0;
	}

	/** Blocking end for the locally consumed direction. */
	public InputStream source() {
		return this.source;
	}

	/** Origin end for the locally produced direction. */
	public OutputStream sink() {
		return this.sink;
	}

	/**
	 * Feeds a chunk received from the remote side. Windowed: never blocks; payload past
	 * the receive window fails the connection. Legacy: blocks while the DATA bound (128
	 * chunks) is reached, which applies backpressure to the gRPC stream. Markers queued
	 * by {@link #remoteClosed()} / {@link #remoteFailed(String)} are never delayed.
	 */
	public void acceptData(byte[] data) {
		if (this.readClosed || this.closed) {
			return;
		}
		if (windowed()) {
			acceptWindowed(data);
			return;
		}
		Chunk chunk = new Chunk(Arrays.copyOf(data, data.length));
		while (!this.closed && !this.readClosed) {
			boolean acquired;
			try {
				// bounded: close() drops the queued chunks instead of releasing their
				// permits, so a producer parked here must recheck the flags on its own
				acquired = this.inboundPermits.tryAcquire(100, TimeUnit.MILLISECONDS);
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return;
			}
			if (!acquired) {
				continue;
			}
			if (this.closed || this.readClosed) {
				this.inboundPermits.release(); // the chunk would be dropped; keep the
												// permit
				return;
			}
			this.inbound.offerLast(chunk);
			return;
		}
	}

	private void acceptWindowed(byte[] data) {
		if (this.outstanding.addAndGet(data.length) > this.receiveWindow) {
			failWindowExceeded();
			return;
		}
		this.inbound.offerLast(new Chunk(Arrays.copyOf(data, data.length)));
	}

	/**
	 * The remote sent past its credit: fails this connection only. The remote gets an
	 * ERROR, the reader the failure, the writer an {@link IOException}; the buffered
	 * payload is dropped.
	 */
	private void failWindowExceeded() {
		this.writeLock.lock();
		try {
			this.writeClosed = true;
			this.closed = true;
			this.readClosed = true;
			this.creditChanged.signalAll();
		}
		finally {
			this.writeLock.unlock();
		}
		sendErrorOnce(WINDOW_EXCEEDED);
		this.failure = WINDOW_EXCEEDED;
		this.inbound.clear();
		this.eofPending.set(false);
		this.failurePending.set(false);
		this.offerMarker(this.failurePending, new Failure(WINDOW_EXCEEDED));
	}

	private void sendErrorOnce(String message) {
		if (this.errorSent.compareAndSet(false, true)) {
			this.frameWriter.sendError(this.connectionId, message);
		}
	}

	/**
	 * The remote granted {@code increment} (unsigned) more payload bytes; wakes the
	 * writer waiting for credit. Never blocks beyond the brief credit lock, which no
	 * writer holds across a send. Ignored in legacy mode.
	 */
	public void grant(int increment) {
		if (!windowed()) {
			return;
		}
		long added = Integer.toUnsignedLong(increment);
		this.writeLock.lock();
		try {
			// saturating: a remote granting absurd amounts only removes its own bound
			this.sendCredit = this.sendCredit > Long.MAX_VALUE - added ? Long.MAX_VALUE : this.sendCredit + added;
			this.creditChanged.signalAll();
		}
		finally {
			this.writeLock.unlock();
		}
	}

	/**
	 * The remote half-closed its -> local direction; the local side may continue writing.
	 */
	public void remoteClosed() {
		this.remoteDone = true;
		this.offerMarker(this.eofPending, new Eof());
	}

	/** The remote terminated the connection abnormally. */
	public void remoteFailed(String message) {
		this.remoteDone = true;
		this.failure = message;
		this.offerMarker(this.failurePending, new Failure(message == null ? "remote failure" : message));
		if (windowed()) {
			signalCredit(); // the remote grants nothing more
		}
	}

	private void signalCredit() {
		this.writeLock.lock();
		try {
			this.creditChanged.signalAll();
		}
		finally {
			this.writeLock.unlock();
		}
	}

	/** Queues the marker unless one of its kind still waits unread in the queue. */
	private void offerMarker(AtomicBoolean pending, Inbound marker) {
		if (!pending.compareAndSet(false, true)) {
			return;
		}
		this.inbound.offerLast(marker);
	}

	/**
	 * Number of entries waiting in the inbound queue; for tests.
	 */
	int pendingInbound() {
		return this.inbound.size();
	}

	/**
	 * Ends the connection locally. A windowed connection whose remote may still send
	 * (neither CLOSE nor ERROR arrived) sends ERROR instead of CLOSE: its payload would
	 * be dropped and never granted, so a remote waiting for credit would wait forever.
	 */
	@Override
	public void close() {
		if (windowed() && !this.remoteDone) {
			abortSink();
		}
		else {
			closeSink();
		}
		this.closed = true;
		this.readClosed = true;
		this.inbound.clear(); // the dropped chunks' permits die with the connection
		this.eofPending.set(false);
		this.failurePending.set(false);
		this.offerMarker(this.eofPending, new Eof()); // wake a blocked reader; queue was
														// cleared
	}

	/**
	 * Terminates the connection with an ERROR carrying the cause and closes it locally;
	 * for failures on the local side (dial failure, say).
	 */
	public void abort(String message) {
		this.writeLock.lock();
		try {
			this.writeClosed = true;
			this.creditChanged.signalAll();
		}
		finally {
			this.writeLock.unlock();
		}
		sendErrorOnce(message);
		close();
	}

	private void abortSink() {
		this.writeLock.lock();
		try {
			this.writeClosed = true;
			this.creditChanged.signalAll();
		}
		finally {
			this.writeLock.unlock();
		}
		sendErrorOnce("connection closed");
	}

	private void closeSink() {
		this.writeLock.lock();
		boolean notify;
		try {
			notify = !this.writeClosed;
			this.writeClosed = true;
			this.creditChanged.signalAll();
		}
		finally {
			this.writeLock.unlock();
		}
		if (notify) {
			this.frameWriter.sendClose(this.connectionId);
		}
	}

	/**
	 * Takes up to {@code wanted} bytes of send credit (at most one frame), waiting while
	 * there is none. The wait ends on a grant, on {@link #close()} or a half-close, on a
	 * remote failure, and once the stream ended; all but the grant throw. It has no
	 * timeout: it blocks only this connection's producer. The lock is released before the
	 * caller sends.
	 */
	private int awaitCredit(int wanted) throws IOException {
		this.writeLock.lock();
		try {
			while (true) {
				if (this.writeClosed || this.closed) {
					throw new IOException("connection %d is closed".formatted(this.connectionId));
				}
				String failure = this.failure;
				if (failure != null) {
					throw new IOException(failure);
				}
				if (!this.frameWriter.isOpen()) {
					throw new IOException("tunnel stream closed");
				}
				if (this.sendCredit > 0) {
					int granted = (int) Math.min(Math.min(this.sendCredit, wanted), SessionSender.MAX_CHUNK);
					this.sendCredit -= granted;
					return granted;
				}
				try {
					// timed: the stream ending has no signal of its own here
					this.creditChanged.awaitNanos(TimeUnit.MILLISECONDS.toNanos(100));
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					throw new InterruptedIOException("interrupted while waiting for send credit");
				}
			}
		}
		finally {
			this.writeLock.unlock();
		}
	}

	/** Test hook: the send credit left. */
	long sendCredit() {
		this.writeLock.lock();
		try {
			return this.sendCredit;
		}
		finally {
			this.writeLock.unlock();
		}
	}

	private final class Source extends InputStream {

		private byte @Nullable [] pending;

		private int pendingPos;

		private int pendingLength;

		private boolean eofSeen;

		/** Windowed mode: bytes handed to the reader and not yet granted back. */
		private long consumed;

		@Override
		public int read() throws IOException {
			byte[] one = new byte[1];
			int n = read(one, 0, 1);
			return n == 1 ? one[0] & 0xff : -1;
		}

		@Override
		public int read(byte[] b, int off, int len) throws IOException {
			Objects.requireNonNull(b, "b is required");
			if (off < 0 || len < 0 || off + len > b.length) {
				throw new IndexOutOfBoundsException("off=%d len=%d b.length=%d".formatted(off, len, b.length));
			}
			if (len == 0) {
				return 0;
			}
			while (true) {
				if (this.pending != null) {
					int n = Math.min(len, this.pendingLength - this.pendingPos);
					System.arraycopy(this.pending, this.pendingPos, b, off, n);
					this.pendingPos += n;
					if (this.pendingPos == this.pendingLength) {
						this.pending = null;
					}
					consumed(n);
					return n;
				}
				if (this.eofSeen) {
					return -1;
				}
				Inbound inbound;
				try {
					inbound = VirtualConnection.this.inbound.take();
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					throw new IOException("interrupted while waiting for inbound data", e);
				}
				switch (inbound) {
					case Chunk chunk -> {
						if (!windowed()) {
							VirtualConnection.this.inboundPermits.release();
						}
						if (chunk.data().length == 0) {
							this.eofSeen = true;
						}
						else {
							this.pending = chunk.data();
							this.pendingPos = 0;
							this.pendingLength = chunk.data().length;
						}
					}
					case Eof eof -> this.eofSeen = true;
					case Failure failure -> {
						this.eofSeen = true;
						throw new IOException(failure.message());
					}
				}
			}
		}

		/**
		 * Grants the consumed bytes back once they reach half the window; runs on the
		 * reader's thread, never on the receive path.
		 */
		private void consumed(int n) {
			if (!windowed()) {
				return;
			}
			this.consumed += n;
			if (this.consumed < VirtualConnection.this.receiveWindow / 2 || VirtualConnection.this.closed) {
				return;
			}
			int increment = (int) this.consumed;
			this.consumed = 0;
			// lower the count before the grant leaves: the remote's next payload must
			// never be measured against a stale count
			VirtualConnection.this.outstanding.addAndGet(-increment);
			VirtualConnection.this.frameWriter.sendWindowUpdate(VirtualConnection.this.connectionId, increment);
		}

	}

	private final class Sink extends OutputStream {

		@Override
		public void write(int b) throws IOException {
			write(new byte[] { (byte) b }, 0, 1);
		}

		@Override
		public void write(byte[] b, int off, int len) throws IOException {
			Objects.requireNonNull(b, "b is required");
			if (off < 0 || len < 0 || off + len > b.length) {
				throw new IndexOutOfBoundsException("off=%d len=%d b.length=%d".formatted(off, len, b.length));
			}
			if (windowed()) {
				writeWindowed(b, off, len);
				return;
			}
			VirtualConnection.this.writeLock.lock();
			try {
				if (VirtualConnection.this.writeClosed || VirtualConnection.this.closed) {
					throw new IOException("connection %d is closed".formatted(VirtualConnection.this.connectionId));
				}
			}
			finally {
				VirtualConnection.this.writeLock.unlock();
			}
			send(Arrays.copyOfRange(b, off, off + len));
		}

		/**
		 * Sends frame by frame, each against the credit taken for it. As in legacy mode,
		 * the closed check and the send are not atomic: a {@link #close()} whose CLOSE is
		 * queued in between lets this frame follow the CLOSE, and the remote reader,
		 * stopped at the EOF, never reads it.
		 */
		private void writeWindowed(byte[] b, int off, int len) throws IOException {
			int offset = off;
			int remaining = len;
			while (remaining > 0) {
				int granted = awaitCredit(remaining);
				send(Arrays.copyOfRange(b, offset, offset + granted));
				offset += granted;
				remaining -= granted;
			}
		}

		private void send(byte[] payload) throws IOException {
			if (!VirtualConnection.this.frameWriter.sendData(VirtualConnection.this.connectionId, payload)) {
				// the sender refuses DATA once this connection's CLOSE / ERROR is queued
				if (VirtualConnection.this.writeClosed || VirtualConnection.this.closed) {
					throw new IOException("connection %d is closed".formatted(VirtualConnection.this.connectionId));
				}
				throw new IOException("tunnel stream closed");
			}
		}

		/**
		 * Half-closes the local -> remote direction; the remote may keep sending.
		 */
		@Override
		public void close() {
			closeSink();
		}

	}

}
