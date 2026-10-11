package am.ik.sluice.tunnel;

import org.jspecify.annotations.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * One multiplexed virtual TCP connection over the tunnel stream (the port of
 * remotedialer's connection.go).
 * <p>
 * The inbound direction buffers the remote's payload in segments for a single reader; the
 * end of the stream (the remote's CLOSE or ERROR) is never delayed by the DATA bound. The
 * outbound direction forwards to the session's {@link FrameWriter}. CLOSE is honored as a
 * half-close so the opposite direction may keep flowing (required for HTTP keep-alive and
 * half-duplex protocols).
 * <p>
 * A connection runs in one of two flow control modes, fixed at construction:
 * <ul>
 * <li><b>Windowed</b> ({@link #builder}): credit-based per-connection flow control. The
 * remote may have at most the receive window of DATA payload in flight beyond what this
 * side granted back; {@link #acceptData(byte[])} never blocks, and payload past the
 * window fails only this connection ({@code flow control window exceeded}). The payload
 * is coalesced into segments of at most {@link #SEGMENT_SIZE}, so the buffer stays within
 * {@link #MAX_RECEIVE_BUFFER} for the default window whatever the size of the frames. The
 * reader grants the bytes it consumed once they reach half the window. The writer sends
 * only against the credit the remote granted and waits for more on its own thread; the
 * credit lock is never held while the frame writer is called, so a grant on the receive
 * path never waits for a blocked writer. Closing while the remote may still send tells it
 * to stop with an ERROR, since a remote waiting for credit would otherwise wait
 * forever.</li>
 * <li><b>Legacy</b> ({@link #VirtualConnection(long, FrameWriter)}): for peers without
 * per-connection flow control. {@link #acceptData(byte[])} blocks while 128 frames are
 * buffered, one segment each, propagating backpressure to the whole gRPC stream.</li>
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

	/**
	 * Windowed mode: the most payload one receive segment holds. Small frames are
	 * coalesced into segments and large ones split, so neither the per-segment overhead
	 * nor the read part of the head segment grows with the way the remote frames its
	 * payload.
	 */
	static final int SEGMENT_SIZE = 16 * 1024;

	/**
	 * The heap one receive segment costs beyond its payload capacity, rounded up: the
	 * segment object, the array header and its deque slots (about 50 bytes measured with
	 * compressed oops).
	 */
	static final int SEGMENT_OVERHEAD = 64;

	/**
	 * The most heap the receive buffer of a windowed connection with the
	 * {@link #DEFAULT_WINDOW} holds: the window of unread payload, the read part of the
	 * head segment and the room of the tail segment (under one segment each), and the
	 * overhead of {@link #maxSegments} segments.
	 */
	static final long MAX_RECEIVE_BUFFER = DEFAULT_WINDOW + 2L * SEGMENT_SIZE
			+ (long) maxSegments(DEFAULT_WINDOW) * SEGMENT_OVERHEAD;

	private static final int QUEUE_CAPACITY = 128; // legacy: frames, x 64KiB

	/** A receive segment: the payload is {@code data[0, length)}. */
	private static final class Segment {

		final byte[] data;

		int length;

		Segment(int capacity) {
			this.data = new byte[capacity];
		}

	}

	/** The end of the inbound stream: EOF, or a failure the reader gets. */
	private record End(@Nullable String failure) {
	}

	private static final End EOF = new End(null);

	private final long connectionId;

	private final FrameWriter frameWriter;

	/** The receive window announced to the remote; 0 in legacy mode. */
	private final int receiveWindow;

	/**
	 * Guards the receive buffer and its end. Held for O(1) work and the copy of one frame
	 * or one read only, never while waiting, so the receive path never waits on the
	 * reader.
	 */
	private final ReentrantLock inboundLock = new ReentrantLock();

	/** Signals the reader: payload, the end. */
	private final Condition inboundChanged = this.inboundLock.newCondition();

	/**
	 * The payload received and not yet read, in order; guarded by {@link #inboundLock}.
	 */
	private final ArrayDeque<Segment> segments = new ArrayDeque<>();

	/** The read offset in the head segment; guarded by {@link #inboundLock}. */
	private int headPosition;

	/** Test hook: the times the reader waited; guarded by {@link #inboundLock}. */
	private long readerWaits;

	/**
	 * How the inbound stream ends once the segments are read; null while open. The first
	 * end the remote sends stays, and payload after it is dropped; {@link #close()} and a
	 * window violation replace it, dropping the segments. Guarded by
	 * {@link #inboundLock}.
	 */
	private @Nullable End end;

	/**
	 * Legacy mode: bounds the buffered DATA frames; released by {@link Source#read}. The
	 * end is not counted, so remote close/failure can never be delayed by the data bound.
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

	/** This side sent an ERROR for the connection; at most one is sent. */
	private final AtomicBoolean errorSent = new AtomicBoolean();

	private volatile boolean writeClosed;

	/**
	 * {@link #afterQueued(Runnable)} ended the sending side; guarded by
	 * {@link #writeLock}.
	 */
	private boolean sendingEnded;

	/** Sends in flight in the frame writer; guarded by {@link #writeLock}. */
	private int sending;

	/**
	 * Actions of {@link #afterQueued(Runnable)} waiting for the sends in flight; guarded
	 * by {@link #writeLock}.
	 */
	private final List<Runnable> afterSends = new ArrayList<>(1);

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
	 * Feeds a payload received from the remote side; it is copied. Windowed: never
	 * blocks; payload past the receive window fails the connection. Legacy: blocks while
	 * the DATA bound (128 frames) is reached, which applies backpressure to the gRPC
	 * stream. The end set by {@link #remoteClosed()} / {@link #remoteFailed(String)} is
	 * never delayed. An empty payload ends the stream like the remote's CLOSE, after the
	 * buffered payload.
	 */
	public void acceptData(byte[] data) {
		if (this.readClosed || this.closed) {
			return;
		}
		if (data.length == 0) {
			endWith(EOF);
			return;
		}
		if (windowed()) {
			acceptWindowed(data);
			return;
		}
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
			if (this.closed || this.readClosed || !append(data, false)) {
				this.inboundPermits.release(); // the frame was dropped: keep the permit
			}
			return;
		}
	}

	private void acceptWindowed(byte[] data) {
		if (this.outstanding.addAndGet(data.length) > this.receiveWindow) {
			failWindowExceeded();
			return;
		}
		append(data, true);
	}

	/**
	 * Appends a copy of the payload unless the stream already ended; false when dropped.
	 * Coalescing (windowed) fills the tail segment's room first; legacy appends one
	 * segment per frame, which its frame bound counts.
	 */
	private boolean append(byte[] data, boolean coalesce) {
		this.inboundLock.lock();
		try {
			if (this.end != null) {
				return false;
			}
			if (coalesce) {
				appendCoalesced(data);
			}
			else {
				Segment segment = new Segment(data.length);
				System.arraycopy(data, 0, segment.data, 0, data.length);
				segment.length = data.length;
				this.segments.addLast(segment);
			}
			this.inboundChanged.signalAll();
			return true;
		}
		finally {
			this.inboundLock.unlock();
		}
	}

	/**
	 * Fills the tail segment's room, then appends segments of at most
	 * {@link #SEGMENT_SIZE}, each at least twice the capacity of the one before it (up to
	 * the segment size) or the rest of the payload, so every segment but the tail is full
	 * and at most log2({@link #SEGMENT_SIZE}) are smaller than the segment size
	 * ({@link #maxSegments}). The first segment of an empty buffer fits the payload: a
	 * reader that keeps up costs one exact allocation per frame.
	 */
	private void appendCoalesced(byte[] data) {
		Segment tail = this.segments.peekLast();
		int offset = 0;
		while (offset < data.length) {
			int remaining = data.length - offset;
			if (tail == null || tail.length == tail.data.length) {
				int capacity = (tail == null) ? Math.min(remaining, SEGMENT_SIZE)
						: Math.min(SEGMENT_SIZE, Math.max(remaining, 2 * tail.data.length));
				tail = new Segment(capacity);
				this.segments.addLast(tail);
			}
			int n = Math.min(remaining, tail.data.length - tail.length);
			System.arraycopy(data, offset, tail.data, tail.length, n);
			tail.length += n;
			offset += n;
		}
	}

	/**
	 * The most segments the receive buffer of a windowed connection holds: segments
	 * smaller than {@link #SEGMENT_SIZE} at least double one after another, so at most
	 * log2({@link #SEGMENT_SIZE}) of them; every full-size one but the head and the tail
	 * holds only unread payload, which the window bounds.
	 */
	static int maxSegments(int receiveWindow) {
		return receiveWindow / SEGMENT_SIZE + Integer.numberOfTrailingZeros(SEGMENT_SIZE) + 2;
	}

	/** Ends the inbound stream after the buffered payload, unless it already ended. */
	private void endWith(End end) {
		this.inboundLock.lock();
		try {
			if (this.end == null) {
				this.end = end;
				this.inboundChanged.signalAll();
			}
		}
		finally {
			this.inboundLock.unlock();
		}
	}

	/**
	 * Drops the buffered payload and ends the inbound stream at once with {@code end}.
	 */
	private void dropInbound(End end) {
		this.inboundLock.lock();
		try {
			this.segments.clear(); // legacy: the dropped frames' permits die with the
									// connection
			this.headPosition = 0;
			this.end = end;
			this.inboundChanged.signalAll();
		}
		finally {
			this.inboundLock.unlock();
		}
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
		dropInbound(new End(WINDOW_EXCEEDED));
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
		endWith(EOF);
	}

	/** The remote terminated the connection abnormally. */
	public void remoteFailed(String message) {
		this.remoteDone = true;
		this.failure = message;
		endWith(new End(message == null ? "remote failure" : message));
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

	/** Test hook: the segments of the receive buffer. */
	int bufferedSegments() {
		this.inboundLock.lock();
		try {
			return this.segments.size();
		}
		finally {
			this.inboundLock.unlock();
		}
	}

	/** Test hook: the times the reader waited for payload or the end. */
	long readerWaits() {
		this.inboundLock.lock();
		try {
			return this.readerWaits;
		}
		finally {
			this.inboundLock.unlock();
		}
	}

	/** Test hook: the payload received and not yet read. */
	long bufferedBytes() {
		this.inboundLock.lock();
		try {
			return this.segments.stream().mapToLong(segment -> segment.length).sum() - this.headPosition;
		}
		finally {
			this.inboundLock.unlock();
		}
	}

	/** Test hook: the payload capacity the segments hold, read, unread or empty. */
	long bufferedCapacity() {
		this.inboundLock.lock();
		try {
			return this.segments.stream().mapToLong(segment -> segment.data.length).sum();
		}
		finally {
			this.inboundLock.unlock();
		}
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
		dropInbound(EOF); // wakes a blocked reader
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
				if (this.writeClosed || this.closed || this.sendingEnded) {
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

	/**
	 * Ends the sending side for good -- later DATA fails -- and runs {@code action} once
	 * none of the connection's frames waits in the frame writer any more: the sends in
	 * flight return first, then the writer runs it after their frames and the markers
	 * queued before left ({@link FrameWriter#afterQueued}). For the owner of a slot of
	 * the connection limit, which covers what the connection still queues; call it once
	 * the connection ended or was closed. Runs the action exactly once.
	 */
	public void afterQueued(Runnable action) {
		Objects.requireNonNull(action, "action is required");
		this.writeLock.lock();
		try {
			this.sendingEnded = true;
			this.creditChanged.signalAll();
			if (this.sending > 0) {
				this.afterSends.add(action);
				return;
			}
		}
		finally {
			this.writeLock.unlock();
		}
		this.frameWriter.afterQueued(this.connectionId, action);
	}

	/**
	 * Runs {@code action} once, after {@link #afterQueued(Runnable)} ran for every one of
	 * the connections.
	 */
	public static void afterQueued(Collection<VirtualConnection> connections, Runnable action) {
		Objects.requireNonNull(action, "action is required");
		AtomicInteger pending = new AtomicInteger(connections.size() + 1);
		Runnable countDown = () -> {
			if (pending.decrementAndGet() == 0) {
				action.run();
			}
		};
		for (VirtualConnection connection : connections) {
			connection.afterQueued(countDown);
		}
		countDown.run();
	}

	/**
	 * Enters a send: fails once the sending side ended, else counts the send in flight
	 * for {@link #afterQueued(Runnable)}.
	 */
	private void enterSend() throws IOException {
		this.writeLock.lock();
		try {
			if (this.sendingEnded || this.writeClosed || this.closed) {
				throw new IOException("connection %d is closed".formatted(this.connectionId));
			}
			this.sending++;
		}
		finally {
			this.writeLock.unlock();
		}
	}

	/** Leaves a send; the last one out hands the actions waiting for it over. */
	private void exitSend() {
		List<Runnable> ready = List.of();
		this.writeLock.lock();
		try {
			this.sending--;
			if (this.sending == 0 && !this.afterSends.isEmpty()) {
				ready = List.copyOf(this.afterSends);
				this.afterSends.clear();
			}
		}
		finally {
			this.writeLock.unlock();
		}
		for (Runnable action : ready) {
			this.frameWriter.afterQueued(this.connectionId, action);
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

		/** The end was returned (EOF or the failure); later reads return EOF. */
		private boolean endSeen;

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
			if (this.endSeen) {
				return -1;
			}
			int n = take(b, off, len);
			if (n < 0) {
				return -1;
			}
			consumed(n);
			return n;
		}

		/**
		 * Copies up to {@code len} buffered bytes, across segments, waiting while the
		 * buffer is empty and the stream has not ended; -1 at the end.
		 */
		private int take(byte[] b, int off, int len) throws IOException {
			VirtualConnection.this.inboundLock.lock();
			try {
				while (true) {
					int n = 0;
					Segment head;
					while (n < len && (head = VirtualConnection.this.segments.peekFirst()) != null) {
						int position = VirtualConnection.this.headPosition;
						int copied = Math.min(len - n, head.length - position);
						System.arraycopy(head.data, position, b, off + n, copied);
						n += copied;
						if (position + copied == head.length) {
							VirtualConnection.this.segments.pollFirst();
							VirtualConnection.this.headPosition = 0;
							if (!windowed()) {
								VirtualConnection.this.inboundPermits.release();
							}
						}
						else {
							VirtualConnection.this.headPosition = position + copied;
						}
					}
					if (n > 0) {
						return n;
					}
					End end = VirtualConnection.this.end;
					if (end != null) {
						this.endSeen = true;
						String failure = end.failure();
						if (failure != null) {
							throw new IOException(failure);
						}
						return -1;
					}
					try {
						VirtualConnection.this.readerWaits++;
						VirtualConnection.this.inboundChanged.await();
					}
					catch (InterruptedException e) {
						Thread.currentThread().interrupt();
						throw new IOException("interrupted while waiting for inbound data", e);
					}
				}
			}
			finally {
				VirtualConnection.this.inboundLock.unlock();
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
			enterSend();
			boolean sent;
			try {
				sent = VirtualConnection.this.frameWriter.sendData(VirtualConnection.this.connectionId, payload);
			}
			finally {
				exitSend();
			}
			if (!sent) {
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
