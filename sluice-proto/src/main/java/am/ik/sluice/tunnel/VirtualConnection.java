package am.ik.sluice.tunnel;

import org.jspecify.annotations.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import am.ik.sluice.v1.proto.WindowUpdate;

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
 * Credit-based per-connection flow control: the remote may send at most the credit this
 * side granted it, the initial window it announced and the increments since;
 * {@link #acceptData(byte[])} never blocks, and payload past it fails only this
 * connection ({@code flow control window exceeded}). The payload is coalesced into
 * segments of at most {@link #SEGMENT_SIZE}, so the buffer stays within the window plus
 * {@link #receiveBufferSlack} whatever the size of the frames. The writer sends only
 * against the credit the remote granted and waits for more on its own thread; the credit
 * lock is never held while the frame writer is called, so a grant on the receive path
 * never waits for a blocked writer. Closing while the remote may still send tells it to
 * stop with an ERROR, since a remote waiting for credit would otherwise wait forever.
 * <p>
 * The window -- what the remote may still send plus what it sent and the reader did not
 * take, or took and was not granted back yet -- starts at the announced window and moves
 * between it and the maximum window ({@link Builder#growWindow}). The reader grants what
 * it took back at every half window it took, and once the remote said it was blocked and
 * the buffer is empty, if it took a quarter of the window since its latest grant: a
 * remote spending its credit in bursts, one per round trip, would otherwise find up to
 * half the window held back for a round trip, a burst ending short of the half window;
 * the quarter keeps a remote that sends as fast as the reader takes from getting its
 * credit back in many small grants. The reader decides at every half window it took, by
 * what happened since the previous decision: when the remote said it was blocked -- it
 * had DATA and no credit ({@code blocked}) -- it halves the window, granting back less
 * than it took, if the reader never found its buffer empty meanwhile and is a quarter of
 * the window behind (the reader was the bound), and doubles it if not (the window was);
 * it halves it when the remote was not blocked for two decisions in a row (it left the
 * window unused), and keeps it otherwise. A writer of this class tells the remote each
 * time it starts waiting for credit. What the window holds past the announced one is
 * charged to the {@link TunnelBudget} before the grant raising it leaves, within the
 * account's share, and released as the window shrinks; a window the budget has no room
 * for grows by what fits, or not at all, and the connection goes on within what it holds.
 * A reader that waited {@code reclaimAfter} for payload with the window past the
 * announced one asks the remote to give back the credit it holds past it
 * ({@code reclaim_above}); a remote answering ({@code released}) lowers the window by
 * what it gave up. Once the remote ended its direction, what the window holds past what
 * is buffered is released, and on close everything.
 */
public final class VirtualConnection implements AutoCloseable {

	/**
	 * The most a connection's receive window grows to: 1MiB, grpc-java's default HTTP/2
	 * stream window; a connection built without a window announces it.
	 */
	public static final int DEFAULT_WINDOW = 1024 * 1024;

	/**
	 * The receive window a connection announces and holds outside the
	 * {@link TunnelBudget}: 64KiB, the initial window of RFC 9113; what a connection
	 * moves per round trip of the tunnel while the budget has no room.
	 */
	public static final int INITIAL_WINDOW = 64 * 1024;

	/**
	 * How long a reader waits for payload before it reclaims the credit the remote holds
	 * past the announced window.
	 */
	public static final Duration RECLAIM_AFTER = Duration.ofSeconds(1);

	/** The ERROR message sent for payload beyond the granted window. */
	public static final String WINDOW_EXCEEDED = "flow control window exceeded";

	/**
	 * The most payload one receive segment holds. Small frames are coalesced into
	 * segments and large ones split, so neither the per-segment overhead nor the read
	 * part of the head segment grows with the way the remote frames its payload.
	 */
	static final int SEGMENT_SIZE = 16 * 1024;

	/**
	 * The heap one receive segment costs beyond its payload capacity, rounded up: the
	 * segment object, the array header and its deque slots (about 50 bytes measured with
	 * compressed oops).
	 */
	static final int SEGMENT_OVERHEAD = 64;

	/**
	 * The most heap the receive buffer of a connection with the {@link #DEFAULT_WINDOW}
	 * holds: the window of unread payload, the read part of the head segment and the room
	 * of the tail segment (under one segment each), and the overhead of
	 * {@link #maxSegments} segments.
	 */
	static final long MAX_RECEIVE_BUFFER = DEFAULT_WINDOW + receiveBufferSlack(DEFAULT_WINDOW);

	/**
	 * The most heap the receive buffer of a connection holds outside the
	 * {@link TunnelBudget}: the {@link #INITIAL_WINDOW} it announces and the slack of a
	 * window grown to the {@link #DEFAULT_WINDOW}; the budget covers the window past the
	 * announced one.
	 */
	public static final long RECEIVE_ALLOWANCE = INITIAL_WINDOW + receiveBufferSlack(DEFAULT_WINDOW);

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

	/** The receive window announced to the remote; the window never shrinks below it. */
	private final int initialWindow;

	/** The most the receive window grows to. */
	private final int maxWindow;

	/** Charges the window past {@link #initialWindow}. */
	private final TunnelBudget budget;

	private final TunnelBudget.Account account;

	/** How long the reader waits for payload before it reclaims the remote's credit. */
	private final long reclaimAfterNanos;

	/** Runs whenever the writer starts waiting for credit; a metrics hook. */
	private final Runnable creditWaited;

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
	 * The receive window: {@link #credit} + {@link #buffered} + {@link #pending}. Guarded
	 * by {@link #inboundLock}, as are the fields below.
	 */
	private long window;

	/** What the remote may still send: granted minus received minus released. */
	private long credit;

	/** The payload received and not yet read. */
	private long buffered;

	/** The payload read and not granted back yet. */
	private long pending;

	/** The remote said it was blocked since the latest decision. */
	private boolean remoteBlocked;

	/**
	 * The remote said it was blocked and has not been granted at an empty buffer since:
	 * it waits for what the reader took, whatever the decisions granted meanwhile.
	 */
	private boolean remoteWaiting;

	/** The decisions in a row the remote was not blocked. */
	private int unblocked;

	/** The reader found the buffer empty since the latest decision. */
	private boolean waited;

	/** Test hook: the largest window granted. */
	private long peakWindow;

	/** A reclaim was asked since payload last arrived; not asked again until then. */
	private boolean reclaimAsked;

	/** Test hook: the reclaims asked. */
	private long reclaims;

	/**
	 * How the inbound stream ends once the segments are read; null while open. The first
	 * end the remote sends stays, and payload after it is dropped; {@link #close()} and a
	 * window violation replace it, dropping the segments. Guarded by
	 * {@link #inboundLock}.
	 */
	private @Nullable End end;

	private final Source source = new Source();

	private final Sink sink = new Sink();

	private final ReentrantLock writeLock = new ReentrantLock();

	/** Signals the writer waiting for credit: grant, close, remote failure. */
	private final Condition creditChanged = this.writeLock.newCondition();

	/** Payload the remote still accepts; guarded by {@link #writeLock}. */
	private long sendCredit;

	/**
	 * Test hook: the times the writer waited for credit; guarded by {@link #writeLock}.
	 */
	private long creditWaits;

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

	/** Guards {@link #remoteFailed} and {@link #onRemoteFailure}. */
	private final Object remoteFailureLock = new Object();

	/** The remote sent ERROR; guarded by {@link #remoteFailureLock}. */
	private boolean remoteFailed;

	/**
	 * Runs once when the remote sent ERROR; guarded by {@link #remoteFailureLock}.
	 */
	private @Nullable Runnable onRemoteFailure;

	private VirtualConnection(Builder builder) {
		this.connectionId = builder.connectionId;
		this.frameWriter = builder.frameWriter;
		this.initialWindow = builder.receiveWindow;
		this.maxWindow = Math.max(builder.receiveWindow, builder.maxWindow);
		this.budget = builder.budget == null ? TunnelBudget.unlimited() : builder.budget;
		this.account = this.budget.account();
		this.reclaimAfterNanos = builder.reclaimAfter.toNanos();
		this.creditWaited = builder.creditWaited;
		this.sendCredit = builder.sendCredit;
		this.window = this.initialWindow;
		this.credit = this.initialWindow;
	}

	/**
	 * Builds a connection; both windows default to {@link #DEFAULT_WINDOW}, and the
	 * receive window grows no further.
	 */
	public static Builder builder(long connectionId, FrameWriter frameWriter) {
		return new Builder(connectionId, frameWriter);
	}

	public static final class Builder {

		private final long connectionId;

		private final FrameWriter frameWriter;

		private int receiveWindow = DEFAULT_WINDOW;

		private long sendCredit = DEFAULT_WINDOW;

		/** 0 = the announced window. */
		private int maxWindow;

		private @Nullable TunnelBudget budget;

		private Duration reclaimAfter = RECLAIM_AFTER;

		private Runnable creditWaited = () -> {
		};

		private Builder(long connectionId, FrameWriter frameWriter) {
			this.connectionId = connectionId;
			this.frameWriter = Objects.requireNonNull(frameWriter, "frameWriter is required");
		}

		/**
		 * Lets the receive window grow up to {@code maxWindow}, past the announced window
		 * against {@code budget}; without it the window stays at the announced one.
		 */
		public Builder growWindow(int maxWindow, TunnelBudget budget) {
			if (maxWindow <= 0) {
				throw new IllegalArgumentException("maxWindow must be positive: " + maxWindow);
			}
			this.maxWindow = maxWindow;
			this.budget = Objects.requireNonNull(budget, "budget is required");
			return this;
		}

		/**
		 * How long the reader waits for payload before it reclaims the credit the remote
		 * holds past the announced window; {@link #RECLAIM_AFTER} by default.
		 */
		public Builder reclaimAfter(Duration reclaimAfter) {
			if (reclaimAfter.isNegative() || reclaimAfter.isZero()) {
				throw new IllegalArgumentException("reclaimAfter must be positive: " + reclaimAfter);
			}
			this.reclaimAfter = reclaimAfter;
			return this;
		}

		/** Runs whenever the writer starts waiting for the remote's credit. */
		public Builder creditWaited(Runnable creditWaited) {
			this.creditWaited = Objects.requireNonNull(creditWaited, "creditWaited is required");
			return this;
		}

		/**
		 * The receive window this side announced (positive) and the initial send credit,
		 * the remote's announced window; 0 while the remote's window is not known yet,
		 * until a {@link VirtualConnection#grant} supplies it.
		 */
		public Builder window(int receiveWindow, long sendCredit) {
			if (receiveWindow <= 0 || sendCredit < 0) {
				throw new IllegalArgumentException(
						"windows out of range: receive=%d send=%d".formatted(receiveWindow, sendCredit));
			}
			this.receiveWindow = receiveWindow;
			this.sendCredit = sendCredit;
			return this;
		}

		public VirtualConnection build() {
			return new VirtualConnection(this);
		}

	}

	public long connectionId() {
		return this.connectionId;
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
	 * Feeds a payload received from the remote side; it is copied. Never blocks; payload
	 * past the receive window fails the connection. The end set by
	 * {@link #remoteClosed()} / {@link #remoteFailed(String)} is never delayed. An empty
	 * payload ends the stream like the remote's CLOSE, after the buffered payload.
	 */
	public void acceptData(byte[] data) {
		if (this.readClosed || this.closed) {
			return;
		}
		if (data.length == 0) {
			endWith(EOF);
			return;
		}
		if (!append(data)) {
			failWindowExceeded();
		}
	}

	/**
	 * Takes the payload from the remote's credit and appends a copy, coalesced into the
	 * tail segment's room first, unless the stream already ended; false when it passes
	 * the credit.
	 */
	private boolean append(byte[] data) {
		this.inboundLock.lock();
		try {
			if (data.length > this.credit) {
				return false;
			}
			this.credit -= data.length;
			this.reclaimAsked = false;
			if (this.end != null) {
				// dropped: it no longer counts against the window
				this.window -= data.length;
				settleLocked();
				return true;
			}
			appendCoalesced(data);
			this.buffered += data.length;
			this.budget.addBuffered(data.length);
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
	 * The most segments the receive buffer of a connection holds: segments smaller than
	 * {@link #SEGMENT_SIZE} at least double one after another, so at most
	 * log2({@link #SEGMENT_SIZE}) of them; every full-size one but the head and the tail
	 * holds only unread payload, which the window bounds.
	 */
	static int maxSegments(int receiveWindow) {
		return receiveWindow / SEGMENT_SIZE + Integer.numberOfTrailingZeros(SEGMENT_SIZE) + 2;
	}

	/**
	 * Ends the inbound stream after the buffered payload, unless it already ended; the
	 * window's charge past what is buffered is released.
	 */
	private void endWith(End end) {
		this.inboundLock.lock();
		try {
			if (this.end == null) {
				this.end = end;
				this.inboundChanged.signalAll();
			}
			settleLocked();
		}
		finally {
			this.inboundLock.unlock();
		}
	}

	/**
	 * Drops the buffered payload and ends the inbound stream at once with {@code end};
	 * the window's charge is released for good.
	 */
	private void dropInbound(End end) {
		this.inboundLock.lock();
		try {
			this.segments.clear();
			this.headPosition = 0;
			this.budget.addBuffered(-this.buffered);
			this.buffered = 0;
			this.end = end;
			this.inboundChanged.signalAll();
			this.account.close();
		}
		finally {
			this.inboundLock.unlock();
		}
	}

	/**
	 * Once no more payload arrives -- the inbound stream ended -- the window holds only
	 * what is buffered: the charge past it is released. With the inbound lock.
	 */
	private void settleLocked() {
		if (this.end != null) {
			this.account.resize(Math.max(0, this.buffered - this.initialWindow));
		}
	}

	/**
	 * The heap the receive buffer of a connection holds past its unread payload for a
	 * window of {@code window}: the read part of the head segment and the room of the
	 * tail segment (under one segment each), and the overhead of {@link #maxSegments}
	 * segments.
	 */
	static long receiveBufferSlack(int window) {
		return 2L * SEGMENT_SIZE + (long) maxSegments(window) * SEGMENT_OVERHEAD;
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
	 * Applies the remote's WINDOW_UPDATE: its grant, its request to give back the credit
	 * past {@code reclaim_above}, the credit it released. Never blocks.
	 */
	public void windowUpdate(WindowUpdate update) {
		if (update.getIncrement() != 0) {
			grant(update.getIncrement());
		}
		if (update.getReclaimAbove() != 0) {
			reclaim(update.getReclaimAbove());
		}
		if (update.getReleased() != 0) {
			released(update.getReleased());
		}
		if (update.getBlocked()) {
			remoteBlocked();
		}
	}

	/**
	 * The remote has DATA and no credit left: the reader's next decision may grow the
	 * window, and what the reader took goes back once the buffer is empty
	 * ({@link #grantToWaitingRemoteLocked}), at once if it is.
	 */
	void remoteBlocked() {
		long increment;
		this.inboundLock.lock();
		try {
			this.remoteBlocked = true;
			this.remoteWaiting = true;
			increment = grantToWaitingRemoteLocked();
		}
		finally {
			this.inboundLock.unlock();
		}
		if (increment > 0) {
			this.frameWriter.sendWindowUpdate(this.connectionId, (int) increment);
		}
	}

	/**
	 * With the remote waiting and the buffer empty, the remote sent everything it could
	 * before it said it was blocked: grants what the reader took since the latest grant,
	 * if a quarter of the window, keeping the window. The increment to send; with the
	 * inbound lock.
	 */
	private long grantToWaitingRemoteLocked() {
		if (!this.remoteWaiting || this.buffered > 0 || this.pending < this.window / 4 || this.closed
				|| this.end != null) {
			return 0;
		}
		this.remoteWaiting = false;
		long increment = this.window - this.credit;
		this.credit = this.window;
		this.pending = 0;
		return increment;
	}

	/**
	 * The remote asks for the credit this side holds past {@code keep} (unsigned): it is
	 * given up and announced as released, so the writer never sends those bytes.
	 */
	void reclaim(int keep) {
		long kept = Integer.toUnsignedLong(keep);
		long released;
		this.writeLock.lock();
		try {
			released = Math.clamp(this.sendCredit - kept, 0, 0xFFFF_FFFFL);
			this.sendCredit -= released;
		}
		finally {
			this.writeLock.unlock();
		}
		if (released > 0) {
			this.frameWriter.sendWindowReleased(this.connectionId, (int) released);
		}
	}

	/**
	 * The remote gave up {@code released} (unsigned) of its credit: the window drops by
	 * it, and so does its charge. More than the remote may hold fails the connection like
	 * payload past the window. A window left below the announced one is granted back up
	 * to it.
	 */
	void released(int released) {
		long amount = Integer.toUnsignedLong(released);
		long increment = 0;
		boolean exceeded = false;
		this.inboundLock.lock();
		try {
			if (this.end != null) {
				return;
			}
			if (amount > this.credit) {
				exceeded = true;
			}
			else {
				this.credit -= amount;
				this.window -= amount;
				if (this.window < this.initialWindow) {
					increment = this.initialWindow - this.window;
					this.credit += increment;
					this.window = this.initialWindow;
				}
				this.account.resize(this.window - this.initialWindow);
			}
		}
		finally {
			this.inboundLock.unlock();
		}
		if (exceeded) {
			failWindowExceeded();
		}
		else if (increment > 0) {
			this.frameWriter.sendWindowUpdate(this.connectionId, (int) increment);
		}
	}

	/**
	 * The remote granted {@code increment} (unsigned) more payload bytes; wakes the
	 * writer waiting for credit. Never blocks beyond the brief credit lock, which no
	 * writer holds across a send.
	 */
	public void grant(int increment) {
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

	/** Test hook: the receive window. */
	long receiveWindow() {
		this.inboundLock.lock();
		try {
			return this.window;
		}
		finally {
			this.inboundLock.unlock();
		}
	}

	/** Test hook: what the remote may still send. */
	long receiveCredit() {
		this.inboundLock.lock();
		try {
			return this.credit;
		}
		finally {
			this.inboundLock.unlock();
		}
	}

	/** Test hook: the largest window granted. */
	long peakWindow() {
		this.inboundLock.lock();
		try {
			return this.peakWindow;
		}
		finally {
			this.inboundLock.unlock();
		}
	}

	/** Test hook: the reclaims asked. */
	long reclaims() {
		this.inboundLock.lock();
		try {
			return this.reclaims;
		}
		finally {
			this.inboundLock.unlock();
		}
	}

	/** The bytes the connection charges to its {@link TunnelBudget}. */
	public long charged() {
		return this.account.charged();
	}

	/** Test hook: the times the writer waited for credit. */
	long creditWaits() {
		this.writeLock.lock();
		try {
			return this.creditWaits;
		}
		finally {
			this.writeLock.unlock();
		}
	}

	/** The remote terminated the connection abnormally. */
	public void remoteFailed(String message) {
		this.remoteDone = true;
		this.failure = message;
		endWith(new End(message == null ? "remote failure" : message));
		signalCredit(); // the remote grants nothing more
		Runnable action;
		synchronized (this.remoteFailureLock) {
			if (this.remoteFailed) {
				return;
			}
			this.remoteFailed = true;
			action = this.onRemoteFailure;
		}
		if (action != null) {
			action.run();
		}
	}

	/**
	 * Runs {@code action} once the remote sent ERROR -- at once if it already did. Unlike
	 * the failure a reader gets, it also fires after the remote's CLOSE, when the reader
	 * already returned EOF. Runs on the thread delivering the ERROR, so it must not
	 * block; one action per connection.
	 */
	public void onRemoteFailure(Runnable action) {
		boolean failed;
		synchronized (this.remoteFailureLock) {
			this.onRemoteFailure = action;
			failed = this.remoteFailed;
		}
		if (failed) {
			action.run();
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
	 * Ends the connection locally. While the remote may still send (neither CLOSE nor
	 * ERROR arrived) it sends ERROR instead of CLOSE: the remote's payload would be
	 * dropped and never granted, so a remote waiting for credit would wait forever.
	 */
	@Override
	public void close() {
		if (!this.remoteDone) {
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
		boolean waiting = false;
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
				if (!waiting) {
					// tells the remote once per wait, outside the credit lock
					waiting = true;
					this.creditWaits++;
					this.writeLock.unlock();
					try {
						this.creditWaited.run();
						this.frameWriter.sendWindowBlocked(this.connectionId);
					}
					finally {
						this.writeLock.lock();
					}
					continue;
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

		/** What {@link #take} returns when frames are due before it may go on. */
		private static final int RETRY = -2;

		/** The end was returned (EOF or the failure); later reads return EOF. */
		private boolean endSeen;

		/** The grant to send once the reader left the lock; 0 = none. */
		private long grantDue;

		/** A reclaim to ask once the reader left the lock. */
		private boolean reclaimDue;

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
			while (true) {
				int n;
				try {
					n = take(b, off, len);
				}
				finally {
					sendDue();
				}
				if (n != RETRY) {
					return n;
				}
			}
		}

		/**
		 * Copies up to {@code len} buffered bytes, across segments, waiting while the
		 * buffer is empty and the stream has not ended; -1 at the end. Decides the window
		 * at every half window taken ({@link #decideLocked}). Waits at most
		 * {@code reclaimAfter} while the window is past the announced one and the remote
		 * was not asked to give back its credit since payload last arrived: then reclaims
		 * ({@link #reclaimLocked}) and returns {@link #RETRY}.
		 */
		private int take(byte[] b, int off, int len) throws IOException {
			VirtualConnection outer = VirtualConnection.this;
			outer.inboundLock.lock();
			try {
				long deadline = 0;
				while (true) {
					int n = 0;
					Segment head;
					while (n < len && (head = outer.segments.peekFirst()) != null) {
						int position = outer.headPosition;
						int copied = Math.min(len - n, head.length - position);
						System.arraycopy(head.data, position, b, off + n, copied);
						n += copied;
						if (position + copied == head.length) {
							outer.segments.pollFirst();
							outer.headPosition = 0;
						}
						else {
							outer.headPosition = position + copied;
						}
					}
					if (n > 0) {
						outer.buffered -= n;
						outer.budget.addBuffered(-n);
						outer.pending += n;
						decideLocked();
						this.grantDue += outer.grantToWaitingRemoteLocked();
						return n;
					}
					End end = outer.end;
					if (end != null) {
						this.endSeen = true;
						String failure = end.failure();
						if (failure != null) {
							throw new IOException(failure);
						}
						return -1;
					}
					outer.waited = true;
					try {
						if (outer.window > outer.initialWindow && !outer.reclaimAsked) {
							long now = System.nanoTime();
							if (deadline == 0) {
								outer.readerWaits++;
								deadline = now + outer.reclaimAfterNanos;
							}
							else if (now - deadline >= 0) {
								reclaimLocked();
								return RETRY;
							}
							outer.inboundChanged.awaitNanos(deadline - now);
						}
						else {
							if (deadline == 0) {
								outer.readerWaits++;
								deadline = System.nanoTime();
							}
							outer.inboundChanged.await();
						}
					}
					catch (InterruptedException e) {
						Thread.currentThread().interrupt();
						throw new IOException("interrupted while waiting for inbound data", e);
					}
				}
			}
			finally {
				outer.inboundLock.unlock();
			}
		}

		/**
		 * At every half window the reader took, moves the window and grants what makes it
		 * the new window: when the remote was blocked since the previous decision, halves
		 * it (not below the announced window) if the reader never found the buffer empty
		 * meanwhile and is a quarter of the window behind, and doubles it (within the
		 * maximum, the share and what the budget has left) if not; halves it when the
		 * remote was not blocked for two decisions in a row; keeps it otherwise. A window
		 * shrinking below what is outstanding shrinks to that, granting nothing. Never on
		 * a closed or ended connection: what an ended one holds is settled as it drains.
		 * With the inbound lock.
		 */
		private void decideLocked() {
			VirtualConnection outer = VirtualConnection.this;
			if (outer.closed || outer.end != null) {
				outer.pending = 0;
				settleLocked();
				return;
			}
			long window = outer.window;
			if (outer.pending < window / 2) {
				return;
			}
			long target;
			if (outer.remoteBlocked && !outer.waited && outer.buffered >= window / 4) {
				outer.unblocked = 0;
				target = window / 2;
			}
			else if (outer.remoteBlocked) {
				outer.unblocked = 0;
				target = Math.min(outer.maxWindow, 2 * window);
			}
			else if (++outer.unblocked >= 2) {
				outer.unblocked = 0;
				target = window / 2;
			}
			else {
				target = window;
			}
			outer.remoteBlocked = false;
			outer.waited = false;
			long initial = outer.initialWindow;
			if (target > window) {
				target = initial + outer.account.resize(target - initial);
			}
			// over its share, the window gives back what it can
			long share = outer.account.share();
			if (share < outer.maxWindow - initial) {
				target = Math.min(target, initial + share);
			}
			grantLocked(Math.max(target, initial));
		}

		/**
		 * The reader waited {@code reclaimAfter} for payload with the window past the
		 * announced one: grants what it read up to the announced window and asks the
		 * remote to give back the credit it holds past it. With the inbound lock.
		 */
		private void reclaimLocked() {
			VirtualConnection outer = VirtualConnection.this;
			grantLocked(outer.initialWindow);
			outer.reclaimAsked = true;
			if (outer.credit > outer.initialWindow) {
				this.reclaimDue = true;
				outer.reclaims++;
			}
		}

		/**
		 * Makes the window {@code target}, or what is outstanding when that is more:
		 * grants the difference, credited before the grant leaves (the remote's next
		 * payload must never be measured against a stale credit), and resizes the charge
		 * to the window. With the inbound lock.
		 */
		private void grantLocked(long target) {
			VirtualConnection outer = VirtualConnection.this;
			long outstanding = outer.credit + outer.buffered;
			long window = Math.max(target, outstanding);
			long increment = window - outstanding;
			outer.credit += increment;
			outer.pending = 0;
			outer.window = window;
			outer.peakWindow = Math.max(outer.peakWindow, window);
			outer.account.resize(window - outer.initialWindow);
			this.grantDue += increment;
		}

		/** Sends the grant and the reclaim due, outside the lock. */
		private void sendDue() {
			VirtualConnection outer = VirtualConnection.this;
			if (this.grantDue > 0) {
				int increment = (int) this.grantDue;
				this.grantDue = 0;
				outer.frameWriter.sendWindowUpdate(outer.connectionId, increment);
			}
			if (this.reclaimDue) {
				this.reclaimDue = false;
				outer.frameWriter.sendWindowReclaim(outer.connectionId, outer.initialWindow);
			}
		}

	}

	private final class Sink extends OutputStream {

		@Override
		public void write(int b) throws IOException {
			write(new byte[] { (byte) b }, 0, 1);
		}

		/**
		 * Sends frame by frame, each against the credit taken for it. The closed check
		 * and the send are not atomic: a {@link #close()} whose CLOSE is queued in
		 * between lets this frame follow the CLOSE, and the remote reader, stopped at the
		 * EOF, never reads it.
		 */
		@Override
		public void write(byte[] b, int off, int len) throws IOException {
			Objects.requireNonNull(b, "b is required");
			if (off < 0 || len < 0 || off + len > b.length) {
				throw new IndexOutOfBoundsException("off=%d len=%d b.length=%d".formatted(off, len, b.length));
			}
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
