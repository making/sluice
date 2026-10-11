package am.ik.sluice.tunnel;

import org.jspecify.annotations.Nullable;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import java.util.List;

import am.ik.sluice.v1.proto.Advertise;
import am.ik.sluice.v1.proto.AdvertiseAck;
import am.ik.sluice.v1.proto.Close;
import am.ik.sluice.v1.proto.Connect;
import am.ik.sluice.v1.proto.Data;
import am.ik.sluice.v1.proto.Error;
import am.ik.sluice.v1.proto.Frame;
import am.ik.sluice.v1.proto.MembershipUpdate;
import am.ik.sluice.v1.proto.Node;
import am.ik.sluice.v1.proto.Drain;
import am.ik.sluice.v1.proto.Upstream;
import am.ik.sluice.v1.proto.WindowUpdate;
import io.grpc.stub.CallStreamObserver;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

/**
 * Schedules outbound frames to the underlying gRPC stream observer, honoring flow control
 * via {@link CallStreamObserver#isReady()}.
 * <p>
 * Callbacks must never call {@code onNext} directly from a receive callback; everything
 * goes through this class and a dedicated virtual thread instead. Frames are scheduled,
 * not just queued: control frames (CONNECT, ADVERTISE, ADVERTISE_ACK, MEMBERSHIP_UPDATE,
 * DRAIN, WINDOW_UPDATE) wait on an unbounded queue that is always sent first, so
 * enqueueing one never blocks. Connection frames (DATA, CLOSE, ERROR) wait on a
 * per-connection queue holding at most {@link #DATA_QUEUE_CAPACITY} DATA frames; only
 * that connection's producer blocks once the cap is reached, and CLOSE / ERROR bypass it.
 * Queueing CLOSE / ERROR closes the queue to DATA: the producers parked on its cap fail
 * at once, and so does every later chunk while the queue lives.
 * <p>
 * Per-frame work is O(1) in the number of connections: the worker pops the head of a ring
 * of the connections with pending frames, sends one frame, and re-appends the queue while
 * frames remain, so one bulk transfer cannot starve the others. A queue is dropped as
 * soon as it is empty and no {@link #sendData} call runs in it -- CLOSE / ERROR or not --
 * and a late frame for the id starts a new one. Keeping it for the running call means a
 * CLOSE racing a multi-chunk send always lands on the queue that send checks, so no chunk
 * of the call follows the CLOSE. Waiting is targeted: the worker waits on its own
 * condition, and the producers parked on a connection's cap are woken by that
 * connection's DATA pops, its CLOSE / ERROR and liveness changes only -- an enqueue
 * anywhere else never wakes them. CONNECT stays ahead of its connection's DATA: it is
 * enqueued before any relay thread of the connection exists, and control frames drain
 * first. Control and marker frames are dropped once the sender is stopped. A dead worker
 * wakes and fails every waiting producer.
 */
public final class SessionSender implements FrameWriter, AutoCloseable {

	/** Maximum payload per DATA frame, leaving headroom below the 4MiB gRPC limit. */
	static final int MAX_CHUNK = 64 * 1024;

	/** Maximum DATA frames queued per connection: 16 x 64KiB = 1MiB of payload. */
	static final int DATA_QUEUE_CAPACITY = 16;

	/** The DATA payload one connection's queue holds at most: 1MiB. */
	static final int MAX_QUEUED_PAYLOAD = DATA_QUEUE_CAPACITY * MAX_CHUNK;

	/**
	 * The connection frames of one connection; guarded by {@link #lock}. A queue lives
	 * only while it holds frames or a {@link #sendData} call runs in it: it is dropped as
	 * soon as neither holds, so a late frame for the id starts a new queue.
	 */
	private static final class ConnectionQueue {

		final long id;

		final ArrayDeque<Frame> frames = new ArrayDeque<>();

		/** DATA frames within {@code #frames}; the per-connection producer bound. */
		int dataFrames;

		/** The queue is in the active ring; a fresh one joins with its first frame. */
		boolean queued;

		/** CLOSE or ERROR was queued: no DATA may follow it. */
		boolean closed;

		/**
		 * The {@link #sendData} calls inside this queue; it outlives its frames while one
		 * runs, so a CLOSE racing the call lands on the queue the call checks.
		 */
		int writers;

		/**
		 * Test hook: completions of cap-waiter awaits parked on this queue; counted per
		 * live queue, so it resets when the queue is dropped and recreated.
		 */
		long wakes;

		/** Waits the producers parked on the cap; the worker signals it on a DATA pop. */
		final Condition capWaiters;

		ConnectionQueue(long id, ReentrantLock lock) {
			this.id = id;
			this.capWaiters = lock.newCondition();
		}

	}

	private final StreamObserver<Frame> outbound;

	/**
	 * Control frames; always sent before any connection frame. Guarded by {@link #lock}.
	 */
	private final ArrayDeque<Frame> control = new ArrayDeque<>();

	/** Live connection queues by id; only non-empty queues. Guarded by {@link #lock}. */
	private final Map<Long, ConnectionQueue> queues = new HashMap<>();

	/**
	 * The ring of connection queues with pending frames, in serve order: the worker pops
	 * the head and re-appends the queue while frames remain. Guarded by {@link #lock}.
	 */
	private final ArrayDeque<ConnectionQueue> ring = new ArrayDeque<>();

	private final ReentrantLock lock = new ReentrantLock();

	/** Signals the worker: enqueues, readiness changes and shutdown. */
	private final Condition wakeWorker = lock.newCondition();

	private volatile boolean running = true;

	/** The receive side observed the end of the call; waiters give up. */
	private volatile boolean callClosed;

	private final ReentrantLock workerLock = new ReentrantLock();

	private final Condition workerDone = workerLock.newCondition();

	private volatile boolean done;

	private volatile @Nullable Worker worker;

	private volatile @Nullable Thread workerThread;

	/** Runs for every WINDOW_UPDATE queued; a metrics hook. */
	private final Runnable windowUpdateSent;

	public SessionSender(StreamObserver<Frame> outbound) {
		this(outbound, () -> {
		});
	}

	/**
	 * A sender that runs {@code windowUpdateSent} for every WINDOW_UPDATE it queues.
	 */
	public SessionSender(StreamObserver<Frame> outbound, Runnable windowUpdateSent) {
		this.outbound = Objects.requireNonNull(outbound, "outbound is required");
		this.windowUpdateSent = Objects.requireNonNull(windowUpdateSent, "windowUpdateSent is required");
		if (outbound instanceof CallStreamObserver<?> call) {
			call.setOnReadyHandler(this::wakeWorker);
		}
	}

	/** Wakes the worker; a readiness change may let it pop a frame. */
	private void wakeWorker() {
		this.lock.lock();
		try {
			this.wakeWorker.signalAll();
		}
		finally {
			this.lock.unlock();
		}
	}

	/**
	 * Liveness changed -- the sender stopped or the call closed: the worker rechecks and
	 * every producer parked on a cap gives up instead of waiting for {@link #close()}.
	 */
	private void releaseWaiters() {
		this.lock.lock();
		try {
			this.wakeWorker.signalAll();
			for (ConnectionQueue queue : this.queues.values()) {
				queue.capWaiters.signalAll();
			}
		}
		finally {
			this.lock.unlock();
		}
	}

	/**
	 * Starts the drain loop on a dedicated virtual thread. Must be invoked once after
	 * construction.
	 */
	public void start() {
		if (this.worker != null) {
			throw new IllegalStateException("sender already started");
		}
		Worker worker = new Worker();
		this.worker = worker;
		Thread thread = Thread.ofVirtual().name("sluice-sender").start(worker);
		this.workerThread = thread;
	}

	/**
	 * True once no more frames can reach the peer: the sender stopped, or the call is
	 * closed / cancelled. The client observer has no liveness probe, so the owner marks
	 * it via {@link #markCallClosed()} when the receive side ends.
	 */
	private boolean callClosed() {
		return this.callClosed || this.outbound instanceof ServerCallStreamObserver<?> server && server.isCancelled();
	}

	/**
	 * Marks the underlying call as closed: the worker parked on readiness and producers
	 * parked on the per-connection cap give up instead of waiting for {@link #close()}.
	 * Idempotent; the sender stays usable for a final {@link #close()}.
	 */
	public void markCallClosed() {
		this.callClosed = true;
		this.releaseWaiters();
	}

	/**
	 * Test hook: the number of live connection queues. A queue lives only while it holds
	 * frames, so this is zero once everything is drained -- idle connections included.
	 */
	int connectionQueues() {
		this.lock.lock();
		try {
			return this.queues.size();
		}
		finally {
			this.lock.unlock();
		}
	}

	/**
	 * Test hook: completions of cap-waiter awaits parked on the connection's queue; 0
	 * once the queue is dropped. The herd this counts is the wakeups of one connection's
	 * waiters caused by unrelated connections.
	 */
	long capWaiterWakeups(long connectionId) {
		this.lock.lock();
		try {
			ConnectionQueue queue = this.queues.get(connectionId);
			return queue == null ? 0 : queue.wakes;
		}
		finally {
			this.lock.unlock();
		}
	}

	/**
	 * The outbound stream accepts frames right now; a plain observer has no backpressure
	 * and is always ready.
	 */
	private boolean outboundReady() {
		return !(this.outbound instanceof CallStreamObserver<?> call) || call.isReady();
	}

	@Override
	public boolean isOpen() {
		return this.running && !callClosed();
	}

	/**
	 * Queues a control frame; never blocks. Dropped once the sender is stopped: the
	 * worker is gone and nobody would drain it. Returns whether it was queued.
	 */
	private boolean enqueueControl(Frame frame) {
		this.lock.lock();
		try {
			if (!this.running) {
				return false;
			}
			this.control.addLast(frame);
			this.wakeWorker.signal();
			return true;
		}
		finally {
			this.lock.unlock();
		}
	}

	/**
	 * Enters a {@link #sendData} call: the connection's queue, created if absent, stays
	 * live until {@link #exit} even when it empties.
	 */
	private ConnectionQueue enter(long connectionId) {
		this.lock.lock();
		try {
			ConnectionQueue queue = this.queues.computeIfAbsent(connectionId, id -> new ConnectionQueue(id, lock));
			queue.writers++;
			return queue;
		}
		finally {
			this.lock.unlock();
		}
	}

	/** Leaves a {@link #sendData} call; drops the queue if that was all it held. */
	private void exit(ConnectionQueue queue) {
		this.lock.lock();
		try {
			queue.writers--;
			if (queue.writers == 0 && queue.frames.isEmpty()) {
				this.queues.remove(queue.id, queue);
			}
		}
		finally {
			this.lock.unlock();
		}
	}

	/**
	 * Queues a DATA frame, blocking while this connection's queue is at
	 * {@link #DATA_QUEUE_CAPACITY} -- only this connection's producer waits, and only
	 * this connection's pops and markers wake it. Returns false when the sender is gone
	 * or CLOSE / ERROR was queued for the connection.
	 */
	private boolean enqueueData(ConnectionQueue queue, Frame frame) {
		this.lock.lock();
		try {
			while (true) {
				if (!this.running || callClosed() || queue.closed) {
					return false;
				}
				if (queue.dataFrames < DATA_QUEUE_CAPACITY) {
					queue.frames.addLast(frame);
					queue.dataFrames++;
					if (!queue.queued) {
						queue.queued = true;
						this.ring.addLast(queue);
					}
					this.wakeWorker.signal();
					return true;
				}
				try {
					queue.capWaiters.awaitNanos(TimeUnit.MILLISECONDS.toNanos(100));
					queue.wakes++;
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					return false;
				}
			}
		}
		finally {
			this.lock.unlock();
		}
	}

	/**
	 * Queues CLOSE / ERROR for the connection, bypassing the DATA cap; never blocks, so
	 * close paths cannot hang behind a full queue. Closes the queue to DATA and wakes the
	 * producers parked on its cap, which then fail. Dropped once the sender is stopped.
	 */
	private void enqueueMarker(long connectionId, Frame frame) {
		this.lock.lock();
		try {
			if (!this.running) {
				return;
			}
			ConnectionQueue queue = this.queues.computeIfAbsent(connectionId, id -> new ConnectionQueue(id, lock));
			queue.frames.addLast(frame);
			queue.closed = true;
			queue.capWaiters.signalAll();
			if (!queue.queued) {
				queue.queued = true;
				this.ring.addLast(queue);
			}
			this.wakeWorker.signal();
		}
		finally {
			this.lock.unlock();
		}
	}

	@Override
	public void sendConnect(Connect connect) {
		enqueueControl(Frame.newBuilder().setConnect(connect).build());
	}

	/**
	 * Queues the grant on the control queue: it is never delayed by DATA of any
	 * connection, and never blocks.
	 */
	@Override
	public void sendWindowUpdate(long connectionId, int increment) {
		WindowUpdate update = WindowUpdate.newBuilder().setConnId(connectionId).setIncrement(increment).build();
		if (enqueueControl(Frame.newBuilder().setWindowUpdate(update).build())) {
			this.windowUpdateSent.run();
		}
	}

	/**
	 * Queues the payload in chunks of at most {@link #MAX_CHUNK}. Returns false once the
	 * sender is gone or CLOSE / ERROR is queued for the connection while the call runs;
	 * the chunks queued before stay ahead of the marker, none follows it.
	 */
	@Override
	public boolean sendData(long connectionId, byte[] payload) {
		ConnectionQueue queue = enter(connectionId);
		try {
			for (int offset = 0; offset < payload.length; offset += MAX_CHUNK) {
				int length = Math.min(MAX_CHUNK, payload.length - offset);
				byte[] chunk = new byte[length];
				System.arraycopy(payload, offset, chunk, 0, length);
				Data data = Data.newBuilder()
					.setConnId(connectionId)
					.setPayload(com.google.protobuf.UnsafeByteOperations.unsafeWrap(chunk))
					.build();
				if (!enqueueData(queue, Frame.newBuilder().setData(data).build())) {
					return false;
				}
			}
			return true;
		}
		finally {
			exit(queue);
		}
	}

	/**
	 * Announces the upstreams and the client's per-connection receive window (0 = no
	 * per-connection flow control) to the server (client -> server only).
	 */
	public void sendAdvertise(List<Upstream> upstreams, int connectionWindow) {
		Advertise advertise = Advertise.newBuilder()
			.addAllUpstreams(upstreams)
			.setConnectionWindow(connectionWindow)
			.build();
		enqueueControl(Frame.newBuilder().setAdvertise(advertise).build());
	}

	/**
	 * Acknowledges an {@link Advertise} (server -> client only), listing the listen ports
	 * that were not bound and this node's id.
	 */
	public void sendAdvertiseAck(List<Integer> rejectedPorts, String nodeId) {
		AdvertiseAck ack = AdvertiseAck.newBuilder()
			.addAllRejectedPorts(rejectedPorts)
			.setNodeId(nodeId == null ? "" : nodeId)
			.build();
		enqueueControl(Frame.newBuilder().setAdvertiseAck(ack).build());
	}

	/**
	 * Publishes the cluster membership (server -> client only).
	 */
	public void sendMembership(List<Node> nodes, long membershipVersion) {
		MembershipUpdate update = MembershipUpdate.newBuilder()
			.addAllNodes(nodes)
			.setMembershipVersion(membershipVersion)
			.build();
		enqueueControl(Frame.newBuilder().setMembershipUpdate(update).build());
	}

	/**
	 * Notifies the client that this node is draining (server -> client only).
	 */
	public void sendDrain(String reason) {
		Drain drain = Drain.newBuilder().setReason(reason == null ? "" : reason).build();
		enqueueControl(Frame.newBuilder().setDrain(drain).build());
	}

	@Override
	public void sendClose(long connectionId) {
		Close close = Close.newBuilder().setConnId(connectionId).build();
		enqueueMarker(connectionId, Frame.newBuilder().setClose(close).build());
	}

	@Override
	public void sendError(long connectionId, String message) {
		Error error = Error.newBuilder().setConnId(connectionId).setMessage(message == null ? "" : message).build();
		enqueueMarker(connectionId, Frame.newBuilder().setError(error).build());
	}

	/**
	 * The next frame to send: control frames first, then one frame per turn over the
	 * connections with pending frames -- the head of the ring, re-appended while frames
	 * remain. A queue emptied by the pop is dropped, and its slot frees only its own
	 * producers. Waits while nothing is pending or the stream is not ready; null once the
	 * sender is stopped or the call closed, in which case the remaining frames are
	 * dropped -- the peer is gone anyway.
	 */
	private @Nullable Frame next() {
		this.lock.lock();
		try {
			while (true) {
				if (!this.running || callClosed()) {
					return null;
				}
				Frame control = this.control.pollFirst();
				if (control != null) {
					return control;
				}
				// readiness is consulted only for a pending connection frame: polling it
				// unguarded races the delayed call's start, where grpc's isReady NPEs
				ConnectionQueue head = this.ring.peekFirst();
				if (head == null || outboundReady()) {
					ConnectionQueue queue = this.ring.pollFirst();
					if (queue != null) {
						return pop(queue);
					}
				}
				try {
					this.wakeWorker.awaitNanos(TimeUnit.MILLISECONDS.toNanos(100));
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					return null;
				}
			}
		}
		finally {
			this.lock.unlock();
		}
	}

	/**
	 * Pops one frame; drops the queue it emptied unless a {@link #sendData} call runs in
	 * it, and wakes only its own producers.
	 */
	private Frame pop(ConnectionQueue queue) {
		queue.queued = false;
		Frame frame = queue.frames.pollFirst();
		if (frame.getBodyCase() == Frame.BodyCase.DATA) {
			queue.dataFrames--;
			// a producer parked on the cap of this connection may continue
			queue.capWaiters.signal();
		}
		if (!queue.frames.isEmpty()) {
			queue.queued = true;
			this.ring.addLast(queue);
		}
		else if (queue.writers == 0) {
			this.queues.remove(queue.id);
		}
		return frame;
	}

	/**
	 * Flushes queued frames to the gRPC stream. Readiness is part of {@link #next()}, so
	 * no frame is taken from a queue while the peer's flow control window is exhausted.
	 */
	private void drain() {
		try {
			while (true) {
				Frame frame = next();
				if (frame == null) {
					return;
				}
				try {
					this.outbound.onNext(frame);
				}
				catch (RuntimeException e) {
					// stream is dead; drop the remainder
					return;
				}
			}
		}
		finally {
			complete();
		}
	}

	/**
	 * Completes the outbound stream, telling the peer this side sent its last frame. On
	 * the server this ends the tunnel stream for the client; on the client it is the
	 * half-close of the request stream.
	 */
	private void complete() {
		try {
			this.outbound.onCompleted();
		}
		catch (RuntimeException e) {
			// the stream may already be dead
		}
	}

	/**
	 * Stops the worker and completes the outbound stream; waits for the completion to be
	 * delivered so a caller tearing the transport down right after (server shutdown) does
	 * not cut the frame in flight off. Frames still queued are not flushed: the peer is
	 * gone by definition of a teardown, and waiting for them could hang on a full queue
	 * -- close never depends on queue space. Idempotent.
	 */
	@Override
	public void close() {
		this.running = false;
		this.releaseWaiters();
		Worker worker = this.worker;
		Thread thread = this.workerThread;
		if (worker == null || thread == null || Thread.currentThread() == thread) {
			return;
		}
		this.workerLock.lock();
		try {
			while (!this.done) {
				try {
					this.workerDone.await(5, TimeUnit.SECONDS);
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					return;
				}
			}
		}
		finally {
			this.workerLock.unlock();
		}
	}

	private final class Worker implements Runnable {

		@Override
		public void run() {
			try {
				drain();
			}
			finally {
				// a dead worker must release the producers parked in enqueueData; close()
				// alone cannot be relied on to run
				running = false;
				releaseWaiters();
				workerLock.lock();
				try {
					done = true;
					workerDone.signalAll();
				}
				finally {
					workerLock.unlock();
				}
			}
		}

	}

}
