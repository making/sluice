package am.ik.sluice.tunnel;

import org.jspecify.annotations.Nullable;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
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
import io.grpc.stub.CallStreamObserver;
import io.grpc.stub.StreamObserver;

/**
 * Schedules outbound frames to the underlying gRPC stream observer, honoring flow control
 * via {@link CallStreamObserver#isReady()}.
 * <p>
 * Callbacks must never call {@code onNext} directly from a receive callback; everything
 * goes through this class and a dedicated virtual thread instead. Frames are scheduled,
 * not just queued: control frames (CONNECT, KEEP_ALIVE, ADVERTISE, ADVERTISE_ACK,
 * MEMBERSHIP_UPDATE, DRAIN, and WINDOW_UPDATE once it exists) wait on an unbounded queue
 * that is always sent first, so enqueueing one never blocks. Connection frames (DATA,
 * CLOSE, ERROR) wait on a per-connection queue holding at most
 * {@link #DATA_QUEUE_CAPACITY} DATA frames; only that connection's producer blocks once
 * the cap is reached, and CLOSE / ERROR bypass it. The worker drains the connection
 * queues round-robin, one frame per turn over the connections with pending frames, so one
 * bulk transfer cannot starve the others. CONNECT stays ahead of its connection's DATA:
 * it is enqueued before any relay thread of the connection exists, and control frames
 * drain first. A connection's queue is dropped once it empties after its CLOSE or ERROR;
 * a late frame for the id starts a new queue. A dead worker wakes and fails every waiting
 * producer.
 */
public final class SessionSender implements FrameWriter, AutoCloseable {

	/** Maximum payload per DATA frame, leaving headroom below the 4MiB gRPC limit. */
	static final int MAX_CHUNK = 64 * 1024;

	/** Maximum DATA frames queued per connection: 16 x 64KiB = 1MiB of payload. */
	static final int DATA_QUEUE_CAPACITY = 16;

	/** Frames of one connection waiting for the worker; guarded by {@link #lock}. */
	private static final class ConnectionQueue {

		final ArrayDeque<Frame> frames = new ArrayDeque<>();

		/** DATA frames within {@code #frames}; the per-connection producer bound. */
		int dataFrames;

		/** A CLOSE or ERROR is queued; the empty queue can be dropped. */
		boolean ended;

	}

	private final StreamObserver<Frame> outbound;

	/**
	 * Control frames; always sent before any connection frame. Guarded by {@link #lock}.
	 */
	private final ArrayDeque<Frame> control = new ArrayDeque<>();

	/** Connection frames by id; guarded by {@link #lock}. */
	private final TreeMap<Long, ConnectionQueue> queues = new TreeMap<>();

	private final ReentrantLock lock = new ReentrantLock();

	/** Signals enqueues, freed slots, readiness changes and shutdown. */
	private final Condition activity = lock.newCondition();

	private volatile boolean running = true;

	/**
	 * The connection served last; the round-robin resumes after it. Guarded by
	 * {@link #lock}.
	 */
	private long lastServed = -1;

	private final ReentrantLock workerLock = new ReentrantLock();

	private final Condition workerDone = workerLock.newCondition();

	private volatile boolean done;

	private volatile @Nullable Worker worker;

	private volatile @Nullable Thread workerThread;

	public SessionSender(StreamObserver<Frame> outbound) {
		this.outbound = Objects.requireNonNull(outbound, "outbound is required");
		if (outbound instanceof CallStreamObserver<?> call) {
			call.setOnReadyHandler(this::wakeWaiters);
		}
	}

	/** Wakes the worker and any producer; a readiness change may unblock either. */
	private void wakeWaiters() {
		this.lock.lock();
		try {
			this.activity.signalAll();
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

	private boolean awaitReady() {
		if (!(this.outbound instanceof CallStreamObserver<?> call) || call.isReady()) {
			return true;
		}
		this.lock.lock();
		try {
			while (this.running && !call.isReady()) {
				try {
					this.activity.awaitNanos(TimeUnit.MILLISECONDS.toNanos(100));
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					return false;
				}
			}
		}
		finally {
			lock.unlock();
		}
		return this.running;
	}

	/** Queues a control frame; never blocks. */
	private void enqueueControl(Frame frame) {
		this.lock.lock();
		try {
			this.control.addLast(frame);
			this.activity.signalAll();
		}
		finally {
			this.lock.unlock();
		}
	}

	/**
	 * Queues a DATA frame, blocking while this connection's queue is at
	 * {@link #DATA_QUEUE_CAPACITY} -- only this connection's producer waits. Returns
	 * false when the sender is gone.
	 */
	private boolean enqueueData(long connectionId, Frame frame) {
		this.lock.lock();
		try {
			while (true) {
				if (!this.running) {
					return false;
				}
				ConnectionQueue queue = this.queues.computeIfAbsent(connectionId, id -> new ConnectionQueue());
				if (queue.dataFrames < DATA_QUEUE_CAPACITY) {
					queue.frames.addLast(frame);
					queue.dataFrames++;
					this.activity.signalAll();
					return true;
				}
				try {
					this.activity.awaitNanos(TimeUnit.MILLISECONDS.toNanos(100));
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
	 * close paths cannot hang behind a full queue.
	 */
	private void enqueueMarker(long connectionId, Frame frame) {
		this.lock.lock();
		try {
			ConnectionQueue queue = this.queues.computeIfAbsent(connectionId, id -> new ConnectionQueue());
			queue.frames.addLast(frame);
			queue.ended = true;
			this.activity.signalAll();
		}
		finally {
			this.lock.unlock();
		}
	}

	@Override
	public void sendConnect(long connectionId, String address) {
		Connect connect = Connect.newBuilder()
			.setConnId(connectionId)
			.setAddress(address == null ? "" : address)
			.build();
		enqueueControl(Frame.newBuilder().setConnect(connect).build());
	}

	@Override
	public boolean sendData(long connectionId, byte[] payload) {
		for (int offset = 0; offset < payload.length; offset += MAX_CHUNK) {
			int length = Math.min(MAX_CHUNK, payload.length - offset);
			byte[] chunk = new byte[length];
			System.arraycopy(payload, offset, chunk, 0, length);
			Data data = Data.newBuilder()
				.setConnId(connectionId)
				.setPayload(com.google.protobuf.UnsafeByteOperations.unsafeWrap(chunk))
				.build();
			if (!enqueueData(connectionId, Frame.newBuilder().setData(data).build())) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Announces the upstreams to the server (client -> server only).
	 */
	public void sendAdvertise(List<Upstream> upstreams) {
		Advertise advertise = Advertise.newBuilder().addAllUpstreams(upstreams).build();
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
	 * connections with pending frames, resuming after the connection served last. Waits
	 * while nothing is pending; null once the sender is stopped, in which case the
	 * remaining frames are dropped -- the peer is gone anyway.
	 */
	private @Nullable Frame next() {
		this.lock.lock();
		try {
			while (true) {
				if (!this.running) {
					return null;
				}
				Frame control = this.control.pollFirst();
				if (control != null) {
					return control;
				}
				Frame frame = nextConnectionFrame();
				if (frame != null) {
					return frame;
				}
				try {
					this.activity.awaitNanos(TimeUnit.MILLISECONDS.toNanos(100));
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
		}
		finally {
			this.lock.unlock();
		}
	}

	/**
	 * Pops one frame from the next connection in round-robin order; queues empty after
	 * their CLOSE or ERROR are dropped along the way.
	 */
	private @Nullable Frame nextConnectionFrame() {
		Map.Entry<Long, ConnectionQueue> first = null;
		Map.Entry<Long, ConnectionQueue> next = null;
		for (Iterator<Map.Entry<Long, ConnectionQueue>> it = this.queues.entrySet().iterator(); it.hasNext();) {
			Map.Entry<Long, ConnectionQueue> entry = it.next();
			ConnectionQueue queue = entry.getValue();
			if (queue.frames.isEmpty()) {
				if (queue.ended) {
					it.remove();
				}
				continue;
			}
			if (first == null) {
				first = entry;
			}
			if (entry.getKey() > this.lastServed && next == null) {
				next = entry;
			}
		}
		Map.Entry<Long, ConnectionQueue> chosen = next != null ? next : first;
		if (chosen == null) {
			return null;
		}
		this.lastServed = chosen.getKey();
		ConnectionQueue queue = chosen.getValue();
		Frame frame = queue.frames.pollFirst();
		if (frame.getBodyCase() == Frame.BodyCase.DATA) {
			queue.dataFrames--;
			// a producer parked on the cap of this connection may continue
			this.activity.signalAll();
		}
		return frame;
	}

	/**
	 * Flushes queued frames to the gRPC stream, blocking while the peer's flow control
	 * window is exhausted.
	 */
	private void drain() {
		try {
			while (true) {
				Frame frame = next();
				if (frame == null) {
					return;
				}
				if (!awaitReady()) {
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
	 * not cut the final frames off. Idempotent.
	 */
	@Override
	public void close() {
		this.running = false;
		this.wakeWaiters();
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
				// a dead worker must release the producers parked in enqueueData and the
				// awaitReady loop; close() alone cannot be relied on to run
				running = false;
				wakeWaiters();
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
