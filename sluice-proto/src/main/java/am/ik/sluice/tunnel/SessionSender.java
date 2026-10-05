package am.ik.sluice.tunnel;

import org.jspecify.annotations.Nullable;

import java.util.Objects;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import java.util.Map;

import am.ik.sluice.v1.proto.Frame;
import am.ik.sluice.v1.proto.Upstream;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.StreamObserver;

/**
 * Serializes outbound frames through a bounded queue and flushes them to the underlying
 * gRPC stream observer, honoring flow control via
 * {@link ClientCallStreamObserver#isReady()}.
 * <p>
 * Callbacks must never call {@code onNext} directly from a receive callback; everything
 * goes through this queue and a dedicated virtual thread instead.
 */
public final class SessionSender implements FrameWriter, AutoCloseable {

	/** Maximum payload per DATA frame, leaving headroom below the 4MiB gRPC limit. */
	static final int MAX_CHUNK = 64 * 1024;

	private static final int QUEUE_CAPACITY = 256;

	private sealed interface Entry permits Payload, Poison {

	}

	private record Payload(Frame frame) implements Entry {
	}

	private record Poison() implements Entry {
	}

	private final StreamObserver<Frame> outbound;

	private final LinkedBlockingDeque<Entry> queue = new LinkedBlockingDeque<>(QUEUE_CAPACITY);

	private final ReentrantLock lock = new ReentrantLock();

	private final Condition onReady = lock.newCondition();

	private volatile boolean running = true;

	private volatile @Nullable Worker worker;

	public SessionSender(StreamObserver<Frame> outbound) {
		this.outbound = Objects.requireNonNull(outbound, "outbound is required");
		if (outbound instanceof ClientCallStreamObserver<?> call) {
			call.setOnReadyHandler(this::signalReady);
		}
	}

	private void signalReady() {
		lock.lock();
		try {
			onReady.signalAll();
		}
		finally {
			lock.unlock();
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
		this.worker = new Worker();
		Thread.ofVirtual().name("sluice-sender").start(this.worker);
	}

	private boolean awaitReady() {
		if (!(this.outbound instanceof ClientCallStreamObserver<?> call) || call.isReady()) {
			return true;
		}
		this.lock.lock();
		try {
			while (this.running && !call.isReady()) {
				try {
					this.onReady.awaitNanos(TimeUnit.MILLISECONDS.toNanos(100));
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

	private boolean enqueue(Entry entry) {
		try {
			while (this.running) {
				if (this.queue.offer(entry, 50, TimeUnit.MILLISECONDS)) {
					return true;
				}
			}
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		return false;
	}

	@Override
	public void sendConnect(long connectionId, String address) {
		Frame frame = Frame.newBuilder()
			.setType(Frame.Type.CONNECT)
			.setConnId(connectionId)
			.setAddress(address == null ? "" : address)
			.build();
		enqueue(new Payload(frame));
	}

	@Override
	public void sendData(long connectionId, byte[] payload) {
		for (int offset = 0; offset < payload.length; offset += MAX_CHUNK) {
			int length = Math.min(MAX_CHUNK, payload.length - offset);
			byte[] chunk = new byte[length];
			System.arraycopy(payload, offset, chunk, 0, length);
			Frame frame = Frame.newBuilder()
				.setType(Frame.Type.DATA)
				.setConnId(connectionId)
				.setPayload(com.google.protobuf.UnsafeByteOperations.unsafeWrap(chunk))
				.build();
			if (!enqueue(new Payload(frame))) {
				return;
			}
		}
	}

	/**
	 * Announces the upstream map to the server (client -> server only).
	 */
	public void sendAdvertise(Map<String, String> upstreams) {
		Frame frame = Frame.newBuilder()
			.setType(Frame.Type.ADVERTISE)
			.addAllUpstreams(upstreams.entrySet()
				.stream()
				.map(entry -> Upstream.newBuilder().setHost(entry.getKey()).setTargetUrl(entry.getValue()).build())
				.toList())
			.build();
		enqueue(new Payload(frame));
	}

	@Override
	public void sendClose(long connectionId) {
		Frame frame = Frame.newBuilder().setType(Frame.Type.CLOSE).setConnId(connectionId).build();
		enqueue(new Payload(frame));
	}

	@Override
	public void sendError(long connectionId, String message) {
		Frame frame = Frame.newBuilder()
			.setType(Frame.Type.ERROR)
			.setConnId(connectionId)
			.setMessage(message == null ? "" : message)
			.build();
		enqueue(new Payload(frame));
	}

	/**
	 * Flushes queued frames to the gRPC stream, blocking while the peer's flow control
	 * window is exhausted.
	 */
	private void drain() {
		while (true) {
			Entry entry;
			try {
				entry = this.queue.takeFirst();
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				continue;
			}
			if (entry instanceof Poison) {
				return;
			}
			Frame frame = ((Payload) entry).frame();
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

	@Override
	public void close() {
		this.running = false;
		this.queue.offerFirst(new Poison());
		this.signalReady();
	}

	private final class Worker implements Runnable {

		@Override
		public void run() {
			drain();
		}

	}

}
