package am.ik.sluice.spike.h2demux;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * Serializes every operation on the netty channels of one relay (or one test peer). An
 * {@code EmbeddedChannel} has no thread of its own: {@code EmbeddedEventLoop#inEventLoop}
 * answers {@code true} on every thread and its task queue is a plain {@code ArrayDeque},
 * so netty's own event-loop assertions never catch a second thread entering it. This lock
 * is the only guard: every reader, writer, timer and test thread enters the channels
 * through {@link #run} or {@link #call}.
 * <p>
 * After each operation the channels are settled: pending tasks run, every channel is
 * flushed, and what the channels wrote moves to the writer queues of their hops -- until
 * no channel writes anything more, since a write on one channel (a leg) can complete a
 * promise whose listener writes on another (the client's WINDOW_UPDATE).
 */
public final class Loop implements AutoCloseable {

	/**
	 * How often scheduled tasks run: netty schedules its own timers (the graceful close
	 * after a GOAWAY, say) on the channel's executor, which an EmbeddedChannel runs only
	 * when told to.
	 */
	private static final Duration TICK = Duration.ofMillis(20);

	private final ReentrantLock lock = new ReentrantLock();

	private final List<Hop> hops = new CopyOnWriteArrayList<>();

	private final Thread ticker;

	private volatile boolean closed;

	public Loop(String name) {
		this.ticker = Thread.ofVirtual().name(name + "-tick").start(this::tick);
	}

	/** Runs the action on the channels, then settles them. */
	public void run(Runnable action) {
		this.lock.lock();
		try {
			action.run();
			settleLocked();
		}
		finally {
			this.lock.unlock();
		}
	}

	/** Runs the action on the channels, then settles them, and returns its result. */
	public <T> T call(Supplier<T> action) {
		this.lock.lock();
		try {
			T result = action.get();
			settleLocked();
			return result;
		}
		finally {
			this.lock.unlock();
		}
	}

	boolean heldByCurrentThread() {
		return this.lock.isHeldByCurrentThread();
	}

	void register(Hop hop) {
		this.hops.add(hop);
	}

	void unregister(Hop hop) {
		this.hops.remove(hop);
	}

	private void settleLocked() {
		for (int round = 0; round < 1000; round++) {
			boolean moved = false;
			for (Hop hop : this.hops) {
				moved |= hop.settleLocked();
			}
			if (!moved) {
				return;
			}
		}
		throw new IllegalStateException("the channels did not settle");
	}

	private void tick() {
		while (!this.closed) {
			try {
				Thread.sleep(TICK);
			}
			catch (InterruptedException e) {
				return;
			}
			run(() -> this.hops.forEach(Hop::runScheduledLocked));
		}
	}

	@Override
	public void close() {
		this.closed = true;
		this.ticker.interrupt();
	}

}
