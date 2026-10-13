package am.ik.sluice.server.proxy;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The bytes the h2 demux relays of a node may hold, both ways, beyond each connection's
 * own allowance, shared by all of them. A relay charges what it holds past its allowance
 * and releases it as its hops drain. What a relay may refuse -- a new stream, the
 * connection windows -- it takes only while the budget has room ({@link #tryCharge},
 * {@link #hasRoom}); what it may not refuse is charged regardless ({@link #charge}).
 * <p>
 * A relay that withheld the client's connection window waits for room with
 * {@link #awaitRoom}: once a release leaves the budget below its limit, every waiter runs
 * once on a virtual thread of its own, so no relay's socket write runs on the thread of
 * the relay that released.
 */
final class H2DemuxBudget {

	private final long max;

	private final AtomicLong used = new AtomicLong();

	private final Set<Runnable> waiters = ConcurrentHashMap.newKeySet();

	H2DemuxBudget(long max) {
		this.max = max;
	}

	/** The budget, in bytes. */
	long max() {
		return this.max;
	}

	/** The bytes charged; may pass {@link #max()} by what was charged regardless. */
	long used() {
		return this.used.get();
	}

	/** Whether the budget is below its limit. */
	boolean hasRoom() {
		return this.used.get() < this.max;
	}

	/** Charges the bytes if they fit; {@code false} leaves the budget as it was. */
	boolean tryCharge(long bytes) {
		while (true) {
			long current = this.used.get();
			if (current + bytes > this.max) {
				return false;
			}
			if (this.used.compareAndSet(current, current + bytes)) {
				return true;
			}
		}
	}

	/** Charges bytes that cannot be refused. */
	void charge(long bytes) {
		this.used.addAndGet(bytes);
	}

	/** Releases charged bytes; wakes the waiters once the budget has room. */
	void release(long bytes) {
		if (this.used.addAndGet(-bytes) < this.max) {
			wake();
		}
	}

	/**
	 * Runs the waiter on a virtual thread once the budget has room -- at once when it has
	 * now. A waiter registered twice runs once.
	 */
	void awaitRoom(Runnable waiter) {
		this.waiters.add(waiter);
		if (hasRoom()) {
			wake();
		}
	}

	/** Forgets a waiter that has not run yet. */
	void cancel(Runnable waiter) {
		this.waiters.remove(waiter);
	}

	/** The waiters not run yet; for tests. */
	int waiting() {
		return this.waiters.size();
	}

	private void wake() {
		if (this.waiters.isEmpty()) {
			return;
		}
		for (Runnable waiter : this.waiters) {
			if (this.waiters.remove(waiter)) {
				Thread.ofVirtual().name("sluice-h2demux-budget").start(waiter);
			}
		}
	}

}
