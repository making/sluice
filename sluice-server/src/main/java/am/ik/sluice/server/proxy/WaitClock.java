package am.ik.sluice.server.proxy;

import java.util.function.BooleanSupplier;

/**
 * Measures the time a condition held, for a relay's stall watch: the relay calls
 * {@link #update()} after every change of what the condition reads, and the clock adds
 * the time between the update that found it true and the one that found it false. The
 * condition is evaluated under the clock's lock, so the last update always reflects the
 * last change, whichever thread made it.
 */
final class WaitClock {

	private final BooleanSupplier condition;

	/** The time of the waits ended; with this. */
	private long ended;

	/** When the wait in progress began; with this. */
	private long since;

	/** Whether a wait is in progress; with this. */
	private boolean waiting;

	WaitClock(BooleanSupplier condition) {
		this.condition = condition;
	}

	/** Evaluates the condition anew; it must not block. */
	synchronized void update() {
		boolean now = this.condition.getAsBoolean();
		if (now == this.waiting) {
			return;
		}
		long time = System.nanoTime();
		if (now) {
			this.since = time;
		}
		else {
			this.ended += time - this.since;
		}
		this.waiting = now;
	}

	/** The time the condition held so far, the wait in progress included. */
	synchronized long nanos() {
		return this.waiting ? this.ended + System.nanoTime() - this.since : this.ended;
	}

}
