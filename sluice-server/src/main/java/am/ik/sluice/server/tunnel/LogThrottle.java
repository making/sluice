package am.ik.sluice.server.tunnel;

import java.time.Duration;
import java.util.function.LongSupplier;

/**
 * Lets at most one log line per period through: the first event of a period is logged,
 * the others of that period are counted, and the next line logged reports that count.
 */
final class LogThrottle {

	private final long periodNanos;

	private final LongSupplier clock;

	/** When the line last let through was; meaningful once {@link #started}. */
	private long lastLogged;

	private boolean started;

	private long suppressed;

	/**
	 * A throttle of one line per {@code period}, timed by {@code clock} (nanoseconds, as
	 * {@link System#nanoTime()}).
	 */
	LogThrottle(Duration period, LongSupplier clock) {
		this.periodNanos = period.toNanos();
		this.clock = clock;
	}

	/**
	 * Records one event: {@code -1} when its line is suppressed, otherwise the number of
	 * lines suppressed since the last one let through, which the caller logs with it.
	 */
	synchronized long tryAcquire() {
		long now = this.clock.getAsLong();
		if (this.started && now - this.lastLogged < this.periodNanos) {
			this.suppressed++;
			return -1;
		}
		this.started = true;
		this.lastLogged = now;
		long reported = this.suppressed;
		this.suppressed = 0;
		return reported;
	}

}
