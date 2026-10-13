package am.ik.sluice.server.tunnel;

import java.time.Duration;
import java.util.function.LongSupplier;

/**
 * Lets at most a number of log lines per period through: a period starts with the first
 * event after the previous one ended, its first {@code permits} events are logged, the
 * others counted, and the next line logged reports that count.
 */
public final class LogThrottle {

	private final long periodNanos;

	private final int permits;

	private final LongSupplier clock;

	/** When the current period started; meaningful once {@link #started}. */
	private long periodStart;

	private boolean started;

	/** Events let through in the current period. */
	private int granted;

	private long suppressed;

	/**
	 * A throttle of one line per {@code period}, timed by {@code clock} (nanoseconds, as
	 * {@link System#nanoTime()}).
	 */
	public LogThrottle(Duration period, LongSupplier clock) {
		this(period, 1, clock);
	}

	/** A throttle of {@code permits} lines per {@code period}, timed by {@code clock}. */
	public LogThrottle(Duration period, int permits, LongSupplier clock) {
		if (permits < 1) {
			throw new IllegalArgumentException("permits must be positive: " + permits);
		}
		this.periodNanos = period.toNanos();
		this.permits = permits;
		this.clock = clock;
	}

	/** A throttle that lets every line through. */
	public static LogThrottle unlimited() {
		return new LogThrottle(Duration.ofSeconds(1), Integer.MAX_VALUE, System::nanoTime);
	}

	/**
	 * Records one event: {@code -1} when its line is suppressed, otherwise the number of
	 * lines suppressed since the last one let through, which the caller logs with it.
	 */
	public synchronized long tryAcquire() {
		long now = this.clock.getAsLong();
		if (!this.started || now - this.periodStart >= this.periodNanos) {
			this.started = true;
			this.periodStart = now;
			this.granted = 0;
		}
		if (this.granted >= this.permits) {
			this.suppressed++;
			return -1;
		}
		this.granted++;
		long reported = this.suppressed;
		this.suppressed = 0;
		return reported;
	}

}
