package am.ik.sluice.server.proxy.hop;

import java.time.Duration;

/**
 * A limit on the stream errors of a peer within a window: the window starts at the first
 * error counted and, once it passed, at the next one. Not thread-safe; a hop uses it with
 * its loop's lock.
 */
final class StreamErrorLimit {

	private final int maxErrors;

	private final Duration window;

	private final long windowNanos;

	/** The errors counted in the current window; zero = no window started. */
	private int errors;

	/** When the current window started, in {@link System#nanoTime()}. */
	private long windowStart;

	StreamErrorLimit(int maxErrors, Duration window) {
		if (maxErrors <= 0) {
			throw new IllegalArgumentException("maxErrors must be positive");
		}
		if (window.isNegative() || window.isZero()) {
			throw new IllegalArgumentException("window must be positive");
		}
		this.maxErrors = maxErrors;
		this.window = window;
		this.windowNanos = window.toNanos();
	}

	int maxErrors() {
		return this.maxErrors;
	}

	Duration window() {
		return this.window;
	}

	/**
	 * Counts one error at {@code nanoTime}; whether the errors of its window stay within
	 * the limit.
	 */
	boolean count(long nanoTime) {
		if (this.errors == 0 || nanoTime - this.windowStart >= this.windowNanos) {
			this.windowStart = nanoTime;
			this.errors = 1;
		}
		else if (this.errors <= this.maxErrors) {
			this.errors++;
		}
		return this.errors <= this.maxErrors;
	}

}
