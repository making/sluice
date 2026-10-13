package am.ik.sluice.client.tunnel;

import java.time.Duration;
import java.util.random.RandomGenerator;

/**
 * Exponential backoff with equal jitter: the ceiling doubles from {@link #INITIAL} up to
 * {@link #MAX}, and each delay is drawn uniformly from the upper half of the ceiling
 * (ceiling/2 to ceiling), so clients that failed together do not retry together. Methods
 * are synchronized; the generator is only touched under that lock.
 */
final class ReconnectBackoff {

	static final Duration INITIAL = Duration.ofSeconds(1);

	static final Duration MAX = Duration.ofSeconds(30);

	private final RandomGenerator random;

	private long ceilingMillis = INITIAL.toMillis();

	ReconnectBackoff(RandomGenerator random) {
		this.random = random;
	}

	/** The delay before the next attempt; the ceiling of the one after doubles. */
	synchronized Duration next() {
		long ceiling = this.ceilingMillis;
		this.ceilingMillis = Math.min(MAX.toMillis(), ceiling * 2);
		long half = ceiling / 2;
		return Duration.ofMillis(half + this.random.nextLong(half + 1));
	}

	/** Back to the initial ceiling, after the stream was accepted. */
	synchronized void reset() {
		this.ceilingMillis = INITIAL.toMillis();
	}

}
