package am.ik.sluice.server.tunnel;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class LogThrottleTest {

	private static final Duration PERIOD = Duration.ofSeconds(10);

	private final AtomicLong now = new AtomicLong(123_456_789L);

	private final LogThrottle throttle = new LogThrottle(PERIOD, this.now::get);

	@Test
	void theFirstLineOfAPeriodPassesAndTheNextReportsTheSuppressedCount() {
		assertThat(this.throttle.tryAcquire()).as("the first line passes, nothing suppressed yet").isZero();
		for (int i = 0; i < 5; i++) {
			this.now.addAndGet(Duration.ofSeconds(1).toNanos());
			assertThat(this.throttle.tryAcquire()).as("within the period").isEqualTo(-1);
		}
		this.now.addAndGet(Duration.ofSeconds(5).toNanos());

		assertThat(this.throttle.tryAcquire()).as("a period after the last line: reports the five").isEqualTo(5);
		assertThat(this.throttle.tryAcquire()).as("a new period started").isEqualTo(-1);
	}

	@Test
	void linesAPeriodApartAllPass() {
		for (int i = 0; i < 3; i++) {
			assertThat(this.throttle.tryAcquire()).isZero();
			this.now.addAndGet(PERIOD.toNanos());
		}
	}

}
