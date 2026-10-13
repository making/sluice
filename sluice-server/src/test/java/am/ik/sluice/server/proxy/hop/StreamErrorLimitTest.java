package am.ik.sluice.server.proxy.hop;

import java.time.Duration;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StreamErrorLimitTest {

	private static final long SECOND = Duration.ofSeconds(1).toNanos();

	@Test
	void theErrorPastTheLimitWithinTheWindowIsOverIt() {
		StreamErrorLimit limit = new StreamErrorLimit(3, Duration.ofSeconds(30));

		assertThat(limit.count(0)).isTrue();
		assertThat(limit.count(SECOND)).isTrue();
		assertThat(limit.count(29 * SECOND)).isTrue();
		assertThat(limit.count(29 * SECOND + 1)).isFalse();
		assertThat(limit.count(29 * SECOND + 2)).as("still over it").isFalse();
	}

	@Test
	void theWindowStartsAtItsFirstErrorAndAgainOnceItPassed() {
		StreamErrorLimit limit = new StreamErrorLimit(2, Duration.ofSeconds(30));

		assertThat(limit.count(100 * SECOND)).isTrue();
		assertThat(limit.count(129 * SECOND)).isTrue();
		// the window of the first error passed: a new one starts
		assertThat(limit.count(130 * SECOND)).isTrue();
		assertThat(limit.count(159 * SECOND)).isTrue();
		assertThat(limit.count(159 * SECOND + 1)).isFalse();
		// a window passed over the errors past the limit too
		assertThat(limit.count(190 * SECOND)).isTrue();
	}

	@Test
	void aLimitOrWindowOfZeroIsRejected() {
		assertThatThrownBy(() -> new StreamErrorLimit(0, Duration.ofSeconds(30)))
			.isInstanceOf(IllegalArgumentException.class)
			.hasMessage("maxErrors must be positive");
		assertThatThrownBy(() -> new StreamErrorLimit(1, Duration.ZERO)).isInstanceOf(IllegalArgumentException.class)
			.hasMessage("window must be positive");
	}

}
