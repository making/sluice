package am.ik.sluice.client.tunnel;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ReconnectBackoffTest {

	/** The ceilings of consecutive failures: 1s doubling up to the 30s cap. */
	private static final long[] CEILINGS_MILLIS = { 1000, 2000, 4000, 8000, 16000, 30000, 30000, 30000 };

	@Test
	void eachDelayLiesInTheUpperHalfOfItsCeiling() {
		for (long seed = 0; seed < 50; seed++) {
			ReconnectBackoff backoff = new ReconnectBackoff(new Random(seed));
			for (long ceiling : CEILINGS_MILLIS) {
				assertThat(backoff.next().toMillis()).isBetween(ceiling / 2, ceiling);
			}
		}
	}

	@Test
	void delaysVaryAcrossAttemptsOfOneClientAndAcrossClients() {
		Set<Long> firstDelays = new HashSet<>();
		for (long seed = 0; seed < 20; seed++) {
			ReconnectBackoff backoff = new ReconnectBackoff(new Random(seed));
			firstDelays.add(backoff.next().toMillis());
		}
		assertThat(firstDelays).as("clients with different seeds do not retry on the same schedule")
			.hasSizeGreaterThan(10);
		ReconnectBackoff capped = new ReconnectBackoff(new Random(1));
		List<Long> atCap = new ArrayList<>();
		for (int i = 0; i < 20; i++) {
			long delay = capped.next().toMillis();
			if (i >= 5) {
				atCap.add(delay);
			}
		}
		assertThat(new HashSet<>(atCap)).as("attempts at the cap are not all equal").hasSizeGreaterThan(5);
	}

	@Test
	void theSameSeedGivesTheSameSchedule() {
		ReconnectBackoff a = new ReconnectBackoff(new Random(7));
		ReconnectBackoff b = new ReconnectBackoff(new Random(7));
		for (int i = 0; i < 8; i++) {
			assertThat(a.next()).isEqualTo(b.next());
		}
	}

	@Test
	void resetStartsOverFromTheInitialCeiling() {
		ReconnectBackoff backoff = new ReconnectBackoff(new Random(3));
		for (int i = 0; i < 6; i++) {
			backoff.next();
		}
		backoff.reset();
		assertThat(backoff.next()).isBetween(Duration.ofMillis(500), Duration.ofMillis(1000));
		assertThat(backoff.next()).isBetween(Duration.ofMillis(1000), Duration.ofMillis(2000));
	}

}
