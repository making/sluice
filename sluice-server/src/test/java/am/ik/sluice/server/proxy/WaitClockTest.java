package am.ik.sluice.server.proxy;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class WaitClockTest {

	private static final long MILLIS_50 = TimeUnit.MILLISECONDS.toNanos(50);

	@Test
	void countsTheWaitsAndTheWaitInProgressButNotTheTimeBetween() throws Exception {
		AtomicBoolean waiting = new AtomicBoolean();
		WaitClock clock = new WaitClock(waiting::get);
		long start = System.nanoTime();
		waiting.set(true);
		clock.update();
		Thread.sleep(50);
		waiting.set(false);
		clock.update();
		long firstWait = clock.nanos();
		assertThat(firstWait).as("the first wait").isBetween(MILLIS_50, System.nanoTime() - start);
		Thread.sleep(50);
		assertThat(clock.nanos()).as("nothing between the waits").isEqualTo(firstWait);
		waiting.set(true);
		clock.update();
		clock.update();
		Thread.sleep(50);
		assertThat(clock.nanos()).as("the wait in progress").isGreaterThanOrEqualTo(firstWait + MILLIS_50);
	}

}
