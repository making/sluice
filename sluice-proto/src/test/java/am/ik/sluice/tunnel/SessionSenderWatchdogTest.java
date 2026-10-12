package am.ik.sluice.tunnel;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The sender's progress watchdog: connection frames queued, the stream not ready and no
 * frame handed to it for the stall timeout stop the sender -- dropping the queues and
 * running the actions held on them -- and run the stall action once; anything that is not
 * such a stall never does.
 */
@Timeout(60)
class SessionSenderWatchdogTest {

	private static final Duration TIMEOUT = Duration.ofMillis(300);

	@Test
	void framesQueuedOnAStreamNotReadyStopTheSenderPastTheTimeout() throws Exception {
		FakeServerObserver observer = new FakeServerObserver();
		observer.gate.countDown();
		CountDownLatch fired = new CountDownLatch(1);
		AtomicReference<@Nullable Duration> reported = new AtomicReference<>();
		AtomicLong firedAt = new AtomicLong();
		AtomicInteger heldAtFire = new AtomicInteger(-1);
		AtomicInteger held = new AtomicInteger();
		SessionSender sender = SessionSender.builder(observer).stallTimeout(TIMEOUT).stallAction(stalled -> {
			firedAt.set(System.nanoTime());
			heldAtFire.set(held.get());
			reported.set(stalled);
			fired.countDown();
		}).build();
		try {
			assertThat(sender.stallTimeout()).as("precondition: the timeout took effect").isEqualTo(TIMEOUT);
			sender.start();
			long queuedAt = System.nanoTime();
			assertThat(sender.sendData(1, new byte[1])).isTrue();
			sender.sendClose(1);
			sender.afterQueued(1, held::incrementAndGet);

			assertThat(fired.await(5, TimeUnit.SECONDS)).as("the watchdog fired").isTrue();
			assertThat(Duration.ofNanos(firedAt.get() - queuedAt)).as("not before the timeout")
				.isGreaterThanOrEqualTo(TIMEOUT)
				.isLessThan(TIMEOUT.plusSeconds(1));
			assertThat(reported.get()).as("the stall's length").isGreaterThanOrEqualTo(TIMEOUT);
			assertThat(heldAtFire).as("the held action ran before the owner fails the call").hasValue(1);
			assertThat(observer.sent).as("nothing was handed to the stream").isEmpty();
			assertThat(sender.isOpen()).isFalse();
			assertThat(sender.sendData(2, new byte[1])).as("a stopped sender refuses DATA").isFalse();
			assertThat(sender.connectionQueues()).isZero();
		}
		finally {
			sender.close();
		}
		assertThat(held).as("run exactly once, the close included").hasValue(1);
	}

	@Test
	void aStreamThatKeepsTakingFramesSlowlyIsNotCut() throws Exception {
		FakeServerObserver observer = new FakeServerObserver();
		observer.gate.countDown();
		observer.oneFramePerReady.set(true);
		AtomicInteger fired = new AtomicInteger();
		SessionSender sender = SessionSender.builder(observer)
			.stallTimeout(TIMEOUT)
			.stallAction(stalled -> fired.incrementAndGet())
			.build();
		int frames = 12;
		Duration grantEvery = TIMEOUT.dividedBy(2);
		try {
			sender.start();
			Thread producer = Thread.ofVirtual().start(() -> {
				for (int i = 0; i < frames; i++) {
					sender.sendData(1, new byte[1]);
				}
			});
			long longestStall = 0;
			for (int i = 0; i < frames; i++) {
				long until = System.nanoTime() + grantEvery.toNanos();
				while (System.nanoTime() < until) {
					longestStall = Math.max(longestStall, sender.stalled().toNanos());
					TimeUnit.MILLISECONDS.sleep(10);
				}
				observer.becomeReady();
			}
			producer.join(TimeUnit.SECONDS.toMillis(5));
			awaitSent(observer, frames);

			assertThat(Duration.ofNanos(longestStall)).as("precondition: the stream stalled between grants")
				.isGreaterThan(grantEvery.dividedBy(2));
			assertThat(grantEvery.multipliedBy(frames)).as("precondition: the run outlasts the timeout many times")
				.isGreaterThan(TIMEOUT.multipliedBy(5));
			assertThat(fired).as("no stall reached the timeout").hasValue(0);
			assertThat(sender.isOpen()).isTrue();
		}
		finally {
			sender.close();
		}
	}

	@Test
	void aStreamNotReadyWithNothingQueuedIsNeverCut() throws Exception {
		FakeServerObserver observer = new FakeServerObserver();
		observer.gate.countDown();
		AtomicInteger fired = new AtomicInteger();
		SessionSender sender = SessionSender.builder(observer)
			.stallTimeout(Duration.ofMillis(100))
			.stallAction(stalled -> fired.incrementAndGet())
			.build();
		try {
			sender.start();
			// a control frame leaves whatever the readiness: it queues nothing
			sender.sendWindowUpdate(1, 1);
			awaitSent(observer, 1);
			TimeUnit.MILLISECONDS.sleep(500);
			assertThat(observer.isReady()).as("precondition: the stream is not ready").isFalse();
			assertThat(fired).hasValue(0);
			assertThat(sender.stalled()).isEqualTo(Duration.ZERO);
			assertThat(sender.isOpen()).isTrue();
		}
		finally {
			sender.close();
		}
	}

	@Test
	void controlFramesHandedToTheStreamAreNoProgress() throws Exception {
		FakeServerObserver observer = new FakeServerObserver();
		observer.gate.countDown();
		CountDownLatch fired = new CountDownLatch(1);
		SessionSender sender = SessionSender.builder(observer)
			.stallTimeout(TIMEOUT)
			.stallAction(stalled -> fired.countDown())
			.build();
		try {
			sender.start();
			assertThat(sender.sendData(1, new byte[1])).isTrue();
			// grants keep leaving every 20ms until the watchdog fires
			long until = System.nanoTime() + TIMEOUT.multipliedBy(5).toNanos();
			while (fired.getCount() > 0 && System.nanoTime() < until) {
				sender.sendWindowUpdate(2, 1);
				TimeUnit.MILLISECONDS.sleep(20);
			}
			assertThat(fired.getCount()).as("the watchdog fired while control frames kept leaving").isZero();
			assertThat(observer.sent).as("precondition: control frames left while it stalled").isNotEmpty();
			assertThat(observer.sent).allMatch(frame -> frame.hasWindowUpdate());
		}
		finally {
			sender.close();
		}
	}

	@Test
	void aZeroTimeoutMeasuresTheStallButNeverStops() throws Exception {
		FakeServerObserver observer = new FakeServerObserver();
		observer.gate.countDown();
		AtomicInteger fired = new AtomicInteger();
		SessionSender sender = SessionSender.builder(observer)
			.stallTimeout(Duration.ZERO)
			.stallAction(stalled -> fired.incrementAndGet())
			.build();
		try {
			sender.start();
			assertThat(sender.sendData(1, new byte[1])).isTrue();
			TimeUnit.MILLISECONDS.sleep(500);
			assertThat(sender.stalled()).as("the stall is measured").isGreaterThan(Duration.ofMillis(300));
			assertThat(fired).hasValue(0);
			assertThat(sender.isOpen()).isTrue();
			observer.becomeReady();
			awaitSent(observer, 1);
			assertThat(sender.stalled()).as("a frame handed to the stream ends the stall").isEqualTo(Duration.ZERO);
		}
		finally {
			sender.close();
		}
	}

	@Test
	void aThrowingStallActionStillTearsTheSenderDown() throws Exception {
		FakeServerObserver observer = new FakeServerObserver();
		observer.gate.countDown();
		SessionSender sender = SessionSender.builder(observer)
			.stallTimeout(Duration.ofMillis(100))
			.stallAction(stalled -> {
				throw new IllegalStateException("boom");
			})
			.build();
		AtomicInteger held = new AtomicInteger();
		try {
			sender.start();
			assertThat(sender.sendData(1, new byte[1])).isTrue();
			sender.afterQueued(1, held::incrementAndGet);
			long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
			while (sender.isOpen() && System.nanoTime() < deadline) {
				TimeUnit.MILLISECONDS.sleep(10);
			}
			assertThat(sender.isOpen()).isFalse();
			assertThat(held).hasValue(1);
		}
		finally {
			sender.close();
		}
	}

	private static void awaitSent(FakeServerObserver observer, int count) throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
		while (observer.sent.size() < count && System.nanoTime() < deadline) {
			TimeUnit.MILLISECONDS.sleep(10);
		}
		assertThat(observer.sent).hasSizeGreaterThanOrEqualTo(count);
	}

}
