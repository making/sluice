package am.ik.sluice.server.proxy;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class H2DemuxBudgetTest {

	@Test
	void tryChargeTakesOnlyWhatFits() {
		H2DemuxBudget budget = new H2DemuxBudget(100);
		assertThat(budget.tryCharge(60)).isTrue();
		assertThat(budget.tryCharge(41)).isFalse();
		assertThat(budget.used()).isEqualTo(60);
		assertThat(budget.tryCharge(40)).isTrue();
		assertThat(budget.hasRoom()).isFalse();
	}

	@Test
	void chargePassesTheLimit() {
		H2DemuxBudget budget = new H2DemuxBudget(100);
		budget.charge(150);
		assertThat(budget.used()).isEqualTo(150);
		assertThat(budget.tryCharge(1)).isFalse();
		budget.release(150);
		assertThat(budget.used()).isZero();
	}

	@Test
	void aWaiterRunsOnceTheBudgetHasRoomAgain() throws Exception {
		H2DemuxBudget budget = new H2DemuxBudget(100);
		budget.charge(120);
		AtomicInteger runs = new AtomicInteger();
		Runnable waiter = runs::incrementAndGet;
		budget.awaitRoom(waiter);
		budget.awaitRoom(waiter);
		budget.release(10);
		Thread.sleep(100);
		assertThat(runs).as("still at the limit").hasValue(0);
		assertThat(budget.waiting()).isEqualTo(1);
		budget.release(20);
		Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(runs).hasValue(1));
		budget.release(10);
		Thread.sleep(100);
		assertThat(runs).as("a waiter runs once per registration").hasValue(1);
		assertThat(budget.waiting()).isZero();
	}

	@Test
	void aWaiterRegisteredWithRoomRunsAtOnce() {
		H2DemuxBudget budget = new H2DemuxBudget(100);
		AtomicInteger runs = new AtomicInteger();
		budget.awaitRoom(runs::incrementAndGet);
		Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(runs).hasValue(1));
	}

	@Test
	void aCancelledWaiterNeverRuns() throws Exception {
		H2DemuxBudget budget = new H2DemuxBudget(100);
		budget.charge(100);
		AtomicInteger runs = new AtomicInteger();
		Runnable waiter = runs::incrementAndGet;
		budget.awaitRoom(waiter);
		budget.cancel(waiter);
		budget.release(100);
		Thread.sleep(100);
		assertThat(runs).hasValue(0);
	}

}
