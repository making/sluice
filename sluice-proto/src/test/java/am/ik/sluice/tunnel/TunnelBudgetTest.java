package am.ik.sluice.tunnel;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class TunnelBudgetTest {

	private static final long MIB = 1024 * 1024;

	@Test
	void anAccountGrowsByWhatIsLeftAndReleasesOnShrinkAndClose() {
		TunnelBudget budget = new TunnelBudget(1000);
		TunnelBudget.Account first = budget.account();
		TunnelBudget.Account second = budget.account();
		// alone, an account takes up to half: the other half is a newcomer's
		assertThat(first.resize(800)).isEqualTo(500);
		assertThat(budget.used()).isEqualTo(500);
		assertThat(budget.holders()).isEqualTo(1);
		// the second shares with the first and one more: a third
		assertThat(second.resize(800)).isEqualTo(333);
		assertThat(budget.used()).isEqualTo(833);
		assertThat(budget.holders()).isEqualTo(2);
		// the first is over its share now: it keeps what it holds, but grows no further
		assertThat(first.share()).isEqualTo(333);
		assertThat(first.resize(900)).isEqualTo(500);
		assertThat(first.resize(100)).isEqualTo(100);
		assertThat(budget.used()).isEqualTo(433);
		second.close();
		second.close();
		assertThat(second.resize(10)).as("a closed account takes nothing").isZero();
		assertThat(budget.used()).isEqualTo(100);
		assertThat(budget.holders()).isEqualTo(1);
		assertThat(first.resize(0)).isZero();
		assertThat(budget.holders()).isZero();
		assertThat(budget.used()).isZero();
	}

	@Test
	void theBudgetIsNeverPassed() {
		TunnelBudget budget = new TunnelBudget(1000);
		TunnelBudget.Account[] accounts = new TunnelBudget.Account[10];
		for (int i = 0; i < accounts.length; i++) {
			accounts[i] = budget.account();
			accounts[i].resize(1000);
			assertThat(budget.used()).isLessThanOrEqualTo(1000);
		}
		long charged = 0;
		for (TunnelBudget.Account account : accounts) {
			charged += account.charged();
		}
		assertThat(charged).isEqualTo(budget.used());
	}

	@Test
	void tryGrowTakesAllOrNothing() {
		TunnelBudget budget = new TunnelBudget(1000);
		TunnelBudget.Account first = budget.account();
		TunnelBudget.Account second = budget.account();
		assertThat(first.tryGrow(400)).isTrue();
		assertThat(second.tryGrow(400)).as("past its share of a third").isFalse();
		assertThat(budget.used()).isEqualTo(400);
		assertThat(second.tryGrow(300)).isTrue();
		assertThat(second.tryGrow(200)).as("held already").isTrue();
		assertThat(budget.used()).isEqualTo(700);
		TunnelBudget.Account third = budget.account();
		// its share is a quarter, and 300 are left
		assertThat(third.tryGrow(250)).isTrue();
		assertThat(budget.account().tryGrow(100)).as("50 are left").isFalse();
		assertThat(budget.used()).isEqualTo(950);
		first.close();
		assertThat(first.tryGrow(1)).isFalse();
		assertThat(budget.used()).isEqualTo(550);
	}

	@Test
	void anUnsetBudgetIsAnEighthOfHalfTheHeap() {
		assertThat(TunnelBudget.derive(400 * MIB)).isEqualTo(25 * MIB);
		assertThat(TunnelBudget.resolve(0, 400 * MIB)).isEqualTo(25 * MIB);
		assertThat(TunnelBudget.resolve(MIB, 400 * MIB)).isEqualTo(MIB);
		assertThat(TunnelBudget.derive(-1)).isZero();
		assertThatIllegalArgumentException().isThrownBy(() -> new TunnelBudget(-1));
	}

	@Test
	void theTransportWindowIsTwiceTheBudgetWithinTheHttp2Bounds() {
		assertThat(new TunnelBudget(25 * MIB).transportWindow()).isEqualTo(50 * MIB);
		assertThat(new TunnelBudget(MIB / 2).transportWindow()).as("gRPC's default at least").isEqualTo(MIB);
		assertThat(new TunnelBudget(0).transportWindow()).isEqualTo(MIB);
		assertThat(new TunnelBudget(Integer.MAX_VALUE / 2).transportWindow()).isEqualTo(Integer.MAX_VALUE - 1);
		assertThat(new TunnelBudget(Integer.MAX_VALUE / 2 + 1).transportWindow()).as("the largest HTTP/2 window")
			.isEqualTo(Integer.MAX_VALUE);
		assertThat(TunnelBudget.unlimited().transportWindow()).isEqualTo(Integer.MAX_VALUE);
	}

}
