package am.ik.sluice.server.proxy;

import java.util.ArrayList;
import java.util.List;
import java.util.function.LongPredicate;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * The connection window a relay grants a hop's peer: refilled at half its target, doubled
 * once the peer sent half the target within a PING's round trip, its part past RFC 9113's
 * window charged first, and back at that window when the charge does not take it.
 */
class HopWindowTest {

	private static final int FLOOR = 65535;

	private static final int KIB = 1024;

	private static final int MAX = 1024 * KIB;

	/** A peer of a far hop: owed always covers a refill. */
	private static final int OWED = MAX;

	private final List<Long> charges = new ArrayList<>();

	private final LongPredicate always = bytes -> this.charges.add(bytes);

	private final LongPredicate never = bytes -> {
		this.charges.add(bytes);
		return false;
	};

	@Test
	void theFloorIsRfc9113sWindowAndChargesNothing() {
		assertThat(HopWindow.FLOOR).isEqualTo(FLOOR);
		assertThat(HopWindow.charged(0)).isZero();
		assertThat(HopWindow.charged(FLOOR)).isZero();
		assertThat(HopWindow.charged(MAX)).isEqualTo(MAX - FLOOR);
		assertThat(new HopWindow(MAX).target()).isEqualTo(FLOOR);
		assertThatIllegalArgumentException().isThrownBy(() -> new HopWindow(FLOOR - 1));
	}

	@Test
	void aPeerIsRefilledAtHalfTheFloorForFreeWithoutASample() {
		HopWindow window = new HopWindow(MAX);
		assertThat(window.grant(FLOOR / 2 + 1, OWED, this.never)).as("above half: nothing yet")
			.isEqualTo(HopWindow.Grant.NONE);
		assertThat(window.grant(0, OWED, this.never)).as("spent to nothing: refilled, not grown")
			.isEqualTo(new HopWindow.Grant(FLOOR, false));
		assertThat(window.grant(FLOOR / 2, OWED, this.never)).isEqualTo(new HopWindow.Grant(FLOOR - FLOOR / 2, false));
		assertThat(window.target()).isEqualTo(FLOOR);
		assertThat(this.charges).as("within the floor: no charge asked").isEmpty();
	}

	@Test
	void aPingIsWantedWithTheFirstDataUntilAckedAndNotAtTheMax() {
		HopWindow window = new HopWindow(MAX);
		assertThat(window.wantsPing(100)).isTrue();
		window.pinged();
		assertThat(window.wantsPing(100)).as("one under way").isFalse();
		window.acked();
		assertThat(window.wantsPing(100)).as("acked: the next").isTrue();
		HopWindow largest = new HopWindow(FLOOR);
		assertThat(largest.wantsPing(100)).as("the target cannot grow").isFalse();
	}

	@Test
	void aSampleOfHalfTheTargetDoublesItUpToTheMaxEachGrowthCharged() {
		HopWindow window = new HopWindow(MAX);
		List<Integer> targets = new ArrayList<>();
		while (window.target() < MAX && targets.size() < 5) {
			int target = window.target();
			sample(window, target / 2);
			assertThat(window.wantsPing(0)).as("no PING while the growth is due").isFalse();
			int credit = target / 4;
			HopWindow.Grant grant = window.grant(credit, OWED, this.always);
			assertThat(grant.declined()).isFalse();
			assertThat(grant.bytes()).as("a refill to the doubled target").isEqualTo(window.target() - credit);
			targets.add(window.target());
		}
		// powers of two: the floor is one byte short of 64KiB
		assertThat(targets).containsExactly(128 * KIB, 256 * KIB, 512 * KIB, MAX);
		// each grant's charge: what it takes the credit past the floor up by (the
		// credit a quarter of the target before it)
		assertThat(this.charges).containsExactly(128L * KIB - FLOOR, 256L * KIB - FLOOR, 512L * KIB - FLOOR - 1,
				(long) MAX - 128 * KIB);
		assertThat(window.wantsPing(100)).as("at the max: no more samples").isFalse();
	}

	@Test
	void aSampleBelowHalfTheTargetKeepsIt() {
		HopWindow window = new HopWindow(MAX);
		sample(window, FLOOR / 2 - 1);
		assertThat(window.grant(0, OWED, this.always)).isEqualTo(new HopWindow.Grant(FLOOR, false));
		assertThat(window.target()).isEqualTo(FLOOR);
		window.acked();
		assertThat(window.target()).as("an ack without a PING under way counts nothing").isEqualTo(FLOOR);
	}

	@Test
	void aGrowthTheChargeDoesNotTakeKeepsTheTargetItTakes() {
		HopWindow window = grownTo(256 * KIB);
		sample(window, 128 * KIB);
		LongPredicate small = bytes -> bytes <= 256L * KIB;
		HopWindow.Grant grant = window.grant(64 * KIB, OWED, small);
		assertThat(grant).as("the current target's refill").isEqualTo(new HopWindow.Grant(192 * KIB, true));
		assertThat(window.target()).isEqualTo(256 * KIB);
		assertThat(window.wantsPing(0)).as("measured again").isTrue();
	}

	@Test
	void aRefillTheChargeDoesNotTakeFallsToTheFloor() {
		HopWindow window = grownTo(256 * KIB);
		// spent to the floor's half: the floor's refill, which costs nothing
		HopWindow.Grant grant = window.grant(FLOOR / 2, OWED, this.never);
		assertThat(grant).isEqualTo(new HopWindow.Grant(FLOOR - FLOOR / 2, true));
		assertThat(window.target()).isEqualTo(FLOOR);
		// still above the floor's half: nothing, at the floor from now on
		HopWindow other = grownTo(512 * KIB);
		assertThat(other.grant(3 * FLOOR, OWED, this.never)).isEqualTo(new HopWindow.Grant(0, true));
		assertThat(other.target()).isEqualTo(FLOOR);
		assertThat(other.grant(FLOOR, OWED, this.never)).as("no refill above the floor's half")
			.isEqualTo(HopWindow.Grant.NONE);
	}

	@Test
	void nothingIsGrantedPastWhatIsOwed() {
		HopWindow window = new HopWindow(MAX);
		sample(window, FLOOR);
		assertThat(window.grant(0, 0, this.always)).isEqualTo(HopWindow.Grant.NONE);
		assertThat(window.target()).as("no growth without a grant").isEqualTo(FLOOR);
		assertThat(window.grant(0, 1000, this.always)).isEqualTo(new HopWindow.Grant(1000, false));
		assertThat(window.target()).as("the growth taken with the grant").isEqualTo(128 * KIB);
	}

	/** One PING's round trip in which the peer sent {@code bytes}. */
	private static void sample(HopWindow window, int bytes) {
		assertThat(window.wantsPing(0)).isTrue();
		window.pinged();
		assertThat(window.wantsPing(bytes)).isFalse();
		window.acked();
	}

	/** A window grown to {@code target} with every charge taken. */
	private static HopWindow grownTo(int target) {
		HopWindow window = new HopWindow(MAX);
		for (int doublings = 0; window.target() < target; doublings++) {
			assertThat(doublings).as("doublings to %d", target).isLessThan(5);
			sample(window, window.target());
			window.grant(0, OWED, bytes -> true);
		}
		assertThat(window.target()).isEqualTo(target);
		return window;
	}

}
