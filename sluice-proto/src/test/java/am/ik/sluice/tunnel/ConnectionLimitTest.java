package am.ik.sluice.tunnel;

import java.util.Objects;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class ConnectionLimitTest {

	private static final long MIB = 1024 * 1024;

	@Test
	void admitsUpToTheLimit() {
		ConnectionLimit limit = new ConnectionLimit(2);
		assertThat(limit.tryAcquire()).isNotNull();
		assertThat(limit.tryAcquire()).isNotNull();
		assertThat(limit.tryAcquire()).isNull();
		assertThat(limit.open()).isEqualTo(2);
	}

	@Test
	void aPermitFreesItsSlotOnce() {
		ConnectionLimit limit = new ConnectionLimit(2);
		ConnectionLimit.Permit first = Objects.requireNonNull(limit.tryAcquire());
		assertThat(limit.tryAcquire()).isNotNull();
		first.release();
		first.release();
		assertThat(limit.open()).isEqualTo(1);
		assertThat(limit.tryAcquire()).isNotNull();
		assertThat(limit.tryAcquire()).isNull();
	}

	@Test
	void theConnectionBytesAddUpTheReceiveAllowanceTheSendAllowanceAndTheRelayBuffers() {
		// the window, two 16KiB segments of slack, 80 segments of 64 bytes overhead
		assertThat(VirtualConnection.MAX_RECEIVE_BUFFER).isEqualTo(MIB + 2 * 16 * 1024 + 80 * 64);
		// the initial window and the slack of a window grown to 1MiB
		assertThat(VirtualConnection.RECEIVE_ALLOWANCE).isEqualTo(64 * 1024 + 2 * 16 * 1024 + 80 * 64);
		assertThat(SessionSender.SEND_ALLOWANCE).isEqualTo(64 * 1024);
		assertThat(ConnectionLimit.CONNECTION_BYTES)
			.isEqualTo(VirtualConnection.RECEIVE_ALLOWANCE + 64 * 1024 + 2 * 64 * 1024)
			.isEqualTo(293 * 1024);
	}

	@Test
	void deriveFitsTheAllowancesInHalfTheHeapLessTheBudget() {
		// (200MiB - 25MiB) / 293KiB
		assertThat(ConnectionLimit.deriveFromHeap(400 * MIB, TunnelBudget.derive(400 * MIB))).isEqualTo(611);
		assertThat(ConnectionLimit.deriveFromHeap(4096 * MIB, TunnelBudget.derive(4096 * MIB))).isEqualTo(6262);
		assertThat(ConnectionLimit.deriveFromHeap(MIB, 0)).isEqualTo(1);
		assertThat(ConnectionLimit.deriveFromHeap(400 * MIB, 400 * MIB)).isEqualTo(1);
		assertThat(ConnectionLimit.deriveFromHeap(Long.MAX_VALUE, 0)).isEqualTo(Integer.MAX_VALUE);
	}

	@Test
	void deriveFitsConnectionsOfTheGivenSizeInTheGivenBytes() {
		assertThat(ConnectionLimit.derive(10 * MIB, 3 * MIB)).isEqualTo(3);
		assertThat(ConnectionLimit.derive(0, MIB)).isEqualTo(1);
		assertThat(ConnectionLimit.derive(-MIB, MIB)).isEqualTo(1);
	}

	@Test
	void resolvePrefersTheConfiguredLimit() {
		assertThat(ConnectionLimit.resolve(7, MIB)).isEqualTo(7);
		assertThat(ConnectionLimit.resolve(0, MIB))
			.isEqualTo(ConnectionLimit.deriveFromHeap(Runtime.getRuntime().maxMemory(), MIB));
	}

	@Test
	void rejectsANonPositiveLimit() {
		assertThatIllegalArgumentException().isThrownBy(() -> new ConnectionLimit(0));
	}

}
