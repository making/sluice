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
	void theConnectionBytesAddUpTheWindowTheSendQueueAndTheRelayBuffers() {
		assertThat(ConnectionLimit.CONNECTION_BYTES).isEqualTo(MIB + MIB + 2 * 64 * 1024);
	}

	@Test
	void deriveFitsTheWorstCaseInHalfTheHeap() {
		// 200MiB / 2.125MiB
		assertThat(ConnectionLimit.derive(400 * MIB)).isEqualTo(94);
		assertThat(ConnectionLimit.derive(4096 * MIB)).isEqualTo(963);
		assertThat(ConnectionLimit.derive(MIB)).isEqualTo(1);
		assertThat(ConnectionLimit.derive(Long.MAX_VALUE)).isEqualTo(Integer.MAX_VALUE);
	}

	@Test
	void deriveFitsConnectionsOfTheGivenSizeInTheGivenBytes() {
		assertThat(ConnectionLimit.derive(10 * MIB, 3 * MIB)).isEqualTo(3);
		assertThat(ConnectionLimit.derive(0, MIB)).isEqualTo(1);
		assertThat(ConnectionLimit.derive(-MIB, MIB)).isEqualTo(1);
	}

	@Test
	void resolvePrefersTheConfiguredLimit() {
		assertThat(ConnectionLimit.resolve(7)).isEqualTo(7);
		assertThat(ConnectionLimit.resolve(0)).isEqualTo(ConnectionLimit.derive(Runtime.getRuntime().maxMemory()));
	}

	@Test
	void rejectsANonPositiveLimit() {
		assertThatIllegalArgumentException().isThrownBy(() -> new ConnectionLimit(0));
	}

}
