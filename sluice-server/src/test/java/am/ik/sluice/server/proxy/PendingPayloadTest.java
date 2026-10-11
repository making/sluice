package am.ik.sluice.server.proxy;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PendingPayloadTest {

	@Test
	void capacityFollowsThePayloadInGranules() {
		PendingPayload pending = new PendingPayload();
		pending.append(bytes(1, 0), 0, 1);
		assertThat(pending.capacity()).isEqualTo(PendingPayload.GRANULE);
		pending.append(bytes(PendingPayload.GRANULE, 1), 0, PendingPayload.GRANULE);
		assertThat(pending.size()).isEqualTo(PendingPayload.GRANULE + 1);
		assertThat(pending.capacity()).isEqualTo(2 * PendingPayload.GRANULE);
	}

	@Test
	void takeReturnsTheFrontAndShrinksTheArrayToTheRest() {
		PendingPayload pending = new PendingPayload();
		byte[] payload = bytes(5000, 0);
		pending.append(payload, 0, payload.length);
		assertThat(pending.capacity()).isEqualTo(5 * PendingPayload.GRANULE);
		byte[] front = pending.take(3000);
		assertThat(front).isEqualTo(java.util.Arrays.copyOf(payload, 3000));
		assertThat(pending.size()).isEqualTo(2000);
		assertThat(pending.capacity()).isEqualTo(2 * PendingPayload.GRANULE);
		assertThat(pending.take(2000)).isEqualTo(java.util.Arrays.copyOfRange(payload, 3000, 5000));
		assertThat(pending.capacity()).as("a drained buffer keeps no array").isZero();
	}

	@Test
	void appendAfterAPartialTakeKeepsTheOrder() {
		PendingPayload pending = new PendingPayload();
		byte[] payload = bytes(3000, 0);
		pending.append(payload, 0, 1500);
		pending.take(1000);
		pending.append(payload, 1500, 1500);
		assertThat(pending.take(2000)).isEqualTo(java.util.Arrays.copyOfRange(payload, 1000, 3000));
	}

	@Test
	void clearDropsTheArray() {
		PendingPayload pending = new PendingPayload();
		pending.append(bytes(100, 0), 0, 100);
		pending.clear();
		assertThat(pending.size()).isZero();
		assertThat(pending.capacity()).isZero();
	}

	private static byte[] bytes(int length, int seed) {
		byte[] bytes = new byte[length];
		for (int i = 0; i < length; i++) {
			bytes[i] = (byte) (seed + i);
		}
		return bytes;
	}

}
