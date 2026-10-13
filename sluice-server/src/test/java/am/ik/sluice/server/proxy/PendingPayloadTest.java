package am.ik.sluice.server.proxy;

import java.util.Arrays;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PendingPayloadTest {

	@Test
	void capacityFollowsThePayloadInGranules() {
		PendingPayload pending = new PendingPayload();
		pending.append(Unpooled.wrappedBuffer(bytes(1, 0)));
		assertThat(pending.capacity()).isEqualTo(PendingPayload.GRANULE);
		pending.append(Unpooled.wrappedBuffer(bytes(PendingPayload.GRANULE, 1)));
		assertThat(pending.size()).isEqualTo(PendingPayload.GRANULE + 1);
		assertThat(pending.capacity()).isEqualTo(2 * PendingPayload.GRANULE);
	}

	@Test
	void appendCopiesTheReadableBytesAndLeavesTheSourceIndexes() {
		PendingPayload pending = new PendingPayload();
		byte[] payload = bytes(3000, 0);
		ByteBuf first = Unpooled.wrappedBuffer(payload, 0, 1500);
		ByteBuf second = Unpooled.wrappedBuffer(payload);
		second.readerIndex(1500);
		pending.append(first);
		pending.append(second);
		assertThat(first.readerIndex()).isZero();
		assertThat(second.readerIndex()).isEqualTo(1500);
		assertThat(pending.size()).isEqualTo(3000);
		assertThat(Arrays.copyOf(pending.takeAll(), 3000)).isEqualTo(payload);
	}

	@Test
	void takeAllHandsTheWholeArrayOverAndLeavesItEmpty() {
		PendingPayload pending = new PendingPayload();
		byte[] payload = bytes(5000, 0);
		pending.append(Unpooled.wrappedBuffer(payload));
		byte[] taken = pending.takeAll();
		assertThat(taken).hasSize(5 * PendingPayload.GRANULE);
		assertThat(Arrays.copyOf(taken, payload.length)).isEqualTo(payload);
		assertThat(pending.size()).isZero();
		assertThat(pending.capacity()).as("a drained buffer keeps no array").isZero();
	}

	@Test
	void clearDropsTheArray() {
		PendingPayload pending = new PendingPayload();
		pending.append(Unpooled.wrappedBuffer(bytes(100, 0)));
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
