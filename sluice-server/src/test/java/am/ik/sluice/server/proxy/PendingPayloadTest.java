package am.ik.sluice.server.proxy;

import java.lang.management.ManagementFactory;
import java.util.Arrays;

import com.sun.management.ThreadMXBean;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PendingPayloadTest {

	private static final ThreadMXBean THREADS = (ThreadMXBean) ManagementFactory.getThreadMXBean();

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
		assertThat(ByteBufUtil.getBytes(pending.takeAll())).isEqualTo(payload);
	}

	@Test
	void takeAllHandsTheBytesOverUncopiedAndLeavesItEmpty() {
		PendingPayload pending = new PendingPayload();
		byte[] payload = bytes(5000, 0);
		pending.append(Unpooled.wrappedBuffer(payload));
		assertThat(pending.capacity()).isEqualTo(5 * PendingPayload.GRANULE);
		long before = allocated();
		ByteBuf taken = pending.takeAll();
		assertThat(allocated() - before).as("no copy of the payload").isLessThan(payload.length);
		assertThat(ByteBufUtil.getBytes(taken)).isEqualTo(payload);
		assertThat(pending.size()).isZero();
		assertThat(pending.capacity()).as("a drained buffer keeps no array").isZero();
		assertThat(pending.takeAll().readableBytes()).as("taken again: nothing").isZero();
	}

	@Test
	void clearDropsTheArrays() {
		PendingPayload pending = new PendingPayload();
		pending.append(Unpooled.wrappedBuffer(bytes(100, 0)));
		pending.append(Unpooled.wrappedBuffer(bytes(3000, 0)));
		pending.clear();
		assertThat(pending.size()).isZero();
		assertThat(pending.capacity()).isZero();
	}

	/**
	 * A backlog of a large stream window's worth of frames: each byte appended is copied
	 * once and the bytes allocated are the capacity accounted (an array grown by copying
	 * on every append would allocate the sum of all its sizes, quadratic in the frames),
	 * while the capacity stays within a granule of the payload after every append, and
	 * the bytes come out whole and in order.
	 */
	@Test
	void appendingManyFramesAllocatesTheCapacityOnceAndKeepsItWithinAGranule() {
		assertThat(THREADS.isThreadAllocatedMemorySupported() && THREADS.isThreadAllocatedMemoryEnabled())
			.as("the allocation counter this test reads")
			.isTrue();
		// a frame size off the granule, so frames straddle the arrays
		int frame = 16 * 1024 + 7;
		int frames = 64;
		byte[] payload = bytes(frame * frames, 3);
		ByteBuf[] sources = new ByteBuf[frames];
		for (int i = 0; i < frames; i++) {
			sources[i] = Unpooled.wrappedBuffer(payload, i * frame, frame);
		}
		PendingPayload pending = new PendingPayload();
		int[] slack = new int[frames];
		long before = allocated();
		for (int i = 0; i < frames; i++) {
			pending.append(sources[i]);
			slack[i] = pending.capacity() - pending.size();
		}
		long allocated = allocated() - before;
		assertThat(pending.size()).isEqualTo(payload.length);
		assertThat(Arrays.stream(slack).min().orElseThrow()).as("capacity past the payload, least").isNotNegative();
		assertThat(Arrays.stream(slack).max().orElseThrow()).as("capacity past the payload, most")
			.isLessThan(PendingPayload.GRANULE);
		// the arrays' headers and the list holding them: well under a granule per frame
		assertThat(allocated).as("bytes allocated by %d appends", frames)
			.isBetween((long) pending.capacity(), pending.capacity() + (long) frames * PendingPayload.GRANULE);
		assertThat(ByteBufUtil.getBytes(pending.takeAll())).isEqualTo(payload);
	}

	private static long allocated() {
		return THREADS.getCurrentThreadAllocatedBytes();
	}

	private static byte[] bytes(int length, int seed) {
		byte[] bytes = new byte[length];
		for (int i = 0; i < length; i++) {
			bytes[i] = (byte) (seed + i);
		}
		return bytes;
	}

}
