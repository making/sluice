package am.ik.sluice.spike.h2demux;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.buffer.AdaptiveByteBufAllocator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The mechanism behind the payload corruption the spike hit while it forwarded DATA as
 * slices of the reading hop's cumulation buffer: a thread reading a direct buffer of
 * netty's default (adaptive) allocator with {@code getBytes(int, OutputStream, int)}
 * resets the buffer's cached internal NIO buffer ({@code internalNioBuffer().clear()}),
 * which the event-loop side uses for its own in-place writes
 * ({@code ByteToMessageDecoder} appending the next read to the cumulation) -- so the
 * appended bytes land at the wrong offset. Reads of disjoint regions are enough; netty's
 * buffers assume one thread at a time.
 * <p>
 * A race probe, not a regression test: it shows the misplacement in most JVM runs, not in
 * every one, so it is excluded from the default run ({@code -Dgroups=probe} runs it). The
 * pooled and unpooled allocators showed none in this probe, which does not make them safe
 * for cross-thread use.
 */
@Tag("probe")
class ByteBufCrossThreadProbeTest {

	@Test
	void readingASliceOnAnotherThreadMisplacesAnAppendOfTheOwningThread() throws Exception {
		ByteBufAllocator allocator = new AdaptiveByteBufAllocator();
		int chunk = 16 * 1024;
		int appends = 2000;
		AtomicInteger misplaced = new AtomicInteger();
		AtomicBoolean done = new AtomicBoolean();
		for (int round = 0; round < 20 && misplaced.get() == 0; round++) {
			// room for every append: no reallocation, only in-place writes
			ByteBuf cumulation = allocator.directBuffer(chunk + appends * 64);
			byte[] head = new byte[chunk];
			Arrays.fill(head, (byte) 'h');
			cumulation.writeBytes(head);
			ByteBuf slice = cumulation.retainedSlice(0, chunk);
			done.set(false);
			Thread reader = Thread.ofPlatform().start(() -> {
				while (!done.get()) {
					try {
						slice.getBytes(0, new ByteArrayOutputStream(chunk), chunk);
					}
					catch (IOException | RuntimeException e) {
						// a misplaced position can also throw
					}
				}
			});
			try {
				// the owning thread appends what it reads, as the decoder does; it never
				// touches the slice's region
				byte[] input = new byte[64];
				for (int i = 0; i < appends; i++) {
					Arrays.fill(input, (byte) (i % 100 + 1));
					int at = cumulation.writerIndex();
					cumulation.writeBytes(input);
					byte[] back = new byte[input.length];
					cumulation.getBytes(at, back);
					if (!Arrays.equals(back, input)) {
						misplaced.incrementAndGet();
					}
				}
			}
			finally {
				done.set(true);
				reader.join();
				slice.release();
				cumulation.release();
			}
		}
		System.out.println("PROBE adaptive misplaced=" + misplaced.get());
		assertThat(misplaced.get()).isPositive();
	}

}
