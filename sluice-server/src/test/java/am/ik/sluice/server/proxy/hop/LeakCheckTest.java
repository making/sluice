package am.ik.sluice.server.proxy.hop;

import java.time.Duration;

import io.netty.buffer.ByteBufAllocator;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The leak check of this package's tests works: a ByteBuf dropped without release is
 * reported, with its touch hint.
 */
@Tag("netty-hop")
@ExtendWith(LeakCheck.class)
class LeakCheckTest {

	@Test
	void aBufferDroppedWithoutReleaseIsReported() throws InterruptedException {
		leak();

		LeakCheck.collect(Duration.ofSeconds(20),
				leaks -> leaks.stream().anyMatch(leak -> leak.contains(LeakCheck.DELIBERATE)));

		assertThat(RecordingLeakDetector.leaks()).anyMatch(leak -> leak.contains(LeakCheck.DELIBERATE));
	}

	private static void leak() {
		ByteBufAllocator.DEFAULT.buffer(16).writeInt(42).touch(LeakCheck.DELIBERATE);
	}

}
