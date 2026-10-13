package am.ik.sluice.server.proxy.hop;

import java.time.Duration;
import java.util.List;
import java.util.function.Predicate;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.ResourceLeakDetector;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Fails a test class when netty reports a leaked ByteBuf: checks before the class that
 * the paranoid detector is the recording one (the preconditions of the check), and after
 * it collects garbage until the detector has reported what was collected. A leak
 * collected only after its class is reported by a later class: the suite fails either
 * way. Leaks touched with {@link #DELIBERATE} are {@link LeakCheckTest}'s own and do not
 * count.
 */
final class LeakCheck implements BeforeAllCallback, AfterAllCallback {

	/** The touch hint of a buffer leaked on purpose. */
	static final String DELIBERATE = "deliberate leak of LeakCheckTest";

	@Override
	public void beforeAll(ExtensionContext context) {
		assertThat(ResourceLeakDetector.getLevel()).isEqualTo(ResourceLeakDetector.Level.PARANOID);
		// a tracked allocation creates the detector if no buffer did yet
		ByteBufAllocator.DEFAULT.buffer(1).release();
		assertThat(RecordingLeakDetector.installed()).as("the recording leak detector is installed").isTrue();
	}

	@Override
	public void afterAll(ExtensionContext context) throws InterruptedException {
		collect(Duration.ofSeconds(1), leaks -> false);
		assertThat(RecordingLeakDetector.leaks()).filteredOn(leak -> !leak.contains(DELIBERATE)).isEmpty();
	}

	/**
	 * Collects garbage and lets the detector report until the condition holds on the
	 * reported leaks, or the time is up.
	 */
	static void collect(Duration time, Predicate<List<String>> condition) throws InterruptedException {
		long deadline = System.nanoTime() + time.toNanos();
		while (!condition.test(RecordingLeakDetector.leaks()) && System.nanoTime() < deadline) {
			System.gc();
			Thread.sleep(50);
			// the detector reports what was collected as it tracks new buffers
			for (int i = 0; i < 100; i++) {
				ByteBufAllocator.DEFAULT.buffer(1).release();
				Unpooled.buffer(1).release();
			}
		}
	}

}
