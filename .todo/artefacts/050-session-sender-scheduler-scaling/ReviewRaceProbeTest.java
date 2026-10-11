package am.ik.sluice.tunnel;

import java.io.IOException;
import java.util.Map;

import org.junit.jupiter.api.Test;

class ReviewRaceProbeTest {

	@Test
	void writeRacingCloseLeavesQueues() throws Exception {
		ReviewProbeTest.ReadyObserver observer = new ReviewProbeTest.ReadyObserver();
		SessionSender sender = new SessionSender(observer);
		sender.start();
		try {
			int n = 20_000;
			for (int i = 1; i <= n; i++) {
				VirtualConnection vc = new VirtualConnection(i, sender);
				Thread writer = Thread.ofPlatform().start(() -> {
					try {
						while (true) {
							vc.sink().write(new byte[] { 1 });
						}
					}
					catch (IOException e) {
						// closed
					}
				});
				Thread.onSpinWait();
				vc.close();
				writer.join();
			}
			Thread.sleep(500);
			sender.sendData(Long.MAX_VALUE - 1, new byte[1]);
			Thread.sleep(500);
			Map<Long, ?> queues = ReviewProbeTest.queues(sender);
			System.out.println("PROBE connections=" + n + " leaked queues=" + (queues.size() - 1));
		}
		finally {
			sender.close();
		}
	}

}
