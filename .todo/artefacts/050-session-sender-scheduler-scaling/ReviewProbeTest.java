package am.ik.sluice.tunnel;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import am.ik.sluice.v1.proto.Frame;
import io.grpc.stub.ServerCallStreamObserver;
import org.junit.jupiter.api.Test;

class ReviewProbeTest {

	static final class ReadyObserver extends ServerCallStreamObserver<Frame> {

		final AtomicLong sent = new AtomicLong();

		volatile long target = Long.MAX_VALUE;

		final CountDownLatch reached = new CountDownLatch(1);

		@Override
		public boolean isReady() {
			return true;
		}

		@Override
		public void setOnReadyHandler(Runnable onReadyHandler) {
		}

		@Override
		public void onNext(Frame frame) {
			if (this.sent.incrementAndGet() == this.target) {
				this.reached.countDown();
			}
		}

		@Override
		public void onError(Throwable t) {
		}

		@Override
		public void onCompleted() {
		}

		@Override
		public boolean isCancelled() {
			return false;
		}

		@Override
		public void setOnCancelHandler(Runnable onCancelHandler) {
		}

		@Override
		public void setCompression(String compression) {
		}

		@Override
		public void setMessageCompression(boolean messageCompression) {
		}

		@Override
		public void request(int count) {
		}

		@Override
		public void disableAutoInboundFlowControl() {
		}

	}

	@SuppressWarnings("unchecked")
	static Map<Long, ?> queues(SessionSender sender) throws Exception {
		Field field = SessionSender.class.getDeclaredField("queues");
		field.setAccessible(true);
		ReentrantLock lock = lockOf(sender);
		lock.lock();
		try {
			return Map.copyOf((Map<Long, ?>) field.get(sender));
		}
		finally {
			lock.unlock();
		}
	}

	static ReentrantLock lockOf(SessionSender sender) throws Exception {
		Field field = SessionSender.class.getDeclaredField("lock");
		field.setAccessible(true);
		return (ReentrantLock) field.get(sender);
	}

	static void awaitSent(ReadyObserver observer, long count) throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
		while (observer.sent.get() < count && System.nanoTime() < deadline) {
			Thread.sleep(1);
		}
		if (observer.sent.get() < count) {
			throw new AssertionError("sent " + observer.sent.get() + " < " + count);
		}
	}

	@Test
	void lateDataAfterRemovalLeavesQueueForever() throws Exception {
		ReadyObserver observer = new ReadyObserver();
		SessionSender sender = new SessionSender(observer);
		sender.start();
		try {
			sender.sendData(1, new byte[1]);
			sender.sendClose(1);
			awaitSent(observer, 2);
			sender.sendData(7, new byte[1]); // let the worker scan once more
			awaitSent(observer, 3);
			Thread.sleep(100);
			System.out.println("PROBE after CLOSE: queues=" + queues(sender).keySet());
			sender.sendData(1, new byte[1]); // late DATA (Sink.write race)
			awaitSent(observer, 4);
			Thread.sleep(200);
			System.out.println("PROBE after late DATA: queues=" + queues(sender).keySet());
		}
		finally {
			sender.close();
		}
	}

	static long runBulk(String name, boolean newImpl, int producers, int framesPerProducer, int idle)
			throws Exception {
		ReadyObserver observer = new ReadyObserver();
		FrameWriter writer;
		AutoCloseable closer;
		if (newImpl) {
			SessionSender s = new SessionSender(observer);
			s.start();
			writer = s;
			closer = s;
		}
		else {
			OldSessionSender s = new OldSessionSender(observer);
			s.start();
			writer = s;
			closer = s;
		}
		try {
			// idle connections: one DATA each, never closed -> their (empty) queues stay
			for (int i = 0; i < idle; i++) {
				writer.sendData(1_000_000 + i, new byte[1]);
			}
			awaitSent(observer, idle);
			long total = idle + (long) producers * framesPerProducer;
			observer.target = total;
			byte[] payload = new byte[16];
			long start = System.nanoTime();
			List<Thread> threads = new ArrayList<>();
			for (int p = 0; p < producers; p++) {
				long id = p + 1;
				threads.add(Thread.ofVirtual().start(() -> {
					for (int i = 0; i < framesPerProducer; i++) {
						writer.sendData(id, payload);
					}
				}));
			}
			if (!observer.reached.await(120, TimeUnit.SECONDS)) {
				throw new AssertionError("timeout " + observer.sent.get());
			}
			long elapsed = System.nanoTime() - start;
			for (Thread t : threads) {
				t.join();
			}
			System.out.printf("PROBE %s %s producers=%d frames/producer=%d idle=%d -> %d ms (%.0f frames/s)%n", name,
					newImpl ? "new" : "old", producers, framesPerProducer, idle,
					TimeUnit.NANOSECONDS.toMillis(elapsed), (producers * (double) framesPerProducer) / (elapsed / 1e9));
			return elapsed;
		}
		finally {
			closer.close();
		}
	}

	@Test
	void benchmark() throws Exception {
		// warm up
		runBulk("warmup", true, 10, 20_000, 0);
		runBulk("warmup", false, 10, 20_000, 0);
		for (int producers : new int[] { 1, 10, 100, 1000 }) {
			int frames = 400_000 / producers;
			runBulk("fanout", false, producers, frames, 0);
			runBulk("fanout", true, producers, frames, 0);
		}
		for (int idle : new int[] { 0, 1_000, 10_000 }) {
			runBulk("idle", false, 1, 200_000, idle);
			runBulk("idle", true, 1, 200_000, idle);
		}
	}

}
