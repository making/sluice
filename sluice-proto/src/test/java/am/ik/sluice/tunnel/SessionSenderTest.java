package am.ik.sluice.tunnel;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import am.ik.sluice.v1.proto.Frame;
import io.grpc.stub.ServerCallStreamObserver;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * The sender must honor the peer's flow control on both ends of the tunnel (the server
 * observer is a {@link ServerCallStreamObserver}, the client's a
 * {@link io.grpc.stub.ClientCallStreamObserver}), must schedule fairly across connections
 * -- control frames first, then one frame per turn per connection -- and must fail fast
 * once its worker is gone.
 */
class SessionSenderTest {

	/** Fake server-side observer whose readiness is controlled by the test. */
	private static final class FakeServerObserver extends ServerCallStreamObserver<Frame> {

		private final AtomicBoolean ready = new AtomicBoolean();

		private final List<Frame> sent = new CopyOnWriteArrayList<>();

		private final AtomicReference<Runnable> onReadyHandler = new AtomicReference<>();

		private final AtomicBoolean die = new AtomicBoolean();

		private final CountDownLatch enteredOnNext = new CountDownLatch(1);

		private final CountDownLatch gate = new CountDownLatch(1);

		private final CountDownLatch firstSent = new CountDownLatch(1);

		@Override
		public boolean isReady() {
			return this.ready.get();
		}

		@Override
		public void setOnReadyHandler(Runnable onReadyHandler) {
			this.onReadyHandler.set(onReadyHandler);
		}

		/** Flips to ready and fires the handler registered by the sender. */
		void becomeReady() {
			this.ready.set(true);
			Runnable onReadyHandler = this.onReadyHandler.get();
			if (onReadyHandler != null) {
				onReadyHandler.run();
			}
		}

		@Override
		public void onNext(Frame frame) {
			this.enteredOnNext.countDown();
			try {
				this.gate.await();
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			if (this.die.get()) {
				throw new RuntimeException("stream died");
			}
			this.sent.add(frame);
			this.firstSent.countDown();
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

	/** Waits until the worker has written {@code count} frames. */
	private static void awaitSentFrames(FakeServerObserver observer, int count) throws InterruptedException {
		long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(5);
		while (observer.sent.size() < count && System.currentTimeMillis() < deadline) {
			TimeUnit.MILLISECONDS.sleep(10);
		}
		assertThat(observer.sent).hasSizeGreaterThanOrEqualTo(count);
	}

	@Test
	void serverObserverOnReadyHandlerIsRegisteredAndHonored() throws Exception {
		FakeServerObserver observer = new FakeServerObserver();
		SessionSender sender = new SessionSender(observer);
		try {
			sender.start();
			// the single onNext of this scenario must not park on the gate
			observer.gate.countDown();
			// the handler must be registered for the ready signal to ever arrive
			assertThat(observer.onReadyHandler.get()).as("onReady handler registered").isNotNull();
			sender.sendData(1, new byte[10]);
			// not ready yet: nothing written
			Thread.sleep(200);
			assertThat(observer.sent).as("no write before ready").isEmpty();
			observer.becomeReady();
			assertThat(observer.firstSent.await(1, TimeUnit.SECONDS)).as("written once ready").isTrue();
		}
		finally {
			sender.close();
		}
	}

	@Test
	void sendFailsFastWhenWorkerDiedWithFullQueue() throws Exception {
		FakeServerObserver observer = new FakeServerObserver();
		observer.ready.set(true);
		SessionSender sender = new SessionSender(observer);
		try {
			sender.start();
			// hand the worker one frame: it takes it and parks inside onNext
			assertThat(sender.sendData(1, new byte[1])).isTrue();
			assertThat(observer.enteredOnNext.await(5, TimeUnit.SECONDS)).isTrue();
			// fill connection 1's queue to its cap
			for (int i = 0; i < SessionSender.DATA_QUEUE_CAPACITY; i++) {
				assertThat(sender.sendData(1, new byte[1])).isTrue();
			}
			// kill the worker: it dies inside onNext once the gate opens
			observer.die.set(true);
			observer.gate.countDown();
			// the producer parked on the cap and a fresh sender both fail fast
			AtomicBoolean parkedResult = new AtomicBoolean(true);
			Thread parked = Thread.ofVirtual().start(() -> {
				sender.sendClose(1);
				parkedResult.set(sender.sendData(1, new byte[1]));
			});
			parked.join(TimeUnit.SECONDS.toMillis(1));
			assertThat(parked.isAlive()).as("send returns within 1s").isFalse();
			assertThat(parkedResult).as("sendData reports the failure").isFalse();
			assertThat(sender.sendData(2, new byte[1])).as("a new sender fails too").isFalse();
		}
		finally {
			sender.close();
		}
	}

	@Test
	void secondConnectionIsServedWithinOneTurnOfTheFirst() throws Exception {
		FakeServerObserver observer = new FakeServerObserver();
		SessionSender sender = new SessionSender(observer);
		try {
			sender.start();
			// onNext must not park; the worker blocks on readiness instead
			observer.gate.countDown();
			// a bulk producer for connection 1 piles up while the observer is not ready
			Thread bulk = Thread.ofVirtual().start(() -> {
				for (int i = 0; i < 100; i++) {
					sender.sendData(1, new byte[1]);
				}
			});
			// let the bulk reach the per-connection cap before connection 2 shows up
			Thread.sleep(200);
			assertThat(sender.sendData(2, new byte[1])).isTrue();
			observer.becomeReady();
			awaitSentFrames(observer, 2);
			assertThat(observer.sent.subList(0, 2)).as("connection 2's frame is among the first 2 written")
				.anyMatch(SessionSenderTest::isDataForConnection2);
			bulk.join(TimeUnit.SECONDS.toMillis(5));
			assertThat(bulk.isAlive()).as("bulk producer drained").isFalse();
		}
		finally {
			sender.close();
		}
	}

	private static boolean isDataForConnection2(Frame frame) {
		return frame.getBodyCase() == Frame.BodyCase.DATA && frame.getData().getConnId() == 2;
	}

	@Test
	void controlFrameIsSentBeforeQueuedConnectionData() throws Exception {
		FakeServerObserver observer = new FakeServerObserver();
		observer.ready.set(true);
		SessionSender sender = new SessionSender(observer);
		try {
			sender.start();
			// park the worker inside a write so nothing else is in flight
			sender.sendData(9, new byte[1]);
			assertThat(observer.enteredOnNext.await(5, TimeUnit.SECONDS)).isTrue();
			// queue data, then a control frame, while the worker cannot take either
			sender.sendData(1, new byte[1]);
			sender.sendDrain("closing");
			observer.gate.countDown();
			awaitSentFrames(observer, 3);
			assertThat(observer.sent.get(0).getBodyCase()).as("the frame held by the worker")
				.isEqualTo(Frame.BodyCase.DATA);
			assertThat(observer.sent.get(1).getBodyCase()).as("control before queued data")
				.isEqualTo(Frame.BodyCase.DRAIN);
			assertThat(observer.sent.get(2).getBodyCase()).as("queued data after control")
				.isEqualTo(Frame.BodyCase.DATA);
			assertThat(observer.sent.get(2).getData().getConnId()).isEqualTo(1);
		}
		finally {
			sender.close();
		}
	}

	@Test
	void perConnectionCapBlocksOnlyItsOwnProducer() throws Exception {
		FakeServerObserver observer = new FakeServerObserver();
		observer.ready.set(true);
		SessionSender sender = new SessionSender(observer);
		try {
			sender.start();
			// park the worker inside a write so the cap is reachable deterministically
			sender.sendData(9, new byte[1]);
			assertThat(observer.enteredOnNext.await(5, TimeUnit.SECONDS)).isTrue();
			for (int i = 0; i < SessionSender.DATA_QUEUE_CAPACITY; i++) {
				assertThat(sender.sendData(1, new byte[1])).isTrue();
			}
			// the 17th chunk of connection 1 waits for a free slot
			AtomicBoolean capResult = new AtomicBoolean();
			Thread producer = Thread.ofVirtual().start(() -> capResult.set(sender.sendData(1, new byte[1])));
			observeBlocked(producer);
			// another connection's producer is unaffected by the full queue of conn 1
			assertThat(sender.sendData(2, new byte[1])).isTrue();
			// release the worker: slots free up and the parked producer completes
			observer.gate.countDown();
			producer.join(TimeUnit.SECONDS.toMillis(5));
			assertThat(producer.isAlive()).as("parked producer resumed").isFalse();
			assertThat(capResult).as("the chunk is queued once a slot frees").isTrue();
		}
		finally {
			// idempotent; releases the worker so close() returns on failure paths too
			observer.gate.countDown();
			sender.close();
		}
	}

	/** Observes the thread parked on the cap; its state must be a waiting one. */
	private static void observeBlocked(Thread producer) throws InterruptedException {
		long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(5);
		boolean blocked = false;
		while (!blocked && System.currentTimeMillis() < deadline) {
			Thread.State state = producer.getState();
			blocked = state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING;
			if (!blocked) {
				TimeUnit.MILLISECONDS.sleep(10);
			}
		}
		if (!blocked) {
			fail("producer never observed blocked: " + producer.getState());
		}
		assertThat(producer.isAlive()).as("producer blocked, not done").isTrue();
	}

	@Test
	void closeAndErrorBypassTheCapInConnectionOrder() throws Exception {
		FakeServerObserver observer = new FakeServerObserver();
		observer.ready.set(true);
		SessionSender sender = new SessionSender(observer);
		try {
			sender.start();
			// park the worker inside a write so the cap is reachable deterministically
			sender.sendData(9, new byte[1]);
			assertThat(observer.enteredOnNext.await(5, TimeUnit.SECONDS)).isTrue();
			for (int i = 0; i < SessionSender.DATA_QUEUE_CAPACITY; i++) {
				assertThat(sender.sendData(1, new byte[1])).isTrue();
			}
			// markers are accepted with the connection at its cap; never blocked
			sender.sendClose(1);
			sender.sendError(1, "boom");
			observer.gate.countDown();
			// 1 held frame + cap + 2 markers
			awaitSentFrames(observer, 1 + SessionSender.DATA_QUEUE_CAPACITY + 2);
			assertThat(observer.sent).hasSize(1 + SessionSender.DATA_QUEUE_CAPACITY + 2);
			assertThat(observer.sent.get(0).getData().getConnId()).isEqualTo(9);
			for (int i = 1; i <= SessionSender.DATA_QUEUE_CAPACITY; i++) {
				Frame frame = observer.sent.get(i);
				assertThat(frame.getBodyCase()).as("frame %d", i).isEqualTo(Frame.BodyCase.DATA);
				assertThat(frame.getData().getConnId()).as("frame %d", i).isEqualTo(1);
			}
			Frame close = observer.sent.get(SessionSender.DATA_QUEUE_CAPACITY + 1);
			assertThat(close.getBodyCase()).isEqualTo(Frame.BodyCase.CLOSE);
			assertThat(close.getClose().getConnId()).isEqualTo(1);
			Frame error = observer.sent.get(SessionSender.DATA_QUEUE_CAPACITY + 2);
			assertThat(error.getBodyCase()).isEqualTo(Frame.BodyCase.ERROR);
			assertThat(error.getError().getConnId()).isEqualTo(1);
			assertThat(error.getError().getMessage()).isEqualTo("boom");
		}
		finally {
			sender.close();
		}
	}

	@Test
	void dataAfterTheQueueIsRemovedIsStillDelivered() throws Exception {
		FakeServerObserver observer = new FakeServerObserver();
		observer.ready.set(true);
		SessionSender sender = new SessionSender(observer);
		try {
			sender.start();
			// the queue of connection 1 empties and is dropped once its CLOSE is sent
			observer.gate.countDown();
			sender.sendData(1, new byte[] { 1 });
			assertThat(observer.firstSent.await(5, TimeUnit.SECONDS)).isTrue();
			sender.sendClose(1);
			awaitSentFrames(observer, 2);
			// give the worker a turn to drop the emptied queue
			Thread.sleep(100);
			// a late frame for the removed id starts a new queue and drains normally
			sender.sendData(1, new byte[] { 2 });
			awaitSentFrames(observer, 3);
			assertThat(observer.sent).hasSize(3);
			assertThat(observer.sent.get(0).getBodyCase()).isEqualTo(Frame.BodyCase.DATA);
			assertThat(observer.sent.get(0).getData().getPayload().toByteArray()).containsExactly(1);
			assertThat(observer.sent.get(1).getBodyCase()).isEqualTo(Frame.BodyCase.CLOSE);
			assertThat(observer.sent.get(2).getBodyCase()).as("late frame delivered").isEqualTo(Frame.BodyCase.DATA);
			assertThat(observer.sent.get(2).getData().getPayload().toByteArray()).containsExactly(2);
		}
		finally {
			sender.close();
		}
	}

}
