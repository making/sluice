package am.ik.sluice.tunnel;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.stream.IntStream;

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

	/** Waits until the worker has written {@code count} frames. */
	private static void awaitSentFrames(FakeServerObserver observer, int count) throws InterruptedException {
		long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(5);
		while (observer.sent.size() < count && System.currentTimeMillis() < deadline) {
			TimeUnit.MILLISECONDS.sleep(10);
		}
		assertThat(observer.sent).hasSizeGreaterThanOrEqualTo(count);
	}

	/** Waits until the sender holds the expected number of live connection queues. */
	private static void awaitConnectionQueues(SessionSender sender, int expected) throws InterruptedException {
		long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(5);
		while (sender.connectionQueues() != expected && System.currentTimeMillis() < deadline) {
			TimeUnit.MILLISECONDS.sleep(10);
		}
		assertThat(sender.connectionQueues()).as("live connection queues").isEqualTo(expected);
	}

	@Test
	void queueIsDroppedOnceCloseDrains() throws Exception {
		FakeServerObserver observer = new FakeServerObserver();
		observer.ready.set(true);
		observer.gate.countDown();
		SessionSender sender = new SessionSender(observer);
		try {
			sender.start();
			sender.sendData(1, new byte[] { 1 });
			sender.sendClose(1);
			awaitSentFrames(observer, 2);
			awaitConnectionQueues(sender, 0);
		}
		finally {
			sender.close();
		}
	}

	@Test
	void queueOfOpenConnectionIsDroppedOnceItsDataIsSent() throws Exception {
		FakeServerObserver observer = new FakeServerObserver();
		observer.ready.set(true);
		observer.gate.countDown();
		SessionSender sender = new SessionSender(observer);
		try {
			sender.start();
			// no CLOSE: the connection stays open, yet nothing must remain to scan
			sender.sendData(1, new byte[] { 1 });
			awaitSentFrames(observer, 1);
			awaitConnectionQueues(sender, 0);
		}
		finally {
			sender.close();
		}
	}

	@Test
	void idleConnectionsLeaveNoQueueOnceTheirDataIsSent() throws Exception {
		FakeServerObserver observer = new FakeServerObserver();
		observer.ready.set(true);
		observer.gate.countDown();
		SessionSender sender = new SessionSender(observer);
		try {
			sender.start();
			int connections = 10_000;
			for (int id = 1; id <= connections; id++) {
				sender.sendData(id, new byte[] { 1 });
			}
			awaitSentFrames(observer, connections);
			// no CLOSE anywhere: the per-frame scan must not grow with idle connections
			awaitConnectionQueues(sender, 0);
		}
		finally {
			sender.close();
		}
	}

	@Test
	void capWaitersAreWokenOnlyByTheirOwnConnection() throws Exception {
		FakeServerObserver observer = new FakeServerObserver();
		observer.ready.set(true);
		SessionSender sender = new SessionSender(observer);
		try {
			sender.start();
			// park the worker inside a write: nothing pops during the measurement
			sender.sendData(1, new byte[1]);
			assertThat(observer.enteredOnNext.await(5, TimeUnit.SECONDS)).isTrue();
			for (int i = 0; i < SessionSender.DATA_QUEUE_CAPACITY; i++) {
				assertThat(sender.sendData(1, new byte[1])).isTrue();
			}
			AtomicBoolean parkedResult = new AtomicBoolean();
			Thread parked = Thread.ofVirtual().start(() -> parkedResult.set(sender.sendData(1, new byte[1])));
			observeBlocked(parked);
			long baseline = sender.capWaiterWakeups(1);
			// 99 other connections fill their queues; none of it touches conn 1's waiter
			for (int id = 2; id <= 100; id++) {
				for (int i = 0; i < SessionSender.DATA_QUEUE_CAPACITY; i++) {
					assertThat(sender.sendData(id, new byte[1])).isTrue();
				}
			}
			// let delivered signals and the 100ms timer wakes settle
			TimeUnit.MILLISECONDS.sleep(200);

			assertThat(sender.capWaiterWakeups(1)).as("wakes of connection 1's cap waiter").isLessThanOrEqualTo(100);
			observer.gate.countDown();
			parked.join(TimeUnit.SECONDS.toMillis(5));
			assertThat(parked.isAlive()).as("parked producer resumed").isFalse();
			assertThat(parkedResult).as("the chunk is queued once a slot frees").isTrue();
		}
		finally {
			observer.gate.countDown();
			sender.close();
		}
	}

	@Test
	void perConnectionOrderSurvivesInterleavedProducers() throws Exception {
		FakeServerObserver observer = new FakeServerObserver();
		observer.ready.set(true);
		observer.gate.countDown();
		SessionSender sender = new SessionSender(observer);
		try {
			sender.start();
			int frames = 500;
			Thread low = Thread.ofVirtual().start(() -> {
				for (int i = 0; i < frames; i++) {
					sender.sendData(1, new byte[] { (byte) (i >>> 8), (byte) i });
				}
			});
			Thread high = Thread.ofVirtual().start(() -> {
				for (int i = 0; i < frames; i++) {
					sender.sendData(2, new byte[] { (byte) (i >>> 8), (byte) i });
				}
			});
			low.join(TimeUnit.SECONDS.toMillis(5));
			high.join(TimeUnit.SECONDS.toMillis(5));
			assertThat(low.isAlive()).isFalse();
			assertThat(high.isAlive()).isFalse();
			awaitSentFrames(observer, 2 * frames);
			assertThat(sequenceOf(observer.sent, 1)).as("connection 1 subsequence")
				.containsExactlyElementsOf(expectedSequence(frames));
			assertThat(sequenceOf(observer.sent, 2)).as("connection 2 subsequence")
				.containsExactlyElementsOf(expectedSequence(frames));
		}
		finally {
			sender.close();
		}
	}

	private static List<Integer> expectedSequence(int frames) {
		return IntStream.range(0, frames).mapToObj(Integer::valueOf).toList();
	}

	/** The payload values of the connection's DATA frames, in sent order. */
	private static List<Integer> sequenceOf(List<Frame> sent, long connectionId) {
		return sent.stream()
			.filter(frame -> frame.getBodyCase() == Frame.BodyCase.DATA && frame.getData().getConnId() == connectionId)
			.map(frame -> (frame.getData().getPayload().byteAt(0) & 0xff) << 8
					| frame.getData().getPayload().byteAt(1) & 0xff)
			.toList();
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
			// a marker on another connection must not block either; one on connection 1
			// would end its producer by itself and hide the worker's death
			Thread parked = Thread.ofVirtual().start(() -> {
				sender.sendClose(3);
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
			// the bulk producer is parked on its cap before connection 2 shows up: the
			// fairness assertion below cannot pass vacuously
			observeBlocked(bulk);
			assertThat(sender.sendData(2, new byte[1])).isTrue();
			observer.becomeReady();
			awaitSentFrames(observer, 2);
			// snapshot: the worker keeps appending while the assertion runs
			List<Frame> firstTwo = List.copyOf(observer.sent.subList(0, 2));
			assertThat(firstTwo).as("connection 2's frame is among the first 2 written")
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
			// another connection's producer is unaffected by the full queue of conn 1; on
			// a thread with a timeout so a regression hangs the join, not the build
			AtomicBoolean otherResult = new AtomicBoolean();
			Thread other = Thread.ofVirtual().start(() -> otherResult.set(sender.sendData(2, new byte[1])));
			other.join(TimeUnit.SECONDS.toMillis(1));
			assertThat(other.isAlive()).as("another connection's send never blocks").isFalse();
			assertThat(otherResult).as("queued on the other connection").isTrue();
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
	void capWaiterGivesUpOnceCloseIsQueuedForItsConnection() throws Exception {
		assertCapWaiterGivesUp((sender, id) -> sender.sendClose(id));
	}

	@Test
	void capWaiterGivesUpOnceErrorIsQueuedForItsConnection() throws Exception {
		assertCapWaiterGivesUp((sender, id) -> sender.sendError(id, "boom"));
	}

	/**
	 * Parks one producer on the cap of connection 1 and one on connection 2 (the observer
	 * never becomes ready, so nothing pops); the marker for connection 1 ends its
	 * producer within 100ms and leaves connection 2's parked.
	 */
	private static void assertCapWaiterGivesUp(BiConsumer<SessionSender, Long> marker) throws Exception {
		FakeServerObserver observer = new FakeServerObserver();
		SessionSender sender = new SessionSender(observer);
		try {
			sender.start();
			for (long id = 1; id <= 2; id++) {
				for (int i = 0; i < SessionSender.DATA_QUEUE_CAPACITY; i++) {
					assertThat(sender.sendData(id, new byte[1])).isTrue();
				}
			}
			AtomicReference<Boolean> closedResult = new AtomicReference<>();
			Thread closed = Thread.ofVirtual().start(() -> closedResult.set(sender.sendData(1, new byte[1])));
			AtomicReference<Boolean> otherResult = new AtomicReference<>();
			Thread other = Thread.ofVirtual().start(() -> otherResult.set(sender.sendData(2, new byte[1])));
			observeBlocked(closed);
			observeBlocked(other);
			marker.accept(sender, 1L);
			closed.join(100);
			assertThat(closed.isAlive()).as("the producer of the closed connection returns within 100ms").isFalse();
			assertThat(closedResult.get()).as("its chunk is refused").isFalse();
			TimeUnit.MILLISECONDS.sleep(200);
			assertThat(other.isAlive()).as("the other connection's producer stays parked").isTrue();
			assertThat(otherResult.get()).as("and has not returned").isNull();
			assertThat(observer.sent).as("nothing was sent: the stream never became ready").isEmpty();
		}
		finally {
			sender.close();
		}
	}

	@Test
	void dataQueuedAfterCloseIsRefusedWhileTheCloseIsPending() throws Exception {
		FakeServerObserver observer = new FakeServerObserver();
		SessionSender sender = new SessionSender(observer);
		try {
			sender.start();
			assertThat(sender.sendData(1, new byte[] { 1 })).isTrue();
			sender.sendClose(1);
			assertThat(sender.sendData(1, new byte[] { 2 })).as("DATA after the queued CLOSE").isFalse();
			observer.gate.countDown();
			observer.becomeReady();
			awaitSentFrames(observer, 2);
			awaitConnectionQueues(sender, 0);
			assertThat(observer.sent).extracting(Frame::getBodyCase)
				.containsExactly(Frame.BodyCase.DATA, Frame.BodyCase.CLOSE);
			assertThat(observer.sent.get(0).getData().getPayload().toByteArray()).containsExactly(1);
		}
		finally {
			sender.close();
		}
	}

	/**
	 * A multi-chunk send racing its connection's CLOSE stops at the CLOSE: no DATA of the
	 * call follows it on the stream, even when the stream drains the queue between two
	 * chunks. Repeated because the interleaving is up to the scheduler.
	 */
	@Test
	void sendRacingCloseNeverPutsDataAfterTheClose() throws Exception {
		FakeServerObserver observer = new FakeServerObserver();
		observer.ready.set(true);
		observer.gate.countDown();
		SessionSender sender = new SessionSender(observer);
		try {
			sender.start();
			byte[] payload = new byte[SessionSender.DATA_QUEUE_CAPACITY * SessionSender.MAX_CHUNK];
			AtomicInteger refused = new AtomicInteger();
			for (long id = 1; id <= 200; id++) {
				long connectionId = id;
				Thread producer = Thread.ofVirtual().start(() -> {
					if (!sender.sendData(connectionId, payload)) {
						refused.incrementAndGet();
					}
				});
				awaitSent(observer, frame -> frame.getBodyCase() == Frame.BodyCase.DATA);
				sender.sendClose(connectionId);
				producer.join(TimeUnit.SECONDS.toMillis(5));
				assertThat(producer.isAlive()).as("producer of connection %d returned", connectionId).isFalse();
				awaitSent(observer, frame -> frame.getBodyCase() == Frame.BodyCase.CLOSE);
				awaitConnectionQueues(sender, 0);
				List<Frame.BodyCase> frames = observer.sent.stream().map(Frame::getBodyCase).toList();
				assertThat(frames.indexOf(Frame.BodyCase.CLOSE)).as("CLOSE of connection %d is its last frame", id)
					.isEqualTo(frames.size() - 1);
				observer.sent.clear();
			}
			// the race happened: some CLOSE landed while its send was still running
			assertThat(refused.get()).as("sends cut short by their CLOSE").isGreaterThan(0);
		}
		finally {
			sender.close();
		}
	}

	/** Waits until a frame matching the predicate was sent. */
	private static void awaitSent(FakeServerObserver observer, Predicate<Frame> predicate) {
		long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(5);
		while (observer.sent.stream().noneMatch(predicate) && System.currentTimeMillis() < deadline) {
			Thread.onSpinWait();
		}
		assertThat(observer.sent).anyMatch(predicate);
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
			// markers are accepted with the connection at its cap; never blocked -- on a
			// thread with a timeout so a regression hangs the join, not the build
			Thread markers = Thread.ofVirtual().start(() -> {
				sender.sendClose(1);
				sender.sendError(1, "boom");
			});
			markers.join(TimeUnit.SECONDS.toMillis(1));
			assertThat(markers.isAlive()).as("markers never block on the cap").isFalse();
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
			// both connections drained: nothing must remain to scan per frame
			awaitConnectionQueues(sender, 0);
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
			// the emptied queue is really gone, not kept for later scans
			awaitConnectionQueues(sender, 0);
			// a late frame for the removed id starts a new queue and drains normally
			sender.sendData(1, new byte[] { 2 });
			awaitSentFrames(observer, 3);
			// and the new queue is dropped too once it drains
			awaitConnectionQueues(sender, 0);
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

	@Test
	void closeReturnsWithFullQueueOnReadyStream() throws Exception {
		FakeServerObserver observer = new FakeServerObserver();
		observer.ready.set(true);
		SessionSender sender = new SessionSender(observer);
		sender.start();
		// the worker takes one frame and parks inside onNext; then fill the queue to the
		// per-connection cap
		assertThat(sender.sendData(1, new byte[1])).isTrue();
		assertThat(observer.enteredOnNext.await(5, TimeUnit.SECONDS)).isTrue();
		for (int i = 0; i < SessionSender.DATA_QUEUE_CAPACITY; i++) {
			assertThat(sender.sendData(1, new byte[1])).isTrue();
		}
		Thread closer = Thread.ofVirtual().start(sender::close);
		Thread.sleep(200);
		// the stream is healthy: the worker resumes and drains
		observer.gate.countDown();
		closer.join(TimeUnit.SECONDS.toMillis(10));
		assertThat(closer.isAlive()).as("close returns with a full queue on a ready stream").isFalse();
	}

}
