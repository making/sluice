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

/**
 * The sender must honor the peer's flow control on both ends of the tunnel (the server
 * observer is a {@link ServerCallStreamObserver}, the client's a
 * {@link io.grpc.stub.ClientCallStreamObserver}) and must fail fast once its worker is
 * gone.
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
			// fill the queue to its capacity of 256
			for (int i = 0; i < 256; i++) {
				assertThat(sender.sendData(1, new byte[1])).isTrue();
			}
			// kill the worker: it dies inside onNext once the gate opens
			observer.die.set(true);
			observer.gate.countDown();
			AtomicBoolean returned = new AtomicBoolean();
			AtomicBoolean sendResult = new AtomicBoolean(true);
			Thread sender1 = Thread.ofVirtual().start(() -> {
				sender.sendClose(1);
				sendResult.set(sender.sendData(2, new byte[1]));
				returned.set(true);
			});
			sender1.join(TimeUnit.SECONDS.toMillis(1));
			assertThat(sender1.isAlive()).as("send returns within 1s").isFalse();
			assertThat(returned).isTrue();
			assertThat(sendResult).as("sendData reports the failure").isFalse();
		}
		finally {
			sender.close();
		}
	}

}
