package am.ik.sluice.tunnel;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import am.ik.sluice.v1.proto.Frame;
import io.grpc.stub.ServerCallStreamObserver;

/**
 * Fake server-side observer whose readiness is controlled by the test. Every
 * {@code onNext} waits on {@link #gate}; {@link #die} makes it throw instead of recording
 * the frame.
 */
final class FakeServerObserver extends ServerCallStreamObserver<Frame> {

	final AtomicBoolean ready = new AtomicBoolean();

	final List<Frame> sent = new CopyOnWriteArrayList<>();

	final AtomicReference<Runnable> onReadyHandler = new AtomicReference<>();

	final AtomicBoolean die = new AtomicBoolean();

	final CountDownLatch enteredOnNext = new CountDownLatch(1);

	final CountDownLatch gate = new CountDownLatch(1);

	final CountDownLatch firstSent = new CountDownLatch(1);

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
