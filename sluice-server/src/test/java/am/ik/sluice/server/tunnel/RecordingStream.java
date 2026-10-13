package am.ik.sluice.server.tunnel;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import am.ik.sluice.v1.proto.Frame;
import io.grpc.stub.StreamObserver;

import static org.assertj.core.api.Assertions.assertThat;

/** An always-ready stream recording the frames a session sends. */
final class RecordingStream implements StreamObserver<Frame> {

	final List<Frame> frames = new CopyOnWriteArrayList<>();

	final List<Throwable> failures = new CopyOnWriteArrayList<>();

	final AtomicInteger completions = new AtomicInteger();

	List<Frame> awaitConnects(int count) throws InterruptedException {
		return await(Frame.BodyCase.CONNECT, count);
	}

	List<Frame> await(Frame.BodyCase bodyCase, int count) throws InterruptedException {
		long deadline = System.currentTimeMillis() + 5000;
		while (System.currentTimeMillis() < deadline) {
			List<Frame> matching = matching(bodyCase);
			if (matching.size() >= count) {
				return matching;
			}
			TimeUnit.MILLISECONDS.sleep(10);
		}
		throw new AssertionError("fewer than %d %s frames: %s".formatted(count, bodyCase, this.frames));
	}

	List<Frame> matching(Frame.BodyCase bodyCase) {
		return this.frames.stream().filter(frame -> frame.getBodyCase() == bodyCase).toList();
	}

	@Override
	public void onNext(Frame value) {
		this.frames.add(value);
	}

	@Override
	public void onError(Throwable t) {
		this.failures.add(t);
	}

	@Override
	public void onCompleted() {
		this.completions.incrementAndGet();
	}

	Throwable awaitFailure() throws InterruptedException {
		long deadline = System.currentTimeMillis() + 5000;
		while (this.failures.isEmpty() && System.currentTimeMillis() < deadline) {
			TimeUnit.MILLISECONDS.sleep(1);
		}
		assertThat(this.failures).as("the stream failed").isNotEmpty();
		return this.failures.getFirst();
	}

}
