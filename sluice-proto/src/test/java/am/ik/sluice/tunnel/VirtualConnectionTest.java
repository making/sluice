package am.ik.sluice.tunnel;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

import am.ik.sluice.v1.proto.Connect;
import am.ik.sluice.v1.proto.Frame;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

@Timeout(30)
class VirtualConnectionTest {

	/** Records every frame without a real stream. */
	private static final class RecordingWriter implements FrameWriter {

		final List<String> frames = new CopyOnWriteArrayList<>();

		final AtomicLong bytes = new AtomicLong();

		volatile boolean open = true;

		@Override
		public boolean isOpen() {
			return this.open;
		}

		@Override
		public void sendConnect(Connect connect) {
			this.frames.add("connect:%d:%s".formatted(connect.getConnId(), connect.getAddress()));
		}

		@Override
		public void sendWindowUpdate(long connectionId, int increment) {
			this.frames.add("window:%d:%d".formatted(connectionId, increment));
		}

		@Override
		public boolean sendData(long connectionId, byte[] payload) {
			this.frames.add("data:%d:%d".formatted(connectionId, payload.length));
			this.bytes.addAndGet(payload.length);
			return true;
		}

		@Override
		public void sendClose(long connectionId) {
			this.frames.add("close:%d".formatted(connectionId));
		}

		@Override
		public void sendError(long connectionId, String message) {
			this.frames.add("error:%d:%s".formatted(connectionId, message));
		}

	}

	@Test
	void chunkBoundaryReassembly() throws IOException {
		RecordingWriter writer = new RecordingWriter();
		VirtualConnection connection = VirtualConnection.builder(1, writer).build();
		InputStream source = connection.source();
		Executors.newSingleThreadExecutor().execute(() -> {
			connection.acceptData("hel".getBytes());
			connection.acceptData("lo".getBytes());
			connection.remoteClosed();
		});
		byte[] buffer = new byte[5];
		int read = 0;
		int n;
		while (read < 5 && (n = source.read(buffer, read, 5 - read)) > 0) {
			read += n;
		}
		assertThat(new String(buffer, 0, read)).isEqualTo("hello");
		assertThat(source.read()).isEqualTo(-1); // half close: EOF, writer still usable
		connection.sink().write('x');
		assertThat(writer.frames).contains("data:1:1");
	}

	@Test
	void largePayloadSplitIntoSingleReads() throws Exception {
		RecordingWriter writer = new RecordingWriter();
		VirtualConnection connection = VirtualConnection.builder(2, writer).build();
		byte[] payload = new byte[10_000];
		for (int i = 0; i < payload.length; i++) {
			payload[i] = (byte) i;
		}
		Executors.newSingleThreadExecutor().execute(() -> {
			connection.acceptData(payload);
			connection.remoteClosed();
		});
		byte[] buffer = new byte[10_000];
		int read = 0;
		int n;
		while (read < payload.length && (n = connection.source().read(buffer, read, payload.length - read)) > 0) {
			read += n;
		}
		assertThat(read).isEqualTo(payload.length);
		assertThat(buffer).isEqualTo(payload);
	}

	@Test
	void sinkSplitsLargeWritesIntoFrames() throws Exception {
		// SessionSender splits payloads > 64KiB into multiple DATA frames
		List<Frame> sent = new CopyOnWriteArrayList<>();
		CountDownLatch drained = new CountDownLatch(1);
		StreamObserver<Frame> observer = new StreamObserver<>() {

			@Override
			public void onNext(Frame frame) {
				sent.add(frame);
				if (sent.size() == 2) {
					drained.countDown();
				}
			}

			@Override
			public void onError(Throwable t) {
			}

			@Override
			public void onCompleted() {
			}

		};
		SessionSender sender = new SessionSender(observer);
		sender.start();
		byte[] large = new byte[SessionSender.MAX_CHUNK + 10];
		sender.sendData(3, large);
		assertThat(drained.await(5, TimeUnit.SECONDS)).isTrue();
		assertThat(sent).hasSize(2);
		assertThat(sent.get(0).getData().getConnId()).isEqualTo(3);
		assertThat(sent.get(0).getData().getPayload().size()).isEqualTo(SessionSender.MAX_CHUNK);
		assertThat(sent.get(1).getData().getPayload().size()).isEqualTo(10);
	}

	@Test
	void sinkHalfCloseNotifiesRemoteAndRejectsFurtherWrites() throws IOException {
		RecordingWriter writer = new RecordingWriter();
		VirtualConnection connection = VirtualConnection.builder(4, writer).build();
		OutputStream sink = connection.sink();
		sink.close();
		assertThat(writer.frames).contains("close:4");
		assertThatThrownBy(() -> sink.write('x')).isInstanceOf(IOException.class);
		// half close: reads still work
		connection.acceptData("ok".getBytes());
		connection.remoteClosed();
	}

	@Test
	void remoteFailureSurfacesAsIOException() {
		RecordingWriter writer = new RecordingWriter();
		VirtualConnection connection = VirtualConnection.builder(5, writer).build();
		connection.remoteFailed("boom");
		assertThatThrownBy(() -> connection.source().read()).isInstanceOf(IOException.class).hasMessage("boom");
	}

	@Test
	void closeWakesBlockedReader() throws Exception {
		RecordingWriter writer = new RecordingWriter();
		VirtualConnection connection = VirtualConnection.builder(6, writer).build();
		Thread reader = Thread.ofVirtual().start(() -> {
			try {
				connection.source().read();
			}
			catch (IOException e) {
				// closed
			}
		});
		Thread.sleep(100);
		connection.close();
		reader.join(1000);
		assertThat(reader.isAlive()).isFalse();
		assertThat(writer.frames).contains("error:6:connection closed");
	}

	@Test
	void remoteClosedIsNotBlockedByAFullWindow() throws Exception {
		RecordingWriter writer = new RecordingWriter();
		VirtualConnection connection = windowed(7, writer, 128 * 1000, 1);
		byte[] chunk = new byte[1000];
		for (int i = 0; i < 128; i++) { // fill the whole window
			connection.acceptData(chunk);
		}
		Thread marker = Thread.ofVirtual().start(connection::remoteClosed);
		marker.join(TimeUnit.MILLISECONDS.toMillis(100));
		assertThat(marker.isAlive()).as("remoteClosed returns").isFalse();
		byte[] all = connection.source().readNBytes(128 * 1000);
		assertThat(all).hasSize(128 * 1000);
		assertThat(connection.source().read()).as("EOF after all data").isEqualTo(-1);
	}

	@Test
	void remoteFailedIsNotBlockedByAFullWindow() throws Exception {
		RecordingWriter writer = new RecordingWriter();
		VirtualConnection connection = windowed(8, writer, 128 * 1000, 1);
		byte[] chunk = new byte[1000];
		for (int i = 0; i < 128; i++) { // fill the whole window
			connection.acceptData(chunk);
		}
		Thread marker = Thread.ofVirtual().start(() -> connection.remoteFailed("boom"));
		marker.join(TimeUnit.MILLISECONDS.toMillis(100));
		assertThat(marker.isAlive()).as("remoteFailed returns").isFalse();
		byte[] all = connection.source().readNBytes(128 * 1000);
		assertThat(all).hasSize(128 * 1000);
		assertThatThrownBy(() -> connection.source().read()).isInstanceOf(IOException.class).hasMessage("boom");
	}

	@Test
	void repeatedRemoteClosedEndTheStreamOnce() throws Exception {
		RecordingWriter writer = new RecordingWriter();
		VirtualConnection connection = windowed(10, writer, 128 * 1000, 1);
		byte[] chunk = new byte[1000];
		for (int i = 0; i < 128; i++) { // fill the whole window
			connection.acceptData(chunk);
		}
		for (int i = 0; i < 1000; i++) {
			connection.remoteClosed();
		}
		assertThat(connection.bufferedBytes()).as("precondition: the window is full").isEqualTo(128 * 1000);
		byte[] all = connection.source().readNBytes(128 * 1000);
		assertThat(all).hasSize(128 * 1000);
		assertThat(connection.source().read()).as("EOF after all data").isEqualTo(-1);
		connection.remoteClosed();
		assertThat(connection.source().read()).as("EOF stays").isEqualTo(-1);
	}

	@Test
	void repeatedRemoteFailedEndTheStreamOnce() throws Exception {
		RecordingWriter writer = new RecordingWriter();
		VirtualConnection connection = windowed(11, writer, 128 * 1000, 1);
		byte[] chunk = new byte[1000];
		for (int i = 0; i < 128; i++) { // fill the whole window
			connection.acceptData(chunk);
		}
		for (int i = 0; i < 1000; i++) {
			connection.remoteFailed("boom");
		}
		assertThat(connection.bufferedBytes()).as("precondition: the window is full").isEqualTo(128 * 1000);
		assertThat(connection.source().readNBytes(128 * 1000)).hasSize(128 * 1000);
		assertThatThrownBy(() -> connection.source().read()).isInstanceOf(IOException.class).hasMessage("boom");
		assertThat(connection.source().read()).as("EOF after the failure").isEqualTo(-1);
	}

	/**
	 * A write parked at the session sender's per-connection cap (the stream never becomes
	 * ready) fails within 100ms of the local close, which queued the connection's ERROR.
	 */
	@Test
	void sinkWriteParkedAtTheSenderCapFailsOnClose() throws Exception {
		FakeServerObserver observer = new FakeServerObserver();
		SessionSender sender = new SessionSender(observer);
		sender.start();
		try {
			VirtualConnection connection = VirtualConnection.builder(1, sender).build();
			OutputStream sink = connection.sink();
			for (int i = 0; i < SessionSender.DATA_QUEUE_CAPACITY; i++) {
				sink.write(new byte[1]);
			}
			CompletableFuture<Void> parked = CompletableFuture.runAsync(() -> {
				try {
					sink.write(new byte[1]);
				}
				catch (IOException e) {
					throw new CompletionException(e);
				}
			}, task -> Thread.ofVirtual().start(task));
			TimeUnit.MILLISECONDS.sleep(200);
			assertThat(parked).as("the write waits at the cap").isNotDone();
			connection.close();
			assertThatThrownBy(() -> parked.get(100, TimeUnit.MILLISECONDS)).isInstanceOf(ExecutionException.class)
				.cause()
				.isInstanceOf(IOException.class)
				.hasMessage("connection 1 is closed");
		}
		finally {
			sender.close();
		}
	}

	private static VirtualConnection windowed(long id, FrameWriter writer, int receiveWindow, long sendCredit) {
		return VirtualConnection.builder(id, writer).window(receiveWindow, sendCredit).build();
	}

	/** A write running on its own virtual thread; the future fails with its exception. */
	private record AsyncWrite(Thread thread, CompletableFuture<Void> done) {

		static AsyncWrite start(OutputStream sink, byte[] bytes) {
			CompletableFuture<Void> done = new CompletableFuture<>();
			Thread thread = Thread.ofVirtual().name("test-writer").start(() -> {
				try {
					sink.write(bytes);
					done.complete(null);
				}
				catch (IOException | RuntimeException e) {
					done.completeExceptionally(e);
				}
			});
			return new AsyncWrite(thread, done);
		}

		/** Waits until the writer is parked in the credit wait, not merely slow. */
		void awaitParkedInCreditWait() throws InterruptedException {
			long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(5);
			while (System.currentTimeMillis() < deadline) {
				Thread.State state = this.thread.getState();
				boolean waiting = state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING;
				if (waiting && Arrays.stream(this.thread.getStackTrace())
					.anyMatch(frame -> frame.getMethodName().equals("awaitCredit"))) {
					return;
				}
				TimeUnit.MILLISECONDS.sleep(10);
			}
			fail("writer never parked in awaitCredit: " + this.thread.getState());
		}

	}

	private static void awaitFrames(RecordingWriter writer, int count) throws InterruptedException {
		long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(5);
		while (writer.frames.size() < count && System.currentTimeMillis() < deadline) {
			TimeUnit.MILLISECONDS.sleep(10);
		}
		assertThat(writer.frames).hasSizeGreaterThanOrEqualTo(count);
	}

	@Test
	void windowedAcceptDataNeverBlocksWithinTheWindow() throws Exception {
		RecordingWriter writer = new RecordingWriter();
		VirtualConnection connection = windowed(20, writer, 200 * 1024, 1);
		byte[] chunk = new byte[1024];
		// the whole window with no reader, in many small chunks; on a thread so a
		// regression fails the join instead of hanging the build
		Thread producer = Thread.ofVirtual().start(() -> {
			for (int i = 0; i < 200; i++) {
				connection.acceptData(chunk);
			}
		});
		producer.join(100);
		assertThat(producer.isAlive()).as("acceptData of a full window returns within 100ms").isFalse();
		assertThat(connection.bufferedBytes()).isEqualTo(200 * 1024);
		assertThat(writer.frames).as("no ERROR within the window").isEmpty();
	}

	@Test
	void windowedAcceptDataBeyondTheWindowFailsOnlyThisConnection() throws Exception {
		RecordingWriter writer = new RecordingWriter();
		VirtualConnection connection = windowed(21, writer, 1000, 1);
		connection.acceptData(new byte[1000]);
		assertThat(writer.frames).isEmpty();
		connection.acceptData(new byte[1]);
		assertThat(writer.frames).containsExactly("error:21:flow control window exceeded");
		assertThatThrownBy(() -> connection.source().read()).isInstanceOf(IOException.class)
			.hasMessage("flow control window exceeded");
		assertThatThrownBy(() -> connection.sink().write('x')).isInstanceOf(IOException.class);
		// more violating payload neither queues nor repeats the ERROR
		connection.acceptData(new byte[1000]);
		connection.close();
		assertThat(writer.frames).containsExactly("error:21:flow control window exceeded");
	}

	@Test
	void windowedReaderGrantsOnceHalfTheWindowIsConsumed() throws Exception {
		RecordingWriter writer = new RecordingWriter();
		VirtualConnection connection = windowed(22, writer, 1000, 1);
		connection.acceptData(new byte[1000]);
		InputStream source = connection.source();
		assertThat(source.readNBytes(499)).hasSize(499);
		assertThat(writer.frames).as("below half the window").isEmpty();
		assertThat(source.readNBytes(1)).hasSize(1);
		assertThat(writer.frames).containsExactly("window:22:500");
		assertThat(source.readNBytes(500)).hasSize(500);
		assertThat(writer.frames).containsExactly("window:22:500", "window:22:500");
		// the grants made room: a whole window fits again
		connection.acceptData(new byte[1000]);
		assertThat(writer.frames).as("no ERROR after the grants").noneMatch(frame -> frame.startsWith("error"));
	}

	@Test
	void windowedWriteWaitsForCreditAndResumesOnGrant() throws Exception {
		RecordingWriter writer = new RecordingWriter();
		VirtualConnection connection = windowed(23, writer, 1000, 10);
		OutputStream sink = connection.sink();
		sink.write(new byte[10]);
		assertThat(connection.sendCredit()).isZero();
		AsyncWrite parked = AsyncWrite.start(sink, new byte[1]);
		parked.awaitParkedInCreditWait();
		assertThat(parked.done()).isNotDone();
		assertThat(writer.frames).containsExactly("data:23:10");
		connection.grant(1);
		parked.done().get(1, TimeUnit.SECONDS);
		assertThat(writer.frames).containsExactly("data:23:10", "data:23:1");
	}

	@Test
	void windowedWriteLargerThanTheCreditIsSplitAtTheCredit() throws Exception {
		RecordingWriter writer = new RecordingWriter();
		VirtualConnection connection = windowed(24, writer, 1000, 100);
		AsyncWrite write = AsyncWrite.start(connection.sink(), new byte[250]);
		write.awaitParkedInCreditWait();
		assertThat(writer.frames).containsExactly("data:24:100");
		connection.grant(100);
		awaitFrames(writer, 2);
		write.awaitParkedInCreditWait();
		assertThat(writer.frames).containsExactly("data:24:100", "data:24:100");
		connection.grant(100);
		write.done().get(1, TimeUnit.SECONDS);
		assertThat(writer.frames).containsExactly("data:24:100", "data:24:100", "data:24:50");
		assertThat(connection.sendCredit()).isEqualTo(50);
	}

	@Test
	void windowedCreditWaitFailsOnClose() throws Exception {
		RecordingWriter writer = new RecordingWriter();
		VirtualConnection connection = windowed(25, writer, 1000, 1);
		connection.sink().write(new byte[1]);
		AsyncWrite parked = AsyncWrite.start(connection.sink(), new byte[1]);
		parked.awaitParkedInCreditWait();
		connection.close();
		assertThatThrownBy(() -> parked.done().get(100, TimeUnit.MILLISECONDS)).isInstanceOf(ExecutionException.class)
			.cause()
			.isInstanceOf(IOException.class)
			.hasMessage("connection 25 is closed");
	}

	@Test
	void windowedCreditWaitFailsOnRemoteFailure() throws Exception {
		RecordingWriter writer = new RecordingWriter();
		VirtualConnection connection = windowed(26, writer, 1000, 1);
		connection.sink().write(new byte[1]);
		AsyncWrite parked = AsyncWrite.start(connection.sink(), new byte[1]);
		parked.awaitParkedInCreditWait();
		connection.remoteFailed("upstream reset");
		assertThatThrownBy(() -> parked.done().get(100, TimeUnit.MILLISECONDS)).isInstanceOf(ExecutionException.class)
			.cause()
			.isInstanceOf(IOException.class)
			.hasMessage("upstream reset");
	}

	/** The stream ending has no signal on the connection; the timed wait notices it. */
	@Test
	void windowedCreditWaitFailsOnceTheStreamEnded() throws Exception {
		RecordingWriter writer = new RecordingWriter();
		VirtualConnection connection = windowed(27, writer, 1000, 1);
		connection.sink().write(new byte[1]);
		AsyncWrite parked = AsyncWrite.start(connection.sink(), new byte[1]);
		parked.awaitParkedInCreditWait();
		writer.open = false;
		assertThatThrownBy(() -> parked.done().get(500, TimeUnit.MILLISECONDS)).isInstanceOf(ExecutionException.class)
			.cause()
			.isInstanceOf(IOException.class)
			.hasMessage("tunnel stream closed");
	}

	/**
	 * The credit lock pitfall: a windowed writer parked at the session sender's
	 * per-connection cap (the stream is never ready) must not hold the lock a grant on
	 * the receive path takes.
	 */
	@Test
	void grantReturnsWhileTheWriterIsParkedAtTheSenderCap() throws Exception {
		FakeServerObserver observer = new FakeServerObserver();
		SessionSender sender = new SessionSender(observer);
		sender.start();
		try {
			VirtualConnection connection = windowed(28, sender, 1000, 64L * 1024 * 1024);
			byte[] beyondTheCap = new byte[(SessionSender.DATA_QUEUE_CAPACITY + 1) * SessionSender.MAX_CHUNK];
			AsyncWrite parked = AsyncWrite.start(connection.sink(), beyondTheCap);
			long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(5);
			while (sender.capWaiterWakeups(28) == 0 && System.currentTimeMillis() < deadline) {
				TimeUnit.MILLISECONDS.sleep(10);
			}
			assertThat(sender.capWaiterWakeups(28)).as("precondition: the writer waits at the sender cap")
				.isGreaterThan(0);
			CompletableFuture<Void> grant = CompletableFuture.runAsync(() -> connection.grant(1),
					task -> Thread.ofVirtual().start(task));
			grant.get(100, TimeUnit.MILLISECONDS);
			assertThat(parked.done()).isNotDone();
		}
		finally {
			sender.close();
		}
	}

	/**
	 * A windowed connection closed while the remote may still send tells it to stop: the
	 * remote's payload would be dropped and never granted.
	 */
	@Test
	void windowedCloseWhileTheRemoteSendsAborts() {
		RecordingWriter writer = new RecordingWriter();
		VirtualConnection connection = windowed(29, writer, 1000, 1);
		connection.close();
		assertThat(writer.frames).containsExactly("error:29:connection closed");
	}

	@Test
	void windowedCloseAfterTheRemoteClosedHalfCloses() {
		RecordingWriter writer = new RecordingWriter();
		VirtualConnection connection = windowed(30, writer, 1000, 1);
		connection.remoteClosed();
		connection.close();
		assertThat(writer.frames).containsExactly("close:30");
	}

	@Test
	void grantIncrementIsUnsigned() throws Exception {
		RecordingWriter writer = new RecordingWriter();
		VirtualConnection connection = windowed(33, writer, 1000, 1);
		connection.sink().write(new byte[1]);
		connection.grant(-1); // uint32 max on the wire
		assertThat(connection.sendCredit()).isEqualTo(0xFFFFFFFFL);
	}

	@Test
	void abortSendsOneErrorWithTheCause() {
		RecordingWriter writer = new RecordingWriter();
		VirtualConnection connection = windowed(32, writer, 1000, 1);
		connection.abort("dial failed");
		assertThat(writer.frames).containsExactly("error:32:dial failed");
		assertThatThrownBy(() -> connection.sink().write('x')).isInstanceOf(IOException.class);
	}

	/** The byte at {@code position} of the test streams. */
	private static byte patternAt(long position) {
		return (byte) (position % 251);
	}

	private static byte[] pattern(long from, int length) {
		byte[] bytes = new byte[length];
		for (int i = 0; i < length; i++) {
			bytes[i] = patternAt(from + i);
		}
		return bytes;
	}

	@Test
	void windowedTinyFramesKeepABoundedNumberOfSegments() throws Exception {
		RecordingWriter writer = new RecordingWriter();
		int window = VirtualConnection.DEFAULT_WINDOW;
		VirtualConnection connection = windowed(30, writer, window, 1);
		for (int i = 0; i < window; i++) {
			connection.acceptData(pattern(i, 1));
		}
		assertThat(writer.frames).as("precondition: a full window is no violation").isEmpty();
		assertThat(connection.bufferedBytes()).isEqualTo(window);
		assertThat(connection.bufferedSegments()).isLessThanOrEqualTo(VirtualConnection.maxSegments(window));
		assertThat(connection.bufferedCapacity()).isLessThanOrEqualTo(window + 2L * VirtualConnection.SEGMENT_SIZE);
		assertThat(connection.source().readNBytes(window)).isEqualTo(pattern(0, window));
		assertThat(connection.bufferedSegments()).as("read segments are dropped").isZero();
	}

	@Test
	void windowedFramesOfMixedSizesKeepABoundedNumberOfSegments() throws Exception {
		RecordingWriter writer = new RecordingWriter();
		int window = VirtualConnection.DEFAULT_WINDOW;
		int segment = VirtualConnection.SEGMENT_SIZE;
		VirtualConnection connection = windowed(31, writer, window, 1);
		// sizes around the segment size, so frames straddle segment boundaries
		int[] sizes = { 1, segment - 1, segment + 1, 2, 3 * segment, 7, segment, 1 };
		int accepted = 0;
		for (int i = 0; accepted < window; i++) {
			int size = Math.min(sizes[i % sizes.length], window - accepted);
			connection.acceptData(pattern(accepted, size));
			accepted += size;
		}
		assertThat(writer.frames).as("precondition: a full window is no violation").isEmpty();
		assertThat(connection.bufferedBytes()).isEqualTo(window);
		assertThat(connection.bufferedSegments()).isLessThanOrEqualTo(VirtualConnection.maxSegments(window));
		assertThat(connection.bufferedCapacity()).isLessThanOrEqualTo(window + 2L * segment);
		assertThat(connection.source().readNBytes(window)).isEqualTo(pattern(0, window));
	}

	/**
	 * A frame of a whole window, half read and granted, then the granted half again: the
	 * read half must not stay referenced beside the new payload.
	 */
	@Test
	void windowedReadPartOfALargeFrameIsReleased() throws Exception {
		RecordingWriter writer = new RecordingWriter();
		int window = VirtualConnection.DEFAULT_WINDOW;
		VirtualConnection connection = windowed(32, writer, window, 1);
		connection.acceptData(pattern(0, window));
		assertThat(connection.source().readNBytes(window / 2)).isEqualTo(pattern(0, window / 2));
		assertThat(writer.frames).as("precondition: half the window granted back")
			.containsExactly("window:32:" + window / 2);
		connection.acceptData(pattern(window, window / 2));
		assertThat(writer.frames).as("precondition: no violation").hasSize(1);
		assertThat(connection.bufferedBytes()).isEqualTo(window);
		assertThat(connection.bufferedCapacity()).isLessThanOrEqualTo(window + 2L * VirtualConnection.SEGMENT_SIZE);
		assertThat(connection.source().readNBytes(window)).isEqualTo(pattern(window / 2, window));
	}

	@Test
	void windowedConcurrentFramesAndReadsArriveByteExact() throws Exception {
		RecordingWriter writer = new RecordingWriter();
		int total = 8 * 1024 * 1024;
		// a window above the total: the producer never waits for grants
		VirtualConnection connection = windowed(33, writer, 2 * total, 1);
		java.util.Random sizes = new java.util.Random(58);
		Thread producer = Thread.ofVirtual().start(() -> {
			int sent = 0;
			while (sent < total) {
				int size = Math.min(1 + sizes.nextInt(sizes.nextBoolean() ? 16 : 40_000), total - sent);
				connection.acceptData(pattern(sent, size));
				sent += size;
				// paced, so the reader catches up and waits for frames
				java.util.concurrent.locks.LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(100));
			}
		});
		java.util.Random lengths = new java.util.Random(85);
		InputStream source = connection.source();
		byte[] buffer = new byte[70_000];
		long read = 0;
		// no end before the last byte was read: only the frames wake the reader (a missed
		// wake hangs until the class timeout)
		while (read < total) {
			int n = source.read(buffer, 0, 1 + lengths.nextInt(buffer.length));
			assertThat(n).as("no EOF at byte %d", read).isPositive();
			for (int i = 0; i < n; i++) {
				if (buffer[i] != patternAt(read + i)) {
					fail("byte %d differs".formatted(read + i));
				}
			}
			read += n;
		}
		producer.join();
		assertThat(connection.readerWaits()).as("precondition: the reader waited").isPositive();
		connection.remoteClosed();
		assertThat(source.read()).as("EOF after the CLOSE").isEqualTo(-1);
		assertThat(read).isEqualTo(total);
		assertThat(writer.frames).noneMatch(frame -> frame.startsWith("error"));
	}

	@Test
	void closeDropsTheBufferedSegments() throws Exception {
		RecordingWriter writer = new RecordingWriter();
		VirtualConnection connection = windowed(34, writer, 1000, 1);
		for (int i = 0; i < 100; i++) {
			connection.acceptData(new byte[3]);
		}
		assertThat(connection.bufferedBytes()).as("precondition").isEqualTo(300);
		connection.close();
		assertThat(connection.bufferedSegments()).isZero();
		assertThat(connection.bufferedCapacity()).isZero();
		assertThat(connection.source().read()).isEqualTo(-1);
	}

	@Test
	void windowExceededDropsTheBufferedSegments() throws Exception {
		RecordingWriter writer = new RecordingWriter();
		VirtualConnection connection = windowed(35, writer, 1000, 1);
		for (int i = 0; i < 100; i++) {
			connection.acceptData(new byte[10]);
		}
		assertThat(connection.bufferedBytes()).as("precondition").isEqualTo(1000);
		connection.acceptData(new byte[1]);
		assertThat(writer.frames).containsExactly("error:35:flow control window exceeded");
		assertThat(connection.bufferedSegments()).isZero();
		assertThat(connection.bufferedCapacity()).isZero();
	}

	@Test
	void windowedPayloadAfterTheRemoteClosedIsNotBuffered() throws Exception {
		RecordingWriter writer = new RecordingWriter();
		VirtualConnection connection = windowed(36, writer, 1000, 1);
		connection.acceptData("ab".getBytes());
		connection.remoteClosed();
		connection.acceptData("cd".getBytes());
		assertThat(connection.bufferedBytes()).isEqualTo(2);
		assertThat(connection.source().readAllBytes()).isEqualTo("ab".getBytes());
	}

	@Test
	void emptyPayloadEndsTheStreamAfterTheBufferedData() throws Exception {
		RecordingWriter writer = new RecordingWriter();
		VirtualConnection connection = windowed(37, writer, 1000, 1);
		connection.acceptData("ab".getBytes());
		connection.acceptData(new byte[0]);
		connection.acceptData("cd".getBytes());
		assertThat(connection.source().readAllBytes()).isEqualTo("ab".getBytes());
	}

	/**
	 * A writer whose {@code sendData} parks until {@link #proceed} opens, and whose
	 * {@code afterQueued} holds each action until {@link #drain} runs it -- standing in
	 * for frames still queued.
	 */
	private static final class HoldingWriter implements FrameWriter {

		final List<String> calls = new CopyOnWriteArrayList<>();

		final CountDownLatch entered = new CountDownLatch(1);

		final CountDownLatch proceed = new CountDownLatch(1);

		final java.util.Map<Long, List<Runnable>> held = new java.util.concurrent.ConcurrentHashMap<>();

		@Override
		public void sendConnect(Connect connect) {
		}

		@Override
		public boolean sendData(long connectionId, byte[] payload) {
			this.calls.add("data:%d:%d".formatted(connectionId, payload.length));
			this.entered.countDown();
			try {
				this.proceed.await();
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return false;
			}
			this.calls.add("data-returned:%d".formatted(connectionId));
			return true;
		}

		@Override
		public void sendClose(long connectionId) {
			this.calls.add("close:%d".formatted(connectionId));
		}

		@Override
		public void sendError(long connectionId, String message) {
			this.calls.add("error:%d".formatted(connectionId));
		}

		@Override
		public void sendWindowUpdate(long connectionId, int increment) {
		}

		@Override
		public void afterQueued(long connectionId, Runnable action) {
			this.calls.add("after-queued:%d".formatted(connectionId));
			this.held.computeIfAbsent(connectionId, id -> new CopyOnWriteArrayList<>()).add(action);
		}

		/** The connection's queued frames left: runs what was held for it. */
		void drain(long connectionId) {
			List<Runnable> actions = this.held.remove(connectionId);
			if (actions != null) {
				actions.forEach(Runnable::run);
			}
		}

	}

	/**
	 * A send in flight when the connection is closed may still queue its frames: the
	 * action reaches the frame writer only once that send returned, so it covers them.
	 */
	@Test
	void afterQueuedWaitsForTheSendsInFlight() throws Exception {
		HoldingWriter writer = new HoldingWriter();
		VirtualConnection connection = windowed(40, writer, 1000, 1000);
		AsyncWrite write = AsyncWrite.start(connection.sink(), new byte[10]);
		assertThat(writer.entered.await(5, TimeUnit.SECONDS)).as("precondition: the send is in flight").isTrue();
		connection.close();
		java.util.concurrent.atomic.AtomicInteger runs = new java.util.concurrent.atomic.AtomicInteger();
		connection.afterQueued(runs::incrementAndGet);
		assertThat(writer.calls).as("not handed over while the send runs").doesNotContain("after-queued:40");
		writer.proceed.countDown();
		write.done().get(5, TimeUnit.SECONDS);
		awaitCall(writer, "after-queued:40");
		assertThat(writer.calls.indexOf("after-queued:40")).as("after the send returned")
			.isGreaterThan(writer.calls.indexOf("data-returned:40"));
		assertThat(runs).as("held while frames are queued").hasValue(0);
		writer.drain(40);
		assertThat(runs).hasValue(1);
	}

	@Test
	void afterQueuedEndsTheSendingSide() throws Exception {
		RecordingWriter writer = new RecordingWriter();
		VirtualConnection connection = VirtualConnection.builder(41, writer).build();
		java.util.concurrent.atomic.AtomicInteger runs = new java.util.concurrent.atomic.AtomicInteger();
		connection.afterQueued(runs::incrementAndGet);
		assertThat(runs).as("nothing in flight: handed over at once").hasValue(1);
		assertThatThrownBy(() -> connection.sink().write(new byte[1])).isInstanceOf(IOException.class);
		assertThat(writer.frames).as("no DATA after the action was handed over").isEmpty();
	}

	@Test
	void afterQueuedOfSeveralConnectionsWaitsForEach() {
		HoldingWriter first = new HoldingWriter();
		HoldingWriter second = new HoldingWriter();
		java.util.concurrent.atomic.AtomicInteger runs = new java.util.concurrent.atomic.AtomicInteger();
		VirtualConnection.afterQueued(List.of(VirtualConnection.builder(42, first).build(),
				VirtualConnection.builder(43, second).build(), VirtualConnection.builder(44, second).build()),
				runs::incrementAndGet);
		first.drain(42);
		second.drain(44);
		assertThat(runs).as("connection 43 still has frames queued").hasValue(0);
		second.drain(43);
		assertThat(runs).hasValue(1);
	}

	private static void awaitCall(HoldingWriter writer, String call) throws InterruptedException {
		long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(5);
		while (!writer.calls.contains(call) && System.currentTimeMillis() < deadline) {
			TimeUnit.MILLISECONDS.sleep(10);
		}
		assertThat(writer.calls).contains(call);
	}

}
