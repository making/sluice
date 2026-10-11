package am.ik.sluice.tunnel;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import am.ik.sluice.v1.proto.Frame;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class VirtualConnectionTest {

	/** Records every frame without a real stream. */
	private static final class RecordingWriter implements FrameWriter {

		final List<String> frames = new ArrayList<>();

		final AtomicLong bytes = new AtomicLong();

		@Override
		public void sendConnect(long connectionId, String address) {
			this.frames.add("connect:%d:%s".formatted(connectionId, address));
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
		VirtualConnection connection = new VirtualConnection(1, writer);
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
		VirtualConnection connection = new VirtualConnection(2, writer);
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
		VirtualConnection connection = new VirtualConnection(4, writer);
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
		VirtualConnection connection = new VirtualConnection(5, writer);
		connection.remoteFailed("boom");
		assertThatThrownBy(() -> connection.source().read()).isInstanceOf(IOException.class).hasMessage("boom");
	}

	@Test
	void closeWakesBlockedReader() throws Exception {
		RecordingWriter writer = new RecordingWriter();
		VirtualConnection connection = new VirtualConnection(6, writer);
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
		assertThat(writer.frames).contains("close:6");
	}

	@Test
	void remoteClosedIsNotBlockedByFullInboundQueue() throws Exception {
		RecordingWriter writer = new RecordingWriter();
		VirtualConnection connection = new VirtualConnection(7, writer);
		byte[] chunk = new byte[1000];
		for (int i = 0; i < 128; i++) { // fill the 128-chunk inbound bound
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
	void remoteFailedIsNotBlockedByFullInboundQueue() throws Exception {
		RecordingWriter writer = new RecordingWriter();
		VirtualConnection connection = new VirtualConnection(8, writer);
		byte[] chunk = new byte[1000];
		for (int i = 0; i < 128; i++) { // fill the 128-chunk inbound bound
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
	void acceptDataBlockedOnFullBoundReturnsAfterClose() throws Exception {
		RecordingWriter writer = new RecordingWriter();
		VirtualConnection connection = new VirtualConnection(9, writer);
		byte[] chunk = new byte[1000];
		for (int i = 0; i < 128; i++) { // fill the 128-chunk inbound bound
			connection.acceptData(chunk);
		}
		CountDownLatch entered = new CountDownLatch(1);
		Thread producer = Thread.ofVirtual().start(() -> {
			entered.countDown();
			connection.acceptData(chunk);
		});
		assertThat(entered.await(1, TimeUnit.SECONDS)).isTrue();
		Thread.sleep(200);
		assertThat(producer.isAlive()).as("precondition: producer parked on the full bound").isTrue();
		// the local side gives up on the connection (relay ended)
		connection.close();
		producer.join(1000);
		assertThat(producer.isAlive()).as("acceptData returns once the connection is closed").isFalse();
	}

	@Test
	void repeatedRemoteClosedKeepASinglePendingMarker() throws Exception {
		RecordingWriter writer = new RecordingWriter();
		VirtualConnection connection = new VirtualConnection(10, writer);
		byte[] chunk = new byte[1000];
		for (int i = 0; i < 128; i++) { // fill the 128-chunk inbound bound
			connection.acceptData(chunk);
		}
		for (int i = 0; i < 1000; i++) {
			connection.remoteClosed();
		}
		assertThat(connection.pendingInbound()).as("the chunks plus one EOF marker").isEqualTo(129);
		byte[] all = connection.source().readNBytes(128 * 1000);
		assertThat(all).hasSize(128 * 1000);
		assertThat(connection.source().read()).as("EOF after all data").isEqualTo(-1);
		// past the consumed EOF the reader stops taking: further markers must not pile up
		int drained = connection.pendingInbound();
		for (int i = 0; i < 1000; i++) {
			connection.remoteClosed();
		}
		assertThat(connection.pendingInbound()).as("no growth once the marker was consumed").isEqualTo(drained);
	}

	@Test
	void repeatedRemoteFailedKeepASinglePendingMarker() throws Exception {
		RecordingWriter writer = new RecordingWriter();
		VirtualConnection connection = new VirtualConnection(11, writer);
		byte[] chunk = new byte[1000];
		for (int i = 0; i < 128; i++) { // fill the 128-chunk inbound bound
			connection.acceptData(chunk);
		}
		for (int i = 0; i < 1000; i++) {
			connection.remoteFailed("boom");
		}
		assertThat(connection.pendingInbound()).as("the chunks plus one Failure marker").isEqualTo(129);
		assertThat(connection.source().readNBytes(128 * 1000)).hasSize(128 * 1000);
		assertThatThrownBy(() -> connection.source().read()).isInstanceOf(IOException.class).hasMessage("boom");
	}

}
