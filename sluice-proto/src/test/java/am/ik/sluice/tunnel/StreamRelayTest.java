package am.ik.sluice.tunnel;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import am.ik.sluice.v1.proto.Connect;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StreamRelayTest {

	private static final class RecordingWriter implements FrameWriter {

		final CopyOnWriteArrayList<String> frames = new CopyOnWriteArrayList<>();

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
			this.frames.add("data:%d:%s".formatted(connectionId, new String(payload, StandardCharsets.UTF_8)));
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

	private record SocketPair(Socket client, Socket serverSide) {

		static SocketPair create() throws IOException {
			return create(0);
		}

		/**
		 * Listens on the loopback address the client dials, so a listener of another
		 * process on the same port number cannot take the connection, and bounds
		 * {@code accept} so a stray connection fails the test instead of hanging it.
		 */
		static SocketPair create(int port) throws IOException {
			try (ServerSocket server = new ServerSocket(port, 1, InetAddress.getByName("127.0.0.1"))) {
				server.setSoTimeout(5_000);
				Socket client = new Socket("127.0.0.1", server.getLocalPort());
				Socket serverSide = server.accept();
				return new SocketPair(client, serverSide);
			}
		}

	}

	@Test
	void aListenerOfAnotherProcessOnTheSamePortNumberFailsThePairInsteadOfHangingIt() throws Exception {
		try (ServerSocket stray = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"))) {
			int port = stray.getLocalPort();
			ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
			Future<SocketPair> created = executor.submit(() -> SocketPair.create(port));
			try {
				assertThatThrownBy(() -> created.get(15, TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class)
					.hasCauseInstanceOf(IOException.class);
			}
			finally {
				executor.shutdownNow();
			}
		}
	}

	@Test
	@Timeout(10)
	void prefixIsRelayedBeforeReadBytes() throws Exception {
		SocketPair pair = SocketPair.create();
		RecordingWriter writer = new RecordingWriter();
		VirtualConnection connection = new VirtualConnection(1, writer);
		CountDownLatch done = new CountDownLatch(1);
		StreamRelay relay = StreamRelay.builder(DuplexPipe.of(pair.serverSide()), connection, writer)
			.prefix("HEAD".getBytes(StandardCharsets.UTF_8))
			.onComplete(done::countDown)
			.build();
		relay.start();
		pair.client().getOutputStream().write("BODY".getBytes(StandardCharsets.UTF_8));
		pair.client().shutdownOutput();
		connection.remoteClosed();
		assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
		assertThat(writer.frames).startsWith("data:1:HEAD", "data:1:BODY").contains("close:1");
		pair.client().close();
	}

	@Test
	@Timeout(10)
	void remoteCloseShutsDownLocalOutputOnly() throws Exception {
		SocketPair pair = SocketPair.create();
		RecordingWriter writer = new RecordingWriter();
		VirtualConnection connection = new VirtualConnection(2, writer);
		CountDownLatch done = new CountDownLatch(1);
		StreamRelay relay = StreamRelay.builder(DuplexPipe.of(pair.serverSide()), connection, writer)
			.onComplete(done::countDown)
			.build();
		relay.start();
		// remote (tunnel side) closes its output -> Eof on the connection source
		connection.remoteClosed();
		InputStream clientIn = pair.client().getInputStream();
		byte[] buffer = new byte[16];
		int total = 0;
		int n;
		while ((n = clientIn.read(buffer)) > 0) {
			total += n;
		}
		// reads back EOF but the local socket is not fully closed yet: writes still work
		assertThat(total).isZero();
		pair.client().getOutputStream().write("still-writable".getBytes(StandardCharsets.UTF_8));
		pair.client().shutdownOutput();
		assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
		assertThat(writer.frames).contains("close:2");
	}

	/**
	 * The remote failing the connection after its half-close -- the local pipe's output
	 * already shut down, its input still open -- tears the relay down: the remote's peer
	 * is gone, so nothing read from the pipe can reach it.
	 */
	@Test
	@Timeout(10)
	void remoteErrorAfterItsCloseTearsTheRelayDown() throws Exception {
		SocketPair pair = SocketPair.create();
		RecordingWriter writer = new RecordingWriter();
		VirtualConnection connection = new VirtualConnection(5, writer);
		CountDownLatch done = new CountDownLatch(1);
		StreamRelay relay = StreamRelay.builder(DuplexPipe.of(pair.serverSide()), connection, writer)
			.onComplete(done::countDown)
			.build();
		relay.start();
		connection.remoteClosed();
		assertThat(pair.client().getInputStream().read()).as("the half-close reached the local peer").isEqualTo(-1);
		assertThat(done.await(200, TimeUnit.MILLISECONDS)).as("the relay waits for the local side").isFalse();
		connection.remoteFailed("connection closed");
		assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
		assertThat(pair.serverSide().isClosed()).isTrue();
		pair.client().close();
	}

	@Test
	@Timeout(10)
	void localErrorSendsErrorFrameAndClosesPipe() throws Exception {
		SocketPair pair = SocketPair.create();
		pair.client().close();
		Socket server = pair.serverSide();
		RecordingWriter writer = new RecordingWriter();
		VirtualConnection connection = new VirtualConnection(3, writer);
		connection.acceptData("x".getBytes(StandardCharsets.UTF_8));
		server.close(); // force IO errors on both relay directions
		CountDownLatch done = new CountDownLatch(1);
		StreamRelay relay = StreamRelay.builder(DuplexPipe.of(server), connection, writer)
			.onComplete(done::countDown)
			.build();
		relay.start();
		assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
		assertThat(server.isClosed()).isTrue();
	}

	@Test
	@Timeout(10)
	void listenerCountsRelayedBytes() throws Exception {
		SocketPair pair = SocketPair.create();
		RecordingWriter writer = new RecordingWriter();
		VirtualConnection connection = new VirtualConnection(4, writer);
		AtomicLong bytes = new AtomicLong();
		CountDownLatch done = new CountDownLatch(1);
		StreamRelay relay = StreamRelay.builder(DuplexPipe.of(pair.serverSide()), connection, writer)
			.prefix("P".getBytes(StandardCharsets.UTF_8))
			.listener(bytes::getAndAdd)
			.onComplete(done::countDown)
			.build();
		relay.start();
		OutputStream out = pair.client().getOutputStream();
		out.write("hello".getBytes(StandardCharsets.UTF_8));
		out.flush();
		pair.client().shutdownOutput();
		connection.remoteClosed();
		assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
		assertThat(bytes.get()).isEqualTo(6); // prefix(1) + hello(5)
		pair.client().close();
	}

	@Test
	@Timeout(10)
	void directionLessListenerSeesBothDirections() throws Exception {
		SocketPair pair = SocketPair.create();
		RecordingWriter writer = new RecordingWriter();
		VirtualConnection connection = new VirtualConnection(5, writer);
		AtomicLong bytes = new AtomicLong();
		CountDownLatch done = new CountDownLatch(1);
		StreamRelay relay = StreamRelay.builder(DuplexPipe.of(pair.serverSide()), connection, writer)
			.listener(bytes::getAndAdd)
			.onComplete(done::countDown)
			.build();
		relay.start();
		OutputStream out = pair.client().getOutputStream();
		out.write("abc".getBytes(StandardCharsets.UTF_8));
		out.flush();
		pair.client().shutdownOutput();
		connection.acceptData("xy".getBytes(StandardCharsets.UTF_8));
		connection.remoteClosed();
		assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
		assertThat(bytes.get()).isEqualTo(5); // abc(3) to remote + xy(2) to local
		pair.client().close();
	}

	@Test
	@Timeout(10)
	void directionalListenerReceivesPerDirectionTotals() throws Exception {
		SocketPair pair = SocketPair.create();
		RecordingWriter writer = new RecordingWriter();
		VirtualConnection connection = new VirtualConnection(6, writer);
		AtomicLong toRemote = new AtomicLong();
		AtomicLong toLocal = new AtomicLong();
		CountDownLatch done = new CountDownLatch(1);
		StreamRelay relay = StreamRelay.builder(DuplexPipe.of(pair.serverSide()), connection, writer)
			.listener(new StreamRelay.Listener() {

				@Override
				public void onBytesRelayed(long count) {
				}

				@Override
				public void onBytesRelayed(long count, StreamRelay.Direction direction) {
					(direction == StreamRelay.Direction.TO_REMOTE ? toRemote : toLocal).addAndGet(count);
				}
			})
			.onComplete(done::countDown)
			.build();
		relay.start();
		OutputStream out = pair.client().getOutputStream();
		out.write("hello".getBytes(StandardCharsets.UTF_8));
		out.flush();
		pair.client().shutdownOutput();
		connection.acceptData("hi".getBytes(StandardCharsets.UTF_8));
		connection.remoteClosed();
		assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
		assertThat(toRemote.get()).isEqualTo(5);
		assertThat(toLocal.get()).isEqualTo(2);
		pair.client().close();
	}

	/**
	 * The relay's sending side parked at the session sender's per-connection cap (the
	 * stream never becomes ready) returns once the relay is abandoned, so the relay
	 * completes instead of waiting for the stream to drain.
	 */
	@Test
	@Timeout(10)
	void abandonReleasesTheSendingSideParkedAtTheSenderCap() throws Exception {
		SocketPair pair = SocketPair.create();
		FakeServerObserver observer = new FakeServerObserver();
		SessionSender sender = new SessionSender(observer);
		sender.start();
		try {
			VirtualConnection connection = new VirtualConnection(7, sender);
			CountDownLatch done = new CountDownLatch(1);
			StreamRelay relay = StreamRelay.builder(DuplexPipe.of(pair.serverSide()), connection, sender)
				.onComplete(done::countDown)
				.build();
			relay.start();
			// more than the cap holds, on its own thread: what the relay cannot take
			// stays in the socket buffers or blocks this writer
			Thread.ofVirtual().start(() -> {
				try {
					OutputStream out = pair.client().getOutputStream();
					byte[] chunk = new byte[SessionSender.MAX_CHUNK];
					for (int i = 0; i <= 2 * SessionSender.DATA_QUEUE_CAPACITY; i++) {
						out.write(chunk);
					}
				}
				catch (IOException e) {
					// torn down
				}
			});
			// a cap waiter of connection 7 completed a timed wait: the sending side is
			// parked on the cap, not merely slow
			awaitCapWaiter(sender, 7);
			relay.abandon();
			assertThat(done.await(1, TimeUnit.SECONDS)).as("the relay completes once abandoned").isTrue();
		}
		finally {
			sender.close();
			pair.client().close();
		}
	}

	private static void awaitCapWaiter(SessionSender sender, long connectionId) throws InterruptedException {
		long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(5);
		while (sender.capWaiterWakeups(connectionId) == 0 && System.currentTimeMillis() < deadline) {
			TimeUnit.MILLISECONDS.sleep(10);
		}
		assertThat(sender.capWaiterWakeups(connectionId)).as("cap waits of connection %d", connectionId)
			.isGreaterThan(0);
	}

}
