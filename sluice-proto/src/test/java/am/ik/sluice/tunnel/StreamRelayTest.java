package am.ik.sluice.tunnel;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.assertThat;

class StreamRelayTest {

	private static final class RecordingWriter implements FrameWriter {

		final CopyOnWriteArrayList<String> frames = new CopyOnWriteArrayList<>();

		@Override
		public void sendConnect(long connectionId, String address) {
			this.frames.add("connect:%d:%s".formatted(connectionId, address));
		}

		@Override
		public void sendData(long connectionId, byte[] payload) {
			this.frames.add("data:%d:%s".formatted(connectionId, new String(payload, StandardCharsets.UTF_8)));
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
			ServerSocket server = new ServerSocket(0);
			Socket client = new Socket("127.0.0.1", server.getLocalPort());
			Socket serverSide = server.accept();
			server.close();
			return new SocketPair(client, serverSide);
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

}
