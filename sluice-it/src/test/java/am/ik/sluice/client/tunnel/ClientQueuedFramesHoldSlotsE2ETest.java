package am.ik.sluice.client.tunnel;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.client.config.Upstream;
import am.ik.sluice.tunnel.ConnectionLimit;
import am.ik.sluice.tunnel.VirtualConnection;
import am.ik.sluice.v1.proto.AdvertiseAck;
import am.ik.sluice.v1.proto.Close;
import am.ik.sluice.v1.proto.Connect;
import am.ik.sluice.v1.proto.Frame;
import am.ik.sluice.v1.proto.TunnelGrpc;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The client keeps a connection's slot of its connection limit while frames the
 * connection queued for the tunnel stream still wait in the client's sender. The stream
 * is held not ready by a server that stops reading it (its receive callback blocks).
 */
class ClientQueuedFramesHoldSlotsE2ETest {

	private static final int CONNECTIONS = 6;

	private final List<Socket> upstreamSockets = new CopyOnWriteArrayList<>();

	private final StallingServer service = new StallingServer();

	private final ConnectionLimit limit = new ConnectionLimit(64);

	private @Nullable Server server;

	private @Nullable ServerSocket upstream;

	private @Nullable NodeConnection connection;

	@BeforeEach
	void start() throws IOException {
		Server server = NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0))
			.addService(this.service)
			.build()
			.start();
		this.server = server;
		ServerSocket upstream = new ServerSocket();
		upstream.bind(new InetSocketAddress("127.0.0.1", 0));
		this.upstream = upstream;
		// every dialed connection receives half a window and is half-closed by the
		// upstream
		Thread.ofVirtual().name("e2e-client-slot-upstream").start(() -> {
			while (!upstream.isClosed()) {
				try {
					Socket socket = upstream.accept();
					this.upstreamSockets.add(socket);
					OutputStream out = socket.getOutputStream();
					out.write(new byte[VirtualConnection.DEFAULT_WINDOW / 2]);
					socket.shutdownOutput();
				}
				catch (IOException e) {
					// closed, or the relay went away
				}
			}
		});
		SluiceClientProperties properties = SluiceClientProperties.builder()
			.serverUrl("grpc://127.0.0.1:" + server.getPort())
			.upstream(
					Upstream.builder().host("held.local").target("http://127.0.0.1:" + upstream.getLocalPort()).build())
			.strictForwarding(true)
			.build();
		NodeConnection connection = NodeConnection.builder()
			.key("stalling")
			.url(properties.serverUrl())
			.properties(properties)
			.clientId("client-slots")
			.taskExecutor(task -> Thread.ofVirtual().name("e2e-client-slot-worker").start(task))
			.listener(new NodeConnection.Listener() {

				@Override
				public void onConnected(NodeConnection connection, String nodeId) {
				}

				@Override
				public void onMembership(List<am.ik.sluice.v1.proto.Node> nodes, long membershipVersion) {
				}

				@Override
				public void onDrain(NodeConnection connection, String reason) {
				}
			})
			.meterRegistry(new SimpleMeterRegistry())
			.connectionLimit(this.limit)
			.build();
		this.connection = connection;
		connection.start();
		Awaitility.await().atMost(Duration.ofSeconds(10)).until(connection::isConnected);
	}

	@AfterEach
	void stop() throws IOException {
		this.service.resume.countDown();
		NodeConnection connection = this.connection;
		if (connection != null) {
			connection.close();
		}
		Server server = this.server;
		if (server != null) {
			server.shutdownNow();
		}
		ServerSocket upstream = this.upstream;
		if (upstream != null) {
			upstream.close();
		}
		for (Socket socket : this.upstreamSockets) {
			socket.close();
		}
	}

	/**
	 * Ended connections whose DATA and CLOSE wait in the sender keep their slots until
	 * the frames left; so does a connection whose dial was refused, until its ERROR left.
	 */
	@Test
	void endedConnectionsHoldTheirSlotsWhileTheirFramesAreQueued() throws Exception {
		NodeConnection connection = connection();
		this.service.stall.set(true);
		for (long id = 1; id <= CONNECTIONS; id++) {
			this.service.connectAndClose(id, "127.0.0.1:" + upstream().getLocalPort());
		}
		Awaitility.await()
			.atMost(Duration.ofSeconds(10))
			.alias("every relay ended")
			.until(() -> this.upstreamSockets.size() == CONNECTIONS && connection.activeConnections() == 0);
		int relayed = settledQueues();
		assertThat(relayed).as("precondition: frames of ended connections are queued").isPositive();
		assertThat(this.service.blocked.get()).as("precondition: the server stopped reading").isTrue();
		// on the stalled stream, the ERROR of a refused dial is queued too
		this.service.connectAndClose(CONNECTIONS + 1, "127.0.0.1:1");
		Awaitility.await()
			.atMost(Duration.ofSeconds(5))
			.alias("the refused connection's ERROR is queued")
			.until(() -> connection.queuedConnectionsForTest() == relayed + 1);
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> connection.activeConnections() == 0);
		int queued = relayed + 1;
		long deadline = System.nanoTime() + Duration.ofMillis(300).toNanos();
		do {
			assertThat(this.limit.open()).isEqualTo(connection.queuedConnectionsForTest());
			TimeUnit.MILLISECONDS.sleep(20);
		}
		while (System.nanoTime() < deadline);
		assertThat(this.limit.open()).isEqualTo(queued);

		this.service.resume.countDown();
		Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> connection.queuedConnectionsForTest() == 0);
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> this.limit.open() == 0);
	}

	/** The end of the stream drops the queued frames and frees their slots. */
	@Test
	void theStreamEndFreesTheSlotsOfQueuedFrames() throws Exception {
		NodeConnection connection = connection();
		this.service.stall.set(true);
		for (long id = 1; id <= CONNECTIONS; id++) {
			this.service.connectAndClose(id, "127.0.0.1:" + upstream().getLocalPort());
		}
		Awaitility.await()
			.atMost(Duration.ofSeconds(10))
			.alias("every relay ended")
			.until(() -> this.upstreamSockets.size() == CONNECTIONS && connection.activeConnections() == 0);
		int queued = settledQueues();
		assertThat(queued).as("precondition: frames of ended connections are queued").isPositive();
		assertThat(this.limit.open()).as("precondition: their slots are held").isEqualTo(queued);

		this.service.end();
		Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> this.limit.open() == 0);
	}

	/** The sender's live queues once they stopped changing for 300ms. */
	private int settledQueues() throws InterruptedException {
		long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
		int last = -1;
		long since = System.nanoTime();
		while (System.nanoTime() < deadline) {
			int current = connection().queuedConnectionsForTest();
			if (current != last) {
				last = current;
				since = System.nanoTime();
			}
			else if (System.nanoTime() - since > Duration.ofMillis(300).toNanos()) {
				return current;
			}
			TimeUnit.MILLISECONDS.sleep(20);
		}
		return last;
	}

	private NodeConnection connection() {
		return Objects.requireNonNull(this.connection, "connection");
	}

	private ServerSocket upstream() {
		return Objects.requireNonNull(this.upstream, "upstream");
	}

	/**
	 * A tunnel server that acknowledges the advertise and sends the CONNECTs the test
	 * asks for; once {@link #stall} is set, its receive callback blocks until
	 * {@link #resume}, so it stops reading the stream.
	 */
	private static final class StallingServer extends TunnelGrpc.TunnelImplBase {

		final AtomicBoolean stall = new AtomicBoolean();

		final AtomicBoolean blocked = new AtomicBoolean();

		final CountDownLatch resume = new CountDownLatch(1);

		private final AtomicReference<@Nullable StreamObserver<Frame>> responses = new AtomicReference<>();

		@Override
		public StreamObserver<Frame> connect(StreamObserver<Frame> responses) {
			this.responses.set(responses);
			return new StreamObserver<>() {

				@Override
				public void onNext(Frame frame) {
					if (frame.getBodyCase() == Frame.BodyCase.ADVERTISE) {
						send(Frame.newBuilder()
							.setAdvertiseAck(AdvertiseAck.newBuilder().setNodeId("stalling"))
							.build());
						return;
					}
					if (StallingServer.this.stall.get()) {
						StallingServer.this.blocked.set(true);
						try {
							StallingServer.this.resume.await();
						}
						catch (InterruptedException e) {
							Thread.currentThread().interrupt();
						}
					}
				}

				@Override
				public void onError(Throwable t) {
				}

				@Override
				public void onCompleted() {
				}
			};
		}

		/**
		 * Asks the client to dial {@code address} and half-closes the server's side at
		 * once, so the relay ends once the upstream's side ended.
		 */
		void connectAndClose(long id, String address) {
			send(Frame.newBuilder()
				.setConnect(Connect.newBuilder()
					.setConnId(id)
					.setAddress(address)
					.setConnectionWindow(VirtualConnection.DEFAULT_WINDOW))
				.build());
			send(Frame.newBuilder().setClose(Close.newBuilder().setConnId(id)).build());
		}

		/** Ends the stream from the server side. */
		synchronized void end() {
			StreamObserver<Frame> responses = this.responses.get();
			if (responses != null) {
				responses.onCompleted();
			}
		}

		private synchronized void send(Frame frame) {
			StreamObserver<Frame> responses = this.responses.get();
			if (responses != null) {
				responses.onNext(frame);
			}
		}

	}

}
