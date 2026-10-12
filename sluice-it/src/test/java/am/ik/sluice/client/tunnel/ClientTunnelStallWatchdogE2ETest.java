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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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
import am.ik.sluice.v1.proto.WindowUpdate;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The client's tunnel stall watchdog ({@code sluice.client.tunnel-stall-timeout}): a
 * server that stops reading the stream, while keeping the connection alive, has the
 * stream cancelled once frames waited for the timeout with none taken; the slots of the
 * connections whose frames were queued are freed, the connections of another node's
 * stream keep theirs, and the client reconnects. A server that reads slowly but keeps
 * taking frames is not cut.
 */
class ClientTunnelStallWatchdogE2ETest {

	private static final Duration TIMEOUT = Duration.ofSeconds(2);

	private final List<Socket> upstreamSockets = new CopyOnWriteArrayList<>();

	/** Bytes the upstream writes to each connection before it ends its side. */
	private final AtomicInteger upstreamBytes = new AtomicInteger(VirtualConnection.DEFAULT_WINDOW / 2);

	private final ConnectionLimit limit = new ConnectionLimit(64);

	private final MeterRegistry meterRegistry = new SimpleMeterRegistry();

	private final List<Server> servers = new CopyOnWriteArrayList<>();

	private final List<NodeConnection> connections = new CopyOnWriteArrayList<>();

	private final List<FakeServer> services = new CopyOnWriteArrayList<>();

	private @Nullable ServerSocket upstream;

	@BeforeEach
	void startUpstream() throws IOException {
		ServerSocket upstream = new ServerSocket();
		upstream.bind(new InetSocketAddress("127.0.0.1", 0));
		this.upstream = upstream;
		Thread.ofVirtual().name("e2e-client-stall-upstream").start(() -> {
			while (!upstream.isClosed()) {
				try {
					Socket socket = upstream.accept();
					this.upstreamSockets.add(socket);
					Thread.ofVirtual().start(() -> {
						try {
							OutputStream out = socket.getOutputStream();
							out.write(new byte[this.upstreamBytes.get()]);
							socket.shutdownOutput();
						}
						catch (IOException e) {
							// the relay went away
						}
					});
				}
				catch (IOException e) {
					// closed
				}
			}
		});
	}

	@AfterEach
	void stop() throws IOException {
		for (FakeServer service : this.services) {
			service.resume.countDown();
		}
		for (NodeConnection connection : this.connections) {
			connection.close();
		}
		for (Server server : this.servers) {
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
	 * Ended connections whose frames wait on the stalled stream hold their slots until
	 * the watchdog cancels the stream; then only the other node's live connection holds
	 * one, the server sees its stream cancelled, and the client is back on a new stream.
	 */
	@Test
	void aStalledStreamIsCancelledPastTheTimeoutItsSlotsFreedAndReconnected() throws Exception {
		FakeServer healthy = new FakeServer();
		start(healthy, "healthy");
		FakeServer stalling = new FakeServer();
		NodeConnection connection = start(stalling, "stalling");
		assertThat(connection.stallTimeoutForTest()).as("precondition: the configured timeout took effect")
			.isEqualTo(TIMEOUT);
		// the healthy node's connection: the server never ends its side, so it stays live
		healthy.connect(1, upstreamAddress());
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> this.limit.open() == 1);

		long stallStart = System.nanoTime();
		stalling.stall.set(true);
		int count = 6;
		for (long id = 1; id <= count; id++) {
			stalling.connect(id, upstreamAddress());
			stalling.close(id);
		}
		Awaitility.await()
			.atMost(Duration.ofSeconds(10))
			.alias("every relay of the stalled stream ended")
			.until(() -> this.upstreamSockets.size() == count + 1 && connection.activeConnections() == 0);
		int queued = connection.queuedConnectionsForTest();
		assertThat(queued).as("precondition: frames of ended connections are queued").isPositive();
		assertThat(stalling.blocked.get()).as("precondition: the server stopped reading").isTrue();
		assertThat(this.limit.open()).as("precondition: their slots are held").isEqualTo(1 + queued);
		Awaitility.await().atMost(TIMEOUT).alias("the stall is measured").until(() -> stalledGauge(stalling) > 0);

		Awaitility.await()
			.atMost(TIMEOUT.plusSeconds(5))
			.alias("the slots of the stalled stream are freed")
			.until(() -> this.limit.open() == 1);
		assertThat(Duration.ofNanos(System.nanoTime() - stallStart)).as("not before the timeout")
			.isGreaterThanOrEqualTo(TIMEOUT);
		assertThat(stallFailures(stalling)).as("counted").isEqualTo(1);
		assertThat(stallFailures(healthy)).isZero();

		stalling.resume.countDown();
		assertThat(stalling.firstStreamEnded.await(5, TimeUnit.SECONDS)).as("the server's stream ended").isTrue();
		assertThat(stalling.firstStreamStatus.get()).as("cancelled, not half-closed behind the frames")
			.extracting(Status::getCode)
			.isEqualTo(Status.Code.CANCELLED);
		Awaitility.await()
			.atMost(Duration.ofSeconds(10))
			.alias("the client reconnected")
			.until(() -> stalling.streams.get() == 2 && connection.isConnected());
		stalling.stall.set(false);
		int dialed = this.upstreamSockets.size();
		stalling.connect(1, upstreamAddress());
		Awaitility.await()
			.atMost(Duration.ofSeconds(5))
			.alias("the new stream dials")
			.until(() -> this.upstreamSockets.size() == dialed + 1);
	}

	/**
	 * A server that reads the stream slowly -- the stream not ready for stretches, each
	 * shorter than the timeout -- keeps the stream over many timeouts and gets every
	 * byte.
	 */
	@Test
	void aServerThatKeepsTakingFramesSlowlyIsNotCut() throws Exception {
		int total = 6 * 1024 * 1024;
		this.upstreamBytes.set(total);
		FakeServer slow = new FakeServer();
		slow.millisPer64KiB.set(100);
		NodeConnection connection = start(slow, "slow");
		long start = System.nanoTime();
		slow.connect(1, upstreamAddress());
		long longestStall = 0;
		while (slow.received.get() < total && slow.streams.get() == 1
				&& System.nanoTime() - start < TimeUnit.SECONDS.toNanos(60)) {
			longestStall = Math.max(longestStall, (long) (stalledGauge(slow) * TimeUnit.SECONDS.toNanos(1)));
			TimeUnit.MILLISECONDS.sleep(10);
		}

		assertThat(slow.received.get()).as("every byte arrived").isEqualTo(total);
		assertThat(Duration.ofNanos(System.nanoTime() - start)).as("precondition: the transfer outlasts the timeout")
			.isGreaterThan(TIMEOUT.multipliedBy(2));
		assertThat(Duration.ofNanos(longestStall)).as("precondition: the stream stalled between reads")
			.isGreaterThan(Duration.ofMillis(200));
		assertThat(stallFailures(slow)).as("not cut").isZero();
		assertThat(slow.streams.get()).as("the same stream").isEqualTo(1);
		assertThat(connection.isConnected()).isTrue();
	}

	private NodeConnection start(FakeServer service, String key) throws IOException {
		this.services.add(service);
		Server server = NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0))
			.addService(service)
			.build()
			.start();
		this.servers.add(server);
		service.url = "grpc://127.0.0.1:" + server.getPort();
		SluiceClientProperties properties = SluiceClientProperties.builder()
			.serverUrl(service.url)
			.upstream(Upstream.builder().host("held.local").target("http://" + upstreamAddress()).build())
			.strictForwarding(true)
			.tunnelStallTimeout(TIMEOUT)
			.build();
		NodeConnection connection = NodeConnection.builder()
			.key(key)
			.url(properties.serverUrl())
			.properties(properties)
			.clientId("client-stall")
			.taskExecutor(task -> Thread.ofVirtual().name("e2e-client-stall-worker").start(task))
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
			.meterRegistry(this.meterRegistry)
			.connectionLimit(this.limit)
			.build();
		this.connections.add(connection);
		connection.start();
		Awaitility.await().atMost(Duration.ofSeconds(10)).until(connection::isConnected);
		return connection;
	}

	private String upstreamAddress() {
		return "127.0.0.1:" + Objects.requireNonNull(this.upstream, "upstream").getLocalPort();
	}

	private double stallFailures(FakeServer service) {
		return this.meterRegistry.get("sluice.tunnel.stall.failures").tag("node", service.url).counter().count();
	}

	private double stalledGauge(FakeServer service) {
		return this.meterRegistry.get("sluice.tunnel.stall").tag("node", service.url).gauge().value();
	}

	/**
	 * A tunnel server that acknowledges the advertise and sends the CONNECT / CLOSE
	 * frames the test asks for on its latest stream. Once {@link #stall} is set, its
	 * receive callback blocks until {@link #resume}; with {@link #millisPer64KiB} it
	 * sleeps that long per 64KiB of DATA received and grants the payload back.
	 */
	private static final class FakeServer extends TunnelGrpc.TunnelImplBase {

		final AtomicBoolean stall = new AtomicBoolean();

		final AtomicBoolean blocked = new AtomicBoolean();

		final CountDownLatch resume = new CountDownLatch(1);

		final AtomicLong millisPer64KiB = new AtomicLong();

		final AtomicLong received = new AtomicLong();

		final AtomicInteger streams = new AtomicInteger();

		final CountDownLatch firstStreamEnded = new CountDownLatch(1);

		final AtomicReference<@Nullable Status> firstStreamStatus = new AtomicReference<>();

		volatile String url = "";

		private final AtomicReference<@Nullable StreamObserver<Frame>> responses = new AtomicReference<>();

		@Override
		public StreamObserver<Frame> connect(StreamObserver<Frame> responses) {
			int stream = this.streams.incrementAndGet();
			this.responses.set(responses);
			return new StreamObserver<>() {

				@Override
				public void onNext(Frame frame) {
					if (frame.getBodyCase() == Frame.BodyCase.ADVERTISE) {
						send(Frame.newBuilder()
							.setAdvertiseAck(AdvertiseAck.newBuilder().setNodeId("node-" + url))
							.build());
						return;
					}
					if (FakeServer.this.stall.get()) {
						FakeServer.this.blocked.set(true);
						try {
							FakeServer.this.resume.await();
						}
						catch (InterruptedException e) {
							Thread.currentThread().interrupt();
							return;
						}
					}
					long millis = FakeServer.this.millisPer64KiB.get();
					if (millis > 0 && frame.hasData()) {
						int size = frame.getData().getPayload().size();
						try {
							TimeUnit.MICROSECONDS.sleep(millis * 1000 * size / 65536);
						}
						catch (InterruptedException e) {
							Thread.currentThread().interrupt();
							return;
						}
						FakeServer.this.received.addAndGet(size);
						send(Frame.newBuilder()
							.setWindowUpdate(
									WindowUpdate.newBuilder().setConnId(frame.getData().getConnId()).setIncrement(size))
							.build());
					}
				}

				@Override
				public void onError(Throwable t) {
					ended(stream, Status.fromThrowable(t));
				}

				@Override
				public void onCompleted() {
					ended(stream, Status.OK);
				}
			};
		}

		private void ended(int stream, Status status) {
			if (stream == 1) {
				this.firstStreamStatus.set(status);
				this.firstStreamEnded.countDown();
			}
		}

		void connect(long id, String address) {
			send(Frame.newBuilder()
				.setConnect(Connect.newBuilder()
					.setConnId(id)
					.setAddress(address)
					.setConnectionWindow(VirtualConnection.DEFAULT_WINDOW))
				.build());
		}

		void close(long id) {
			send(Frame.newBuilder().setClose(Close.newBuilder().setConnId(id)).build());
		}

		private synchronized void send(Frame frame) {
			StreamObserver<Frame> responses = this.responses.get();
			if (responses != null) {
				responses.onNext(frame);
			}
		}

	}

}
