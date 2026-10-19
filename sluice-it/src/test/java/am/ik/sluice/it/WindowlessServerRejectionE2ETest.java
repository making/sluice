package am.ik.sluice.it;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.jspecify.annotations.Nullable;

import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.client.config.Upstream;
import am.ik.sluice.client.tunnel.NodeConnection;
import am.ik.sluice.client.tunnel.TunnelClient;
import am.ik.sluice.tunnel.VirtualConnection;
import am.ik.sluice.v1.proto.AdvertiseAck;
import am.ik.sluice.v1.proto.Connect;
import am.ik.sluice.v1.proto.Data;
import am.ik.sluice.v1.proto.Frame;
import am.ik.sluice.v1.proto.TunnelGrpc;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.awaitility.Awaitility;

import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A server without per-connection flow control (its CONNECT carries no window, as every
 * server before 046cd21) gets each such CONNECT answered with an ERROR naming the server
 * build required: nothing is dialed, no connection slot is taken, and one line is logged
 * per rejection. A windowed CONNECT on the same stream is still served. A server that
 * predates build reporting is logged as an older build. A server that predates the client
 * drain acknowledges the withdrawing advertise as any other: the client closes the stream
 * at once instead of waiting out its grace.
 */
@ExtendWith(OutputCaptureExtension.class)
class WindowlessServerRejectionE2ETest {

	private static final int PAYLOAD_BYTES = 64 * 1024;

	private static final int WINDOWLESS_ID = 1;

	private static final int WINDOWED_ID = 2;

	private final MeterRegistry clientMeters = new SimpleMeterRegistry();

	private final FakeServer fake = new FakeServer();

	private final AtomicInteger upstreamAccepts = new AtomicInteger();

	private @Nullable Server server;

	private @Nullable ServerSocket upstream;

	private @Nullable TunnelClient client;

	@AfterEach
	void tearDown() throws Exception {
		if (this.client != null) {
			this.client.stop();
		}
		if (this.server != null) {
			this.server.shutdownNow();
		}
		if (this.upstream != null) {
			this.upstream.close();
		}
	}

	@Test
	void windowlessConnectIsAnsweredWithAnError(CapturedOutput output) throws Exception {
		int echoPort = startEchoUpstream();
		this.fake.dialAddress = "127.0.0.1:" + echoPort;
		int grpcPort = TestPorts.freePort();
		this.server = ServerBuilder.forPort(grpcPort).addService(this.fake).build().start();
		TunnelClient started = TunnelClient.builder()
			.properties(SluiceClientProperties.builder()
				.serverUrl("grpc://127.0.0.1:" + grpcPort)
				.upstream(Upstream.builder().host("echo.local").target("tcp://" + this.fake.dialAddress).build())
				.token("it-token")
				// one slot: a slot leaked by the rejection would refuse the windowed
				// CONNECT
				.maxConnections(1)
				.drainGrace(Duration.ofMinutes(10))
				.build())
			.taskExecutor(task -> Thread.ofVirtual().name("e2e-windowless-server-worker").start(task))
			.meterRegistry(this.clientMeters)
			.build();
		started.start();
		this.client = started;
		Awaitility.await().atMost(Duration.ofSeconds(10)).until(started::isConnected);
		assertThat(this.clientMeters.get("sluice.connections.limit").gauge().value()).as("precondition: one slot")
			.isEqualTo(1.0);
		assertThat(this.fake.advertisedWindows).as("precondition: the client offers its window")
			.containsExactly(VirtualConnection.INITIAL_WINDOW);
		assertThat(output.getAll().lines().filter(line -> line.contains("] server build: "))).singleElement()
			.satisfies(line -> assertThat(line)
				.endsWith("[grpc://127.0.0.1:" + grpcPort + "] server build: unknown (older build)"));

		assertThat(this.fake.rejected.await(10, TimeUnit.SECONDS)).as("the windowless CONNECT answered").isTrue();
		assertThat(this.fake.errors).containsExactly(WINDOWLESS_ID + ": " + NodeConnection.SERVER_WINDOW_REQUIRED);

		byte[] payload = new byte[PAYLOAD_BYTES];
		for (int i = 0; i < payload.length; i++) {
			payload[i] = (byte) (i * 31);
		}
		this.fake.send(Frame.newBuilder()
			.setData(Data.newBuilder().setConnId(WINDOWED_ID).setPayload(ByteString.copyFrom(payload)))
			.build());
		assertThat(this.fake.echoed.await(10, TimeUnit.SECONDS)).as("the windowed connection echoed").isTrue();
		assertThat(this.fake.received.toByteArray()).isEqualTo(payload);
		assertThat(this.upstreamAccepts).as("only the windowed CONNECT was dialed").hasValue(1);
		assertThat(this.fake.framesFor(WINDOWLESS_ID)).as("nothing but the ERROR for the windowless CONNECT")
			.containsExactly(Frame.BodyCase.ERROR);
		assertThat(this.fake.errors).as("no other connection failed").hasSize(1);
		assertThat(output.getAll().lines().filter(line -> line.contains(NodeConnection.SERVER_WINDOW_REQUIRED)))
			.singleElement()
			.satisfies(line -> assertThat(line).endsWith("[grpc://127.0.0.1:" + grpcPort + "] "
					+ NodeConnection.SERVER_WINDOW_REQUIRED + "; rejecting conn " + WINDOWLESS_ID));

		// the windowed connection is still open: an ack without withdrawn ends the drain
		CountDownLatch drained = new CountDownLatch(1);
		Thread.ofVirtual().start(() -> {
			started.drain();
			drained.countDown();
		});
		assertThat(drained.await(30, TimeUnit.SECONDS)).as("the drain ends before its grace").isTrue();
		assertThat(this.fake.frames.stream()
			.filter(frame -> frame.getBodyCase() == Frame.BodyCase.ADVERTISE)
			.map(frame -> frame.getAdvertise().getWithdraw())).containsExactly(false, true);
		assertThat(output.getAll()).contains("[grpc://127.0.0.1:" + grpcPort
				+ "] the server predates the client drain (Advertise.withdraw) and keeps routing to this client; "
				+ "closing the tunnel stream at once");
	}

	private int startEchoUpstream() throws Exception {
		ServerSocket server = new ServerSocket();
		server.bind(new InetSocketAddress("127.0.0.1", 0));
		this.upstream = server;
		Thread.ofVirtual().name("e2e-windowless-echo").start(() -> {
			while (!server.isClosed()) {
				try {
					Socket socket = server.accept();
					this.upstreamAccepts.incrementAndGet();
					Thread.ofVirtual().start(() -> {
						try (socket;
								InputStream in = socket.getInputStream();
								OutputStream out = socket.getOutputStream()) {
							in.transferTo(out);
						}
						catch (Exception e) {
							// torn down
						}
					});
				}
				catch (Exception e) {
					return;
				}
			}
		});
		return server.getLocalPort();
	}

	/**
	 * A minimal server: acknowledges the advertise, sends a CONNECT without a window
	 * (followed by DATA for it, as a pre-window server would) and a windowed one, and
	 * collects what the client sends back.
	 */
	private static final class FakeServer extends TunnelGrpc.TunnelImplBase {

		final List<Integer> advertisedWindows = new CopyOnWriteArrayList<>();

		final List<String> errors = new CopyOnWriteArrayList<>();

		final List<Frame> frames = new CopyOnWriteArrayList<>();

		final ByteArrayOutputStream received = new ByteArrayOutputStream();

		final CountDownLatch rejected = new CountDownLatch(1);

		final CountDownLatch echoed = new CountDownLatch(1);

		volatile String dialAddress = "";

		private volatile @Nullable StreamObserver<Frame> responses;

		synchronized void send(Frame frame) {
			StreamObserver<Frame> responses = this.responses;
			if (responses != null) {
				responses.onNext(frame);
			}
		}

		List<Frame.BodyCase> framesFor(long connectionId) {
			return this.frames.stream().filter(frame -> switch (frame.getBodyCase()) {
				case DATA -> frame.getData().getConnId() == connectionId;
				case CLOSE -> frame.getClose().getConnId() == connectionId;
				case ERROR -> frame.getError().getConnId() == connectionId;
				case WINDOW_UPDATE -> frame.getWindowUpdate().getConnId() == connectionId;
				default -> false;
			}).map(Frame::getBodyCase).toList();
		}

		@Override
		public StreamObserver<Frame> connect(StreamObserver<Frame> responses) {
			this.responses = responses;
			return new StreamObserver<>() {

				@Override
				public void onNext(Frame frame) {
					FakeServer.this.frames.add(frame);
					switch (frame.getBodyCase()) {
						case ADVERTISE -> {
							send(Frame.newBuilder()
								.setAdvertiseAck(AdvertiseAck.newBuilder().setNodeId("windowless-server"))
								.build());
							if (frame.getAdvertise().getWithdraw()) {
								return; // a server predating the field keeps the routes
							}
							FakeServer.this.advertisedWindows.add(frame.getAdvertise().getConnectionWindow());
							send(Frame.newBuilder()
								.setConnect(Connect.newBuilder()
									.setConnId(WINDOWLESS_ID)
									.setAddress(FakeServer.this.dialAddress))
								.build());
							send(Frame.newBuilder()
								.setData(Data.newBuilder()
									.setConnId(WINDOWLESS_ID)
									.setPayload(ByteString.copyFromUtf8("dropped")))
								.build());
							send(Frame.newBuilder()
								.setConnect(Connect.newBuilder()
									.setConnId(WINDOWED_ID)
									.setAddress(FakeServer.this.dialAddress)
									.setConnectionWindow(VirtualConnection.DEFAULT_WINDOW))
								.build());
						}
						case DATA -> {
							synchronized (FakeServer.this.received) {
								FakeServer.this.received.writeBytes(frame.getData().getPayload().toByteArray());
								if (FakeServer.this.received.size() >= PAYLOAD_BYTES) {
									FakeServer.this.echoed.countDown();
								}
							}
						}
						case ERROR -> {
							FakeServer.this.errors
								.add(frame.getError().getConnId() + ": " + frame.getError().getMessage());
							FakeServer.this.rejected.countDown();
						}
						default -> {
							// keep-alive and the like
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

	}

}
