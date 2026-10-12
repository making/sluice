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
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.awaitility.Awaitility;

import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * This client keeps working against a server without per-connection flow control (its
 * CONNECT carries no window, as every server before the window existed): the client still
 * advertises its window, but runs the connection in legacy mode. A server that predates
 * build reporting is logged as an older build.
 */
@ExtendWith(OutputCaptureExtension.class)
class LegacyServerInteropE2ETest {

	/** Several windows, so a wrongly windowed connection would stall. */
	private static final int PAYLOAD_BYTES = 4 * VirtualConnection.DEFAULT_WINDOW;

	private static final int CONNECTION_ID = 1;

	private final MeterRegistry clientMeters = new SimpleMeterRegistry();

	private final LegacyServer legacy = new LegacyServer();

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
	void clientRelaysForALegacyServer(CapturedOutput output) throws Exception {
		int echoPort = startEchoUpstream();
		this.legacy.dialAddress = "127.0.0.1:" + echoPort;
		int grpcPort = TestPorts.freePort();
		this.server = ServerBuilder.forPort(grpcPort).addService(this.legacy).build().start();
		TunnelClient started = TunnelClient.builder()
			.properties(SluiceClientProperties.builder()
				.serverUrl("grpc://127.0.0.1:" + grpcPort)
				.upstream(Upstream.builder().host("echo.local").target("tcp://" + this.legacy.dialAddress).build())
				.token("it-token")
				.build())
			.taskExecutor(task -> Thread.ofVirtual().name("e2e-legacy-server-worker").start(task))
			.meterRegistry(this.clientMeters)
			.build();
		started.start();
		this.client = started;
		Awaitility.await().atMost(Duration.ofSeconds(10)).until(started::isConnected);
		assertThat(this.legacy.advertisedWindows).as("precondition: the client offers its window")
			.containsExactly(VirtualConnection.DEFAULT_WINDOW);
		assertThat(output.getAll().lines().filter(line -> line.contains("] server build: "))).singleElement()
			.satisfies(line -> assertThat(line)
				.endsWith("[grpc://127.0.0.1:" + grpcPort + "] server build: unknown (older build)"));

		byte[] payload = new byte[PAYLOAD_BYTES];
		for (int i = 0; i < payload.length; i++) {
			payload[i] = (byte) (i * 31);
		}
		for (int offset = 0; offset < payload.length; offset += 64 * 1024) {
			this.legacy.send(Frame.newBuilder()
				.setData(Data.newBuilder()
					.setConnId(CONNECTION_ID)
					.setPayload(ByteString.copyFrom(payload, offset, 64 * 1024)))
				.build());
		}
		assertThat(this.legacy.echoed.await(30, TimeUnit.SECONDS)).as("the whole payload echoed").isTrue();
		assertThat(this.legacy.received.toByteArray()).isEqualTo(payload);
		assertThat(this.legacy.windowUpdates).as("no grant reaches a legacy server").hasValue(0);
		assertThat(this.clientMeters.find("sluice.tunnel.window.updates")
			.counters()
			.stream()
			.mapToDouble(Counter::count)
			.sum()).as("legacy connection: no grants either way").isZero();
	}

	private int startEchoUpstream() throws Exception {
		ServerSocket server = new ServerSocket();
		server.bind(new InetSocketAddress("127.0.0.1", 0));
		this.upstream = server;
		Thread.ofVirtual().name("e2e-legacy-echo").start(() -> {
			while (!server.isClosed()) {
				try {
					Socket socket = server.accept();
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
	 * A minimal pre-window server: acknowledges the advertise, sends one CONNECT without
	 * a window, and collects the DATA the client sends back.
	 */
	private static final class LegacyServer extends TunnelGrpc.TunnelImplBase {

		final List<Integer> advertisedWindows = new CopyOnWriteArrayList<>();

		final AtomicInteger windowUpdates = new AtomicInteger();

		final ByteArrayOutputStream received = new ByteArrayOutputStream();

		final CountDownLatch echoed = new CountDownLatch(1);

		volatile String dialAddress = "";

		private volatile @Nullable StreamObserver<Frame> responses;

		synchronized void send(Frame frame) {
			StreamObserver<Frame> responses = this.responses;
			if (responses != null) {
				responses.onNext(frame);
			}
		}

		@Override
		public StreamObserver<Frame> connect(StreamObserver<Frame> responses) {
			this.responses = responses;
			return new StreamObserver<>() {

				@Override
				public void onNext(Frame frame) {
					switch (frame.getBodyCase()) {
						case ADVERTISE -> {
							LegacyServer.this.advertisedWindows.add(frame.getAdvertise().getConnectionWindow());
							send(Frame.newBuilder()
								.setAdvertiseAck(AdvertiseAck.newBuilder().setNodeId("legacy-server"))
								.build());
							send(Frame.newBuilder()
								.setConnect(Connect.newBuilder()
									.setConnId(CONNECTION_ID)
									.setAddress(LegacyServer.this.dialAddress))
								.build());
						}
						case DATA -> {
							synchronized (LegacyServer.this.received) {
								LegacyServer.this.received.writeBytes(frame.getData().getPayload().toByteArray());
								if (LegacyServer.this.received.size() >= PAYLOAD_BYTES) {
									LegacyServer.this.echoed.countDown();
								}
							}
						}
						case WINDOW_UPDATE -> LegacyServer.this.windowUpdates.incrementAndGet();
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
