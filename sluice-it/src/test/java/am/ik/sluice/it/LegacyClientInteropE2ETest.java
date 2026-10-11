package am.ik.sluice.it;

import java.io.OutputStream;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import org.jspecify.annotations.Nullable;

import am.ik.sluice.server.SluiceServerApplication;
import am.ik.sluice.tunnel.VirtualConnection;
import am.ik.sluice.v1.proto.Advertise;
import am.ik.sluice.v1.proto.Close;
import am.ik.sluice.v1.proto.Data;
import am.ik.sluice.v1.proto.Frame;
import am.ik.sluice.v1.proto.TunnelGrpc;
import am.ik.sluice.v1.proto.Upstream;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A client without per-connection flow control (it advertises no window, as every client
 * before the window existed) keeps working against this server: its connections run in
 * legacy mode, with no credit to wait for in either direction.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class LegacyClientInteropE2ETest {

	/** Several windows, so a wrongly windowed connection would stall. */
	private static final int PAYLOAD_BYTES = 4 * VirtualConnection.DEFAULT_WINDOW;

	private static int grpcPort;

	private static int listenPort;

	private @Nullable ManagedChannel channel;

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) {
		grpcPort = TestPorts.freePort();
		listenPort = TestPorts.freePort();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(TestPorts.freePort()));
		registry.add("sluice.tcp-port-range", () -> String.valueOf(listenPort));
		registry.add("sluice.token", () -> "it-token");
		registry.add("server.port", () -> 0);
	}

	@AfterEach
	void tearDown() {
		ManagedChannel channel = this.channel;
		if (channel != null) {
			channel.shutdownNow();
		}
	}

	@Test
	void legacyClientEchoesThroughTheServer() throws Exception {
		LegacyEchoClient client = new LegacyEchoClient();
		ManagedChannel channel = ManagedChannelBuilder.forAddress("127.0.0.1", grpcPort).usePlaintext().build();
		this.channel = channel;
		Metadata metadata = new Metadata();
		metadata.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER), "Bearer it-token");
		metadata.put(Metadata.Key.of("x-sluice-id", Metadata.ASCII_STRING_MARSHALLER), "legacy-client");
		StreamObserver<Frame> requests = TunnelGrpc.newStub(channel)
			.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata))
			.connect(client);
		client.start(requests);
		assertThat(client.acked.await(10, TimeUnit.SECONDS)).as("advertise acknowledged").isTrue();

		byte[] payload = new byte[PAYLOAD_BYTES];
		for (int i = 0; i < payload.length; i++) {
			payload[i] = (byte) i;
		}
		try (Socket peer = new Socket("127.0.0.1", listenPort)) {
			peer.setSoTimeout(10_000);
			Thread.ofVirtual().name("e2e-legacy-writer").start(() -> {
				try {
					OutputStream out = peer.getOutputStream();
					out.write(payload);
					out.flush();
				}
				catch (Exception e) {
					// the read below fails then
				}
			});
			assertThat(peer.getInputStream().readNBytes(PAYLOAD_BYTES)).isEqualTo(payload);
		}
		assertThat(client.connectWindows).as("the server announced no window to a legacy client").containsExactly(0);
		assertThat(client.windowUpdates).as("no grant reaches a legacy client").hasValue(0);
	}

	/**
	 * A minimal pre-window client: advertises one tcp route without a window and echoes
	 * every DATA of a connection back as DATA.
	 */
	private static final class LegacyEchoClient implements StreamObserver<Frame> {

		final CountDownLatch acked = new CountDownLatch(1);

		final List<Integer> connectWindows = new CopyOnWriteArrayList<>();

		final AtomicInteger windowUpdates = new AtomicInteger();

		private @Nullable StreamObserver<Frame> requests;

		void start(StreamObserver<Frame> requests) {
			this.requests = requests;
			Upstream upstream = Upstream.newBuilder()
				.setHost("legacy.local")
				.setTargetUrl("tcp://127.0.0.1:1")
				.setListenPort(listenPort)
				.build();
			send(Frame.newBuilder().setAdvertise(Advertise.newBuilder().addUpstreams(upstream)).build());
		}

		private synchronized void send(Frame frame) {
			StreamObserver<Frame> requests = this.requests;
			if (requests != null) {
				requests.onNext(frame);
			}
		}

		@Override
		public void onNext(Frame frame) {
			switch (frame.getBodyCase()) {
				case ADVERTISE_ACK -> this.acked.countDown();
				case CONNECT -> this.connectWindows.add(frame.getConnect().getConnectionWindow());
				case DATA -> send(Frame.newBuilder()
					.setData(Data.newBuilder()
						.setConnId(frame.getData().getConnId())
						.setPayload(frame.getData().getPayload()))
					.build());
				case CLOSE -> send(Frame.newBuilder()
					.setClose(Close.newBuilder().setConnId(frame.getClose().getConnId()))
					.build());
				case WINDOW_UPDATE -> this.windowUpdates.incrementAndGet();
				default -> {
					// membership and the like
				}
			}
		}

		@Override
		public void onError(Throwable t) {
		}

		@Override
		public void onCompleted() {
		}

	}

}
