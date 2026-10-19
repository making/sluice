package am.ik.sluice.it;

import java.io.IOException;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.extension.ExtendWith;

import am.ik.sluice.server.SluiceServerApplication;
import am.ik.sluice.server.tunnel.SessionRegistry;
import am.ik.sluice.server.tunnel.TunnelSession;
import am.ik.sluice.v1.proto.Advertise;
import am.ik.sluice.v1.proto.Frame;
import am.ik.sluice.v1.proto.TunnelGrpc;
import am.ik.sluice.v1.proto.Upstream;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A client without per-connection flow control (it advertises no window, as every client
 * before 046cd21 and every wicket before f76388d) is rejected: its tunnel stream fails
 * with {@code FAILED_PRECONDITION} naming the builds required, before any route is
 * registered or acknowledged, no session is left behind, and one line is logged per
 * rejected stream.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
@ExtendWith(OutputCaptureExtension.class)
class WindowlessClientRejectionE2ETest {

	private static final String CLIENT_ID = "windowless-client";

	private static int grpcPort;

	private static int listenPort;

	private final List<ManagedChannel> channels = new CopyOnWriteArrayList<>();

	@Autowired
	SessionRegistry sessions;

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
		this.channels.forEach(ManagedChannel::shutdownNow);
	}

	@Test
	void windowlessClientIsRejected(CapturedOutput output) throws Exception {
		// the first stream sends its advertise twice, as a client may before it sees the
		// failure (gRPC may drop the second once the call is closed; that the session
		// ignores it is pinned in TunnelSessionReceivePathTest); the second stream is a
		// reconnect
		WindowlessClient first = connect(2);
		Status status = Status.fromThrowable(first.failure.get(10, TimeUnit.SECONDS));
		WindowlessClient reconnected = connect(1);
		Status reconnectStatus = Status.fromThrowable(reconnected.failure.get(10, TimeUnit.SECONDS));
		for (Status failed : List.of(status, reconnectStatus)) {
			assertThat(failed.getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
			assertThat(failed.getDescription()).isEqualTo(TunnelSession.WINDOW_REQUIRED);
		}
		for (WindowlessClient client : List.of(first, reconnected)) {
			assertThat(client.received.stream().filter(frame -> frame.hasAdvertiseAck() || frame.hasConnect()))
				.as("no ack, no CONNECT")
				.isEmpty();
		}
		assertThat(this.sessions.find(CLIENT_ID)).as("no session left").isEmpty();
		assertThatThrownBy(() -> new Socket("127.0.0.1", listenPort).close()).as("the listen port was never bound")
			.isInstanceOf(IOException.class);
		assertThat(output.getAll().lines().filter(line -> line.contains(TunnelSession.WINDOW_REQUIRED)))
			.as("one line per rejected stream")
			.hasSize(2)
			.allSatisfy(line -> assertThat(line).endsWith("client " + CLIENT_ID
					+ " runs unknown (older build); rejecting its tunnel stream: " + TunnelSession.WINDOW_REQUIRED));
	}

	/** Opens a tunnel stream and sends {@code advertises} advertises without a window. */
	private WindowlessClient connect(int advertises) {
		ManagedChannel channel = ManagedChannelBuilder.forAddress("127.0.0.1", grpcPort).usePlaintext().build();
		this.channels.add(channel);
		Metadata metadata = new Metadata();
		metadata.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER), "Bearer it-token");
		metadata.put(Metadata.Key.of("x-sluice-id", Metadata.ASCII_STRING_MARSHALLER), CLIENT_ID);
		WindowlessClient client = new WindowlessClient();
		StreamObserver<Frame> requests = TunnelGrpc.newStub(channel)
			.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata))
			.connect(client);
		Upstream upstream = Upstream.newBuilder()
			.setHost("windowless.local")
			.setTargetUrl("tcp://127.0.0.1:1")
			.setListenPort(listenPort)
			.build();
		for (int i = 0; i < advertises; i++) {
			requests.onNext(Frame.newBuilder().setAdvertise(Advertise.newBuilder().addUpstreams(upstream)).build());
		}
		return client;
	}

	/** A pre-window client: records what the server sends and how the stream ends. */
	private static final class WindowlessClient implements StreamObserver<Frame> {

		final List<Frame> received = new CopyOnWriteArrayList<>();

		final CompletableFuture<Throwable> failure = new CompletableFuture<>();

		@Override
		public void onNext(Frame frame) {
			this.received.add(frame);
		}

		@Override
		public void onError(Throwable t) {
			this.failure.complete(t);
		}

		@Override
		public void onCompleted() {
			this.failure.completeExceptionally(new AssertionError("completed, not failed"));
		}

	}

}
