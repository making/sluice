package am.ik.sluice.it;

import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.protobuf.ByteString;
import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import am.ik.sluice.server.SluiceServerApplication;
import am.ik.sluice.server.tunnel.SessionRegistry;
import am.ik.sluice.server.tunnel.TunnelSession;
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
import io.grpc.Status;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.MeterRegistry;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The server's advertise timeout ({@code sluice.limits.advertise-timeout}): a tunnel
 * stream whose client sends no {@code Advertise} within it is failed with
 * {@code DEADLINE_EXCEEDED}, and until then it does not displace the client id's live
 * session: the data plane keeps reaching the live client.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class TunnelAdvertiseDeadlineE2ETest {

	private static final Duration TIMEOUT = Duration.ofSeconds(1);

	private static final String CLIENT_ID = "late-client";

	private static final byte[] GREETING = "hello".getBytes(StandardCharsets.US_ASCII);

	private static int grpcPort;

	private static int listenPort;

	@Autowired
	private @Nullable SessionRegistry registry;

	@Autowired
	private @Nullable MeterRegistry meterRegistry;

	private final List<RawClient> clients = new CopyOnWriteArrayList<>();

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) {
		grpcPort = TestPorts.freePort();
		listenPort = TestPorts.freePort();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(TestPorts.freePort()));
		registry.add("sluice.tcp-port-range", () -> String.valueOf(listenPort));
		registry.add("sluice.limits.advertise-timeout", () -> TIMEOUT.toMillis() + "ms");
		registry.add("sluice.token", () -> "it-token");
		registry.add("server.port", () -> 0);
	}

	@BeforeEach
	void awaitIdle() {
		Awaitility.await()
			.atMost(Duration.ofSeconds(10))
			.until(() -> registry().all().isEmpty() && registry().pending().isEmpty());
	}

	@AfterEach
	void disconnect() {
		for (RawClient client : this.clients) {
			client.channel.shutdownNow();
		}
	}

	@Test
	void aStreamThatNeverAdvertisesIsFailedAtTheBound() throws Exception {
		double timeoutsBefore = timeouts();
		long start = System.nanoTime();
		RawClient silent = open();
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> registry().pending().size() == 1);
		assertThat(registry().pending().getFirst().advertiseTimeout()).as("precondition: the configured timeout")
			.isEqualTo(TIMEOUT);
		assertThat(registry().find(CLIENT_ID)).as("not the client's session").isEmpty();

		assertThat(silent.ended.await(TIMEOUT.plusSeconds(5).toMillis(), TimeUnit.MILLISECONDS)).as("the stream ended")
			.isTrue();
		Duration elapsed = Duration.ofNanos(System.nanoTime() - start);

		assertThat(elapsed).as("at the bound").isBetween(TIMEOUT, TIMEOUT.plusSeconds(2));
		Status status = Objects.requireNonNull(silent.status.get());
		assertThat(status.getCode()).isEqualTo(Status.Code.DEADLINE_EXCEEDED);
		assertThat(status.getDescription()).isEqualTo(TunnelSession.advertiseTimeoutDescription(TIMEOUT));
		assertThat(timeouts() - timeoutsBefore).as("counted").isEqualTo(1);
		assertThat(registry().pending()).isEmpty();
		assertThat(registry().find(CLIENT_ID)).isEmpty();
		assertThat(silent.received).as("nothing sent to it, not even the membership").isEmpty();
	}

	@Test
	void aStreamThatHasNotAdvertisedLeavesTheLiveClientServing() throws Exception {
		RawClient live = open();
		live.advertise();
		assertThat(live.acked.await(10, TimeUnit.SECONDS)).as("precondition: the live client is acknowledged").isTrue();
		TunnelSession liveSession = registry().find(CLIENT_ID).orElseThrow();
		Awaitility.await()
			.atMost(Duration.ofSeconds(5))
			.alias("the membership is pushed once the client advertised")
			.until(() -> live.received.contains(Frame.BodyCase.MEMBERSHIP_UPDATE));
		assertThat(greeting()).as("precondition: the live client serves the route").isEqualTo(GREETING);

		RawClient silent = open();
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> registry().pending().size() == 1);
		assertThat(greeting()).as("served by the live client while the new stream waits").isEqualTo(GREETING);
		assertThat(silent.connects).as("nothing reached the waiting stream").isEmpty();

		assertThat(silent.ended.await(TIMEOUT.plusSeconds(5).toMillis(), TimeUnit.MILLISECONDS)).isTrue();
		assertThat(Objects.requireNonNull(silent.status.get()).getCode()).isEqualTo(Status.Code.DEADLINE_EXCEEDED);
		assertThat(registry().find(CLIENT_ID)).contains(liveSession);
		assertThat(greeting()).as("still served after the deadline").isEqualTo(GREETING);
		assertThat(live.connects).hasSize(3);
		assertThat(live.ended.getCount()).as("the live stream lives on").isOne();
	}

	/** Reads the greeting of a connection to the tcp route. */
	private static byte[] greeting() throws IOException {
		try (Socket socket = new Socket("127.0.0.1", listenPort)) {
			socket.setSoTimeout(5_000);
			return socket.getInputStream().readNBytes(GREETING.length);
		}
	}

	private RawClient open() {
		ManagedChannel channel = ManagedChannelBuilder.forAddress("127.0.0.1", grpcPort).usePlaintext().build();
		RawClient client = new RawClient(channel);
		this.clients.add(client);
		Metadata metadata = new Metadata();
		metadata.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER), "Bearer it-token");
		metadata.put(Metadata.Key.of("x-sluice-id", Metadata.ASCII_STRING_MARSHALLER), CLIENT_ID);
		client.requests = TunnelGrpc.newStub(channel)
			.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata))
			.connect(client);
		return client;
	}

	private double timeouts() {
		return Objects.requireNonNull(this.meterRegistry, "meterRegistry")
			.get("sluice.tunnel.advertise.timeouts")
			.counter()
			.count();
	}

	private SessionRegistry registry() {
		return Objects.requireNonNull(this.registry, "registry");
	}

	/**
	 * A tunnel client that advertises only when told to, with a tcp route whose every
	 * connection it answers with {@link #GREETING} and a CLOSE.
	 */
	private static final class RawClient implements StreamObserver<Frame> {

		final ManagedChannel channel;

		final CountDownLatch acked = new CountDownLatch(1);

		final CountDownLatch ended = new CountDownLatch(1);

		final AtomicReference<@Nullable Status> status = new AtomicReference<>();

		final Set<Long> connects = ConcurrentHashMap.newKeySet();

		final List<Frame.BodyCase> received = new CopyOnWriteArrayList<>();

		private @Nullable StreamObserver<Frame> requests;

		RawClient(ManagedChannel channel) {
			this.channel = channel;
		}

		void advertise() {
			send(Frame.newBuilder()
				.setAdvertise(Advertise.newBuilder()
					.addUpstreams(Upstream.newBuilder()
						.setHost("late.tcp")
						.setTargetUrl("tcp://127.0.0.1:1")
						.setListenPort(listenPort))
					.setConnectionWindow(VirtualConnection.DEFAULT_WINDOW))
				.build());
		}

		private synchronized void send(Frame frame) {
			StreamObserver<Frame> requests = this.requests;
			if (requests != null) {
				requests.onNext(frame);
			}
		}

		@Override
		public void onNext(Frame frame) {
			this.received.add(frame.getBodyCase());
			switch (frame.getBodyCase()) {
				case ADVERTISE_ACK -> this.acked.countDown();
				case CONNECT -> {
					long id = frame.getConnect().getConnId();
					this.connects.add(id);
					send(Frame.newBuilder()
						.setData(Data.newBuilder().setConnId(id).setPayload(ByteString.copyFrom(GREETING)))
						.build());
					send(Frame.newBuilder().setClose(Close.newBuilder().setConnId(id)).build());
				}
				default -> {
					// membership and the like
				}
			}
		}

		@Override
		public void onError(Throwable t) {
			this.status.set(Status.fromThrowable(t));
			this.ended.countDown();
		}

		@Override
		public void onCompleted() {
			this.status.set(Status.OK);
			this.ended.countDown();
		}

	}

}
