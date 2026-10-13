package am.ik.sluice.it;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import am.ik.sluice.server.SluiceServerApplication;
import am.ik.sluice.server.config.SluiceServerProperties;
import am.ik.sluice.server.tunnel.SessionRegistry;
import am.ik.sluice.tunnel.VirtualConnection;
import am.ik.sluice.v1.proto.Advertise;
import am.ik.sluice.v1.proto.Frame;
import am.ik.sluice.v1.proto.TunnelGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.MeterRegistry;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The bound on the tunnel streams waiting for their client's first {@code Advertise}
 * ({@code sluice.limits.max-pending-tunnel-streams}): past it a stream is refused at once
 * with {@code RESOURCE_EXHAUSTED}, counted, and logged at most once per period; a pending
 * stream advertising, failed by its deadline, or ending frees its place.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
@ExtendWith(OutputCaptureExtension.class)
class TunnelPendingLimitE2ETest {

	private static final int MAX_PENDING = 2;

	private static final Duration ADVERTISE_TIMEOUT = Duration.ofSeconds(1);

	private static final String REFUSAL_LINE = "too many tunnel streams are waiting for their client's first Advertise";

	private static int grpcPort;

	@Autowired
	private @Nullable SessionRegistry registry;

	@Autowired
	private @Nullable SluiceServerProperties properties;

	@Autowired
	private @Nullable MeterRegistry meterRegistry;

	private final List<RawStream> streams = new CopyOnWriteArrayList<>();

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) {
		grpcPort = TestPorts.freePort();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(TestPorts.freePort()));
		registry.add("sluice.limits.max-pending-tunnel-streams", () -> String.valueOf(MAX_PENDING));
		registry.add("sluice.limits.advertise-timeout", () -> ADVERTISE_TIMEOUT.toMillis() + "ms");
		registry.add("sluice.token", () -> "it-token");
		registry.add("server.port", () -> 0);
	}

	@BeforeEach
	void awaitIdle() {
		assertThat(properties().limits().maxPendingTunnelStreams()).as("precondition: the configured cap")
			.isEqualTo(MAX_PENDING);
		assertThat(properties().limits().maxPendingTunnelStreamsPerAddress()).as("precondition: no bound per address")
			.isZero();
		Awaitility.await()
			.atMost(Duration.ofSeconds(10))
			.until(() -> registry().all().isEmpty() && registry().pending().isEmpty());
	}

	@AfterEach
	void disconnect() {
		for (RawStream stream : this.streams) {
			stream.channel.shutdownNow();
		}
	}

	@Test
	void streamsPastTheCapAreRefusedAtOnceCountedAndLoggedOnce(CapturedOutput output) throws Exception {
		double refusedBefore = refused();
		RawStream first = open("first");
		RawStream second = open("second");
		awaitPending(MAX_PENDING);
		int burst = 10;
		List<RawStream> refused = new ArrayList<>();
		long start = System.nanoTime();
		for (int i = 0; i < burst; i++) {
			refused.add(open("refused-" + i));
		}

		for (RawStream stream : refused) {
			assertThat(stream.ended.await(5, TimeUnit.SECONDS)).as("refused").isTrue();
			Status status = Objects.requireNonNull(stream.status.get());
			assertThat(status.getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
			assertThat(status.getDescription()).isEqualTo(REFUSAL_LINE);
		}
		assertThat(Duration.ofNanos(System.nanoTime() - start)).as("at once, not at the pending streams' deadline")
			.isLessThan(ADVERTISE_TIMEOUT);
		assertThat(first.ended.getCount()).as("the pending streams live on").isOne();
		assertThat(second.ended.getCount()).isOne();
		assertThat(registry().pending()).hasSize(MAX_PENDING);
		assertThat(refused() - refusedBefore).as("every refusal counted").isEqualTo(burst);
		assertThat(output.getAll().lines().filter(line -> line.contains(REFUSAL_LINE))).as("one log line for the burst")
			.singleElement()
			.satisfies(line -> assertThat(line)
				.containsPattern("refusing tunnel stream of client refused-\\d+ \\(127\\.0\\.0\\.1:\\d+\\): "));
	}

	@Test
	void anAdvertiseFreesThePlace() throws Exception {
		RawStream first = open("first");
		open("second");
		awaitPending(MAX_PENDING);

		first.advertise();
		assertThat(first.acked.await(5, TimeUnit.SECONDS)).isTrue();
		awaitPending(1);

		open("third");
		awaitPending(MAX_PENDING);
		assertThat(registry().pending()).extracting(session -> session.clientId())
			.containsExactlyInAnyOrder("second", "third");
	}

	@Test
	void theDeadlineFreesThePlace() throws Exception {
		RawStream first = open("first");
		RawStream second = open("second");
		awaitPending(MAX_PENDING);

		assertThat(first.ended.await(ADVERTISE_TIMEOUT.plusSeconds(5).toMillis(), TimeUnit.MILLISECONDS)).isTrue();
		assertThat(second.ended.await(5, TimeUnit.SECONDS)).isTrue();
		assertThat(Objects.requireNonNull(first.status.get()).getCode()).isEqualTo(Status.Code.DEADLINE_EXCEEDED);
		awaitPending(0);

		RawStream third = open("third");
		awaitPending(1);
		assertThat(third.ended.getCount()).isOne();
	}

	@Test
	void theEndOfAStreamFreesThePlace() {
		RawStream first = open("first");
		open("second");
		awaitPending(MAX_PENDING);

		first.channel.shutdownNow();
		awaitPending(1);

		open("third");
		awaitPending(MAX_PENDING);
		assertThat(registry().pending()).extracting(session -> session.clientId())
			.containsExactlyInAnyOrder("second", "third");
	}

	private void awaitPending(int count) {
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> registry().pending().size() == count);
	}

	private RawStream open(String clientId) {
		ManagedChannel channel = ManagedChannelBuilder.forAddress("127.0.0.1", grpcPort).usePlaintext().build();
		RawStream stream = new RawStream(channel);
		this.streams.add(stream);
		Metadata metadata = new Metadata();
		metadata.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER), "Bearer it-token");
		metadata.put(Metadata.Key.of("x-sluice-id", Metadata.ASCII_STRING_MARSHALLER), clientId);
		stream.requests = TunnelGrpc.newStub(channel)
			.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata))
			.connect(stream);
		return stream;
	}

	private double refused() {
		return Objects.requireNonNull(this.meterRegistry, "meterRegistry")
			.get("sluice.tunnel.streams.refused")
			.tag("reason", "pending-limit")
			.counter()
			.count();
	}

	private SessionRegistry registry() {
		return Objects.requireNonNull(this.registry, "registry");
	}

	private SluiceServerProperties properties() {
		return Objects.requireNonNull(this.properties, "properties");
	}

	/** A tunnel stream that advertises only when told to. */
	private static final class RawStream implements StreamObserver<Frame> {

		final ManagedChannel channel;

		final CountDownLatch acked = new CountDownLatch(1);

		final CountDownLatch ended = new CountDownLatch(1);

		final AtomicReference<@Nullable Status> status = new AtomicReference<>();

		private @Nullable StreamObserver<Frame> requests;

		RawStream(ManagedChannel channel) {
			this.channel = channel;
		}

		synchronized void advertise() {
			Objects.requireNonNull(this.requests, "requests")
				.onNext(Frame.newBuilder()
					.setAdvertise(Advertise.newBuilder().setConnectionWindow(VirtualConnection.DEFAULT_WINDOW))
					.build());
		}

		@Override
		public void onNext(Frame frame) {
			if (frame.hasAdvertiseAck()) {
				this.acked.countDown();
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
