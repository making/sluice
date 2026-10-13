package am.ik.sluice.it;

import java.time.Duration;
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
import am.ik.sluice.server.tunnel.TunnelService;
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
 * The bound on the registered tunnel sessions
 * ({@code sluice.limits.max-tunnel-sessions}): past it a new client id's stream is
 * refused with {@code RESOURCE_EXHAUSTED} -- at once when the sessions are already at the
 * bound, at its advertise when another took the last place meanwhile -- and counted,
 * while the sessions go on; a session ending frees its place; a newer stream of a
 * registered client id replaces it at the bound. Churning streams log a bounded number of
 * lines.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
@ExtendWith(OutputCaptureExtension.class)
class TunnelSessionLimitE2ETest {

	private static final int MAX_SESSIONS = 2;

	private static final String REFUSAL = "too many tunnel clients are connected to this node";

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
		registry.add("sluice.limits.max-tunnel-sessions", () -> String.valueOf(MAX_SESSIONS));
		registry.add("sluice.token", () -> "it-token");
		registry.add("server.port", () -> 0);
	}

	@BeforeEach
	void awaitIdle() {
		assertThat(properties().limits().maxTunnelSessions()).as("precondition: the configured bound")
			.isEqualTo(MAX_SESSIONS);
		assertThat(properties().limits().maxTunnelSessionsPerAddress()).as("precondition: no bound per address")
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
	void pastTheBoundANewClientIdIsRefusedAtOnceWhileTheSessionsGoOn() throws Exception {
		double refusedBefore = refused();
		RawStream a = registered("a");
		RawStream b = registered("b");

		RawStream c = open("c");

		assertThat(c.ended.await(5, TimeUnit.SECONDS)).as("refused").isTrue();
		Status status = Objects.requireNonNull(c.status.get());
		assertThat(status.getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
		assertThat(status.getDescription()).isEqualTo(REFUSAL);
		assertThat(refused() - refusedBefore).isEqualTo(1);
		assertThat(registry().all()).extracting(session -> session.clientId()).containsExactlyInAnyOrder("a", "b");
		assertThat(a.ended.getCount()).as("the sessions go on").isOne();
		assertThat(b.ended.getCount()).isOne();
		a.advertise();
		assertThat(a.acks.await(5, TimeUnit.SECONDS)).as("and keep serving their client").isTrue();
	}

	@Test
	void aSessionEndingFreesItsPlace() throws Exception {
		RawStream a = registered("a");
		registered("b");

		a.channel.shutdownNow();
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> registry().count() == 1);

		registered("c");
		assertThat(registry().all()).extracting(session -> session.clientId()).containsExactlyInAnyOrder("b", "c");
	}

	@Test
	void aNewerStreamOfARegisteredClientIdReplacesItAtTheBound() throws Exception {
		RawStream old = registered("a");
		registered("b");

		registered("a");

		assertThat(old.ended.await(5, TimeUnit.SECONDS)).as("the replaced session's stream ends").isTrue();
		assertThat(registry().count()).isEqualTo(MAX_SESSIONS);
	}

	@Test
	void aStreamAdmittedBelowTheBoundIsRefusedAtItsAdvertiseOnceTheLastPlaceIsTaken() throws Exception {
		double refusedBefore = refused();
		registered("a");
		RawStream b = open("b");
		RawStream c = open("c");
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> registry().pending().size() == 2);

		b.advertise();
		assertThat(b.acked.await(5, TimeUnit.SECONDS)).isTrue();
		c.advertise();

		assertThat(c.ended.await(5, TimeUnit.SECONDS)).as("refused at its advertise").isTrue();
		Status status = Objects.requireNonNull(c.status.get());
		assertThat(status.getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
		assertThat(status.getDescription()).isEqualTo(REFUSAL);
		assertThat(c.acked.getCount()).as("no ack").isOne();
		assertThat(refused() - refusedBefore).isEqualTo(1);
		assertThat(registry().all()).extracting(session -> session.clientId()).containsExactlyInAnyOrder("a", "b");
		assertThat(registry().pending()).isEmpty();
	}

	/**
	 * Streams opened, advertised and ended one after another: their lines are logged for
	 * at most the stream log's permits per period (two periods' worth, in case the churn
	 * straddles a boundary), the others at debug only.
	 */
	@Test
	void churningStreamsLogABoundedNumberOfLines(CapturedOutput output) throws Exception {
		int churn = 3 * TunnelService.STREAM_LOG_PERMITS;
		long start = System.nanoTime();
		for (int i = 0; i < churn; i++) {
			RawStream stream = registered("churn-" + i);
			stream.channel.shutdownNow();
			Awaitility.await()
				.pollDelay(Duration.ZERO)
				.pollInterval(Duration.ofMillis(2))
				.atMost(Duration.ofSeconds(5))
				.until(() -> registry().count() == 0);
		}
		assertThat(Duration.ofNanos(System.nanoTime() - start)).as("precondition: the churn took under two periods")
			.isLessThan(TunnelService.STREAM_LOG_PERIOD.multipliedBy(2));

		int bound = 2 * TunnelService.STREAM_LOG_PERMITS;
		assertThat(count(output, "tunnel stream established for client churn-")).isBetween(1L, (long) bound);
		assertThat(count(output, "closing session for client churn-")).isBetween(1L, (long) bound);
		assertThat(count(output, "tunnel stream failed for client churn-")).isLessThanOrEqualTo(bound);
		assertThat(count(output, " advertised 0 upstream(s)")).isLessThanOrEqualTo(bound);
		assertThat(count(output, "tunnel streams or route changes logged at debug since the last report"))
			.as("the demoted streams are reported once a period ends, not line by line")
			.isLessThanOrEqualTo(2);
	}

	private static long count(CapturedOutput output, String text) {
		return output.getAll().lines().filter(line -> line.contains(text)).count();
	}

	/** Opens a stream for the client id and waits until its advertise registered it. */
	private RawStream registered(String clientId) throws InterruptedException {
		RawStream stream = open(clientId);
		stream.advertise();
		assertThat(stream.acked.await(5, TimeUnit.SECONDS)).as("precondition: %s registered", clientId).isTrue();
		return stream;
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
			.tag("reason", "session-limit")
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

		/** The first ack. */
		final CountDownLatch acked = new CountDownLatch(1);

		/** The second ack. */
		final CountDownLatch acks = new CountDownLatch(2);

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
				this.acks.countDown();
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
