package am.ik.sluice.it;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.random.RandomGenerator;
import java.util.stream.Stream;

import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.client.config.Upstream;
import am.ik.sluice.client.tunnel.TunnelClient;
import am.ik.sluice.server.tunnel.SessionRegistry;
import am.ik.sluice.server.tunnel.TunnelSession;
import am.ik.sluice.v1.proto.Frame;
import am.ik.sluice.v1.proto.TunnelGrpc;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A client whose stream the server fails before acknowledging its advertise -- with the
 * statuses the server sends then: {@code DEADLINE_EXCEEDED} for a missing
 * {@code Advertise}, {@code RESOURCE_EXHAUSTED} at once for a stream past the bound on
 * the streams waiting for theirs -- logs the status and reconnects with its backoff,
 * which keeps growing: a stream that was never acknowledged does not reset it.
 */
@ExtendWith(OutputCaptureExtension.class)
class ClientStreamFailureBackoffE2ETest {

	private static final Duration SERVER_TIMEOUT = Duration.ofMillis(200);

	private static final Status ADVERTISE_TIMEOUT = Status.DEADLINE_EXCEEDED
		.withDescription(TunnelSession.advertiseTimeoutDescription(SERVER_TIMEOUT));

	private final MeterRegistry clientMeters = new SimpleMeterRegistry();

	private @Nullable Server server;

	private @Nullable TunnelClient client;

	@AfterEach
	void tearDown() {
		if (this.client != null) {
			this.client.stop();
		}
		if (this.server != null) {
			this.server.shutdownNow();
		}
	}

	static Stream<Arguments> failures() {
		return Stream.of(Arguments.of(SERVER_TIMEOUT, ADVERTISE_TIMEOUT),
				Arguments.of(Duration.ZERO, SessionRegistry.Refusal.LIMIT.status()));
	}

	/**
	 * A jitter source drawing the top of every range: each wait is its full ceiling (1s,
	 * 2s, ...), which shows the backoff growing.
	 */
	private static final RandomGenerator HIGHEST = new FixedRandom(true);

	/** The bottom of every range: each wait is half its ceiling. */
	private static final RandomGenerator LOWEST = new FixedRandom(false);

	@ParameterizedTest
	@MethodSource("failures")
	void theClientLogsTheStatusAndReconnectsWithAGrowingBackoff(Duration failAfter, Status failure,
			CapturedOutput output) throws Exception {
		FailingServer fake = new FailingServer(failAfter, failure);
		int grpcPort = TestPorts.freePort();
		this.server = ServerBuilder.forPort(grpcPort).addService(fake).build().start();
		TunnelClient started = TunnelClient.builder()
			.properties(SluiceClientProperties.builder()
				.serverUrl("grpc://127.0.0.1:" + grpcPort)
				.upstream(Upstream.builder().host("late.local").target("http://127.0.0.1:1").build())
				.token("it-token")
				.build())
			.taskExecutor(task -> Thread.ofVirtual().name("e2e-advertise-timeout-worker").start(task))
			.meterRegistry(this.clientMeters)
			.randomFactory(() -> HIGHEST)
			.build();
		started.start();
		this.client = started;

		Awaitility.await()
			.atMost(Duration.ofSeconds(15))
			.until(() -> fake.failedAt.size() >= 2 && fake.openedAt.size() >= 3);

		if (!failAfter.isZero()) {
			assertThat(fake.advertised).as("precondition: the client sent its advertise on every stream")
				.hasSizeGreaterThanOrEqualTo(2);
		}
		long firstBackoff = fake.openedAt.get(1) - fake.failedAt.get(0);
		long secondBackoff = fake.openedAt.get(2) - fake.failedAt.get(1);
		assertThat(Duration.ofNanos(firstBackoff)).as("the first reconnect waits the initial backoff")
			.isBetween(Duration.ofMillis(900), Duration.ofMillis(2500));
		assertThat(Duration.ofNanos(secondBackoff)).as("the second waits twice as long")
			.isBetween(Duration.ofMillis(1900), Duration.ofMillis(4000));
		assertThat(started.isConnected()).isFalse();
		assertThat(this.clientMeters.get("sluice.reconnect.total").counter().count()).isGreaterThanOrEqualTo(2);
		String expected = "[grpc://127.0.0.1:" + grpcPort + "] tunnel stream error: io.grpc.StatusRuntimeException: "
				+ failure.getCode() + ": " + failure.getDescription();
		assertThat(output.getAll().lines().filter(line -> line.endsWith(expected))).as("one line per failed stream")
			.hasSizeGreaterThanOrEqualTo(2);
	}

	@Test
	void theJitterShortensTheWaitsWithinTheirCeilings() throws Exception {
		FailingServer fake = new FailingServer(Duration.ZERO, SessionRegistry.Refusal.LIMIT.status());
		int grpcPort = TestPorts.freePort();
		this.server = ServerBuilder.forPort(grpcPort).addService(fake).build().start();
		TunnelClient started = TunnelClient.builder()
			.properties(SluiceClientProperties.builder()
				.serverUrl("grpc://127.0.0.1:" + grpcPort)
				.upstream(Upstream.builder().host("late.local").target("http://127.0.0.1:1").build())
				.token("it-token")
				.build())
			.taskExecutor(task -> Thread.ofVirtual().name("e2e-jitter-worker").start(task))
			.meterRegistry(this.clientMeters)
			.randomFactory(() -> LOWEST)
			.build();
		started.start();
		this.client = started;

		Awaitility.await()
			.atMost(Duration.ofSeconds(15))
			.until(() -> fake.failedAt.size() >= 2 && fake.openedAt.size() >= 3);

		Duration first = Duration.ofNanos(fake.openedAt.get(1) - fake.failedAt.get(0));
		Duration second = Duration.ofNanos(fake.openedAt.get(2) - fake.failedAt.get(1));
		assertThat(first).as("half of the 1s ceiling").isBetween(Duration.ofMillis(450), Duration.ofMillis(900));
		assertThat(second).as("half of the 2s ceiling, not the full 2s")
			.isBetween(Duration.ofMillis(950), Duration.ofMillis(1700));
	}

	/** A jitter source that always draws the top or the bottom of the range asked. */
	private static final class FixedRandom implements RandomGenerator {

		private final boolean highest;

		FixedRandom(boolean highest) {
			this.highest = highest;
		}

		@Override
		public long nextLong() {
			return this.highest ? Long.MAX_VALUE : 0;
		}

		@Override
		public long nextLong(long bound) {
			return this.highest ? bound - 1 : 0;
		}

	}

	/**
	 * Takes every stream's frames but never acknowledges the advertise; fails each stream
	 * with {@code failure} {@code failAfter} after it opened, as the server does at its
	 * advertise timeout, or at once, as it does for a stream past the pending bound.
	 */
	private static final class FailingServer extends TunnelGrpc.TunnelImplBase {

		private final Duration failAfter;

		private final Status failure;

		final List<Long> openedAt = new CopyOnWriteArrayList<>();

		final List<Long> failedAt = new CopyOnWriteArrayList<>();

		final List<Frame> advertised = new CopyOnWriteArrayList<>();

		FailingServer(Duration failAfter, Status failure) {
			this.failAfter = failAfter;
			this.failure = failure;
		}

		@Override
		public StreamObserver<Frame> connect(StreamObserver<Frame> responses) {
			this.openedAt.add(System.nanoTime());
			if (this.failAfter.isZero()) {
				this.failedAt.add(System.nanoTime());
				responses.onError(this.failure.asRuntimeException());
				return new StreamObserver<>() {

					@Override
					public void onNext(Frame frame) {
					}

					@Override
					public void onError(Throwable t) {
					}

					@Override
					public void onCompleted() {
					}

				};
			}
			Thread.ofVirtual().start(() -> {
				try {
					TimeUnit.MILLISECONDS.sleep(this.failAfter.toMillis());
				}
				catch (InterruptedException e) {
					return;
				}
				this.failedAt.add(System.nanoTime());
				responses.onError(this.failure.asRuntimeException());
			});
			return new StreamObserver<>() {

				@Override
				public void onNext(Frame frame) {
					if (frame.hasAdvertise()) {
						FailingServer.this.advertised.add(frame);
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
