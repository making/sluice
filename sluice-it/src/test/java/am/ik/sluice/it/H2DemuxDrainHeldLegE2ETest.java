package am.ik.sluice.it;

import java.time.Duration;
import java.util.Objects;

import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;

import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.client.config.Upstream;
import am.ik.sluice.client.tunnel.TunnelClient;
import am.ik.sluice.server.SluiceServerApplication;
import am.ik.sluice.server.route.Router;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.TestInstance;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The half-close drain waits for every leg to have no stream left. An upstream that
 * answers a request without taking its body leaves DATA the relay cannot deliver: the leg
 * stays open, the drain cannot finish, and the client connection -- no stream open on its
 * hop any more -- ends at the idle timeout.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class H2DemuxDrainHeldLegE2ETest {

	private static final Duration IDLE_TIMEOUT = Duration.ofSeconds(2);

	/**
	 * More than the upstream's stream window, within what the relay takes from a client.
	 */
	private static final int BODY = 30_000;

	/** The stream window the upstream announces: far below the body. */
	private static final int UPSTREAM_WINDOW = 1000;

	private static final String ROUTE = "drain.held.test";

	private static int grpcPort;

	private static int dataPort;

	private @Nullable TunnelClient client;

	private @Nullable ScriptedH2Upstream upstream;

	@Nullable
	@Autowired
	private Router router;

	@Nullable
	@Autowired
	private MeterRegistry meterRegistry;

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) {
		grpcPort = TestPorts.freePort();
		dataPort = TestPorts.freePort();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(dataPort));
		registry.add("sluice.limits.idle-timeout", () -> IDLE_TIMEOUT.toSeconds() + "s");
		registry.add("sluice.token", () -> "it-token");
		registry.add("server.port", () -> String.valueOf(TestPorts.freePort()));
	}

	@AfterAll
	void tearDown() {
		if (this.client != null) {
			this.client.stop();
		}
		if (this.upstream != null) {
			this.upstream.close();
		}
	}

	@Test
	void aLegHoldingRequestDataTheUpstreamDoesNotTakeEndsAtTheIdleTimeoutAfterTheHalfClose() throws Exception {
		// answers the request head at once, never reads on: no WINDOW_UPDATE for the body
		ScriptedH2Upstream upstream = ScriptedH2Upstream.start(
				WindowedH2Upstream.setting(WindowedH2Upstream.SETTINGS_INITIAL_WINDOW_SIZE, UPSTREAM_WINDOW),
				(connection, frame) -> {
					if (frame.type() == TestH2.HEADERS) {
						connection.answer(frame.streamId(), "ok");
					}
				});
		this.upstream = upstream;
		startClient(upstream);
		double idle = idleClosed();
		try (H2TestClient h2 = H2TestClient.connect(dataPort)) {
			// the first stream: the leg has taken the upstream's SETTINGS by the next one
			h2.headers(1, true, TestH2.headerBlock(":authority=" + ROUTE, ":method=GET", ":path=/"));
			assertThat(h2.awaitEnd(1, Duration.ofSeconds(5))).isTrue();
			h2.headers(3, false, TestH2.headerBlock(":authority=" + ROUTE, ":method=POST", ":path=/"));
			assertThat(h2.data(3, BODY, true, Duration.ofSeconds(5))).as("the whole body taken by the relay").isTrue();
			assertThat(h2.awaitEnd(3, Duration.ofSeconds(5))).as("the response").isTrue();
			h2.halfClose();
			long ended = System.nanoTime();
			ScriptedH2Upstream.Connection leg = upstream.awaitConnection(0, Duration.ofSeconds(5));
			long delivered = leg.frames()
				.stream()
				.filter(frame -> frame.type() == TestH2.DATA)
				.mapToLong(frame -> frame.payload().length)
				.sum();
			assertThat(delivered).as("request bytes the upstream got: its window, not the body")
				.isLessThanOrEqualTo(UPSTREAM_WINDOW)
				.isGreaterThan(0);
			// the drain waits on the leg: past the half-close the leg is still there
			assertThat(h2.awaitTrue(h2::closed, Duration.ofMillis(1000))).as("the drain is waiting").isFalse();
			assertThat(leg.awaitEof(Duration.ofMillis(10))).as("the leg is held").isFalse();
			assertThat(admitted()).isEqualTo(1);
			assertThat(h2.awaitTrue(h2::closed, Duration.ofSeconds(5))).as("closed at the idle timeout").isTrue();
			assertThat(Duration.ofNanos(System.nanoTime() - ended)).isGreaterThan(Duration.ofMillis(1500));
			assertThat(h2.goAwayCode()).as("GOAWAY NO_ERROR").isZero();
			assertThat(h2.ended(3)).as("the stream ended on the client hop").isTrue();
			assertThat(h2.reset(3)).as("not reset").isNull();
			assertThat(leg.awaitClosed(Duration.ofSeconds(5))).as("the leg is released").isTrue();
		}
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> admitted() == 0);
		assertThat(idleClosed()).isEqualTo(idle + 1);
	}

	private void startClient(ScriptedH2Upstream upstream) {
		TunnelClient started = TunnelClient.builder()
			.properties(SluiceClientProperties.builder()
				.serverUrl("grpc://127.0.0.1:" + grpcPort)
				.upstream(Upstream.builder().host(ROUTE).target("tcp://127.0.0.1:" + upstream.port()).build())
				.token("it-token")
				.build())
			.taskExecutor(task -> Thread.ofVirtual().name("e2e-drain-held").start(task))
			.meterRegistry(new SimpleMeterRegistry())
			.build();
		started.start();
		this.client = started;
		Awaitility.await()
			.atMost(Duration.ofSeconds(10))
			.until(() -> Objects.requireNonNull(this.router).lookup(ROUTE).isPresent());
	}

	private int admitted() {
		return (int) Objects.requireNonNull(this.meterRegistry).get("sluice.data.connections.admitted").gauge().value();
	}

	private double idleClosed() {
		return Objects.requireNonNull(this.meterRegistry).get("sluice.data.connections.idle.closed").counter().count();
	}

}
