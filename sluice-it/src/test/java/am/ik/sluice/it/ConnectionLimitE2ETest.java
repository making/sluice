package am.ik.sluice.it;

import java.io.IOException;
import java.net.Socket;
import java.time.Duration;
import java.util.Objects;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.client.config.Upstream;
import am.ik.sluice.client.tunnel.TunnelClient;
import am.ik.sluice.server.SluiceServerApplication;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The global data plane connection limit: one count shared by the data port and every tcp
 * route port, taken at accept and released exactly once on every path a connection ends
 * by. A connection beyond the limit is closed unread, the overflow being off.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class ConnectionLimitE2ETest {

	private static final byte[] NONE = new byte[0];

	private static int grpcPort;

	private static int dataPort;

	private static int tcpRoutePort;

	private static @Nullable HoldingUpstream upstream;

	private @Nullable TunnelClient client;

	@Autowired
	private @Nullable MeterRegistry meterRegistry;

	@BeforeAll
	static void startUpstream() throws IOException {
		upstream = HoldingUpstream.start();
	}

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) {
		grpcPort = TestPorts.freePort();
		dataPort = TestPorts.freePort();
		tcpRoutePort = TestPorts.freePort();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(dataPort));
		registry.add("sluice.tcp-port-range", () -> String.valueOf(tcpRoutePort));
		registry.add("sluice.limits.max-connections", () -> "2");
		// past the limit every connection is closed at accept; the overflow answering a
		// few
		// of them is ConnectionLimitOverflowE2ETest's
		registry.add("sluice.limits.overflow-connections", () -> "0");
		registry.add("sluice.token", () -> "it-token");
		registry.add("server.port", () -> 0);
	}

	@BeforeEach
	void startClient() {
		if (this.client != null) {
			return;
		}
		String target = "127.0.0.1:" + upstream().port();
		SluiceClientProperties properties = SluiceClientProperties.builder()
			.serverUrl("grpc://127.0.0.1:" + grpcPort)
			.upstream(Upstream.builder().host("held.local").target("http://" + target).build())
			.upstream(Upstream.builder().host("held.tcp").target("tcp://" + target).listenPort(tcpRoutePort).build())
			.token("it-token")
			.build();
		TunnelClient started = TunnelClient.builder()
			.properties(properties)
			.taskExecutor(task -> Thread.ofVirtual().name("e2e-limit-worker").start(task))
			.meterRegistry(new SimpleMeterRegistry())
			.build();
		started.start();
		this.client = started;
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(started::isConnected);
		Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
			try (Socket probe = TestSockets.establish(tcpRoutePort, NONE, "held.tcp")) {
				// the tcp route listener is bound
			}
		});
	}

	@AfterAll
	void tearDown() throws IOException {
		if (this.client != null) {
			this.client.stop();
		}
		upstream().close();
	}

	@Test
	void aConnectionBeyondTheLimitIsClosedUnreadAndAFreedSlotAdmitsTheNext() throws Exception {
		awaitAdmitted(0);
		assertThat(gauge("sluice.data.connections.limit")).isEqualTo(2);
		double rejected = rejected("limit");
		try (Socket first = TestSockets.establish(dataPort, NONE, "held.local");
				Socket second = TestSockets.establish(dataPort, NONE, "held.local")) {
			assertThat(admitted()).isEqualTo(2);
			try (Socket third = new Socket("127.0.0.1", dataPort)) {
				third.getOutputStream().write(TestSockets.get("held.local"));
				assertThat(TestSockets.awaitClosed(third, Duration.ofSeconds(1))).isLessThan(Duration.ofSeconds(1));
			}
			assertThat(rejected("limit")).isEqualTo(rejected + 1);
			// the admitted connections are unaffected
			TestSockets.request(second, "held.local");
			first.close();
			awaitAdmitted(1);
			try (Socket next = TestSockets.establish(dataPort, NONE, "held.local")) {
				assertThat(admitted()).isEqualTo(2);
			}
		}
	}

	@Test
	void tcpRoutePortsCountAgainstTheSameLimit() throws Exception {
		awaitAdmitted(0);
		double rejected = rejected("limit");
		try (Socket data = TestSockets.establish(dataPort, NONE, "held.local");
				Socket tcp = TestSockets.establish(tcpRoutePort, NONE, "held.tcp")) {
			assertThat(admitted()).isEqualTo(2);
			try (Socket overTcp = new Socket("127.0.0.1", tcpRoutePort);
					Socket overData = new Socket("127.0.0.1", dataPort)) {
				TestSockets.awaitClosed(overTcp, Duration.ofSeconds(1));
				TestSockets.awaitClosed(overData, Duration.ofSeconds(1));
			}
			assertThat(rejected("limit")).isEqualTo(rejected + 2);
		}
	}

	private void awaitAdmitted(int expected) {
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> admitted() == expected);
	}

	private int admitted() {
		return (int) gauge("sluice.data.connections.admitted");
	}

	private double gauge(String name) {
		return meterRegistry().get(name).gauge().value();
	}

	private double rejected(String reason) {
		return meterRegistry().get("sluice.data.connections.rejected").tag("reason", reason).counter().count();
	}

	private MeterRegistry meterRegistry() {
		return Objects.requireNonNull(this.meterRegistry, "meterRegistry");
	}

	private static HoldingUpstream upstream() {
		return Objects.requireNonNull(upstream, "upstream not started");
	}

}
