package am.ik.sluice.it;

import java.io.IOException;
import java.net.Socket;
import java.time.Duration;
import java.util.Objects;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.client.config.Upstream;
import am.ik.sluice.client.tunnel.TunnelClient;
import am.ik.sluice.server.SluiceServerApplication;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The client's connection limit: a CONNECT beyond it is answered with an ERROR without
 * dialing, which closes the public connection, while the connections already relayed are
 * unaffected.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class ClientConnectionLimitE2ETest {

	private static final byte[] NONE = new byte[0];

	private static int grpcPort;

	private static int dataPort;

	private static @Nullable HoldingUpstream upstream;

	private final SimpleMeterRegistry clientMeters = new SimpleMeterRegistry();

	private @Nullable TunnelClient client;

	@BeforeAll
	static void startUpstream() throws IOException {
		upstream = HoldingUpstream.start();
	}

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) {
		grpcPort = TestPorts.freePort();
		dataPort = TestPorts.freePort();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(dataPort));
		registry.add("sluice.token", () -> "it-token");
		registry.add("server.port", () -> 0);
	}

	@AfterAll
	void tearDown() throws IOException {
		if (this.client != null) {
			this.client.stop();
		}
		upstream().close();
	}

	@Test
	void aConnectBeyondTheLimitIsRefusedWithoutDialingAndTheFirstIsUnaffected() throws Exception {
		SluiceClientProperties properties = SluiceClientProperties.builder()
			.serverUrl("grpc://127.0.0.1:" + grpcPort)
			.upstream(Upstream.builder().host("held.local").target("http://127.0.0.1:" + upstream().port()).build())
			.maxConnections(1)
			.token("it-token")
			.build();
		TunnelClient started = TunnelClient.builder()
			.properties(properties)
			.taskExecutor(task -> Thread.ofVirtual().name("e2e-client-limit-worker").start(task))
			.meterRegistry(this.clientMeters)
			.build();
		started.start();
		this.client = started;
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(started::isConnected);
		assertThat(gauge("sluice.connections.limit")).isEqualTo(1);

		try (Socket first = TestSockets.establish(dataPort, NONE, "held.local")) {
			assertThat(gauge("sluice.connections.admitted")).isEqualTo(1);
			int accepted = upstream().accepted();
			try (Socket second = new Socket("127.0.0.1", dataPort)) {
				second.getOutputStream().write(TestSockets.get("held.local"));
				TestSockets.awaitClosed(second, Duration.ofSeconds(5));
			}
			assertThat(rejected()).isEqualTo(1);
			assertThat(upstream().accepted()).as("not dialed").isEqualTo(accepted);
			TestSockets.request(first, "held.local");
		}
		// the first connection's slot is free again
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> gauge("sluice.connections.admitted") == 0);
		try (Socket next = TestSockets.establish(dataPort, NONE, "held.local")) {
			assertThat(gauge("sluice.connections.admitted")).isEqualTo(1);
		}
		assertThat(rejected()).isEqualTo(1);
	}

	private double gauge(String name) {
		return this.clientMeters.get(name).gauge().value();
	}

	private double rejected() {
		return this.clientMeters.get("sluice.connections.rejected").tag("reason", "limit").counter().count();
	}

	private static HoldingUpstream upstream() {
		return Objects.requireNonNull(upstream, "upstream not started");
	}

}
