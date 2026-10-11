package am.ik.sluice.it;

import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
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
 * The per-source-address connection limit behind an SNAT front end speaking the PROXY
 * protocol: the header source is the address limited, so one source at its limit does not
 * keep another out; a headerless connection is limited by its socket peer. The PROXY
 * header of a tcp route connection is under the head deadline.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class PerAddressConnectionLimitE2ETest {

	private static final byte[] SOURCE_A = proxyV1("203.0.113.7");

	private static final byte[] SOURCE_B = proxyV1("198.51.100.9");

	private static final byte[] HEADERLESS = new byte[0];

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
		registry.add("sluice.proxy-protocol", () -> "true");
		registry.add("sluice.limits.max-connections-per-address", () -> "1");
		registry.add("sluice.limits.head-timeout", () -> "2s");
		registry.add("sluice.token", () -> "it-token");
		registry.add("server.port", () -> 0);
	}

	private static byte[] proxyV1(String source) {
		return ("PROXY TCP4 " + source + " 192.0.2.1 40000 80\r\n").getBytes(StandardCharsets.US_ASCII);
	}

	@BeforeEach
	void startClientAndAwaitNoAdmittedConnection() {
		startClient();
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> admitted() == 0);
	}

	private void startClient() {
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
			.taskExecutor(task -> Thread.ofVirtual().name("e2e-per-address-worker").start(task))
			.meterRegistry(new SimpleMeterRegistry())
			.build();
		started.start();
		this.client = started;
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(started::isConnected);
		Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
			try (Socket probe = TestSockets.establish(tcpRoutePort, SOURCE_A, "held.tcp")) {
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
	void aSourceAtItsLimitIsRejectedWhileAnotherSourceIsAdmitted() throws Exception {
		double rejected = rejected("address-limit");
		try (Socket first = TestSockets.establish(dataPort, SOURCE_A, "held.local")) {
			try (Socket second = new Socket("127.0.0.1", dataPort)) {
				second.getOutputStream().write(SOURCE_A);
				second.getOutputStream().write(TestSockets.get("held.local"));
				TestSockets.awaitClosed(second, Duration.ofSeconds(1));
			}
			assertThat(rejected("address-limit")).isEqualTo(rejected + 1);
			try (Socket other = TestSockets.establish(dataPort, SOURCE_B, "held.local")) {
				assertThat(admitted()).isEqualTo(2);
			}
			// the admitted connection of the limited source is unaffected
			TestSockets.request(first, "held.local");
		}
		// its slot is free again
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> admitted() == 0);
		try (Socket again = TestSockets.establish(dataPort, SOURCE_A, "held.local")) {
			assertThat(admitted()).isEqualTo(1);
		}
	}

	@Test
	void tcpRoutePortsLimitTheProxiedSourceToo() throws Exception {
		double rejected = rejected("address-limit");
		try (Socket first = TestSockets.establish(tcpRoutePort, SOURCE_A, "held.tcp");
				Socket second = new Socket("127.0.0.1", tcpRoutePort)) {
			second.getOutputStream().write(SOURCE_A);
			TestSockets.awaitClosed(second, Duration.ofSeconds(1));
			assertThat(rejected("address-limit")).isEqualTo(rejected + 1);
		}
	}

	@Test
	void aHeaderlessConnectionIsLimitedByItsSocketPeer() throws Exception {
		double rejected = rejected("address-limit");
		try (Socket first = TestSockets.establish(dataPort, HEADERLESS, "held.local");
				Socket second = new Socket("127.0.0.1", dataPort)) {
			second.getOutputStream().write(TestSockets.get("held.local"));
			TestSockets.awaitClosed(second, Duration.ofSeconds(1));
			assertThat(rejected("address-limit")).isEqualTo(rejected + 1);
			// a proxied source is another address than the front end's
			try (Socket proxied = TestSockets.establish(dataPort, SOURCE_B, "held.local")) {
				assertThat(admitted()).isEqualTo(2);
			}
		}
	}

	@Test
	void aTcpRouteConnectionWithoutItsProxyHeaderIsClosedAtTheDeadline() throws Exception {
		double timeouts = rejected("head-timeout");
		try (Socket socket = new Socket("127.0.0.1", tcpRoutePort)) {
			assertThat(TestSockets.awaitClosed(socket, Duration.ofSeconds(3))).isGreaterThan(Duration.ofMillis(1500));
		}
		assertThat(rejected("head-timeout")).isEqualTo(timeouts + 1);
	}

	private int admitted() {
		return (int) meterRegistry().get("sluice.data.connections.admitted").gauge().value();
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
