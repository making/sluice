package am.ik.sluice.it;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.time.Duration;
import java.util.List;
import java.util.Objects;

import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;

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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The global data plane connection limit: one count shared by the data port and every tcp
 * route port, taken at accept and released exactly once on every path a connection ends
 * by. A connection beyond the limit is closed unread.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class ConnectionReleaseE2ETest {

	private static final byte[] NONE = new byte[0];

	private static int grpcPort;

	private static int dataPort;

	private static int tcpRoutePort;

	private static int deniedTcpRoutePort;

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
		deniedTcpRoutePort = TestPorts.freePort();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(dataPort));
		registry.add("sluice.tcp-port-range", () -> tcpRoutePort + "," + deniedTcpRoutePort);
		registry.add("sluice.limits.max-connections", () -> "64");
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
			.upstream(Upstream.builder()
				.host("forbidden.local")
				.target("http://" + target)
				.allowedCidrs(List.of("203.0.113.0/24"))
				.build())
			.upstream(
					Upstream.builder().host("passthrough.local").target("tcp://" + target).tlsPassthrough(true).build())
			.upstream(Upstream.builder().host("held.tcp").target("tcp://" + target).listenPort(tcpRoutePort).build())
			.upstream(Upstream.builder()
				.host("denied.tcp")
				.target("tcp://" + target)
				.listenPort(deniedTcpRoutePort)
				.allowedCidrs(List.of("203.0.113.0/24"))
				.build())
			.token("it-token")
			.build();
		TunnelClient started = TunnelClient.builder()
			.properties(properties)
			.taskExecutor(task -> Thread.ofVirtual().name("e2e-release-worker").start(task))
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
	void aConnectionClosedBeforeItsFirstByteReleasesItsSlot() throws Exception {
		int before = baseline();
		new Socket("127.0.0.1", dataPort).close();
		awaitReleased(before);
	}

	@Test
	void aNoRouteAnswerReleasesItsSlot() throws Exception {
		assertAnsweredAndReleased("unknown.local", "HTTP/1.1 503");
	}

	@Test
	void aForbiddenAnswerReleasesItsSlot() throws Exception {
		assertAnsweredAndReleased("forbidden.local", "HTTP/1.1 403");
	}

	@Test
	void theEndOfAnHttp1RelayReleasesItsSlot() throws Exception {
		int before = baseline();
		try (Socket socket = TestSockets.establish(dataPort, NONE, "held.local")) {
			assertThat(admitted()).isEqualTo(before + 1);
		}
		awaitReleased(before);
	}

	@Test
	void theEndOfAnH2RelayReleasesItsSlot() throws Exception {
		int before = baseline();
		int accepted = upstream().accepted();
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			OutputStream out = socket.getOutputStream();
			TestH2.writeClientHead(out, 1, ":method=GET", ":scheme=http", ":authority=held.local", ":path=/");
			out.flush();
			// the demux opened its leg: the upstream holds the h2 connection unanswered
			Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> upstream().accepted() > accepted);
			assertThat(admitted()).isEqualTo(before + 1);
		}
		awaitReleased(before);
	}

	@Test
	void theEndOfATlsPassthroughRelayReleasesItsSlot() throws Exception {
		int before = baseline();
		int accepted = upstream().accepted();
		try (SSLSocket socket = (SSLSocket) SSLContext.getDefault()
			.getSocketFactory()
			.createSocket("127.0.0.1", dataPort)) {
			SSLParameters parameters = socket.getSSLParameters();
			parameters.setServerNames(List.of(new SNIHostName("passthrough.local")));
			socket.setSSLParameters(parameters);
			socket.setSoTimeout(1000);
			// the upstream never answers a ClientHello: the handshake fails on the
			// timeout, with the relay established
			assertThatThrownBy(socket::startHandshake).isInstanceOf(IOException.class);
			assertThat(upstream().accepted()).isGreaterThan(accepted);
			assertThat(admitted()).isEqualTo(before + 1);
		}
		awaitReleased(before);
	}

	@Test
	void aFailedHeadReleasesItsSlot() throws Exception {
		int before = baseline();
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			// a TLS record header followed by no ClientHello: the head parse fails
			socket.getOutputStream().write(new byte[] { 0x16, 0x03, 0x01, 0x00, 0x04, 0x7f, 0x7f, 0x7f, 0x7f });
			socket.shutdownOutput();
			TestSockets.awaitClosed(socket, Duration.ofSeconds(5));
		}
		awaitReleased(before);
	}

	@Test
	void theEndOfATcpRouteRelayReleasesItsSlot() throws Exception {
		int before = baseline();
		try (Socket socket = TestSockets.establish(tcpRoutePort, NONE, "held.tcp")) {
			assertThat(admitted()).isEqualTo(before + 1);
		}
		awaitReleased(before);
	}

	@Test
	void aDeniedTcpRouteConnectionReleasesItsSlot() throws Exception {
		int before = baseline();
		try (Socket socket = new Socket("127.0.0.1", deniedTcpRoutePort)) {
			TestSockets.awaitClosed(socket, Duration.ofSeconds(5));
		}
		awaitReleased(before);
	}

	private void assertAnsweredAndReleased(String host, String status) throws Exception {
		int before = baseline();
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(5000);
			socket.getOutputStream().write(TestSockets.get(host));
			String head = TestSockets.readUntil(socket.getInputStream(), status.length());
			assertThat(head).isEqualTo(status);
		}
		awaitReleased(before);
	}

	/**
	 * The admitted count before a test's connection: 0 once the connections of earlier
	 * tests are released, else what they leaked -- so a leak fails only the test of the
	 * path that leaks.
	 */
	private int baseline() throws InterruptedException {
		long deadline = System.nanoTime() + Duration.ofSeconds(3).toNanos();
		while (admitted() > 0 && System.nanoTime() < deadline) {
			Thread.sleep(50);
		}
		return admitted();
	}

	private void awaitReleased(int before) {
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> admitted() <= before);
	}

	private int admitted() {
		return (int) meterRegistry().get("sluice.data.connections.admitted").gauge().value();
	}

	private MeterRegistry meterRegistry() {
		return Objects.requireNonNull(this.meterRegistry, "meterRegistry");
	}

	private static HoldingUpstream upstream() {
		return Objects.requireNonNull(upstream, "upstream not started");
	}

}
