package am.ik.sluice.it;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.StandardSocketOptions;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Objects;

import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;

import jdk.net.ExtendedSocketOptions;
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
import am.ik.sluice.server.config.SluiceServerProperties;
import am.ik.sluice.server.proxy.HookedAccessLogger;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The global data plane connection limit: one count shared by the data port and every tcp
 * route port, taken at accept and released exactly once on every path a connection ends
 * by. A connection beyond the limit is closed unread. Every accepted socket probes its
 * client with TCP keepalive after the configured idle time.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
@Import(ConnectionReleaseE2ETest.AcceptHook.class)
class ConnectionReleaseE2ETest {

	private static final byte[] NONE = new byte[0];

	private static final Duration TCP_KEEP_ALIVE = Duration.ofSeconds(7);

	/** The client's connect timeout, far below its default. */
	private static final Duration CONNECT_TIMEOUT = Duration.ofMillis(500);

	private static int grpcPort;

	private static int dataPort;

	private static int tcpRoutePort;

	private static int deniedTcpRoutePort;

	/** Reserved and never bound: a dial to it is refused. */
	private static int refusingPort;

	private static @Nullable HoldingUpstream upstream;

	/** Never accepts: its backlog completes the TCP connect, nothing answers after. */
	private static @Nullable ServerSocket silentUpstream;

	private @Nullable TunnelClient client;

	@Autowired
	private @Nullable MeterRegistry meterRegistry;

	@Autowired
	private @Nullable HookedAccessLogger accessLogger;

	@Autowired
	private @Nullable SluiceServerProperties properties;

	@TestConfiguration
	static class AcceptHook {

		@Bean
		@Primary
		HookedAccessLogger hookedAccessLogger(SluiceServerProperties properties) {
			return new HookedAccessLogger(properties);
		}

	}

	@BeforeAll
	static void startUpstream() throws IOException {
		upstream = HoldingUpstream.start();
		silentUpstream = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
	}

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) {
		grpcPort = TestPorts.freePort();
		dataPort = TestPorts.freePort();
		tcpRoutePort = TestPorts.freePort();
		deniedTcpRoutePort = TestPorts.freePort();
		refusingPort = TestPorts.freePort();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(dataPort));
		registry.add("sluice.tcp-port-range", () -> tcpRoutePort + "," + deniedTcpRoutePort);
		registry.add("sluice.limits.max-connections", () -> "64");
		registry.add("sluice.limits.tcp-keep-alive", () -> TCP_KEEP_ALIVE.toSeconds() + "s");
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
			.upstream(Upstream.builder()
				.host("tls-held.local")
				.target("https://127.0.0.1:" + Objects.requireNonNull(silentUpstream).getLocalPort())
				.build())
			.upstream(Upstream.builder().host("refused.local").target("http://127.0.0.1:" + refusingPort).build())
			.upstream(Upstream.builder().host("held.tcp").target("tcp://" + target).listenPort(tcpRoutePort).build())
			.upstream(Upstream.builder()
				.host("denied.tcp")
				.target("tcp://" + target)
				.listenPort(deniedTcpRoutePort)
				.allowedCidrs(List.of("203.0.113.0/24"))
				.build())
			.token("it-token")
			.connectTimeout(CONNECT_TIMEOUT)
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
		Objects.requireNonNull(silentUpstream).close();
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

	@Test
	void acceptedSocketsProbeTheirClientsAfterTheConfiguredIdleTime() throws Exception {
		assertThat(Objects.requireNonNull(this.properties).limits().tcpKeepAlive()).isEqualTo(TCP_KEEP_ALIVE);
		assertKeepAlive(dataPort, "held.local");
		assertKeepAlive(tcpRoutePort, "held.tcp");
	}

	private void assertKeepAlive(int port, String host) throws Exception {
		try (Socket socket = TestSockets.establish(port, NONE, host)) {
			Socket accepted = accessLogger().last();
			// the server's end of this very connection
			assertThat(accepted.getLocalPort()).isEqualTo(port);
			assertThat(accepted.getPort()).isEqualTo(socket.getLocalPort());
			assertThat(accepted.getOption(StandardSocketOptions.SO_KEEPALIVE)).isTrue();
			assertThat(accepted.getOption(ExtendedSocketOptions.TCP_KEEPIDLE))
				.isEqualTo((int) TCP_KEEP_ALIVE.toSeconds());
		}
	}

	/**
	 * The accepted socket fails as its options are set (a client that reset the
	 * connection does that on macOS; here the server's end is closed under the accept
	 * loop): the slot it was admitted with is released.
	 */
	@Test
	void aSocketFailingAsItIsTakenUpReleasesItsSlot() throws Exception {
		assertFailingSocketReleased(dataPort, "held.local");
		assertFailingSocketReleased(tcpRoutePort, "held.tcp");
	}

	private void assertFailingSocketReleased(int port, String host) throws Exception {
		int before = baseline();
		accessLogger().closeNext();
		try (Socket socket = new Socket("127.0.0.1", port)) {
			TestSockets.awaitClosed(socket, Duration.ofSeconds(5));
		}
		// the accept loop takes the connections in turn: one relayed after it is past
		// the failed one
		try (Socket later = TestSockets.establish(port, NONE, host)) {
			assertThat(accessLogger().last().isClosed()).isFalse();
		}
		awaitReleased(before);
	}

	/**
	 * The client's dial of an https upstream that never answers the TLS handshake fails
	 * at the client's connect timeout: the request is answered 502, the connection ends
	 * and releases its slot.
	 */
	@Test
	void aTlsUpstreamNeverAnsweringIsAnswered502AtTheClientsConnectTimeout() throws Exception {
		// well inside the default connect timeout of 10s
		assertThat(assertAnswered502AndReleased("tls-held.local", 0, false)).isGreaterThanOrEqualTo(CONNECT_TIMEOUT);
	}

	/**
	 * The client's dial of an upstream that refuses the connection fails while the client
	 * still uploads its request body: the request is answered 502, and the body's rest is
	 * read, not left to reset the answer.
	 */
	@Test
	void aRefusedUpstreamIsAnswered502() throws Exception {
		assertAnswered502AndReleased("refused.local", 512 * 1024, false);
	}

	/**
	 * The upstream reads the whole request, then closes without answering: the first
	 * request of the upstream connection is answered 502; a later one ends the connection
	 * unanswered, as the reused upstream connection did, so the client may retry it.
	 */
	@Test
	void anUpstreamClosingUnansweredIsAnswered502OnItsFirstRequestOnly() throws Exception {
		// the holding upstream takes a request other than GET to its end, then closes
		assertAnswered502AndReleased("held.local", 16, true);

		int before = baseline();
		try (Socket socket = TestSockets.establish(dataPort, NONE, "held.local")) {
			socket.getOutputStream()
				.write("POST / HTTP/1.1\r\nHost: held.local\r\nContent-Length: 0\r\n\r\n"
					.getBytes(StandardCharsets.US_ASCII));
			socket.shutdownOutput();
			assertThat(socket.getInputStream().readAllBytes()).as("after the GET's 200").isEmpty();
		}
		awaitReleased(before);
	}

	/**
	 * Sends a request to {@code host} -- a POST of {@code bodyLength} bytes, written
	 * while the answer is read, or a GET without one -- and half-closes the connection
	 * after it when {@code endRequest}; asserts the whole 502 arrives, then the
	 * connection's end. Returns how long the 502 took.
	 */
	private Duration assertAnswered502AndReleased(String host, int bodyLength, boolean endRequest) throws Exception {
		int before = baseline();
		Duration took;
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(5000);
			OutputStream out = socket.getOutputStream();
			long start = System.nanoTime();
			out.write((bodyLength == 0 ? new String(TestSockets.get(host), StandardCharsets.US_ASCII)
					: "POST / HTTP/1.1\r\nHost: " + host + "\r\nContent-Length: " + bodyLength + "\r\n\r\n")
				.getBytes(StandardCharsets.US_ASCII));
			Thread upload = Thread.ofVirtual().start(() -> {
				try {
					out.write(new byte[bodyLength]);
					if (endRequest) {
						socket.shutdownOutput();
					}
				}
				catch (IOException e) {
					throw new UncheckedIOException(e);
				}
			});
			// the whole answer, then the end of the connection
			String response = new String(socket.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
			took = Duration.ofNanos(System.nanoTime() - start);
			assertThat(response).startsWith("HTTP/1.1 502 Bad Gateway\r\n")
				.contains("Content-Type: application/problem+json\r\n", "Connection: close\r\n")
				.endsWith("\"status\":502,\"detail\":\"The upstream ended before answering.\"}");
			assertThat(upload.join(Duration.ofSeconds(5))).as("the upload ended").isTrue();
		}
		awaitReleased(before);
		return took;
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

	private HookedAccessLogger accessLogger() {
		return Objects.requireNonNull(this.accessLogger, "accessLogger");
	}

	private MeterRegistry meterRegistry() {
		return Objects.requireNonNull(this.meterRegistry, "meterRegistry");
	}

	private static HoldingUpstream upstream() {
		return Objects.requireNonNull(upstream, "upstream not started");
	}

}
