package am.ik.sluice.server.proxy;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;

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
import am.ik.sluice.it.TestPorts;
import am.ik.sluice.it.TestSockets;
import am.ik.sluice.server.SluiceServerApplication;
import am.ik.sluice.server.route.Router;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The server-wide verbatim idle timeout: it applies to the routes whose upstream sets
 * none, and a route's own timeout replaces it. In the relay's package for
 * {@link ConnectionLimits}.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class VerbatimIdleTimeoutServerDefaultE2ETest {

	private static final Duration SERVER_DEFAULT = Duration.ofSeconds(2);

	/** The route's own timeout: longer than the default, so a cut tells which applied. */
	private static final Duration ROUTE_TIMEOUT = Duration.ofSeconds(4);

	private static final Duration SLACK = Duration.ofSeconds(4);

	private static int grpcPort;

	private static int defaultedPort;

	private static int ownPort;

	private final CommandUpstream upstream = CommandUpstream.start();

	private @Nullable TunnelClient client;

	@Nullable
	@Autowired
	private MeterRegistry meterRegistry;

	@Nullable
	@Autowired
	private ConnectionLimits connectionLimits;

	@Nullable
	@Autowired
	private Router router;

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) {
		grpcPort = TestPorts.freePort();
		defaultedPort = TestPorts.freePort();
		ownPort = TestPorts.freePort();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(TestPorts.freePort()));
		registry.add("sluice.tcp-port-range", () -> defaultedPort + "," + ownPort);
		registry.add("sluice.limits.verbatim-idle-timeout", () -> SERVER_DEFAULT.toSeconds() + "s");
		registry.add("sluice.token", () -> "it-token");
		registry.add("server.port", () -> String.valueOf(TestPorts.freePort()));
	}

	@BeforeAll
	void startClient() {
		String target = "tcp://127.0.0.1:" + this.upstream.port();
		TunnelClient started = TunnelClient.builder()
			.properties(SluiceClientProperties.builder()
				.serverUrl("grpc://127.0.0.1:" + grpcPort)
				.upstream(Upstream.builder().host("defaulted.tcp").target(target).listenPort(defaultedPort).build())
				.upstream(Upstream.builder()
					.host("own.tcp")
					.target(target)
					.listenPort(ownPort)
					.verbatimIdleTimeout(ROUTE_TIMEOUT)
					.build())
				.token("it-token")
				.build())
			.taskExecutor(task -> Thread.ofVirtual().name("e2e-verbatim-default-worker").start(task))
			.meterRegistry(new SimpleMeterRegistry())
			.build();
		started.start();
		this.client = started;
		Awaitility.await()
			.atMost(Duration.ofSeconds(10))
			.until(() -> router().lookupByPort(defaultedPort).isPresent()
					&& router().lookupByPort(ownPort).isPresent());
		Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> reachable(defaultedPort) && reachable(ownPort));
	}

	private static boolean reachable(int port) {
		try (Socket socket = new Socket("127.0.0.1", port)) {
			return socket.isConnected();
		}
		catch (IOException e) {
			return false;
		}
	}

	@BeforeEach
	void awaitQuiet() {
		Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> admitted() == 0);
	}

	@AfterAll
	void tearDown() {
		if (this.client != null) {
			this.client.stop();
		}
		this.upstream.close();
	}

	@Test
	void aRouteSettingNoTimeoutGetsTheServerDefault() throws Exception {
		Router.Route route = router().lookupByPort(defaultedPort).orElseThrow();
		assertThat(route.verbatimIdleTimeout()).isZero();
		assertThat(connectionLimits().verbatimIdleTimeout(route)).isEqualTo(SERVER_DEFAULT);
		assertClosedAfter(defaultedPort, SERVER_DEFAULT);
	}

	@Test
	void aRouteTimeoutReplacesTheServerDefault() throws Exception {
		Router.Route route = router().lookupByPort(ownPort).orElseThrow();
		assertThat(connectionLimits().verbatimIdleTimeout(route)).isEqualTo(ROUTE_TIMEOUT);
		assertClosedAfter(ownPort, ROUTE_TIMEOUT);
	}

	/**
	 * An idle connection to {@code port} is closed after {@code timeout}, before the
	 * slack past it, and counted.
	 */
	private void assertClosedAfter(int port, Duration timeout) throws IOException {
		double closed = verbatimIdleClosed();
		try (Socket socket = new Socket("127.0.0.1", port)) {
			long start = System.nanoTime();
			OutputStream out = socket.getOutputStream();
			out.write("sink\r\n".getBytes(StandardCharsets.US_ASCII));
			out.flush();
			TestSockets.awaitClosed(socket, timeout.plus(SLACK));
			assertThat(Duration.ofNanos(System.nanoTime() - start)).isGreaterThanOrEqualTo(timeout);
		}
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> verbatimIdleClosed() > closed);
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> admitted() == 0);
	}

	private int admitted() {
		return (int) meterRegistry().get("sluice.data.connections.admitted").gauge().value();
	}

	private double verbatimIdleClosed() {
		return meterRegistry().get("sluice.data.connections.verbatim.idle.closed").counter().count();
	}

	private MeterRegistry meterRegistry() {
		return Objects.requireNonNull(this.meterRegistry, "meterRegistry");
	}

	private ConnectionLimits connectionLimits() {
		return Objects.requireNonNull(this.connectionLimits, "connectionLimits");
	}

	private Router router() {
		return Objects.requireNonNull(this.router, "router");
	}

}
