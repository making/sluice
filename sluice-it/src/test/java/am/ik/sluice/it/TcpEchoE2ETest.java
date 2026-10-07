package am.ik.sluice.it;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import org.springframework.core.task.TaskExecutor;

import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.client.config.Upstream;
import am.ik.sluice.client.tunnel.TunnelClient;
import am.ik.sluice.server.SluiceServerApplication;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End to end through a plain TCP echo upstream: the echo server is a raw socket peer
 * speaking no known protocol, so the connection must be relayed verbatim over the tcp
 * route (bidirectional echo plus half-close propagation).
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class TcpEchoE2ETest {

	private static final TaskExecutor TASK_EXECUTOR = task -> Thread.ofVirtual().name("e2e-echo-worker").start(task);

	private static int grpcPort;

	private static int dataPort;

	private static int tcpRoutePort;

	/** a listen port outside the server-side tcp-port-range */
	private static int unclaimedRoutePort;

	/** a listen port in the server-side tcp-port-range that a test holds bound */
	private static int occupiedRoutePort;

	private static @Nullable ServerSocket echoServer;

	private @Nullable TunnelClient client;

	private SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) {
		grpcPort = TestPorts.freePort();
		dataPort = TestPorts.freePort();
		tcpRoutePort = TestPorts.freePort();
		unclaimedRoutePort = TestPorts.freePort();
		occupiedRoutePort = TestPorts.freePort();
		try {
			ServerSocket server = new ServerSocket();
			server.setReuseAddress(true);
			server.bind(new InetSocketAddress("127.0.0.1", 0), 128);
			echoServer = server;
		}
		catch (Exception e) {
			throw new IllegalStateException(e);
		}
		Thread.ofVirtual().name("e2e-echo-accept").start(TcpEchoE2ETest::acceptLoop);
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(dataPort));
		// only the intended route ports are claimable
		registry.add("sluice.tcp-port-range", () -> tcpRoutePort + "," + occupiedRoutePort);
		registry.add("sluice.token", () -> "it-token");
		registry.add("server.port", () -> 0);
	}

	private static ServerSocket server() {
		return java.util.Objects.requireNonNull(echoServer, "echo server not started");
	}

	private static void acceptLoop() {
		while (!server().isClosed()) {
			try {
				Socket socket = server().accept();
				Thread.ofVirtual().name("e2e-echo-conn").start(() -> echo(socket));
			}
			catch (Exception e) {
				return;
			}
		}
	}

	/** Echoes bytes back until the peer half-closes, then closes. */
	private static void echo(Socket socket) {
		try (socket; InputStream in = socket.getInputStream(); OutputStream out = socket.getOutputStream()) {
			byte[] buffer = new byte[1024];
			int n;
			while ((n = in.read(buffer)) > 0) {
				out.write(buffer, 0, n);
				out.flush();
			}
		}
		catch (Exception e) {
			// connection torn down; nothing to do
		}
	}

	/** Starts a fresh client advertising exactly the given upstreams. */
	private void startClient(java.util.List<Upstream> upstreams) {
		if (this.client != null) {
			this.client.stop();
		}
		SluiceClientProperties.Builder properties = SluiceClientProperties.builder()
			.serverUrl("grpc://127.0.0.1:" + grpcPort);
		upstreams.forEach(properties::upstream);
		SluiceClientProperties built = properties.token("it-token").build();
		this.meterRegistry = new SimpleMeterRegistry();
		TunnelClient started = TunnelClient.builder()
			.properties(built)
			.taskExecutor(TASK_EXECUTOR)
			.meterRegistry(this.meterRegistry)
			.build();
		started.start();
		this.client = started;
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(started::isConnected);
	}

	private Upstream echoUpstream(int listenPort) {
		return Upstream.builder()
			.host("echo.local")
			.target("tcp://127.0.0.1:" + server().getLocalPort())
			.listenPort(listenPort)
			.build();
	}

	@AfterAll
	void tearDown() {
		if (this.client != null) {
			this.client.stop();
		}
		try {
			server().close();
		}
		catch (Exception e) {
			// ignore
		}
	}

	@Test
	void roundTripEchoesBytesAndPropagatesHalfClose() throws Exception {
		this.startClient(java.util.List.of(echoUpstream(tcpRoutePort)));
		try (Socket socket = new Socket("127.0.0.1", tcpRoutePort)) {
			socket.setSoTimeout(10_000);
			OutputStream out = socket.getOutputStream();
			out.write("hello sluice".getBytes(StandardCharsets.US_ASCII));
			out.flush();
			InputStream in = socket.getInputStream();
			// bidirectional: the echo comes back while the connection stays open
			byte[] echoed = in.readNBytes("hello sluice".length());
			assertThat(new String(echoed, StandardCharsets.US_ASCII)).isEqualTo("hello sluice");
			// half-close: our EOF travels to the echo server, whose close travels back
			socket.shutdownOutput();
			assertThat(in.readAllBytes()).isEmpty();
			assertThat(in.read()).isEqualTo(-1);
		}
	}

	@Test
	void listenPortOutsideServerRangeIsRejectedAndReadvertised() {
		this.startClient(java.util.List.of(echoUpstream(unclaimedRoutePort)));
		// the out-of-range port is reported back on ADVERTISE_ACK; the client keeps the
		// stream and re-advertises on it (the counter grows beyond the first advertise)
		Awaitility.await()
			.atMost(Duration.ofSeconds(10))
			.until(() -> counted("sluice.advertise.rejected"), count -> count >= 2);
		assertThat(reachable(unclaimedRoutePort)).isFalse();
		assertThat(Objects.requireNonNull(this.client).isConnected()).isTrue();
		assertThat(counted("sluice.reconnect.total")).isZero();
	}

	@Test
	void occupiedListenPortIsClaimedOnceFreedWithoutReconnecting() throws Exception {
		try (ServerSocket blocker = new ServerSocket()) {
			blocker.bind(new InetSocketAddress("0.0.0.0", occupiedRoutePort));
			this.startClient(java.util.List.of(echoUpstream(occupiedRoutePort)));
			Awaitility.await()
				.atMost(Duration.ofSeconds(5))
				.until(() -> counted("sluice.advertise.rejected"), count -> count >= 1);
		}
		Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> reachable(occupiedRoutePort));
		try (Socket socket = new Socket("127.0.0.1", occupiedRoutePort)) {
			socket.setSoTimeout(10_000);
			socket.getOutputStream().write("freed".getBytes(StandardCharsets.US_ASCII));
			socket.shutdownOutput();
			assertThat(new String(socket.getInputStream().readAllBytes(), StandardCharsets.US_ASCII))
				.isEqualTo("freed");
		}
		assertThat(counted("sluice.reconnect.total")).isZero();
	}

	private double counted(String name) {
		return this.meterRegistry.find(name)
			.counters()
			.stream()
			.mapToDouble(io.micrometer.core.instrument.Counter::count)
			.sum();
	}

	private static boolean reachable(int port) {
		try (Socket socket = new Socket("127.0.0.1", port)) {
			return socket.isConnected();
		}
		catch (Exception e) {
			return false;
		}
	}

}
