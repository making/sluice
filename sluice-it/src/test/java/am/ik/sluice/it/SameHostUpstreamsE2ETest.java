package am.ik.sluice.it;

import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.sun.net.httpserver.HttpServer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.jspecify.annotations.Nullable;

import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.client.config.Upstream;
import am.ik.sluice.client.tunnel.TunnelClient;
import am.ik.sluice.server.SluiceServerApplication;
import am.ik.sluice.server.tunnel.SessionRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.task.TaskExecutor;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End to end verification that a single client may declare an http upstream and a tcp
 * upstream sharing one host: both routes stay active instead of the second declaration
 * collapsing the first on the client side.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class SameHostUpstreamsE2ETest {

	private static final String HOST = "dual.local";

	private static final TaskExecutor TASK_EXECUTOR = task -> Thread.ofVirtual().name("e2e-same-host").start(task);

	private static int grpcPort;

	private static int dataPort;

	private static int tcpRoutePort;

	private static @Nullable HttpServer httpUpstream;

	private static @Nullable ServerSocket tcpUpstream;

	private static final ExecutorService UPSTREAM_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

	private final List<TunnelClient> clients = new ArrayList<>();

	@Autowired
	SessionRegistry sessions;

	@BeforeAll
	static void startUpstreams() throws Exception {
		httpUpstream = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		int httpPort = httpUpstream.getAddress().getPort();
		httpUpstream.createContext("/", exchange -> {
			byte[] body = String.valueOf(httpPort).getBytes(StandardCharsets.UTF_8);
			exchange.getResponseHeaders().set("Content-Type", "text/plain");
			exchange.sendResponseHeaders(200, body.length);
			try (var out = exchange.getResponseBody()) {
				out.write(body);
			}
		});
		httpUpstream.start();
		tcpUpstream = new ServerSocket();
		tcpUpstream.setReuseAddress(true);
		tcpUpstream.bind(new InetSocketAddress("127.0.0.1", 0), 128);
		int tcpPort = tcpUpstream.getLocalPort();
		UPSTREAM_EXECUTOR.execute(() -> acceptLoop(tcpUpstream, tcpPort));
	}

	private static void acceptLoop(@Nullable ServerSocket server, int port) {
		while (server != null && !server.isClosed()) {
			try {
				Socket socket = server.accept();
				UPSTREAM_EXECUTOR.execute(() -> echoPort(socket, port));
			}
			catch (Exception e) {
				return;
			}
		}
	}

	/** Reads until the peer half-closes, then answers with this upstream's port. */
	private static void echoPort(Socket socket, int port) {
		try (socket; InputStream in = socket.getInputStream()) {
			byte[] buffer = new byte[1024];
			while (in.read(buffer) > 0) {
				// drain; the response follows the half-close
			}
			socket.getOutputStream().write(String.valueOf(port).getBytes(StandardCharsets.UTF_8));
			socket.getOutputStream().flush();
		}
		catch (Exception e) {
			// connection torn down; nothing to do
		}
	}

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) {
		grpcPort = TestPorts.freePort();
		dataPort = TestPorts.freePort();
		tcpRoutePort = TestPorts.freePort();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(dataPort));
		registry.add("sluice.token", () -> "it-token");
		// only the tcp route port below is claimable
		registry.add("sluice.tcp-port-range", () -> String.valueOf(tcpRoutePort));
		registry.add("server.port", () -> 0);
	}

	private void startClient() {
		java.util.Objects.requireNonNull(httpUpstream, "http upstream not started");
		java.util.Objects.requireNonNull(tcpUpstream, "tcp upstream not started");
		SluiceClientProperties properties = SluiceClientProperties.builder()
			.serverUrl("grpc://127.0.0.1:" + grpcPort)
			.clientId("dual-client")
			// both upstreams share the host on purpose
			.upstream(Upstream.builder()
				.host(HOST)
				.target("http://127.0.0.1:" + httpUpstream.getAddress().getPort())
				.build())
			.upstream(Upstream.builder()
				.host(HOST)
				.target("tcp://127.0.0.1:" + tcpUpstream.getLocalPort())
				.listenPort(tcpRoutePort)
				.build())
			.token("it-token")
			.build();
		TunnelClient started = TunnelClient.builder()
			.properties(properties)
			.taskExecutor(TASK_EXECUTOR)
			.meterRegistry(new SimpleMeterRegistry())
			.build();
		started.start();
		this.clients.add(started);
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(this.sessions::count, count -> count > 0);
	}

	@AfterAll
	void tearDown() {
		for (TunnelClient stopped : this.clients) {
			stopped.stop();
		}
		this.clients.clear();
		if (httpUpstream != null) {
			httpUpstream.stop(0);
		}
		try {
			if (tcpUpstream != null) {
				tcpUpstream.close();
			}
		}
		catch (Exception e) {
			// ignore
		}
		UPSTREAM_EXECUTOR.shutdown();
	}

	@Test
	void httpRouteOnTheSharedHostServesTheHttpUpstream() throws Exception {
		this.startClient();
		String httpPort = String.valueOf(java.util.Objects.requireNonNull(httpUpstream).getAddress().getPort());
		Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> httpGet(HOST), httpPort::equals);
	}

	@Test
	void tcpRouteOnTheSharedHostServesTheTcpUpstream() throws Exception {
		this.startClient();
		String tcpPort = String.valueOf(java.util.Objects.requireNonNull(tcpUpstream).getLocalPort());
		Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> tcpRoundTrip(), tcpPort::equals);
	}

	private String httpGet(String host) throws Exception {
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(10_000);
			socket.getOutputStream()
				.write(("GET / HTTP/1.1\r\nHost: " + host + "\r\nConnection: close\r\n\r\n")
					.getBytes(StandardCharsets.US_ASCII));
			socket.getOutputStream().flush();
			byte[] bytes = socket.getInputStream().readAllBytes();
			String response = new String(bytes, StandardCharsets.UTF_8);
			return response.substring(response.indexOf("\r\n\r\n") + 4);
		}
	}

	/** One tcp round trip; the response body is the serving upstream's port. */
	private String tcpRoundTrip() throws Exception {
		try (Socket socket = new Socket("127.0.0.1", tcpRoutePort)) {
			socket.setSoTimeout(10_000);
			socket.getOutputStream().write("ping".getBytes(StandardCharsets.US_ASCII));
			socket.getOutputStream().flush();
			socket.shutdownOutput();
			return new String(socket.getInputStream().readAllBytes(), StandardCharsets.US_ASCII);
		}
	}

}
