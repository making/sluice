package am.ik.sluice.it;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;

import com.sun.net.httpserver.HttpServer;
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
import am.ik.sluice.server.tunnel.SessionRegistry;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.task.TaskExecutor;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End to end: the server-wide deny list is a blacklist applied before any allow
 * evaluation, so a loopback client is denied on the data plane even when the route's
 * {@code allowed-cidrs} would admit it -- while the control plane keeps serving the
 * tunnel session.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class AccessControlDenyE2ETest {

	private static final TaskExecutor TASK_EXECUTOR = task -> Thread.ofVirtual().name("e2e-deny-worker").start(task);

	private static int grpcPort;

	private static int dataPort;

	private static HttpServer httpUpstream;

	private @Nullable TunnelClient client;

	@BeforeAll
	static void startUpstream() throws Exception {
		httpUpstream = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		httpUpstream.createContext("/", exchange -> {
			byte[] body = "hello-from-upstream".getBytes(StandardCharsets.UTF_8);
			exchange.getResponseHeaders().set("Content-Type", "text/plain");
			exchange.sendResponseHeaders(200, body.length);
			try (var out = exchange.getResponseBody()) {
				out.write(body);
			}
		});
		httpUpstream.start();
	}

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) {
		grpcPort = TestPorts.freePort();
		dataPort = TestPorts.freePort();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(dataPort));
		// the loopback is denied although the route's allow list admits it: deny wins
		registry.add("sluice.access-control.deny-cidrs", () -> "127.0.0.0/8");
		registry.add("sluice.token", () -> "it-token");
		registry.add("server.port", () -> 0);
	}

	@AfterAll
	static void tearDown() {
		httpUpstream.stop(0);
	}

	@Autowired
	SessionRegistry sessions;

	@AfterAll
	void stopClient() {
		if (this.client != null) {
			this.client.stop();
		}
	}

	private void startClient() {
		if (this.client != null) {
			return;
		}
		SluiceClientProperties properties = SluiceClientProperties.builder()
			.serverUrl("grpc://127.0.0.1:" + grpcPort)
			.upstream(Upstream.builder()
				.host("denied.local")
				.target("http://127.0.0.1:" + httpUpstream.getAddress().getPort())
				.allowedCidrs(List.of("127.0.0.0/8"))
				.build())
			.token("it-token")
			.build();
		TunnelClient started = TunnelClient.builder()
			.properties(properties)
			.taskExecutor(TASK_EXECUTOR)
			.meterRegistry(new SimpleMeterRegistry())
			.build();
		started.start();
		this.client = started;
		// the control plane is untouched by the data plane deny list
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> this.sessions.count() > 0);
	}

	@Test
	void denyListWinsOverTheRouteAllowedCidrs() throws Exception {
		this.startClient();
		String response = raw("GET / HTTP/1.1\r\nHost: denied.local\r\nConnection: close\r\n\r\n");
		assertThat(response.lines().findFirst()).hasValue("HTTP/1.1 403 Forbidden");
		assertThat(response).endsWith("</html>\n");
	}

	@Test
	void theTunnelSessionSurvivesTheDeniedDataPlane() {
		this.startClient();
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> this.sessions.count() == 1);
		assertThat(Objects.requireNonNull(this.client).isConnected()).isTrue();
	}

	private static String raw(String request) throws Exception {
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(10_000);
			OutputStream out = socket.getOutputStream();
			out.write(request.getBytes(StandardCharsets.US_ASCII));
			out.flush();
			ByteArrayOutputStream bytes = new ByteArrayOutputStream();
			InputStream in = socket.getInputStream();
			byte[] buffer = new byte[8192];
			int n;
			while ((n = in.read(buffer)) > 0) {
				bytes.write(buffer, 0, n);
			}
			return bytes.toString(StandardCharsets.UTF_8);
		}
	}

}
