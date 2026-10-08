package am.ik.sluice.it;

import java.io.OutputStream;
import java.net.InetSocketAddress;
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
 * End to end verification that an upstream declared with a host pattern instead of a
 * literal host serves every host the regular expression matches, whole match and without
 * the port, while hosts outside it are answered with the no-route page.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class HostPatternE2ETest {

	private static final String HOST_PATTERN = "svc-.*\\.local";

	private static final TaskExecutor TASK_EXECUTOR = task -> Thread.ofVirtual().name("e2e-host-pattern").start(task);

	private static final ExecutorService UPSTREAM_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

	private static int grpcPort;

	private static int dataPort;

	private static @Nullable HttpServer httpUpstream;

	private final List<TunnelClient> clients = new ArrayList<>();

	@Autowired
	SessionRegistry sessions;

	@BeforeAll
	static void startUpstream() throws Exception {
		HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		int port = server.getAddress().getPort();
		server.createContext("/", exchange -> {
			byte[] body = String.valueOf(port).getBytes(StandardCharsets.UTF_8);
			exchange.getResponseHeaders().set("Content-Type", "text/plain");
			exchange.sendResponseHeaders(200, body.length);
			try (OutputStream out = exchange.getResponseBody()) {
				out.write(body);
			}
		});
		server.start();
		httpUpstream = server;
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
	void tearDown() {
		for (TunnelClient stopped : this.clients) {
			stopped.stop();
		}
		this.clients.clear();
		if (httpUpstream != null) {
			httpUpstream.stop(0);
		}
		UPSTREAM_EXECUTOR.shutdown();
	}

	private void startClient() {
		java.util.Objects.requireNonNull(httpUpstream, "http upstream not started");
		SluiceClientProperties properties = SluiceClientProperties.builder()
			.serverUrl("grpc://127.0.0.1:" + grpcPort)
			.clientId("pattern-client")
			.upstream(Upstream.builder()
				.hostPattern(HOST_PATTERN)
				.target("http://127.0.0.1:" + httpUpstream.getAddress().getPort())
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

	@Test
	void everyMatchedHostIsServed() throws Exception {
		this.startClient();
		String httpPort = String.valueOf(java.util.Objects.requireNonNull(httpUpstream).getAddress().getPort());
		Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> httpBody("svc-a.local"), httpPort::equals);
		assertThat(httpBody("svc-b.local")).isEqualTo(httpPort);
	}

	@Test
	void unmatchedHostsGetTheNoRoutePage() throws Exception {
		this.startClient();
		String httpPort = String.valueOf(java.util.Objects.requireNonNull(httpUpstream).getAddress().getPort());
		Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> httpBody("svc-a.local"), httpPort::equals);
		String unmatched = httpExchange("other.local");
		assertThat(unmatched.substring(0, unmatched.indexOf("\r\n"))).isEqualTo("HTTP/1.1 503 Service Unavailable");
	}

	private String httpExchange(String host) throws Exception {
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(10_000);
			socket.getOutputStream()
				.write(("GET / HTTP/1.1\r\nHost: " + host + "\r\nConnection: close\r\n\r\n")
					.getBytes(StandardCharsets.US_ASCII));
			socket.getOutputStream().flush();
			byte[] bytes = socket.getInputStream().readAllBytes();
			return new String(bytes, StandardCharsets.UTF_8);
		}
	}

	private String httpBody(String host) throws Exception {
		String response = httpExchange(host);
		return response.substring(response.indexOf("\r\n\r\n") + 4);
	}

}
