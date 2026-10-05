package am.ik.sluice.it;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

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
import am.ik.sluice.server.proxy.DataProxyServer;
import am.ik.sluice.server.tunnel.SessionRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.task.TaskExecutor;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End to end: the real {@link TunnelClient} connects to the running server, advertises an
 * upstream backed by a local HttpServer, and HTTP requests through the data plane must
 * round trip. The second request on the same connection verifies keep-alive reuse.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class TunnelEndToEndTest {

	private static final TaskExecutor TASK_EXECUTOR = task -> Thread.ofVirtual().name("e2e-client-worker").start(task);

	private static int grpcPort;

	private static int dataPort;

	private static HttpServer upstream;

	private @Nullable TunnelClient client;

	@BeforeAll
	static void startUpstream() throws Exception {
		upstream = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		upstream.createContext("/", exchange -> {
			byte[] body = "hello-from-upstream".getBytes(StandardCharsets.UTF_8);
			exchange.getResponseHeaders().set("Content-Type", "text/plain");
			exchange.sendResponseHeaders(200, body.length);
			try (var out = exchange.getResponseBody()) {
				out.write(body);
			}
		});
		upstream.start();
	}

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) {
		grpcPort = freePort();
		dataPort = freePort();
		int webPort = freePort();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(dataPort));
		registry.add("sluice.token", () -> "it-token");
		registry.add("server.port", () -> String.valueOf(webPort));
	}

	private static int freePort() {
		try (ServerSocket socket = new ServerSocket(0)) {
			return socket.getLocalPort();
		}
		catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	/** Starts the real client against the running server and waits for registration. */
	private void startClient() {
		if (this.client != null) {
			return;
		}
		SluiceClientProperties properties = SluiceClientProperties.builder()
			.serverUrl("grpc://127.0.0.1:" + grpcPort)
			.upstream(Upstream.builder()
				.host("demo.local")
				.target("http://127.0.0.1:" + upstream.getAddress().getPort())
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
		// the ADVERTISE frame is applied asynchronously on the server
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> this.sessions.count() > 0);
	}

	@AfterAll
	void tearDown() {
		if (this.client != null) {
			this.client.stop();
		}
		if (upstream != null) {
			upstream.stop(0);
		}
	}

	@Autowired
	SessionRegistry sessions;

	@Autowired
	DataProxyServer dataProxyServer;

	@Autowired
	MeterRegistry meterRegistry;

	@Test
	void requestRoundTripsThroughTunnel() throws Exception {
		startClient();
		assertThat(this.sessions.count()).isEqualTo(1);
		assertThat(this.dataProxyServer.boundPort()).isEqualTo(dataPort);
		Response response = exchange(false);
		assertThat(response.statusLine).startsWith("HTTP/1.1 200");
		assertThat(response.body).isEqualTo("hello-from-upstream");
	}

	@Test
	void relayedBytesAreCountedPerRoute() throws Exception {
		startClient();
		exchange(false);
		Awaitility.await()
			.atMost(Duration.ofSeconds(5))
			.until(() -> this.meterRegistry.find("sluice.tunnel.bytes")
				.tags("direction", "data", "route", "demo.local")
				.counter() != null);
		Counter counter = this.meterRegistry.find("sluice.tunnel.bytes")
			.tags("direction", "data", "route", "demo.local")
			.counter();
		assertThat(counter).isNotNull();
		assertThat(counter.count()).isPositive();
	}

	@Test
	void consecutiveRequestsReuseKeepAliveConnection() throws Exception {
		startClient();
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(10_000);
			var out = socket.getOutputStream();
			for (int i = 0; i < 2; i++) {
				out.write("GET / HTTP/1.1\r\nHost: demo.local\r\nConnection: keep-alive\r\n\r\n"
					.getBytes(StandardCharsets.US_ASCII));
				out.flush();
				Response response = readResponse(socket.getInputStream());
				assertThat(response.statusLine).startsWith("HTTP/1.1 200");
				assertThat(response.body).isEqualTo("hello-from-upstream");
			}
		}
	}

	private Response exchange(boolean keepAlive) throws Exception {
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(10_000);
			String connectionHeader = keepAlive ? "keep-alive" : "close";
			socket.getOutputStream()
				.write(("GET / HTTP/1.1\r\nHost: demo.local\r\nConnection: " + connectionHeader + "\r\n\r\n")
					.getBytes(StandardCharsets.US_ASCII));
			socket.getOutputStream().flush();
			return readResponse(socket.getInputStream());
		}
	}

	private record Response(String statusLine, String body) {
	}

	private static Response readResponse(InputStream in) throws Exception {
		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		byte[] buffer = new byte[8192];
		int n;
		while ((n = in.read(buffer)) > 0) {
			bytes.write(buffer, 0, n);
			String text = bytes.toString(StandardCharsets.UTF_8);
			int bodyStart = text.indexOf("\r\n\r\n");
			if (bodyStart > 0) {
				int contentLength = headerValue(text, "content-length");
				if (bytes.size() >= bodyStart + 4 + contentLength) {
					return new Response(text.substring(0, text.indexOf("\r\n")),
							text.substring(bodyStart + 4, bodyStart + 4 + contentLength));
				}
			}
		}
		throw new AssertionError("response incomplete: " + bytes);
	}

	private static int headerValue(String head, String name) {
		return Integer.parseInt(head.lines()
			.filter(line -> line.toLowerCase().startsWith(name + ":"))
			.findFirst()
			.orElse(name + ": 0")
			.substring(name.length() + 1)
			.trim());
	}

}
