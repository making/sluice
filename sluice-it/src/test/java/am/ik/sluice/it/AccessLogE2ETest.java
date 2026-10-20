package am.ik.sluice.it;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

import com.sun.net.httpserver.HttpServer;
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
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.task.TaskExecutor;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End to end for the access log: an HTTP request through the data plane must emit one
 * {@code request} line per request head (keep-alive successors included, each with its
 * own route and user agent) and one {@code conn ... event=close} line with the final byte
 * counts, in logfmt.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class AccessLogE2ETest {

	private static final TaskExecutor TASK_EXECUTOR = task -> Thread.ofVirtual().name("e2e-access-worker").start(task);

	private static final ListAppender<ILoggingEvent> ACCESS_EVENTS = new ListAppender<>();

	private static int grpcPort;

	private static int dataPort;

	private static HttpServer upstream;

	private @Nullable TunnelClient client;

	@BeforeAll
	static void startUpstream() throws Exception {
		ACCESS_EVENTS.start();
		((Logger) LoggerFactory.getLogger("sluice.access")).addAppender(ACCESS_EVENTS);
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
		grpcPort = TestPorts.freePort();
		dataPort = TestPorts.freePort();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(dataPort));
		registry.add("sluice.token", () -> "it-token");
		registry.add("server.port", () -> String.valueOf(TestPorts.freePort()));
	}

	private void startClient() {
		if (this.client != null) {
			return;
		}
		SluiceClientProperties properties = SluiceClientProperties.builder()
			.serverUrl("grpc://127.0.0.1:" + grpcPort)
			.upstream(Upstream.builder()
				.host("log.local")
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
		// the advertise acknowledgement turns isConnected() true, and the server sends it
		// once the routes are registered; the session is registered before that
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(started::isConnected);
	}

	@AfterAll
	void tearDown() {
		if (this.client != null) {
			this.client.stop();
		}
		if (upstream != null) {
			upstream.stop(0);
		}
		((Logger) LoggerFactory.getLogger("sluice.access")).detachAppender(ACCESS_EVENTS);
	}

	@Autowired
	SessionRegistry sessions;

	private List<String> events() {
		return ACCESS_EVENTS.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
	}

	private void clearEvents() {
		ACCESS_EVENTS.list.clear();
	}

	private List<String> awaitCloseEvent(String route) {
		Awaitility.await()
			.atMost(Duration.ofSeconds(5))
			.until(() -> events().stream().anyMatch(e -> e.contains("event=close") && e.contains("route=" + route)));
		return events();
	}

	@Test
	void requestAndConnectionCloseAreLogged() throws Exception {
		this.startClient();
		this.clearEvents();
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(10_000);
			socket.getOutputStream()
				.write("GET /index.html HTTP/1.1\r\nHost: log.local\r\nUser-Agent: curl/8.4.0 (e2e)\r\nConnection: close\r\n\r\n"
					.getBytes(StandardCharsets.US_ASCII));
			socket.getInputStream().readAllBytes();
		}
		List<String> events = this.awaitCloseEvent("log.local");
		assertThat(events).anySatisfy(e -> {
			assertThat(e).startsWith("type=conn id=")
				.contains("event=accept")
				.contains("listener=data")
				.contains("remote=127.0.0.1:");
		});
		assertThat(events).anySatisfy(e -> {
			assertThat(e).startsWith("type=req id=")
				.contains("route=log.local")
				.contains("method=GET")
				.contains("path=/index.html")
				.contains("http=1.1")
				.contains("remote=127.0.0.1:")
				.endsWith(" userAgent=\"curl/8.4.0 (e2e)\"");
		});
		assertThat(events).anySatisfy(e -> {
			assertThat(e).startsWith("type=conn id=")
				.contains("event=close")
				.contains("listener=data")
				.contains("route=log.local")
				.contains("transport=h1")
				.contains("connId=")
				.contains("bytesIn=")
				.contains("bytesOut=")
				.contains("durationMs=");
		});
		String close = events.stream().filter(e -> e.contains("event=close")).findFirst().orElseThrow();
		long bytesIn = Long.parseLong(field(close, "bytesIn"));
		long bytesOut = Long.parseLong(field(close, "bytesOut"));
		assertThat(bytesIn).isPositive();
		assertThat(bytesOut).isPositive();
	}

	@Test
	void keepAliveRequestsAreLoggedPerRequest() throws Exception {
		this.startClient();
		this.clearEvents();
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(10_000);
			var out = socket.getOutputStream();
			for (String userAgent : List.of("User-Agent: first/1\r\n", "")) {
				out.write(("GET / HTTP/1.1\r\nHost: log.local\r\n" + userAgent + "Connection: keep-alive\r\n\r\n")
					.getBytes(StandardCharsets.US_ASCII));
				out.flush();
				socket.getInputStream().readNBytes(64);
			}
			socket.shutdownOutput();
			socket.getInputStream().readAllBytes();
		}
		List<String> events = this.awaitCloseEvent("log.local");
		assertThat(events).filteredOn(e -> e.startsWith("type=req id="))
			.as("each request line with its own user agent")
			.map(e -> e.substring(e.indexOf(" userAgent=") + 1))
			.containsExactly("userAgent=\"first/1\"", "userAgent=-");
	}

	private static String field(String line, String key) {
		for (String token : line.split(" ")) {
			if (token.startsWith(key + "=")) {
				return token.substring(key.length() + 1);
			}
		}
		throw new AssertionError("missing " + key + " in: " + line);
	}

}
