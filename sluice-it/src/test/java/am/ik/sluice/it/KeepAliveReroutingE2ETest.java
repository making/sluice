package am.ik.sluice.it;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Objects;

import com.sun.net.httpserver.HttpServer;
import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;

import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.client.config.Upstream;
import am.ik.sluice.client.tunnel.TunnelClient;
import am.ik.sluice.server.SluiceServerApplication;
import am.ik.sluice.server.proxy.DataProxyServer;
import am.ik.sluice.server.route.Router;
import am.ik.sluice.server.tunnel.SessionRegistry;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.task.TaskExecutor;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End to end for per-request re-routing on keep-alive connections: the second request of
 * an HTTP/1.1 keep-alive connection whose Host resolves to another client's upstream must
 * be answered by that upstream (and, with {@code rewrite-host}, by the host of its own
 * route), with a {@code type=req} access log line per request head.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class KeepAliveReroutingE2ETest {

	private static final TaskExecutor TASK_EXECUTOR = task -> Thread.ofVirtual().name("e2e-rerouting").start(task);

	private static final ListAppender<ILoggingEvent> ACCESS_EVENTS = new ListAppender<>();

	private static int grpcPort;

	private static int dataPort;

	private static HttpServer alphaUpstream;

	private static HttpServer betaUpstream;

	private final java.util.Map<String, TunnelClient> clients = new java.util.concurrent.ConcurrentHashMap<>();

	@Nullable
	@Autowired
	private Router router;

	@Autowired
	private SessionRegistry sessions;

	@Nullable
	@Autowired
	private DataProxyServer dataProxyServer;

	@BeforeAll
	static void startUpstreams() {
		ACCESS_EVENTS.start();
		((Logger) LoggerFactory.getLogger("sluice.access")).addAppender(ACCESS_EVENTS);
		// each upstream answers with its own name and the Host header it saw
		alphaUpstream = echo("alpha");
		betaUpstream = echo("beta");
	}

	private static HttpServer echo(String name) {
		try {
			HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
			server.createContext("/", exchange -> {
				String host = exchange.getRequestHeaders().getFirst("Host");
				byte[] body = (name + ":" + host).getBytes(StandardCharsets.UTF_8);
				exchange.getResponseHeaders().set("Content-Type", "text/plain");
				exchange.sendResponseHeaders(200, body.length);
				try (var out = exchange.getResponseBody()) {
					out.write(body);
				}
			});
			server.start();
			return server;
		}
		catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) {
		grpcPort = TestPorts.freePort();
		dataPort = TestPorts.freePort();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(dataPort));
		registry.add("sluice.token", () -> "it-token");
		registry.add("sluice.access-log.rate-limit.enabled", () -> "false");
		registry.add("server.port", () -> String.valueOf(TestPorts.freePort()));
	}

	private void startClient(String host, HttpServer upstream, boolean rewriteHost) {
		clients.computeIfAbsent(host, h -> {
			SluiceClientProperties properties = SluiceClientProperties.builder()
				.serverUrl("grpc://127.0.0.1:" + grpcPort)
				.upstream(Upstream.builder()
					.host(h)
					.target("http://127.0.0.1:" + upstream.getAddress().getPort())
					.rewriteHost(rewriteHost)
					.build())
				.token("it-token")
				.build();
			TunnelClient started = TunnelClient.builder()
				.properties(properties)
				.taskExecutor(TASK_EXECUTOR)
				.meterRegistry(new SimpleMeterRegistry())
				.build();
			started.start();
			Router routes = Objects.requireNonNull(router);
			Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> routes.lookup(h).isPresent());
			return started;
		});
	}

	private String exchange(Socket socket, String host, String path) throws Exception {
		socket.getOutputStream()
			.write(("GET %s HTTP/1.1\r\nHost: %s\r\n\r\n".formatted(path, host)).getBytes(StandardCharsets.US_ASCII));
		socket.getOutputStream().flush();
		BufferedReader reader = new BufferedReader(
				new InputStreamReader(socket.getInputStream(), StandardCharsets.US_ASCII));
		String status = reader.readLine();
		int contentLength = 0;
		String line;
		while ((line = reader.readLine()) != null && !line.isEmpty()) {
			if (line.toLowerCase(java.util.Locale.ROOT).startsWith("content-length:")) {
				contentLength = Integer.parseInt(line.substring(line.indexOf(':') + 1).trim());
			}
		}
		ByteArrayOutputStream body = new ByteArrayOutputStream();
		for (int i = 0; i < contentLength; i++) {
			body.write(reader.read());
		}
		return status + "|" + body.toString(StandardCharsets.UTF_8);
	}

	private List<String> requestLines(String route) {
		return ACCESS_EVENTS.list.stream()
			.map(e -> e.getFormattedMessage())
			.filter(m -> m.startsWith("type=req") && m.contains("route=" + route + " "))
			.toList();
	}

	@AfterAll
	void tearDown() {
		clients.values().forEach(TunnelClient::stop);
		alphaUpstream.stop(0);
		betaUpstream.stop(0);
		((Logger) LoggerFactory.getLogger("sluice.access")).detachAppender(ACCESS_EVENTS);
	}

	@Test
	void reroutesTheSecondKeepAliveRequestToTheOtherUpstream() throws Exception {
		startClient("reroute.a.test", alphaUpstream, false);
		startClient("reroute.b.test", betaUpstream, false);
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(10_000);
			assertThat(exchange(socket, "reroute.a.test", "/one")).startsWith("HTTP/1.1 200 OK|alpha:reroute.a.test");
			assertThat(exchange(socket, "reroute.b.test", "/one")).startsWith("HTTP/1.1 200 OK|beta:reroute.b.test");
		}
	}

	@Test
	void reroutesBackToTheFirstUpstreamOnTheThirdRequest() throws Exception {
		startClient("thrice.a.test", alphaUpstream, false);
		startClient("thrice.b.test", betaUpstream, false);
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(10_000);
			assertThat(exchange(socket, "thrice.a.test", "/x")).startsWith("HTTP/1.1 200 OK|alpha:");
			assertThat(exchange(socket, "thrice.b.test", "/x")).startsWith("HTTP/1.1 200 OK|beta:");
			assertThat(exchange(socket, "thrice.a.test", "/x")).startsWith("HTTP/1.1 200 OK|alpha:");
		}
	}

	@Test
	void keepsTheSameUpstreamWithoutHostChanges() throws Exception {
		startClient("same.a.test", alphaUpstream, false);
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(10_000);
			assertThat(exchange(socket, "same.a.test", "/x")).startsWith("HTTP/1.1 200 OK|alpha:");
			assertThat(exchange(socket, "same.a.test", "/x")).startsWith("HTTP/1.1 200 OK|alpha:");
		}
	}

	@Test
	void rewritesTheHostOfEveryReroutedRequest() throws Exception {
		int alphaPort = alphaUpstream.getAddress().getPort();
		int betaPort = betaUpstream.getAddress().getPort();
		startClient("rw.a.test", alphaUpstream, true);
		startClient("rw.b.test", betaUpstream, true);
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(10_000);
			assertThat(exchange(socket, "rw.a.test", "/x")).startsWith("HTTP/1.1 200 OK|alpha:127.0.0.1:" + alphaPort);
			assertThat(exchange(socket, "rw.b.test", "/x")).startsWith("HTTP/1.1 200 OK|beta:127.0.0.1:" + betaPort);
		}
	}

	@Test
	void answersNoRouteForAnUnroutableLaterRequest() throws Exception {
		startClient("gone.a.test", alphaUpstream, false);
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(10_000);
			assertThat(exchange(socket, "gone.a.test", "/x")).startsWith("HTTP/1.1 200 OK|alpha:");
			assertThat(exchange(socket, "nowhere.test", "/x")).startsWith("HTTP/1.1 503 Service Unavailable|");
		}
	}

	@Test
	void answersNoRouteWhenTheReroutedRouteVanishes() throws Exception {
		startClient("vanish.a.test", alphaUpstream, false);
		startClient("vanish.b.test", betaUpstream, false);
		TunnelClient beta = Objects.requireNonNull(clients.get("vanish.b.test"));
		Router routes = Objects.requireNonNull(router);
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(10_000);
			assertThat(exchange(socket, "vanish.a.test", "/x")).startsWith("HTTP/1.1 200 OK|alpha:");
			beta.stop();
			Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> routes.lookup("vanish.b.test").isEmpty());
			assertThat(exchange(socket, "vanish.b.test", "/x")).startsWith("HTTP/1.1 503 Service Unavailable|");
		}
		finally {
			clients.remove("vanish.b.test");
		}
	}

	@Test
	void midHeadDisconnectLeavesNoConnectionBehind() throws Exception {
		startClient("midhead.a.test", alphaUpstream, false);
		DataProxyServer proxy = Objects.requireNonNull(dataProxyServer);
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(10_000);
			socket.getOutputStream()
				.write("GET /x HTTP/1.1\r\nHost: midhead.a.test\r\nX-Trunca".getBytes(StandardCharsets.US_ASCII));
			socket.getOutputStream().flush();
			socket.shutdownOutput();
			socket.getInputStream().readAllBytes();
		}
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> proxy.activeConnections() == 0);
	}

	@Test
	void closingTheConnectionReleasesEveryLeg() throws Exception {
		startClient("release.a.test", alphaUpstream, false);
		startClient("release.b.test", betaUpstream, false);
		DataProxyServer proxy = Objects.requireNonNull(dataProxyServer);
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(10_000);
			exchange(socket, "release.a.test", "/x");
			exchange(socket, "release.b.test", "/x");
		}
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> proxy.activeConnections() == 0);
	}

	@Test
	void logsOneRequestLinePerRequestWithItsOwnRoute() throws Exception {
		startClient("logged.a.test", alphaUpstream, false);
		startClient("logged.b.test", betaUpstream, false);
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(10_000);
			exchange(socket, "logged.a.test", "/one");
			exchange(socket, "logged.b.test", "/one");
		}
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> requestLines("logged.b.test").size() >= 1);
		assertThat(requestLines("logged.a.test")).anyMatch(l -> l.contains("path=/one"));
		assertThat(requestLines("logged.b.test")).anyMatch(l -> l.contains("path=/one"));
	}

}
