package am.ik.sluice.it;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.CookieManager;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;
import java.util.regex.Pattern;

import org.awaitility.Awaitility;

import am.ik.sluice.client.SluiceClientApplication;
import am.ik.sluice.server.SluiceServerApplication;
import am.ik.sluice.server.tunnel.SessionRegistry;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Full stack route gate test: the console shuts a route on one node, the toggle reaches
 * the peer node, both data planes stop serving it (503, closed tcp connection) while the
 * other routes keep serving, and reopening restores them.
 */
@TestInstance(Lifecycle.PER_CLASS)
class RouteGateE2ETest {

	private static final String HOST = "gate.local";

	private static final String CATCH_ALL_HOST = "anything.example";

	private static final String TOKEN = "gate-it-token";

	private static final String USER = "gate-e2e";

	private static final String PASSWORD = "gate-pass";

	private static final String BODY = "gate-ok";

	private static final String CATCH_ALL_BODY = "gate-catch-all";

	private @org.jspecify.annotations.Nullable HttpServer httpUpstream;

	private @org.jspecify.annotations.Nullable HttpServer catchAllUpstream;

	private @org.jspecify.annotations.Nullable ServerSocket echoUpstream;

	private @org.jspecify.annotations.Nullable Thread echoLoop;

	private int httpPort;

	private int catchAllPort;

	private int tcpPort;

	private int echoPort;

	private final int[] grpcPorts = new int[2];

	private final int[] dataPorts = new int[2];

	private final int[] consolePorts = new int[2];

	private final @org.jspecify.annotations.Nullable ConfigurableApplicationContext[] servers = new ConfigurableApplicationContext[2];

	private @org.jspecify.annotations.Nullable ConfigurableApplicationContext clientContext;

	private @org.jspecify.annotations.Nullable HttpClient console;

	@AfterAll
	void stopAll() {
		closeContext(this.clientContext);
		for (ConfigurableApplicationContext server : this.servers) {
			closeContext(server);
		}
		Objects.requireNonNull(this.httpUpstream).stop(0);
		Objects.requireNonNull(this.catchAllUpstream).stop(0);
		if (this.echoLoop != null) {
			this.echoLoop.interrupt();
		}
		closeSocket(this.echoUpstream);
	}

	private void startUpstreams() throws Exception {
		this.httpUpstream = httpServer(BODY);
		this.httpPort = Objects.requireNonNull(this.httpUpstream).getAddress().getPort();
		this.catchAllUpstream = httpServer(CATCH_ALL_BODY);
		this.catchAllPort = Objects.requireNonNull(this.catchAllUpstream).getAddress().getPort();
		this.echoUpstream = new ServerSocket(0, 16, java.net.InetAddress.getByName("127.0.0.1"));
		this.echoLoop = Thread.ofVirtual().name("it-gate-echo").start(this::acceptEchoes);
		this.echoPort = this.echoUpstream.getLocalPort();
		this.tcpPort = TestPorts.freePort();
	}

	private static HttpServer httpServer(String body) throws Exception {
		HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		server.createContext("/", exchange -> {
			byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
			exchange.sendResponseHeaders(200, bytes.length);
			try (OutputStream out = exchange.getResponseBody()) {
				out.write(bytes);
			}
		});
		server.start();
		return server;
	}

	private void acceptEchoes() {
		while (this.echoUpstream != null && !this.echoUpstream.isClosed()) {
			try {
				Socket socket = Objects.requireNonNull(this.echoUpstream).accept();
				Thread.ofVirtual().start(() -> {
					try (socket) {
						InputStream in = socket.getInputStream();
						OutputStream out = socket.getOutputStream();
						out.write(in.readAllBytes());
						out.flush();
					}
					catch (Exception ignored) {
					}
				});
			}
			catch (Exception e) {
				return;
			}
		}
	}

	private void startServers() {
		for (int i = 0; i < 2; i++) {
			this.grpcPorts[i] = TestPorts.freePort();
			this.dataPorts[i] = TestPorts.freePort();
			this.consolePorts[i] = TestPorts.freePort();
		}
		String membership = "node-1=grpc://127.0.0.1:" + this.grpcPorts[0] + ",node-2=grpc://127.0.0.1:"
				+ this.grpcPorts[1];
		for (int i = 0; i < 2; i++) {
			this.servers[i] = new SpringApplicationBuilder(SluiceServerApplication.class).run(
					"--server.port=" + this.consolePorts[i], "--spring.grpc.server.port=" + this.grpcPorts[i],
					"--sluice.data-port=" + this.dataPorts[i], "--sluice.token=" + TOKEN,
					"--sluice.node.id=node-" + (i + 1), "--sluice.cluster.nodes=" + membership,
					"--sluice.cluster.warmup=1s", "--sluice.tcp-port-range=" + this.tcpPort,
					"--spring.grpc.server.shutdown.grace-period=2s", "--spring.security.user.name=" + USER,
					"--spring.security.user.password={noop}" + PASSWORD);
		}
	}

	private void startClient() {
		this.clientContext = new SpringApplicationBuilder(SluiceClientApplication.class).run(
				"--sluice.server-url=grpc://127.0.0.1:" + this.grpcPorts[0], "--sluice.client.id=gate-client",
				"--sluice.client.upstream[0].host=" + HOST,
				"--sluice.client.upstream[0].target=http://127.0.0.1:" + this.httpPort,
				"--sluice.client.upstream[1].host=",
				"--sluice.client.upstream[1].target=http://127.0.0.1:" + this.catchAllPort,
				"--sluice.client.upstream[2].host=",
				"--sluice.client.upstream[2].target=tcp://127.0.0.1:" + this.echoPort,
				"--sluice.client.upstream[2].listen-port=" + this.tcpPort, "--sluice.token=" + TOKEN,
				"--server.port=0");
	}

	private static void closeContext(@org.jspecify.annotations.Nullable ConfigurableApplicationContext context) {
		if (context != null) {
			context.close();
		}
	}

	private static void closeSocket(@org.jspecify.annotations.Nullable ServerSocket socket) {
		if (socket != null && !socket.isClosed()) {
			try {
				socket.close();
			}
			catch (Exception ignored) {
			}
		}
	}

	private long routedSessions(int index) {
		ConfigurableApplicationContext server = this.servers[index];
		return server == null ? -1
				: server.getBean(SessionRegistry.class)
					.all()
					.stream()
					.filter(session -> !session.upstreams().isEmpty())
					.count();
	}

	private String httpRoundTrip(int index, String host) throws Exception {
		try (Socket socket = new Socket("127.0.0.1", this.dataPorts[index])) {
			socket.setSoTimeout(10_000);
			OutputStream out = socket.getOutputStream();
			out.write(("GET / HTTP/1.1\r\nHost: " + host + "\r\nConnection: close\r\n\r\n")
				.getBytes(StandardCharsets.US_ASCII));
			out.flush();
			return new String(socket.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
		}
	}

	private String echoRoundTrip(String payload) throws Exception {
		try (Socket socket = new Socket("127.0.0.1", this.tcpPort)) {
			socket.setSoTimeout(10_000);
			socket.getOutputStream().write(payload.getBytes(StandardCharsets.UTF_8));
			socket.getOutputStream().flush();
			socket.shutdownOutput();
			return new String(socket.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
		}
	}

	private void awaitEcho(String expected) {
		Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> {
			try {
				return echoRoundTrip("ping").equals(expected);
			}
			catch (Exception e) {
				return false;
			}
		});
	}

	private void awaitRoutedSession() {
		Awaitility.await().atMost(Duration.ofSeconds(20)).until(() -> routedSessions(0) == 1 && routedSessions(1) == 1);
	}

	private String consoleBase() {
		return "http://127.0.0.1:" + this.consolePorts[0];
	}

	private HttpClient console() {
		return Objects.requireNonNull(this.console);
	}

	private HttpResponse<String> consoleGet(String path) {
		try {
			return this.console()
				.send(HttpRequest.newBuilder(URI.create(consoleBase() + path)).timeout(Duration.ofSeconds(10)).build(),
						HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
		}
		catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	private HttpResponse<String> postForm(String path, String body) {
		try {
			return this.console()
				.send(HttpRequest.newBuilder(URI.create(consoleBase() + path))
					.header("Content-Type", "application/x-www-form-urlencoded")
					.timeout(Duration.ofSeconds(10))
					.POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
					.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
		}
		catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	private static String csrfOf(String page) {
		return Pattern.compile("name=\"_csrf\" value=\"([^\"]+)\"")
			.matcher(page)
			.results()
			.findFirst()
			.orElseThrow(() -> new AssertionError("no csrf parameter in the page"))
			.group(1);
	}

	private void login() {
		this.console = HttpClient.newBuilder()
			.version(HttpClient.Version.HTTP_1_1)
			.cookieHandler(new CookieManager())
			.followRedirects(HttpClient.Redirect.NORMAL)
			.build();
		String csrf = csrfOf(consoleGet("/login").body());
		postForm("/login", "username=" + USER + "&password=" + URLEncoder.encode(PASSWORD, StandardCharsets.UTF_8)
				+ "&_csrf=" + URLEncoder.encode(csrf, StandardCharsets.UTF_8));
	}

	private void setGate(String key, boolean open, String csrf) {
		HttpResponse<String> response = postForm("/console/routes/gate",
				"key=" + URLEncoder.encode(key, StandardCharsets.UTF_8) + "&open=" + open + "&_csrf="
						+ URLEncoder.encode(csrf, StandardCharsets.UTF_8));
		assertThat(response.statusCode()).isEqualTo(200);
	}

	@Test
	void consoleShutsAndReopensARouteOnEveryNode() throws Exception {
		startUpstreams();
		startServers();
		login();
		startClient();
		awaitRoutedSession();

		// the routes table shows the gates, open by default
		String page = consoleGet("/console").body();
		assertThat(page).contains("route-gate").contains("data-gate=\"Open\"");

		String csrf = csrfOf(page);

		// both nodes serve the host route, the catch-all and the tcp route
		assertThat(httpRoundTrip(0, HOST)).contains(BODY);
		assertThat(httpRoundTrip(1, HOST)).contains(BODY);
		assertThat(httpRoundTrip(0, CATCH_ALL_HOST)).contains(CATCH_ALL_BODY);
		awaitEcho("ping");

		// shut the host route from node 1's console: node 2 stops serving it too, the
		// request falls through to the catch-all
		setGate(HOST, false, csrf);
		assertThat(httpRoundTrip(0, HOST)).contains(CATCH_ALL_BODY);
		assertThat(httpRoundTrip(1, HOST)).contains(CATCH_ALL_BODY);
		// the catch-all and the tcp route of the same client keep serving
		assertThat(httpRoundTrip(0, CATCH_ALL_HOST)).contains(CATCH_ALL_BODY);
		assertThat(httpRoundTrip(1, CATCH_ALL_HOST)).contains(CATCH_ALL_BODY);
		awaitEcho("ping");

		// shut the catch-all as well: now the host has no route left, 503 on both nodes
		setGate("*", false, csrf);
		assertThat(httpRoundTrip(0, HOST)).contains("503");
		assertThat(httpRoundTrip(1, HOST)).contains("503");
		awaitEcho("ping");

		// reopen the host route: it serves again though the catch-all stays shut
		setGate(HOST, true, csrf);
		assertThat(httpRoundTrip(0, HOST)).contains(BODY);
		assertThat(httpRoundTrip(1, HOST)).contains(BODY);
		page = consoleGet("/console").body();
		assertThat(page).contains("data-gate=\"Shut\"");

		// shut the tcp route as well: the echo connects and is closed with no answer, the
		// catch-all stays shut so the http route answers 503 still
		setGate("tcp:" + this.tcpPort, false, csrf);
		awaitEcho("");
		assertThat(httpRoundTrip(0, HOST)).contains(BODY);

		// reopen the rest
		setGate("*", true, csrf);
		setGate("tcp:" + this.tcpPort, true, csrf);
		assertThat(httpRoundTrip(0, CATCH_ALL_HOST)).contains(CATCH_ALL_BODY);
		assertThat(httpRoundTrip(1, CATCH_ALL_HOST)).contains(CATCH_ALL_BODY);
		awaitEcho("ping");
	}

}
