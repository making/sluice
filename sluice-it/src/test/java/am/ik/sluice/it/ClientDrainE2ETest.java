package am.ik.sluice.it;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.CookieManager;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.sun.net.httpserver.HttpServer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.slf4j.LoggerFactory;

import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.client.config.Upstream;
import am.ik.sluice.client.tunnel.TunnelClient;
import am.ik.sluice.server.SluiceServerApplication;
import am.ik.sluice.server.proxy.DataProxyServer;
import am.ik.sluice.server.proxy.HeadSeams;
import am.ik.sluice.server.tunnel.SessionRegistry;
import am.ik.sluice.server.tunnel.TunnelSession;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.core.task.TaskExecutor;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End to end: a client stopped by its application context ({@code stop(Runnable)})
 * withdraws its routes and keeps relaying the connections open on it. Two clients serve
 * one host, one h2 host and one listen port; the smaller id (A) serves them. A slow
 * request (closing its connection), an idle keep-alive connection, an idle h2 connection
 * and a tcp connection are open on A when A is stopped: the idle legs on A are closed at
 * once, the slow request completes on A, every request made after the withdrawal -- on
 * new connections, on the keep-alive one and on the h2 one -- and every new tcp
 * connection is served by B, the tcp connection open keeps relaying on A, and A's drain
 * ends once the slow request and the tcp connection ended, well before its grace: the
 * idle connections stay open and hold it no longer. A request head on a keep-alive
 * connection, routed to its leg on A before the withdrawal and held there across it,
 * keeps that leg and is served by A. A pipelined request resolved to A before the
 * withdrawal, but relayed after it, goes to B. The console shows A as draining. A drain
 * whose connections outlast the grace, or that is aborted, closes them.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class ClientDrainE2ETest {

	private static final String HOST = "drain.test";

	private static final String TCP_HOST = "drain-tcp.test";

	private static final String HELD_HOST = "held.test";

	private static final String B_ONLY_HOST = "b-only.test";

	private static final String H2_HOST = "drain-h2.test";

	private static final String CONSOLE_USER = "drain-e2e";

	private static final String CONSOLE_PASSWORD = "drain-pass";

	private static final Duration HANG_GUARD = Duration.ofSeconds(30);

	private static final TaskExecutor TASK_EXECUTOR = task -> Thread.ofVirtual().name("e2e-drain").start(task);

	private static final ExecutorService UPSTREAM_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

	private static final ListAppender<ILoggingEvent> CLIENT_EVENTS = new ConcurrentListAppender<>();

	private static int grpcPort;

	private static int dataPort;

	private static int tcpRoutePort;

	private final SessionRegistry sessions;

	private final DataProxyServer dataProxyServer;

	private final List<AutoCloseable> resources = new CopyOnWriteArrayList<>();

	private final List<TunnelClient> clients = new CopyOnWriteArrayList<>();

	private final int consolePort;

	ClientDrainE2ETest(@Autowired SessionRegistry sessions, @Autowired DataProxyServer dataProxyServer,
			@LocalServerPort int consolePort) {
		this.sessions = sessions;
		this.dataProxyServer = dataProxyServer;
		this.consolePort = consolePort;
	}

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) {
		grpcPort = TestPorts.freePort();
		dataPort = TestPorts.freePort();
		tcpRoutePort = TestPorts.freePort();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(dataPort));
		registry.add("sluice.token", () -> "it-token");
		registry.add("sluice.tcp-port-range", () -> String.valueOf(tcpRoutePort));
		registry.add("server.port", () -> 0);
		registry.add("spring.security.user.name", () -> CONSOLE_USER);
		registry.add("spring.security.user.password", () -> "{noop}" + CONSOLE_PASSWORD);
	}

	@BeforeAll
	static void captureClientLog() {
		CLIENT_EVENTS.start();
		((Logger) LoggerFactory.getLogger(TunnelClient.class)).addAppender(CLIENT_EVENTS);
	}

	@BeforeEach
	void clearClientLog() {
		CLIENT_EVENTS.list.clear();
	}

	@AfterAll
	void tearDown() throws Exception {
		((Logger) LoggerFactory.getLogger(TunnelClient.class)).detachAppender(CLIENT_EVENTS);
		this.clients.forEach(TunnelClient::stop);
		for (AutoCloseable resource : this.resources) {
			resource.close();
		}
		UPSTREAM_EXECUTOR.shutdownNow();
	}

	@Test
	void stoppedClientFinishesItsConnectionsWhileNewOnesGoToTheOtherClient() throws Exception {
		CountDownLatch slowEntered = new CountDownLatch(1);
		CountDownLatch releaseSlow = new CountDownLatch(1);
		AtomicInteger servedByA = new AtomicInteger();
		AtomicInteger servedByB = new AtomicInteger();
		HttpServer upstreamA = httpUpstream("A", servedByA, slowEntered, releaseSlow);
		CountDownLatch slowEnteredB = new CountDownLatch(1);
		CountDownLatch releaseSlowB = new CountDownLatch(1);
		HttpServer upstreamB = httpUpstream("B", servedByB, slowEnteredB, releaseSlowB);
		ServerSocket tcpA = tcpUpstream("A");
		ServerSocket tcpB = tcpUpstream("B");
		ScriptedH2Upstream h2A = h2Upstream("A");
		ScriptedH2Upstream h2B = h2Upstream("B");
		// B binds the listen port first; A, the smaller id, takes it over
		startClient("drain-b", Duration.ofSeconds(60), http(HOST, upstreamB), http(B_ONLY_HOST, upstreamB), tcp(tcpB),
				h2(h2B));
		TunnelClient clientA = startClient("drain-a", Duration.ofSeconds(60), http(HOST, upstreamA), tcp(tcpA),
				h2(h2A));
		Awaitility.await().atMost(HANG_GUARD).until(() -> "A:0".equals(tcpLine("0")));
		// holds the request head relayed while it is armed after the choice of its leg
		AtomicBoolean armed = new AtomicBoolean();
		CountDownLatch headHeld = new CountDownLatch(1);
		CountDownLatch releaseHead = new CountDownLatch(1);
		HeadSeams.set(this.dataProxyServer, () -> {
			if (armed.compareAndSet(true, false)) {
				headHeld.countDown();
				await(releaseHead);
			}
		});

		try (Socket keepAlive = dataPlane();
				Socket racing = dataPlane();
				Socket held = new Socket("127.0.0.1", tcpRoutePort);
				Socket slow = dataPlane();
				Socket pipelined = dataPlane();
				H2TestClient h2 = H2TestClient.connect(dataPort, 1, 3)) {
			assertThat(get(keepAlive, "/", true)).isEqualTo("200 A");
			assertThat(get(racing, "/", true)).isEqualTo("200 A");
			assertThat(h2Get(h2, 1)).isEqualTo("A");
			held.setSoTimeout((int) HANG_GUARD.toMillis());
			BufferedReader heldIn = reader(held);
			PrintWriter heldOut = writer(held);
			heldOut.println("1");
			assertThat(heldIn.readLine()).isEqualTo("A:1");
			sendGet(slow, "/slow", false);
			assertThat(slowEntered.await(HANG_GUARD.toSeconds(), TimeUnit.SECONDS)).isTrue();
			// a request resolved to A waits behind the slow response of B's route, to be
			// relayed once A withdrew
			sendGet(pipelined, "/slow", true, B_ONLY_HOST);
			sendGet(pipelined, "/", true);
			assertThat(slowEnteredB.await(HANG_GUARD.toSeconds(), TimeUnit.SECONDS)).isTrue();
			Awaitility.await()
				.atMost(HANG_GUARD)
				.until(() -> ThreadDumps.anyThreadIn("am.ik.sluice.server.proxy.ReroutingRelay", "awaitDrained"));
			int servedBeforeStop = servedByA.get();
			// a request resolved to A, on A's leg, is held before its head is relayed
			armed.set(true);
			sendGet(racing, "/", true);
			assertThat(headHeld.await(HANG_GUARD.toSeconds(), TimeUnit.SECONDS)).isTrue();

			CountDownLatch stopped = new CountDownLatch(1);
			Thread.ofVirtual().start(() -> clientA.stop(stopped::countDown));
			TunnelSession sessionA = this.sessions.find("drain-a").orElseThrow();
			Awaitility.await().atMost(HANG_GUARD).until(sessionA::withdrawn);
			assertThat(console()).contains("<article class=\"client client--draining\" data-client=\"drain-a\">")
				.contains("<article class=\"client\" data-client=\"drain-b\">");
			// the idle legs on A -- the keep-alive connection's, the h2 connection's --
			// are
			// closed; the held request's, the slow request's and the tcp connection are
			// left
			Awaitility.await().atMost(HANG_GUARD).until(() -> sessionA.connectionCount() == 3);
			releaseHead.countDown();
			assertThat(readOne(racing.getInputStream())).as("the request on the leg kept for it").isEqualTo("200 A");
			releaseSlowB.countDown();
			assertThat(readOne(pipelined.getInputStream())).isEqualTo("200 B-slow");
			assertThat(readOne(pipelined.getInputStream())).as("resolved to A before the withdrawal")
				.isEqualTo("200 B");

			for (int i = 0; i < 3; i++) {
				try (Socket fresh = dataPlane()) {
					assertThat(get(fresh, "/", false)).isEqualTo("200 B");
				}
			}
			// the port is handed over to B; the connection open on A keeps relaying
			assertThat(tcpLine("2")).isEqualTo("B:2");
			heldOut.println("3");
			assertThat(heldIn.readLine()).isEqualTo("A:3");
			assertThat(stopped.getCount()).as("A drains while connections are open").isOne();

			releaseSlow.countDown();
			assertThat(read(slow)).as("the response closing its connection").isEqualTo("200 A-slow");
			held.close();
			// the keep-alive, the h2 and the racing connection stay open, idle
			assertThat(stopped.await(HANG_GUARD.toSeconds(), TimeUnit.SECONDS)).isTrue();
			assertThat(get(keepAlive, "/", true)).as("the next request of a keep-alive connection").isEqualTo("200 B");
			assertThat(h2Get(h2, 3)).as("the next stream of the h2 connection").isEqualTo("B");
			assertThat(h2A.connections()).hasSize(1);
			assertThat(h2A.connections().get(0).awaitClosed(HANG_GUARD)).as("A's h2 leg is closed").isTrue();
			assertThat(servedByA).as("no request reached A after the withdrawal but the one held")
				.hasValue(servedBeforeStop + 1);
			assertThat(servedByB).hasValue(6);
		}
		finally {
			HeadSeams.clear(this.dataProxyServer);
			releaseHead.countDown();
		}
		assertThat(messages()).contains("drain ended with 0 open connection(s)")
			.noneMatch(message -> message.contains("expired"));
		Awaitility.await().atMost(HANG_GUARD).until(() -> this.sessions.find("drain-a").isEmpty());
	}

	@Test
	void connectionsOutlastingTheGraceOrAnAbortedDrainAreClosed() throws Exception {
		HttpServer holding = httpUpstream("H", new AtomicInteger(), new CountDownLatch(0), new CountDownLatch(1));
		TunnelClient expiring = startClient("held-1", Duration.ofSeconds(1), http(HELD_HOST, holding));
		assertDrainEndsWithTheConnectionClosed(expiring, "held-1", false,
				"drain grace PT1S expired; closing 1 open connection(s)");
		// the abort, not the grace, ends this drain
		TunnelClient aborted = startClient("held-2", Duration.ofMinutes(10), http(HELD_HOST, holding));
		assertDrainEndsWithTheConnectionClosed(aborted, "held-2", true, "drain aborted; closing 1 open connection(s)");
	}

	/**
	 * Holds a request on the client, stops it, waits for the withdrawal -- the client is
	 * the host's only candidate, so a new request gets 503 from then on -- aborts the
	 * drain when asked, and expects the drain to end with the logged line and the held
	 * request cut.
	 */
	private void assertDrainEndsWithTheConnectionClosed(TunnelClient client, String clientId, boolean abort,
			String line) throws Exception {
		try (Socket held = dataPlane()) {
			sendGet(held, "/slow", false, HELD_HOST);
			Awaitility.await()
				.atMost(HANG_GUARD)
				.until(() -> this.sessions.find(clientId).map(TunnelSession::connectionCount).orElse(0) == 1);
			CountDownLatch stopped = new CountDownLatch(1);
			Thread.ofVirtual().start(() -> client.stop(stopped::countDown));
			TunnelSession session = this.sessions.find(clientId).orElseThrow();
			Awaitility.await().atMost(HANG_GUARD).until(session::withdrawn);
			try (Socket fresh = dataPlane()) {
				sendGet(fresh, "/", false, HELD_HOST);
				assertThat(read(fresh)).startsWith("503");
			}
			if (abort) {
				client.abortDrain();
			}
			assertThat(stopped.await(HANG_GUARD.toSeconds(), TimeUnit.SECONDS)).isTrue();
			assertThat(messages()).contains(line);
			assertThat(read(held)).as("the held request is cut").doesNotStartWith("200");
		}
	}

	private TunnelClient startClient(String clientId, Duration drainGrace, Upstream... upstreams) {
		SluiceClientProperties.Builder properties = SluiceClientProperties.builder()
			.serverUrl("grpc://127.0.0.1:" + grpcPort)
			.clientId(clientId)
			.drainGrace(drainGrace)
			.token("it-token");
		for (Upstream upstream : upstreams) {
			properties.upstream(upstream);
		}
		TunnelClient client = TunnelClient.builder()
			.properties(properties.build())
			.taskExecutor(TASK_EXECUTOR)
			.meterRegistry(new SimpleMeterRegistry())
			.build();
		client.start();
		this.clients.add(client);
		Awaitility.await().atMost(HANG_GUARD).until(client::isConnected);
		return client;
	}

	private static Upstream http(String host, HttpServer server) {
		return Upstream.builder().host(host).target("http://127.0.0.1:" + server.getAddress().getPort()).build();
	}

	private static Upstream h2(ScriptedH2Upstream server) {
		return Upstream.builder().host(H2_HOST).target("tcp://127.0.0.1:" + server.port()).build();
	}

	private static Upstream tcp(ServerSocket server) {
		return Upstream.builder()
			.host(TCP_HOST)
			.target("tcp://127.0.0.1:" + server.getLocalPort())
			.listenPort(tcpRoutePort)
			.build();
	}

	/**
	 * An upstream answering {@code tag} for every path, counting what it served;
	 * {@code /slow} answers {@code <tag>-slow} once {@code release} opens.
	 */
	private HttpServer httpUpstream(String tag, AtomicInteger served, CountDownLatch entered, CountDownLatch release)
			throws Exception {
		HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		server.setExecutor(UPSTREAM_EXECUTOR);
		server.createContext("/", exchange -> {
			served.incrementAndGet();
			String body = tag;
			if (exchange.getRequestURI().getPath().equals("/slow")) {
				entered.countDown();
				try {
					release.await();
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
				body = tag + "-slow";
			}
			byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
			exchange.sendResponseHeaders(200, bytes.length);
			try (OutputStream out = exchange.getResponseBody()) {
				out.write(bytes);
			}
		});
		server.start();
		this.resources.add(() -> {
			release.countDown();
			server.stop(0);
		});
		return server;
	}

	/** An h2 upstream answering every request with {@code tag}. */
	private ScriptedH2Upstream h2Upstream(String tag) throws Exception {
		ScriptedH2Upstream server = ScriptedH2Upstream.start(new byte[0], ScriptedH2Upstream.answering(tag));
		this.resources.add(server);
		return server;
	}

	/** A GET on a new stream of the h2 connection; the body of its response. */
	private static String h2Get(H2TestClient h2, int streamId) throws Exception {
		h2.headers(streamId, true, TestH2.headerBlock(":authority=" + H2_HOST, ":method=GET", ":path=/"));
		assertThat(h2.awaitEnd(streamId, HANG_GUARD)).as("stream %d ended", streamId).isTrue();
		return TestH2.bodyOf(h2.frames(streamId));
	}

	/**
	 * Waits for the latch; past the test's own waits, so a held relay is let go by the
	 * test, not by this guard.
	 */
	private static void await(CountDownLatch latch) {
		try {
			latch.await(HANG_GUARD.toSeconds() * 2, TimeUnit.SECONDS);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	/** A tcp upstream answering every line with {@code <tag>:<line>}. */
	private ServerSocket tcpUpstream(String tag) throws Exception {
		ServerSocket server = new ServerSocket();
		server.bind(new InetSocketAddress("127.0.0.1", 0), 128);
		UPSTREAM_EXECUTOR.execute(() -> {
			while (!server.isClosed()) {
				try {
					Socket socket = server.accept();
					UPSTREAM_EXECUTOR.execute(() -> {
						try (socket; BufferedReader in = reader(socket)) {
							PrintWriter out = writer(socket);
							for (String line = in.readLine(); line != null; line = in.readLine()) {
								out.println(tag + ":" + line);
							}
						}
						catch (Exception e) {
							// torn down
						}
					});
				}
				catch (Exception e) {
					return;
				}
			}
		});
		this.resources.add(server);
		return server;
	}

	/**
	 * One line through a new connection to the tcp route port; the failure when it
	 * failed.
	 */
	private static String tcpLine(String line) {
		try (Socket socket = new Socket("127.0.0.1", tcpRoutePort)) {
			socket.setSoTimeout((int) HANG_GUARD.toMillis());
			writer(socket).println(line);
			return String.valueOf(reader(socket).readLine());
		}
		catch (Exception e) {
			return e.toString();
		}
	}

	/** The console's live regions, the clients among them, after a form login. */
	private String console() throws Exception {
		String base = "http://127.0.0.1:" + this.consolePort;
		try (HttpClient http = HttpClient.newBuilder().cookieHandler(new CookieManager()).build()) {
			String login = http
				.send(HttpRequest.newBuilder(URI.create(base + "/login")).build(), HttpResponse.BodyHandlers.ofString())
				.body();
			Matcher csrf = Pattern.compile("name=\"_csrf\" value=\"([^\"]+)\"").matcher(login);
			assertThat(csrf.find()).as("the login form carries a CSRF token").isTrue();
			String form = "username=" + CONSOLE_USER + "&password=" + CONSOLE_PASSWORD + "&_csrf="
					+ URLEncoder.encode(csrf.group(1), StandardCharsets.UTF_8);
			http.send(HttpRequest.newBuilder(URI.create(base + "/login"))
				.header("Content-Type", "application/x-www-form-urlencoded")
				.POST(HttpRequest.BodyPublishers.ofString(form))
				.build(), HttpResponse.BodyHandlers.discarding());
			return http
				.send(HttpRequest.newBuilder(URI.create(base + "/console/live")).build(),
						HttpResponse.BodyHandlers.ofString())
				.body();
		}
	}

	private static Socket dataPlane() throws Exception {
		Socket socket = new Socket("127.0.0.1", dataPort);
		socket.setSoTimeout((int) HANG_GUARD.toMillis());
		return socket;
	}

	private static void sendGet(Socket socket, String path, boolean keepAlive) throws Exception {
		sendGet(socket, path, keepAlive, HOST);
	}

	private static void sendGet(Socket socket, String path, boolean keepAlive, String host) throws Exception {
		String head = "GET " + path + " HTTP/1.1\r\nHost: " + host + "\r\n" + (keepAlive ? "" : "Connection: close\r\n")
				+ "\r\n";
		socket.getOutputStream().write(head.getBytes(StandardCharsets.US_ASCII));
		socket.getOutputStream().flush();
	}

	/** Sends a GET and reads its response as {@code <status> <body>}. */
	private static String get(Socket socket, String path, boolean keepAlive) throws Exception {
		sendGet(socket, path, keepAlive);
		return keepAlive ? readOne(socket.getInputStream()) : read(socket);
	}

	/** Reads one response framed by its Content-Length as {@code <status> <body>}. */
	private static String readOne(InputStream in) throws Exception {
		StringBuilder head = new StringBuilder();
		while (!head.toString().endsWith("\r\n\r\n")) {
			int b = in.read();
			if (b < 0) {
				return "EOF " + head;
			}
			head.append((char) b);
		}
		int length = 0;
		for (String headerLine : head.toString().split("\r\n")) {
			if (headerLine.toLowerCase(java.util.Locale.ROOT).startsWith("content-length:")) {
				length = Integer.parseInt(headerLine.substring("content-length:".length()).trim());
			}
		}
		String status = head.substring(head.indexOf(" ") + 1, head.indexOf(" ") + 4);
		return status + " " + new String(in.readNBytes(length), StandardCharsets.UTF_8);
	}

	/** Reads the response up to the end of the connection as {@code <status> <body>}. */
	private static String read(Socket socket) {
		try {
			String response = new String(socket.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
			int bodyStart = response.indexOf("\r\n\r\n");
			if (!response.startsWith("HTTP/1.1 ") || bodyStart < 0) {
				return "EOF " + response;
			}
			return response.substring(9, 12) + " " + response.substring(bodyStart + 4);
		}
		catch (Exception e) {
			return "EOF " + e;
		}
	}

	private static BufferedReader reader(Socket socket) throws Exception {
		return new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
	}

	private static PrintWriter writer(Socket socket) throws Exception {
		return new PrintWriter(socket.getOutputStream(), true, StandardCharsets.UTF_8);
	}

	private static List<String> messages() {
		return CLIENT_EVENTS.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
	}

}
