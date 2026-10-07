package am.ik.sluice.it;

import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
 * End to end verification of the round-robin load balancing
 * ({@code sluice.http-load-balance} / {@code sluice.tcp-load-balance}): two clients
 * serving the same domain or listen port alternate across successive connections instead
 * of pinning the route to the smallest client id.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class RoundRobinE2ETest {

	private static final String HOST = "rr.local";

	private static final TaskExecutor TASK_EXECUTOR = task -> Thread.ofVirtual().name("e2e-round-robin").start(task);

	private static int grpcPort;

	private static int dataPort;

	private static int tcpRoutePort;

	private static @Nullable HttpServer httpUpstreamA;

	private static @Nullable HttpServer httpUpstreamB;

	/**
	 * Raw tcp upstreams echoing back their own listen port once the peer half-closes; the
	 * response tells which upstream served the connection.
	 */
	private static @Nullable ServerSocket tcpUpstreamA;

	private static @Nullable ServerSocket tcpUpstreamB;

	private static final ExecutorService UPSTREAM_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

	private final java.util.Map<String, TunnelClient> clients = new java.util.concurrent.ConcurrentHashMap<>();

	private final SessionRegistry sessions;

	RoundRobinE2ETest(@Autowired SessionRegistry sessions) {
		this.sessions = sessions;
	}

	@BeforeAll
	static void startUpstreams() throws Exception {
		httpUpstreamA = echoHttpUpstream();
		httpUpstreamB = echoHttpUpstream();
		tcpUpstreamA = listen();
		tcpUpstreamB = listen();
		for (ServerSocket server : new ServerSocket[] { tcpUpstreamA, tcpUpstreamB }) {
			int port = server.getLocalPort();
			UPSTREAM_EXECUTOR.execute(() -> acceptLoop(server, port));
		}
	}

	private static HttpServer echoHttpUpstream() throws Exception {
		// the upstream echoes its own listen port back as the body
		HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		int port = server.getAddress().getPort();
		server.createContext("/", exchange -> {
			byte[] body = String.valueOf(port).getBytes(StandardCharsets.UTF_8);
			exchange.getResponseHeaders().set("Content-Type", "text/plain");
			exchange.sendResponseHeaders(200, body.length);
			try (var out = exchange.getResponseBody()) {
				out.write(body);
			}
		});
		server.start();
		return server;
	}

	private static ServerSocket listen() throws Exception {
		ServerSocket server = new ServerSocket();
		server.setReuseAddress(true);
		server.bind(new InetSocketAddress("127.0.0.1", 0), 128);
		return server;
	}

	private static void acceptLoop(ServerSocket server, int port) {
		while (!server.isClosed()) {
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
		registry.add("sluice.http-load-balance", () -> "round-robin");
		registry.add("sluice.tcp-load-balance", () -> "round-robin");
		// only the tcp route port below is claimable
		registry.add("sluice.tcp-port-range", () -> String.valueOf(tcpRoutePort));
		registry.add("server.port", () -> 0);
	}

	/**
	 * Connects both clients. The larger client id must own the listen port first: the
	 * smaller id takes the port over on connect, while the reverse order would reject the
	 * second advertiser forever (the owner is alive) and keep its session churning.
	 */
	private void startClients() {
		this.clients.computeIfAbsent("client-b", id -> {
			TunnelClient client = startClient(id, httpUpstream(httpUpstreamB), tcpUpstream(tcpUpstreamB));
			Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> this.sessions.count() >= 1);
			return client;
		});
		this.clients.computeIfAbsent("client-a", id -> {
			TunnelClient client = startClient(id, httpUpstream(httpUpstreamA), tcpUpstream(tcpUpstreamA));
			Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> this.sessions.count() >= 2);
			return client;
		});
		// both listen ports bound (client-a took over from client-b)
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> reachable(tcpRoutePort));
	}

	private static Upstream httpUpstream(@Nullable HttpServer server) {
		java.util.Objects.requireNonNull(server, "http upstream not started");
		return Upstream.builder().host(HOST).target("http://127.0.0.1:" + server.getAddress().getPort()).build();
	}

	private static Upstream tcpUpstream(@Nullable ServerSocket server) {
		java.util.Objects.requireNonNull(server, "tcp upstream not started");
		// a distinct host: the client side keys its upstream map by host, so a second
		// upstream on the same host would replace the http one
		return Upstream.builder()
			.host("rr-tcp.local")
			.target("tcp://127.0.0.1:" + server.getLocalPort())
			.listenPort(tcpRoutePort)
			.build();
	}

	private TunnelClient startClient(String clientId, Upstream... upstreams) {
		SluiceClientProperties.Builder properties = SluiceClientProperties.builder()
			.serverUrl("grpc://127.0.0.1:" + grpcPort)
			.clientId(clientId);
		for (Upstream upstream : upstreams) {
			properties.upstream(upstream);
		}
		TunnelClient client = TunnelClient.builder()
			.properties(properties.token("it-token").build())
			.taskExecutor(TASK_EXECUTOR)
			.meterRegistry(new SimpleMeterRegistry())
			.build();
		client.start();
		return client;
	}

	private static boolean reachable(int port) {
		try (Socket socket = new Socket("127.0.0.1", port)) {
			return true;
		}
		catch (Exception e) {
			return false;
		}
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

	@AfterAll
	void tearDown() {
		this.clients.values().forEach(TunnelClient::stop);
		java.util.Objects.requireNonNull(httpUpstreamA, "http upstream not started").stop(0);
		java.util.Objects.requireNonNull(httpUpstreamB, "http upstream not started").stop(0);
		try {
			java.util.Objects.requireNonNull(tcpUpstreamA, "tcp upstream not started").close();
			java.util.Objects.requireNonNull(tcpUpstreamB, "tcp upstream not started").close();
		}
		catch (Exception e) {
			// ignore
		}
		// closing the listeners ends the accept loops
		UPSTREAM_EXECUTOR.shutdown();
	}

	@Test
	void httpRequestsAlternateBetweenTheTwoClients() throws Exception {
		startClients();
		String portA = String.valueOf(java.util.Objects.requireNonNull(httpUpstreamA).getAddress().getPort());
		String portB = String.valueOf(java.util.Objects.requireNonNull(httpUpstreamB).getAddress().getPort());
		String first = httpGet(HOST);
		String second = httpGet(HOST);
		String third = httpGet(HOST);
		String fourth = httpGet(HOST);
		// the first hit depends on registration order; the alternation must hold
		assertThat(first).isIn(portA, portB);
		assertThat(second).isEqualTo(portA.equals(first) ? portB : portA);
		assertThat(third).isEqualTo(first);
		assertThat(fourth).isEqualTo(second);
	}

	@Test
	void tcpConnectionsAlternateBetweenTheTwoClients() throws Exception {
		startClients();
		String portA = String.valueOf(java.util.Objects.requireNonNull(tcpUpstreamA).getLocalPort());
		String portB = String.valueOf(java.util.Objects.requireNonNull(tcpUpstreamB).getLocalPort());
		// the alternation may need a retry while the route table settles after the
		// listen port take-over
		org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> {
			String first = tcpRoundTrip();
			String second = tcpRoundTrip();
			return (first.equals(portA) && second.equals(portB)) || (first.equals(portB) && second.equals(portA));
		});
	}

}
