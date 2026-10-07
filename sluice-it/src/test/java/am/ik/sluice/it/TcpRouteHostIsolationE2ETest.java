package am.ik.sluice.it;

import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
import am.ik.sluice.server.route.Router;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.task.TaskExecutor;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End to end verification that upstreams with a listen port are reached only through
 * their port: their host takes no part in Host routing on the data plane, so a tcp
 * upstream declared first on a shared host does not capture its http traffic, and one
 * without a host does not become the catch-all.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class TcpRouteHostIsolationE2ETest {

	private static final String HOST = "dual.local";

	private static final TaskExecutor TASK_EXECUTOR = task -> Thread.ofVirtual().name("e2e-tcp-isolation").start(task);

	private static final ExecutorService UPSTREAM_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

	private static int grpcPort;

	private static int dataPort;

	private static int sharedHostPort;

	private static int hostlessPort;

	private @Nullable HttpServer httpUpstream;

	private @Nullable ServerSocket tcpUpstream;

	private @Nullable TunnelClient client;

	@Autowired
	Router router;

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) {
		grpcPort = freePort();
		dataPort = freePort();
		sharedHostPort = freePort();
		hostlessPort = freePort();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(dataPort));
		registry.add("sluice.token", () -> "it-token");
		registry.add("sluice.tcp-port-range", () -> sharedHostPort + "," + hostlessPort);
		registry.add("server.port", () -> 0);
	}

	private static int freePort() {
		try (ServerSocket socket = new ServerSocket(0)) {
			return socket.getLocalPort();
		}
		catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	@BeforeAll
	void setUp() throws Exception {
		HttpServer http = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		http.createContext("/", exchange -> {
			byte[] body = "http-upstream".getBytes(StandardCharsets.UTF_8);
			exchange.sendResponseHeaders(200, body.length);
			try (var out = exchange.getResponseBody()) {
				out.write(body);
			}
		});
		http.start();
		this.httpUpstream = http;
		ServerSocket tcp = new ServerSocket();
		tcp.bind(new InetSocketAddress("127.0.0.1", 0), 16);
		this.tcpUpstream = tcp;
		UPSTREAM_EXECUTOR.execute(() -> answerTcp(tcp));
		String tcpTarget = "tcp://127.0.0.1:" + tcp.getLocalPort();
		TunnelClient started = TunnelClient.builder()
			.properties(SluiceClientProperties.builder()
				.serverUrl("grpc://127.0.0.1:" + grpcPort)
				.clientId("isolation-client")
				// the tcp upstream comes first on the shared host on purpose
				.upstream(Upstream.builder().host(HOST).target(tcpTarget).listenPort(sharedHostPort).build())
				.upstream(
						Upstream.builder().host(HOST).target("http://127.0.0.1:" + http.getAddress().getPort()).build())
				.upstream(Upstream.builder().host("").target(tcpTarget).listenPort(hostlessPort).build())
				.token("it-token")
				.build())
			.taskExecutor(TASK_EXECUTOR)
			.meterRegistry(new SimpleMeterRegistry())
			.build();
		started.start();
		this.client = started;
		// one advertise registers every upstream at once: the last port route marks it
		// done
		Awaitility.await()
			.atMost(Duration.ofSeconds(10))
			.until(() -> this.router.lookupByPort(hostlessPort).isPresent());
	}

	/** Answers every connection with a fixed marker, whatever it receives. */
	private static void answerTcp(ServerSocket server) {
		while (!server.isClosed()) {
			try {
				Socket socket = server.accept();
				UPSTREAM_EXECUTOR.execute(() -> {
					try (socket) {
						socket.getOutputStream().write("tcp-upstream".getBytes(StandardCharsets.US_ASCII));
						socket.getOutputStream().flush();
						socket.shutdownOutput();
						socket.getInputStream().readAllBytes();
					}
					catch (Exception e) {
						// peer gone
					}
				});
			}
			catch (Exception e) {
				return;
			}
		}
	}

	@AfterAll
	void tearDown() throws Exception {
		if (this.client != null) {
			this.client.stop();
		}
		if (this.httpUpstream != null) {
			this.httpUpstream.stop(0);
		}
		if (this.tcpUpstream != null) {
			this.tcpUpstream.close();
		}
		UPSTREAM_EXECUTOR.shutdownNow();
	}

	@Test
	void httpRequestOnTheSharedHostReachesTheHttpUpstream() throws Exception {
		assertThat(httpGet(HOST)).isEqualToNormalizingWhitespace("""
				HTTP/1.1 200 OK
				http-upstream
				""");
	}

	@Test
	void httpRequestForAnUnknownHostIsRefusedInsteadOfRelayedToATcpUpstream() throws Exception {
		assertThat(httpGet("unknown.local").lines().findFirst()).hasValue("HTTP/1.1 503 Service Unavailable");
	}

	@Test
	void tcpPortsStillReachTheTcpUpstream() throws Exception {
		assertThat(tcpRead(sharedHostPort)).isEqualTo("tcp-upstream");
		assertThat(tcpRead(hostlessPort)).isEqualTo("tcp-upstream");
	}

	/** Status line and body of a GET through the data plane. */
	private static String httpGet(String host) throws Exception {
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(10_000);
			socket.getOutputStream()
				.write(("GET / HTTP/1.1\r\nHost: " + host + "\r\nConnection: close\r\n\r\n")
					.getBytes(StandardCharsets.US_ASCII));
			socket.getOutputStream().flush();
			String response = new String(socket.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
			int headEnd = response.indexOf("\r\n\r\n");
			if (headEnd < 0) {
				// not an HTTP response, e.g. bytes from a raw tcp upstream
				return response;
			}
			return response.substring(0, response.indexOf("\r\n")) + "\n" + response.substring(headEnd + 4);
		}
	}

	private static String tcpRead(int port) throws Exception {
		try (Socket socket = new Socket("127.0.0.1", port); InputStream in = socket.getInputStream()) {
			socket.setSoTimeout(10_000);
			socket.shutdownOutput();
			return new String(Objects.requireNonNull(in).readAllBytes(), StandardCharsets.US_ASCII);
		}
	}

}
