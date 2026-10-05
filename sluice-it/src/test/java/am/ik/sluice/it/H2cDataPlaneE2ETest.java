package am.ik.sluice.it;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
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
 * End to end over h2c: prior-knowledge connections are routed by {@code :authority} and
 * relayed frame-by-frame to an h2 stub upstream; HTTP/1.1 Upgrade requests pass through
 * transparently.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class H2cDataPlaneE2ETest {

	private static final TaskExecutor TASK_EXECUTOR = task -> Thread.ofVirtual().name("e2e-h2c-worker").start(task);

	private static final String H2_BODY = "hello-from-h2-upstream";

	private static int grpcPort;

	private static int dataPort;

	private static HttpServer http1Upstream;

	private static ServerSocket h2Upstream;

	private static final ExecutorService UPSTREAM_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

	private @Nullable TunnelClient h2Client;

	private @Nullable TunnelClient http1Client;

	@BeforeAll
	static void startUpstreams() throws Exception {
		// stub h2 upstream: replies with canned response frames to any connection
		h2Upstream = new ServerSocket();
		h2Upstream.bind(new InetSocketAddress("127.0.0.1", 0));
		UPSTREAM_EXECUTOR.execute(() -> {
			while (!h2Upstream.isClosed()) {
				try {
					Socket socket = h2Upstream.accept();
					UPSTREAM_EXECUTOR.execute(() -> {
						try (socket) {
							socket.getInputStream().readNBytes(64); // preface + settings
																	// + headers, contents
																	// unused
							socket.getOutputStream().write(TestH2.cannedResponse(1, H2_BODY));
							socket.getOutputStream().flush();
							socket.getInputStream().read(new byte[1]); // keep the relay
																		// open until the
																		// client hangs up
						}
						catch (Exception e) {
							// connection ended
						}
					});
				}
				catch (Exception e) {
					return;
				}
			}
		});
		http1Upstream = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		http1Upstream.createContext("/", exchange -> {
			byte[] body = "hello-from-upstream".getBytes(StandardCharsets.UTF_8);
			exchange.getResponseHeaders().set("Content-Type", "text/plain");
			exchange.sendResponseHeaders(200, body.length);
			try (var out = exchange.getResponseBody()) {
				out.write(body);
			}
		});
		http1Upstream.start();
	}

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) {
		grpcPort = freePort();
		dataPort = freePort();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(dataPort));
		registry.add("sluice.token", () -> "it-token");
		registry.add("server.port", () -> String.valueOf(freePort()));
	}

	private static int freePort() {
		try (ServerSocket socket = new ServerSocket(0)) {
			return socket.getLocalPort();
		}
		catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	private TunnelClient startClient(String host, String targetUrl) {
		SluiceClientProperties properties = SluiceClientProperties.builder()
			.serverUrl("grpc://127.0.0.1:" + grpcPort)
			.upstream(host, targetUrl)
			.token("it-token")
			.build();
		TunnelClient started = new TunnelClient(properties, TASK_EXECUTOR, new SimpleMeterRegistry());
		started.start();
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> this.sessions.count() > 0);
		return started;
	}

	private SessionRegistry sessions;

	@AfterAll
	void tearDown() {
		if (this.h2Client != null) {
			this.h2Client.stop();
		}
		if (this.http1Client != null) {
			this.http1Client.stop();
		}
		try {
			h2Upstream.close();
		}
		catch (Exception e) {
			// ignore
		}
		if (http1Upstream != null) {
			http1Upstream.stop(0);
		}
		UPSTREAM_EXECUTOR.shutdown();
	}

	@Autowired
	void inject(SessionRegistry sessions) {
		this.sessions = sessions;
	}

	@Test
	void h2cPriorKnowledgeIsRoutedByAuthority() throws Exception {
		if (this.h2Client == null) {
			this.h2Client = this.startClient("h2.local", "http://127.0.0.1:" + h2Upstream.getLocalPort());
		}
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(10_000);
			TestH2.writeClientHead(socket.getOutputStream(), 1, ":authority=h2.local", ":method=GET", ":path=/");
			List<TestH2.Frame> frames = TestH2.readResponseFrames(socket.getInputStream());
			assertThat(frames).anyMatch(frame -> frame.type() == TestH2.HEADERS);
			assertThat(TestH2.bodyOf(frames)).isEqualTo(H2_BODY);
		}
	}

	@Test
	void h2cUpgradeRequestPassesThroughTransparently() throws Exception {
		if (this.http1Client == null) {
			this.http1Client = this.startClient("upgrade.local",
					"http://127.0.0.1:" + http1Upstream.getAddress().getPort());
		}
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(10_000);
			socket.getOutputStream()
				.write(("GET / HTTP/1.1\r\nHost: upgrade.local\r\nUpgrade: h2c\r\nHTTP2-Settings: \r\n"
						+ "Connection: Upgrade, HTTP2-Settings\r\n\r\n")
					.getBytes(StandardCharsets.US_ASCII));
			socket.getOutputStream().flush();
			// the h1 upstream answers with a plain response; the proxy must relay it
			// as-is
			ByteArrayOutputStream bytes = new ByteArrayOutputStream();
			byte[] buffer = new byte[1024];
			InputStream in = socket.getInputStream();
			int n;
			while ((n = in.read(buffer)) > 0) {
				bytes.write(buffer, 0, n);
				if (bytes.toString(StandardCharsets.UTF_8).contains("hello-from-upstream")) {
					break;
				}
			}
			assertThat(bytes.toString(StandardCharsets.UTF_8)).startsWith("HTTP/1.1 200");
		}
	}

}
