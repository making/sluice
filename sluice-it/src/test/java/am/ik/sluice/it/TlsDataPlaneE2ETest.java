package am.ik.sluice.it;

import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import com.sun.net.httpserver.HttpServer;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;

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
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.task.TaskExecutor;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static java.util.Objects.requireNonNull;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End to end over TLS: the data plane terminates TLS using an SSL bundle (the same
 * configuration surface as production), ALPN negotiates h2 and http/1.1, and plaintext
 * HTTP/1.1 coexists on the same port.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class TlsDataPlaneE2ETest {

	private static final TaskExecutor TASK_EXECUTOR = task -> Thread.ofVirtual().name("e2e-tls-worker").start(task);

	private static final String H2_BODY = "hello-from-h2-upstream";

	private static final String H1_BODY = "hello-from-tls-upstream";

	private static int grpcPort;

	private static int dataPort;

	private static @Nullable Path keystorePath;

	private static ServerSocket h2Upstream;

	private static final ExecutorService UPSTREAM_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

	private static java.net.InetSocketAddress h1UpstreamAddress;

	private static HttpServer h1Upstream;

	private @Nullable TunnelClient tlsClient;

	@BeforeAll
	static void startUpstreams() throws Exception {
		// stub h2 upstream: see H2cDataPlaneE2ETest
		h2Upstream = new ServerSocket();
		h2Upstream.bind(new InetSocketAddress("127.0.0.1", 0));
		UPSTREAM_EXECUTOR.execute(() -> {
			while (!h2Upstream.isClosed()) {
				try {
					Socket socket = h2Upstream.accept();
					UPSTREAM_EXECUTOR.execute(() -> {
						try (socket) {
							socket.getInputStream().readNBytes(64);
							socket.getOutputStream().write(TestH2.cannedResponse(1, H2_BODY));
							socket.getOutputStream().flush();
							socket.getInputStream().read(new byte[1]);
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
		h1Upstream = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		h1Upstream.createContext("/", exchange -> {
			byte[] body = H1_BODY.getBytes(StandardCharsets.UTF_8);
			exchange.sendResponseHeaders(200, body.length);
			try (var out = exchange.getResponseBody()) {
				out.write(body);
			}
		});
		h1Upstream.start();
		h1UpstreamAddress = h1Upstream.getAddress();
	}

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) throws Exception {
		grpcPort = freePort();
		dataPort = freePort();
		keystorePath = createKeystore();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(dataPort));
		registry.add("sluice.token", () -> "it-token");
		registry.add("server.port", () -> String.valueOf(freePort()));
		registry.add("spring.ssl.bundle.jks.data-plane.keystore.location",
				() -> "file:" + requireNonNull(keystorePath).toAbsolutePath());
		registry.add("spring.ssl.bundle.jks.data-plane.keystore.password", () -> "changeit");
		registry.add("sluice.data-tls-bundle", () -> "data-plane");
	}

	/** Generates a self-signed keystore with SANs for localhost via keytool. */
	private static Path createKeystore() throws Exception {
		Path keystore = Files.createTempFile("sluice-e2e", ".p12");
		Files.delete(keystore);
		String keytool = Path.of(System.getProperty("java.home"), "bin", "keytool").toString();
		Process process = new ProcessBuilder(keytool, "-genkeypair", "-alias", "e2e", "-keyalg", "RSA", "-keysize",
				"2048", "-storetype", "PKCS12", "-storepass", "changeit", "-dname", "CN=localhost", "-ext",
				"san=dns:localhost,ip:127.0.0.1", "-validity", "1", "-keystore", keystore.toAbsolutePath().toString())
			.redirectErrorStream(true)
			.start();
		if (!process.waitFor(60, java.util.concurrent.TimeUnit.SECONDS) || process.exitValue() != 0) {
			throw new IllegalStateException(
					"keytool failed: " + new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8));
		}
		keystore.toFile().deleteOnExit();
		return keystore;
	}

	private static SSLContext clientSslContext() throws Exception {
		KeyStore keyStore = KeyStore.getInstance("PKCS12");
		try (InputStream in = Files.newInputStream(requireNonNull(keystorePath))) {
			keyStore.load(in, "changeit".toCharArray());
		}
		TrustManagerFactory trustManagerFactory = TrustManagerFactory
			.getInstance(TrustManagerFactory.getDefaultAlgorithm());
		trustManagerFactory.init(keyStore);
		SSLContext sslContext = SSLContext.getInstance("TLS");
		sslContext.init(null, trustManagerFactory.getTrustManagers(), null);
		return sslContext;
	}

	private static int freePort() {
		try (ServerSocket socket = new ServerSocket(0)) {
			return socket.getLocalPort();
		}
		catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	@Autowired
	SessionRegistry sessions;

	@AfterAll
	void tearDown() {
		if (this.tlsClient != null) {
			this.tlsClient.stop();
		}
		try {
			h2Upstream.close();
		}
		catch (Exception e) {
			// ignore
		}
		if (h1Upstream != null) {
			h1Upstream.stop(0);
		}
		UPSTREAM_EXECUTOR.shutdown();
	}

	private void startClient() {
		if (this.tlsClient != null) {
			return;
		}
		SluiceClientProperties properties = SluiceClientProperties.builder()
			.serverUrl("grpc://127.0.0.1:" + grpcPort)
			// the h2 stub doubles as the target for both the h2 and the h1-over-TLS path
			.upstream(Upstream.builder()
				.host("tls.local")
				.target("http://127.0.0.1:" + h2Upstream.getLocalPort())
				.build())
			.upstream(Upstream.builder()
				.host("127.0.0.1")
				.target("http://127.0.0.1:" + h1UpstreamAddress.getPort())
				.build())
			.token("it-token")
			.build();
		TunnelClient started = TunnelClient.builder()
			.properties(properties)
			.taskExecutor(TASK_EXECUTOR)
			.meterRegistry(new SimpleMeterRegistry())
			.build();
		started.start();
		this.tlsClient = started;
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> this.sessions.count() > 0);
	}

	@Test
	void httpsH2ViaAlpn() throws Exception {
		this.startClient();
		try (SSLSocket socket = (SSLSocket) clientSslContext().getSocketFactory().createSocket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(10_000);
			javax.net.ssl.SSLParameters parameters = socket.getSSLParameters();
			parameters.setApplicationProtocols(new String[] { "h2" });
			socket.setSSLParameters(parameters);
			socket.startHandshake();
			assertThat(socket.getApplicationProtocol()).isEqualTo("h2");
			TestH2.writeClientHead(socket.getOutputStream(), 1, ":authority=tls.local", ":method=GET", ":path=/");
			List<TestH2.Frame> frames = TestH2.readResponseFrames(socket.getInputStream());
			assertThat(TestH2.bodyOf(frames)).isEqualTo(H2_BODY);
		}
	}

	@Test
	void httpsHttp1_1Fallback() throws Exception {
		this.startClient();
		HttpClient client = HttpClient.newBuilder()
			.version(HttpClient.Version.HTTP_1_1)
			.sslContext(clientSslContext())
			.build();
		HttpResponse<String> response = client.send(
				HttpRequest.newBuilder(URI.create("https://127.0.0.1:" + dataPort + "/")).GET().build(),
				HttpResponse.BodyHandlers.ofString());
		assertThat(response.statusCode()).isEqualTo(200);
		assertThat(response.body()).isEqualTo(H1_BODY);
	}

	@Test
	void plaintextHttp1_1StillWorksOnTheSamePort() throws Exception {
		this.startClient();
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(10_000);
			socket.getOutputStream()
				.write("GET / HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n"
					.getBytes(StandardCharsets.US_ASCII));
			socket.getOutputStream().flush();
			InputStream in = socket.getInputStream();
			String head = new String(in.readNBytes(12), StandardCharsets.US_ASCII);
			assertThat(head).startsWith("HTTP/1.1 200");
		}
	}

}
