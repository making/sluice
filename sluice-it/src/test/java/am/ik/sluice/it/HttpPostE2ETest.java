package am.ik.sluice.it;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

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
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.core.task.TaskExecutor;
import org.springframework.test.web.servlet.client.RestTestClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static java.util.Objects.requireNonNull;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End to end POST requests: the request head and body often arrive in a single TCP
 * segment, so the head parser must complete at the blank line and the body bytes consumed
 * with it must still reach the upstream -- over http/1.1, h2 prior knowledge, and TLS.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class HttpPostE2ETest {

	private static final TaskExecutor TASK_EXECUTOR = task -> Thread.ofVirtual().name("e2e-post-worker").start(task);

	private static final String POST_BODY = "a=1&b=hello-tunnel";

	private static int grpcPort;

	private static int dataPort;

	private static @Nullable Path keystorePath;

	private static HttpServer h1Upstream;

	private static ServerSocket h2Upstream;

	private static final ExecutorService UPSTREAM_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

	private @Nullable TunnelClient h1Client;

	private @Nullable TunnelClient h2Client;

	private @Nullable TunnelClient tlsClient;

	@BeforeAll
	static void startUpstreams() throws Exception {
		// h1 echo upstream: responds with the request body
		h1Upstream = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		h1Upstream.createContext("/", exchange -> {
			byte[] body = exchange.getRequestBody().readAllBytes();
			exchange.getResponseHeaders().set("Content-Type", "text/plain");
			exchange.sendResponseHeaders(200, body.length);
			try (var out = exchange.getResponseBody()) {
				out.write(body);
			}
		});
		h1Upstream.start();
		// h2 echo upstream: accumulates the DATA frames of the request stream and echoes
		// them back
		h2Upstream = new ServerSocket();
		h2Upstream.bind(new InetSocketAddress("127.0.0.1", 0));
		UPSTREAM_EXECUTOR.execute(() -> {
			while (!h2Upstream.isClosed()) {
				try {
					Socket socket = h2Upstream.accept();
					UPSTREAM_EXECUTOR.execute(() -> {
						try (socket) {
							InputStream in = socket.getInputStream();
							in.readNBytes(TestH2.PREFACE.length + 9); // magic + client
																		// SETTINGS
							ByteArrayOutputStream body = new ByteArrayOutputStream();
							int endStream = -1;
							TestH2.Frame frame;
							while (endStream < 0 && (frame = TestH2.readFrame(in)) != null) {
								if (frame.type() == TestH2.DATA) {
									body.writeBytes(frame.payload());
									if (frame.endStream()) {
										endStream = frame.streamId();
									}
								}
							}
							socket.getOutputStream()
								.write(TestH2.cannedResponse(Math.max(endStream, 1),
										body.toString(StandardCharsets.UTF_8)));
							socket.getOutputStream().flush();
							in.read(new byte[1]); // keep the relay open until the client
													// hangs up
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
		Path keystore = Files.createTempFile("sluice-e2e-post", ".p12");
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

	private TunnelClient startClient(String host, String targetUrl) {
		SluiceClientProperties properties = SluiceClientProperties.builder()
			.serverUrl("grpc://127.0.0.1:" + grpcPort)
			.upstream(Upstream.builder().host(host).target(targetUrl).build())
			.token("it-token")
			.build();
		TunnelClient started = TunnelClient.builder()
			.properties(properties)
			.taskExecutor(TASK_EXECUTOR)
			.meterRegistry(new SimpleMeterRegistry())
			.build();
		started.start();
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> this.sessions.count() > 0);
		return started;
	}

	@Autowired
	SessionRegistry sessions;

	@AfterAll
	void tearDown() {
		if (this.h1Client != null) {
			this.h1Client.stop();
		}
		if (this.h2Client != null) {
			this.h2Client.stop();
		}
		if (this.tlsClient != null) {
			this.tlsClient.stop();
		}
		if (h1Upstream != null) {
			h1Upstream.stop(0);
		}
		try {
			h2Upstream.close();
		}
		catch (Exception e) {
			// ignore
		}
		UPSTREAM_EXECUTOR.shutdown();
	}

	@Test
	void http1PostWithBodyInTheSameSegmentIsRelayed() throws Exception {
		if (this.h1Client == null) {
			this.h1Client = this.startClient("post.local", "http://127.0.0.1:" + h1Upstream.getAddress().getPort());
		}
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(10_000);
			// head and body in a single write: the head parser must not wait for more
			// bytes to see the end of the request
			socket.getOutputStream()
				.write(("POST / HTTP/1.1\r\nHost: post.local\r\nContent-Type: text/plain\r\nContent-Length: "
						+ POST_BODY.length() + "\r\nConnection: close\r\n\r\n" + POST_BODY)
					.getBytes(StandardCharsets.US_ASCII));
			socket.getOutputStream().flush();
			String response = new String(socket.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
			assertThat(response).startsWith("HTTP/1.1 200");
			assertThat(response).endsWith(POST_BODY);
		}
	}

	@Test
	void h2PriorKnowledgePostCarriesTheDataFrames() throws Exception {
		if (this.h2Client == null) {
			this.h2Client = this.startClient("post-h2.local", "http://127.0.0.1:" + h2Upstream.getLocalPort());
		}
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(10_000);
			// HEADERS (END_HEADERS, no END_STREAM) then DATA with END_STREAM, written
			// back to back
			var out = socket.getOutputStream();
			out.write(TestH2.PREFACE);
			TestH2.writeFrame(out, TestH2.SETTINGS, 0x0, 0, new byte[0]);
			byte[] head = TestH2.headerBlock(":authority=post-h2.local", ":method=POST", ":path=/",
					"content-length=" + POST_BODY.length());
			TestH2.writeFrame(out, TestH2.HEADERS, TestH2.FLAG_END_HEADERS, 1, head);
			TestH2.writeFrame(out, TestH2.DATA, TestH2.FLAG_END_STREAM, 1, POST_BODY.getBytes(StandardCharsets.UTF_8));
			List<TestH2.Frame> frames = TestH2.readResponseFrames(socket.getInputStream());
			assertThat(TestH2.bodyOf(frames)).isEqualTo(POST_BODY);
		}
	}

	@Test
	void httpsPostOverTlsTerminationIsRelayed() throws Exception {
		if (this.tlsClient == null) {
			// the client derives the Host header from the URI, so the upstream is
			// registered as localhost
			this.tlsClient = this.startClient("localhost", "http://127.0.0.1:" + h1Upstream.getAddress().getPort());
		}
		HttpClient httpClient = HttpClient.newBuilder()
			.version(HttpClient.Version.HTTP_1_1)
			.sslContext(clientSslContext())
			.build();
		RestTestClient client = RestTestClient.bindToServer(new JdkClientHttpRequestFactory(httpClient)).build();
		client.post()
			.uri("https://localhost:" + dataPort + "/")
			.contentType(MediaType.TEXT_PLAIN)
			.body(POST_BODY)
			.exchange()
			.expectStatus()
			.isOk()
			.expectBody(String.class)
			.isEqualTo(POST_BODY);
	}

}
