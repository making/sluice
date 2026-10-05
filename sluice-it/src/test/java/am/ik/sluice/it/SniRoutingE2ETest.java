package am.ik.sluice.it;

import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.KeyManagerFactory;
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
 * End to end over TLS passthrough: the data plane routes each connection by the SNI host
 * name of the ClientHello and relays the TLS bytes untouched; each upstream terminates
 * TLS with its own certificate.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class SniRoutingE2ETest {

	private static final TaskExecutor TASK_EXECUTOR = task -> Thread.ofVirtual().name("e2e-sni-worker").start(task);

	private static final String HOST_A = "a.sni.test";

	private static final String HOST_B = "b.sni.test";

	private static final String BODY_A = "echo-from-a";

	private static final String BODY_B = "echo-from-b";

	private static int grpcPort;

	private static int dataPort;

	private static @Nullable Path keystorePath;

	private static ServerSocket upstreamA;

	private static ServerSocket upstreamB;

	private static final ExecutorService UPSTREAM_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

	private static SSLContext backendSslContext;

	private static SSLContext clientSslContext;

	private @Nullable TunnelClient client;

	@BeforeAll
	static void startUpstreams() throws Exception {
		keystorePath = createKeystore();
		backendSslContext = backendSslContext();
		clientSslContext = clientSslContext();
		upstreamA = echoUpstream("a:");
		upstreamB = echoUpstream("b:");
	}

	/**
	 * A TLS echo upstream that answers one line per received line and honors half-close.
	 */
	private static ServerSocket echoUpstream(String prefix) {
		SSLServerSocketFactory factory = backendSslContext.getServerSocketFactory();
		try {
			ServerSocket listener = factory.createServerSocket();
			listener.bind(new InetSocketAddress("127.0.0.1", 0));
			UPSTREAM_EXECUTOR.execute(() -> {
				while (!listener.isClosed()) {
					try {
						SSLSocket socket = (SSLSocket) listener.accept();
						UPSTREAM_EXECUTOR.execute(() -> {
							try (socket) {
								socket.setUseClientMode(false);
								socket.startHandshake();
								InputStream in = socket.getInputStream();
								byte[] buffer = new byte[256];
								int n;
								StringBuilder line = new StringBuilder();
								while ((n = in.read(buffer)) > 0) {
									for (int i = 0; i < n; i++) {
										if (buffer[i] == '\n') {
											String echoed = prefix + line + "\n";
											socket.getOutputStream().write(echoed.getBytes(StandardCharsets.US_ASCII));
											socket.getOutputStream().flush();
											line.setLength(0);
										}
										else {
											line.append((char) buffer[i]);
										}
									}
								}
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
			return listener;
		}
		catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) throws Exception {
		grpcPort = freePort();
		dataPort = freePort();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(dataPort));
		registry.add("sluice.token", () -> "it-token");
		registry.add("server.port", () -> String.valueOf(freePort()));
	}

	/** Generates a self-signed keystore whose SANs cover both upstream host names. */
	private static Path createKeystore() throws Exception {
		Path keystore = Files.createTempFile("sluice-sni-e2e", ".p12");
		Files.delete(keystore);
		String keytool = Path.of(System.getProperty("java.home"), "bin", "keytool").toString();
		Process process = new ProcessBuilder(keytool, "-genkeypair", "-alias", "e2e", "-keyalg", "RSA", "-keysize",
				"2048", "-storetype", "PKCS12", "-storepass", "changeit", "-dname", "CN=sni", "-ext",
				"san=dns:" + HOST_A + ",dns:" + HOST_B, "-validity", "1", "-keystore",
				keystore.toAbsolutePath().toString())
			.redirectErrorStream(true)
			.start();
		if (!process.waitFor(60, TimeUnit.SECONDS) || process.exitValue() != 0) {
			throw new IllegalStateException(
					"keytool failed: " + new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8));
		}
		keystore.toFile().deleteOnExit();
		return keystore;
	}

	private static SSLContext keyStoresContext(boolean client) throws Exception {
		KeyStore keyStore = KeyStore.getInstance("PKCS12");
		try (InputStream in = Files.newInputStream(requireNonNull(keystorePath))) {
			keyStore.load(in, "changeit".toCharArray());
		}
		if (client) {
			TrustManagerFactory trustManagerFactory = TrustManagerFactory
				.getInstance(TrustManagerFactory.getDefaultAlgorithm());
			trustManagerFactory.init(keyStore);
			SSLContext sslContext = SSLContext.getInstance("TLS");
			sslContext.init(null, trustManagerFactory.getTrustManagers(), null);
			return sslContext;
		}
		KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
		keyManagerFactory.init(keyStore, "changeit".toCharArray());
		SSLContext sslContext = SSLContext.getInstance("TLS");
		sslContext.init(keyManagerFactory.getKeyManagers(), null, null);
		return sslContext;
	}

	private static SSLContext backendSslContext() throws Exception {
		return keyStoresContext(false);
	}

	private static SSLContext clientSslContext() throws Exception {
		return keyStoresContext(true);
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
		if (this.client != null) {
			this.client.stop();
		}
		try {
			upstreamA.close();
			upstreamB.close();
		}
		catch (Exception e) {
			// ignore
		}
		UPSTREAM_EXECUTOR.shutdown();
	}

	private void startClient() {
		if (this.client != null) {
			return;
		}
		SluiceClientProperties properties = SluiceClientProperties.builder()
			.serverUrl("grpc://127.0.0.1:" + grpcPort)
			.upstream(Upstream.builder()
				.host(HOST_A)
				.target("tcp://127.0.0.1:" + upstreamA.getLocalPort())
				.tlsPassthrough(true)
				.build())
			.upstream(Upstream.builder()
				.host(HOST_B)
				.target("tcp://127.0.0.1:" + upstreamB.getLocalPort())
				.tlsPassthrough(true)
				.build())
			.token("it-token")
			.build();
		TunnelClient started = new TunnelClient(properties, TASK_EXECUTOR, new SimpleMeterRegistry());
		started.start();
		this.client = started;
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> this.sessions.count() > 0);
	}

	/** A fresh context per connection: the JSSE session cache must not span rounds. */
	private SSLContext freshContext() throws Exception {
		return keyStoresContext(true);
	}

	/** TLS round trip through the data plane: the SNI name selects the upstream. */
	private String roundTrip(String sni, String prefix, String body) throws Exception {
		try (SSLSocket socket = (SSLSocket) freshContext().getSocketFactory().createSocket()) {
			socket.connect(new InetSocketAddress("127.0.0.1", dataPort), 5000);
			socket.setSoTimeout(10_000);
			javax.net.ssl.SSLParameters parameters = socket.getSSLParameters();
			parameters.setServerNames(java.util.List.of(new javax.net.ssl.SNIHostName(sni)));
			socket.setSSLParameters(parameters);
			socket.startHandshake();
			socket.getOutputStream().write((body + "\n").getBytes(StandardCharsets.US_ASCII));
			socket.getOutputStream().flush();
			String echoed = new String(socket.getInputStream().readNBytes(prefix.length() + body.length() + 1),
					StandardCharsets.US_ASCII);
			// half-close: the EOF must propagate through the data plane to the upstream
			socket.shutdownOutput();
			assertThat(socket.getInputStream().read()).isEqualTo(-1);
			return echoed;
		}
	}

	@Test
	void routesBySniToUpstreamA() throws Exception {
		this.startClient();
		assertThat(this.roundTrip(HOST_A, "a:", BODY_A)).isEqualTo("a:" + BODY_A + "\n");
	}

	@Test
	void routesBySniToUpstreamB() throws Exception {
		this.startClient();
		assertThat(this.roundTrip(HOST_B, "b:", BODY_B)).isEqualTo("b:" + BODY_B + "\n");
	}

	@Test
	void closesUnknownSniWithoutHandshake() throws Exception {
		this.startClient();
		try (SSLSocket socket = (SSLSocket) freshContext().getSocketFactory().createSocket()) {
			socket.connect(new InetSocketAddress("127.0.0.1", dataPort), 5000);
			socket.setSoTimeout(10_000);
			javax.net.ssl.SSLParameters parameters = socket.getSSLParameters();
			parameters.setServerNames(java.util.List.of(new javax.net.ssl.SNIHostName("unknown.sni.test")));
			socket.setSSLParameters(parameters);
			org.assertj.core.api.Assertions.assertThatThrownBy(socket::startHandshake).isInstanceOf(Exception.class);
		}
	}

	@Test
	void upstreamPortsStayDistinct() {
		this.startClient();
		assertThat(upstreamA.getLocalPort()).isNotEqualTo(upstreamB.getLocalPort());
		assertThat(URI.create("http://127.0.0.1:" + upstreamA.getLocalPort()).getHost()).isEqualTo("127.0.0.1");
	}

}
