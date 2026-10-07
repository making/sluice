package am.ik.sluice.it;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.interfaces.RSAPrivateCrtKey;
import java.time.Duration;
import java.util.Base64;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import am.ik.sluice.client.SluiceClientApplication;
import am.ik.sluice.client.tunnel.TunnelClient;
import am.ik.sluice.server.SluiceServerApplication;
import am.ik.sluice.server.tunnel.SessionRegistry;
import com.sun.net.httpserver.HttpServer;

import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Full stack cluster test with a TLS control plane: every server node serves gRPC over
 * TLS from a pem SSL bundle so a front end can route per node by SNI without terminating
 * TLS. The membership URLs and the client bootstrap use {@code grpcs://} (ALPN h2),
 * trusting the self-signed server certificate via {@code sluice.insecure}.
 */
@TestInstance(Lifecycle.PER_CLASS)
class ClusterGrpcTlsE2ETests {

	private static final String HOST = "cluster.local";

	private static final String TOKEN = "cluster-tls-it-token";

	private static final String GRPC_TLS_BUNDLE = "grpc-control";

	private static Path keystorePath;

	private static Path certPemPath;

	private static Path keyPemPath;

	private @org.jspecify.annotations.Nullable HttpServer httpUpstream;

	private int httpPort;

	private final int[] grpcPorts = new int[2];

	private final int[] dataPorts = new int[2];

	private final @org.jspecify.annotations.Nullable ConfigurableApplicationContext[] servers = new ConfigurableApplicationContext[2];

	private @org.jspecify.annotations.Nullable ConfigurableApplicationContext clientContext;

	private final String[] nodeNames = { "node-1", "node-2" };

	@BeforeAll
	static void createServerCertificate() throws Exception {
		// one self-signed key pair shared by both nodes; SANs for localhost
		Path keystore = Files.createTempFile("sluice-grpc-tls", ".p12");
		Files.delete(keystore);
		String keytool = Path.of(System.getProperty("java.home"), "bin", "keytool").toString();
		Process process = new ProcessBuilder(keytool, "-genkeypair", "-alias", "grpc-tls", "-keyalg", "RSA", "-keysize",
				"2048", "-storetype", "PKCS12", "-storepass", "changeit", "-dname", "CN=localhost", "-ext",
				"san=dns:localhost,ip:127.0.0.1", "-validity", "1", "-keystore", keystore.toAbsolutePath().toString())
			.redirectErrorStream(true)
			.start();
		if (!process.waitFor(60, TimeUnit.SECONDS) || process.exitValue() != 0) {
			throw new IllegalStateException(
					"keytool failed: " + new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8));
		}
		keystore.toFile().deleteOnExit();
		keystorePath = keystore;
		certPemPath = exportPem(keystore, "CERTIFICATE", store -> store.getCertificate("grpc-tls").getEncoded());
		keyPemPath = exportPem(keystore, "PRIVATE KEY",
				store -> ((RSAPrivateCrtKey) store.getKey("grpc-tls", "changeit".toCharArray())).getEncoded());
	}

	private interface KeyExtractor {

		byte[] extract(KeyStore store) throws Exception;

	}

	private static Path exportPem(Path keystore, String label, KeyExtractor extractor) throws Exception {
		KeyStore store = KeyStore.getInstance("PKCS12");
		try (InputStream in = Files.newInputStream(keystore)) {
			store.load(in, "changeit".toCharArray());
		}
		String pem = "-----BEGIN " + label + "-----\n"
				+ Base64.getMimeEncoder(64, new byte[] { '\n' }).encodeToString(extractor.extract(store)) + "\n"
				+ "-----END " + label + "-----\n";
		Path pemFile = Files.createTempFile("sluice-grpc-tls-", ".pem");
		Files.writeString(pemFile, pem);
		pemFile.toFile().deleteOnExit();
		return pemFile;
	}

	@AfterEach
	void stopApps() {
		closeContext(this.clientContext);
		this.clientContext = null;
		for (int i = 0; i < this.servers.length; i++) {
			closeContext(this.servers[i]);
			this.servers[i] = null;
		}
	}

	@AfterAll
	void stopAll() {
		closeContext(this.clientContext);
		for (ConfigurableApplicationContext server : this.servers) {
			closeContext(server);
		}
		closeServer(this.httpUpstream);
	}

	private void startUpstreams() throws Exception {
		this.httpUpstream = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		this.httpUpstream.createContext("/", exchange -> {
			byte[] body = "cluster-tls-ok".getBytes(StandardCharsets.UTF_8);
			exchange.sendResponseHeaders(200, body.length);
			try (OutputStream out = exchange.getResponseBody()) {
				out.write(body);
			}
		});
		this.httpUpstream.start();
		this.httpPort = this.httpUpstream.getAddress().getPort();
	}

	private void startServer(int index) {
		String membership = this.nodeNames[0] + "=grpcs://127.0.0.1:" + this.grpcPorts[0] + "," + this.nodeNames[1]
				+ "=grpcs://127.0.0.1:" + this.grpcPorts[1];
		// @formatter:off
		ConfigurableApplicationContext context = new SpringApplicationBuilder(SluiceServerApplication.class).run(
				"--server.port=" + TestPorts.freePort(), "--spring.grpc.server.port=" + this.grpcPorts[index],
				"--sluice.data-port=" + this.dataPorts[index], "--sluice.token=" + TOKEN,
				"--sluice.node.id=" + this.nodeNames[index], "--sluice.cluster.nodes=" + membership,
				"--sluice.cluster.warmup=1s", "--spring.grpc.server.ssl.bundle=" + GRPC_TLS_BUNDLE,
				"--spring.ssl.bundle.pem." + GRPC_TLS_BUNDLE + ".keystore.certificate=file:" + certPemPath,
				"--spring.ssl.bundle.pem." + GRPC_TLS_BUNDLE + ".keystore.private-key=file:" + keyPemPath);
		// @formatter:on
		this.servers[index] = context;
	}

	private void startClient() {
		// @formatter:off
		ConfigurableApplicationContext context = new SpringApplicationBuilder(SluiceClientApplication.class).run(
				"--sluice.server-url=grpcs://127.0.0.1:" + this.grpcPorts[0], "--sluice.insecure=true",
				"--sluice.client.id=it-client",
				"--sluice.client.upstream[0].host=" + HOST,
				"--sluice.client.upstream[0].target=http://127.0.0.1:" + this.httpPort,
				"--sluice.token=" + TOKEN, "--server.port=0", "--management.server.port=0");
		// @formatter:on
		this.clientContext = context;
	}

	private static void closeContext(@org.jspecify.annotations.Nullable ConfigurableApplicationContext context) {
		if (context != null) {
			context.close();
		}
	}

	private static void closeServer(@org.jspecify.annotations.Nullable HttpServer server) {
		if (server != null) {
			server.stop(0);
		}
	}

	private long sessionCount(int index) {
		ConfigurableApplicationContext server = this.servers[index];
		return server == null ? -1 : server.getBean(SessionRegistry.class).count();
	}

	private TunnelClient tunnelClient() {
		return Objects.requireNonNull(this.clientContext).getBean(TunnelClient.class);
	}

	/** One HTTP round trip through the data plane of the given node. */
	private String httpRoundTrip(int index) throws Exception {
		try (Socket socket = new Socket("127.0.0.1", this.dataPorts[index])) {
			socket.setSoTimeout(10_000);
			OutputStream out = socket.getOutputStream();
			out.write(("GET / HTTP/1.1\r\nHost: " + HOST + "\r\nConnection: close\r\n\r\n")
				.getBytes(StandardCharsets.US_ASCII));
			out.flush();
			return new String(socket.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
		}
	}

	private void awaitSessionOnAllNodes(int nodes, Duration timeout) {
		Awaitility.await().atMost(timeout).until(() -> {
			for (int i = 0; i < nodes; i++) {
				if (this.servers[i] == null || sessionCount(i) != 1) {
					return false;
				}
			}
			return true;
		});
	}

	@Test
	void bothNodesServeOverTlsAndClientFailsOver() throws Exception {
		startUpstreams();
		for (int i = 0; i < this.grpcPorts.length; i++) {
			this.grpcPorts[i] = TestPorts.freePort();
			this.dataPorts[i] = TestPorts.freePort();
		}
		startServer(0);
		startServer(1);
		startClient();
		this.awaitSessionOnAllNodes(2, Duration.ofSeconds(20));

		// the tunnel serves through both TLS control planes
		assertThat(this.httpRoundTrip(0)).contains("cluster-tls-ok");
		assertThat(this.httpRoundTrip(1)).contains("cluster-tls-ok");

		// node failure: the remaining node keeps serving, the client stays up
		closeContext(this.servers[1]);
		this.servers[1] = null;
		assertThat(this.tunnelClient().isConnected()).isTrue();
		assertThat(this.httpRoundTrip(0)).contains("cluster-tls-ok");
	}

	@Test
	void controlPlaneNegotiatesH2OverTls() throws Exception {
		startUpstreams();
		for (int i = 0; i < this.grpcPorts.length; i++) {
			this.grpcPorts[i] = TestPorts.freePort();
			this.dataPorts[i] = TestPorts.freePort();
		}
		startServer(0);
		// the server cert is the trust anchor: the TLS endpoint really is the gRPC port
		KeyStore trustStore = KeyStore.getInstance("PKCS12");
		try (InputStream in = Files.newInputStream(keystorePath)) {
			trustStore.load(in, "changeit".toCharArray());
		}
		TrustManagerFactory trustManagerFactory = TrustManagerFactory
			.getInstance(TrustManagerFactory.getDefaultAlgorithm());
		trustManagerFactory.init(trustStore);
		SSLContext sslContext = SSLContext.getInstance("TLS");
		sslContext.init(null, trustManagerFactory.getTrustManagers(), null);
		try (SSLSocket socket = (SSLSocket) sslContext.getSocketFactory()
			.createSocket("127.0.0.1", this.grpcPorts[0])) {
			socket.setSoTimeout(10_000);
			javax.net.ssl.SSLParameters parameters = socket.getSSLParameters();
			parameters.setApplicationProtocols(new String[] { "h2" });
			socket.setSSLParameters(parameters);
			socket.startHandshake();
			assertThat(socket.getApplicationProtocol()).isEqualTo("h2");
		}
	}

}
