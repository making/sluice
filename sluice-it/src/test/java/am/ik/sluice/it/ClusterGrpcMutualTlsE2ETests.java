package am.ik.sluice.it;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.interfaces.RSAPrivateCrtKey;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SNIServerName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.StandardConstants;
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
 * Full stack mutual TLS test: both control planes require a client certificate
 * ({@code client-auth=REQUIRE}) and the client authenticates through an SSL bundle
 * ({@code sluice.tls-bundle}) instead of {@code sluice.insecure}. Also covers the data
 * plane over the mTLS tunnel, once with TLS termination at the data plane and once with
 * TLS passthrough to the upstream.
 */
@TestInstance(Lifecycle.PER_CLASS)
class ClusterGrpcMutualTlsE2ETests {

	private static final String HOST = "mtls.local";

	private static final String PASSTHRU_HOST = "passthru.mtls.local";

	private static final String TOKEN = "cluster-mtls-it-token";

	private static final String SERVER_BUNDLE = "grpc-control";

	private static final String CLIENT_BUNDLE = "grpc-client";

	private static final String DATA_BUNDLE = "data-plane";

	private static final char[] STORE_PASS = "changeit".toCharArray();

	private static Path caPemPath;

	private static Path serverPemPath;

	private static Path serverKeyPemPath;

	private static final Path[] clientPemPaths = new Path[2];

	private static final Path[] clientKeyPemPaths = new Path[2];

	private static KeyStore caStore;

	private static KeyStore upstreamStore;

	private static final ExecutorService UPSTREAM_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

	private final int[] grpcPorts = new int[2];

	private final int[] dataPorts = new int[2];

	private final @org.jspecify.annotations.Nullable ConfigurableApplicationContext[] servers = new ConfigurableApplicationContext[2];

	private final @org.jspecify.annotations.Nullable ConfigurableApplicationContext[] clients = new ConfigurableApplicationContext[2];

	private final List<HttpServer> plainUpstreams = new ArrayList<>();

	private @org.jspecify.annotations.Nullable ServerSocket tlsUpstream;

	private int tlsUpstreamPort;

	private final String[] nodeNames = { "node-1", "node-2" };

	@BeforeAll
	static void createCertificates() throws Exception {
		Path caStorePath = keyStore("sluice-mtls-ca", "ca");
		// the CA is a self-signed pair marked as a certificate authority
		keytool("-genkeypair", "-alias", "ca", "-keyalg", "RSA", "-keysize", "2048", "-storetype", "PKCS12",
				"-storepass", "changeit", "-dname", "CN=sluice-it-ca", "-ext", "bc:c", "-validity", "1", "-keystore",
				caStorePath.toAbsolutePath().toString());
		Path serverStorePath = keyStore("sluice-mtls-server", "server");
		keytool("-genkeypair", "-alias", "server", "-keyalg", "RSA", "-keysize", "2048", "-storetype", "PKCS12",
				"-storepass", "changeit", "-dname", "CN=localhost", "-ext", "san=dns:localhost,ip:127.0.0.1",
				"-validity", "1", "-keystore", serverStorePath.toAbsolutePath().toString());
		signWithCa(caStorePath, serverStorePath, "server", "san=dns:localhost,ip:127.0.0.1");
		// one certificate per client identity
		for (int i = 0; i < 2; i++) {
			String alias = "client-" + (i + 1);
			Path clientStorePath = keyStore("sluice-mtls-" + alias, alias);
			keytool("-genkeypair", "-alias", alias, "-keyalg", "RSA", "-keysize", "2048", "-storetype", "PKCS12",
					"-storepass", "changeit", "-dname", "CN=" + alias, "-validity", "1", "-keystore",
					clientStorePath.toAbsolutePath().toString());
			signWithCa(caStorePath, clientStorePath, alias, null);
			clientPemPaths[i] = exportPem(loadStore(clientStorePath, alias), alias, "CERTIFICATE",
					store -> store.getCertificate(alias).getEncoded());
			clientKeyPemPaths[i] = exportPem(loadStore(clientStorePath, alias), alias, "PRIVATE KEY",
					store -> ((RSAPrivateCrtKey) store.getKey(alias, STORE_PASS)).getEncoded());
		}
		// the upstream store doubles as trust anchor and upstream keystore (passthrough)
		Path upstreamStorePath = keyStore("sluice-mtls-upstream", "upstream");
		keytool("-genkeypair", "-alias", "upstream", "-keyalg", "RSA", "-keysize", "2048", "-storetype", "PKCS12",
				"-storepass", "changeit", "-dname", "CN=upstream", "-ext", "san=dns:" + PASSTHRU_HOST, "-validity", "1",
				"-keystore", upstreamStorePath.toAbsolutePath().toString());
		upstreamStore = loadStore(upstreamStorePath, "upstream");
		caStore = loadStore(caStorePath, "ca");
		caPemPath = exportPem(caStore, "ca", "CERTIFICATE", store -> store.getCertificate("ca").getEncoded());
		serverPemPath = exportPem(loadStore(serverStorePath, "server"), "server", "CERTIFICATE",
				store -> store.getCertificate("server").getEncoded());
		serverKeyPemPath = exportPem(loadStore(serverStorePath, "server"), "server", "PRIVATE KEY",
				store -> ((RSAPrivateCrtKey) store.getKey("server", STORE_PASS)).getEncoded());
	}

	/**
	 * Has the CA of the given CA store sign the pending CSR of the alias in the store.
	 */
	private static void signWithCa(Path caStorePath, Path storePath, String alias,
			@org.jspecify.annotations.Nullable String extensions) throws Exception {
		Path csr = Files.createTempFile("sluice-mtls-" + alias, ".csr");
		Files.delete(csr);
		keytool("-certreq", "-alias", alias, "-storetype", "PKCS12", "-storepass", "changeit", "-keystore",
				storePath.toAbsolutePath().toString(), "-file", csr.toAbsolutePath().toString());
		Path signed = Files.createTempFile("sluice-mtls-" + alias, ".cer");
		Files.delete(signed);
		Path caCert = Files.createTempFile("sluice-mtls-ca-cert", ".cer");
		Files.delete(caCert);
		String[] suffix = extensions == null ? new String[0] : new String[] { "-ext", extensions };
		List<String> gencert = new ArrayList<>(List.of("-gencert", "-alias", "ca", "-storetype", "PKCS12", "-storepass",
				"changeit", "-keystore", caStorePath.toAbsolutePath().toString(), "-infile",
				csr.toAbsolutePath().toString(), "-outfile", signed.toAbsolutePath().toString()));
		gencert.addAll(List.of(suffix));
		keytool(gencert.toArray(String[]::new));
		// the CA cert must be a trusted entry before the signed reply can be imported
		keytool("-exportcert", "-alias", "ca", "-storetype", "PKCS12", "-storepass", "changeit", "-keystore",
				caStorePath.toAbsolutePath().toString(), "-file", caCert.toAbsolutePath().toString());
		keytool("-importcert", "-alias", "ca", "-storetype", "PKCS12", "-storepass", "changeit", "-noprompt",
				"-keystore", storePath.toAbsolutePath().toString(), "-file", caCert.toAbsolutePath().toString());
		keytool("-importcert", "-alias", alias, "-storetype", "PKCS12", "-storepass", "changeit", "-noprompt",
				"-keystore", storePath.toAbsolutePath().toString(), "-file", signed.toAbsolutePath().toString());
		csr.toFile().deleteOnExit();
		signed.toFile().deleteOnExit();
	}

	private static Path keyStore(String name, String alias) throws Exception {
		// seed the store with a self-signed pair; the cert is replaced by the CA signed
		// one for the server / client aliases
		Path keystore = Files.createTempFile(name, ".p12");
		Files.delete(keystore);
		keystore.toFile().deleteOnExit();
		return keystore;
	}

	private static void keytool(String... args) throws Exception {
		List<String> command = new ArrayList<>();
		command.add(Path.of(System.getProperty("java.home"), "bin", "keytool").toString());
		command.addAll(List.of(args));
		Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
		if (!process.waitFor(60, TimeUnit.SECONDS) || process.exitValue() != 0) {
			throw new IllegalStateException(
					"keytool failed: " + new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8));
		}
	}

	private static KeyStore loadStore(Path path, String alias) throws Exception {
		KeyStore store = KeyStore.getInstance("PKCS12");
		try (InputStream in = Files.newInputStream(path)) {
			store.load(in, STORE_PASS);
		}
		return store;
	}

	private interface KeyExtractor {

		byte[] extract(KeyStore store) throws Exception;

	}

	private static Path exportPem(KeyStore store, String alias, String label, KeyExtractor extractor) throws Exception {
		String pem = "-----BEGIN " + label + "-----\n"
				+ Base64.getMimeEncoder(64, new byte[] { '\n' }).encodeToString(extractor.extract(store)) + "\n"
				+ "-----END " + label + "-----\n";
		Path pemFile = Files.createTempFile("sluice-mtls-" + alias + "-", ".pem");
		Files.writeString(pemFile, pem);
		pemFile.toFile().deleteOnExit();
		return pemFile;
	}

	@AfterEach
	void stopApps() {
		for (int i = 0; i < this.clients.length; i++) {
			closeContext(this.clients[i]);
			this.clients[i] = null;
		}
		for (int i = 0; i < this.servers.length; i++) {
			closeContext(this.servers[i]);
			this.servers[i] = null;
		}
		for (HttpServer upstream : this.plainUpstreams) {
			upstream.stop(0);
		}
		this.plainUpstreams.clear();
		if (this.tlsUpstream != null) {
			try {
				this.tlsUpstream.close();
			}
			catch (Exception e) {
				// ignore
			}
			this.tlsUpstream = null;
		}
	}

	@AfterAll
	static void stopExecutor() {
		UPSTREAM_EXECUTOR.shutdown();
	}

	/** Plain HTTP upstream; the caller keeps the returned port. */
	private int startPlainUpstream(String body) throws Exception {
		HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		server.createContext("/", exchange -> {
			byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
			exchange.getResponseHeaders().add("Content-Type", "text/plain");
			exchange.sendResponseHeaders(200, bytes.length);
			try (OutputStream out = exchange.getResponseBody()) {
				out.write(bytes);
			}
		});
		server.start();
		this.plainUpstreams.add(server);
		return server.getAddress().getPort();
	}

	/** TLS echo upstream for the passthrough case: answers one line per received line. */
	private void startTlsUpstream(String prefix) throws Exception {
		KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
		keyManagerFactory.init(upstreamStore, STORE_PASS);
		SSLContext sslContext = sslContext(keyManagerFactory, null);
		SSLServerSocket listener = (SSLServerSocket) sslContext.getServerSocketFactory().createServerSocket();
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
							StringBuilder line = new StringBuilder();
							int n;
							while ((n = in.read(buffer)) > 0) {
								for (int i = 0; i < n; i++) {
									if (buffer[i] == '\n') {
										line.append('\n');
										socket.getOutputStream()
											.write((prefix + line).getBytes(StandardCharsets.US_ASCII));
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
		this.tlsUpstream = listener;
		this.tlsUpstreamPort = listener.getLocalPort();
	}

	private static TrustManagerFactory trustManagerFactory(KeyStore store) throws Exception {
		TrustManagerFactory trustManagerFactory = TrustManagerFactory
			.getInstance(TrustManagerFactory.getDefaultAlgorithm());
		trustManagerFactory.init(store);
		return trustManagerFactory;
	}

	private static SSLContext sslContext(@org.jspecify.annotations.Nullable KeyManagerFactory keyManagerFactory,
			@org.jspecify.annotations.Nullable TrustManagerFactory trustManagerFactory) throws Exception {
		SSLContext sslContext = SSLContext.getInstance("TLS");
		sslContext.init(keyManagerFactory == null ? null : keyManagerFactory.getKeyManagers(),
				trustManagerFactory == null ? null : trustManagerFactory.getTrustManagers(), null);
		return sslContext;
	}

	private void startServer(int index, String... extraArgs) {
		String membership = this.nodeNames[0] + "=grpcs://127.0.0.1:" + this.grpcPorts[0] + "," + this.nodeNames[1]
				+ "=grpcs://127.0.0.1:" + this.grpcPorts[1];
		// @formatter:off
		String[] args = new String[] {
				"--server.port=" + TestPorts.freePort(), "--spring.grpc.server.port=" + this.grpcPorts[index],
				"--sluice.data-port=" + this.dataPorts[index], "--sluice.token=" + TOKEN,
				"--sluice.node.id=" + this.nodeNames[index], "--sluice.cluster.nodes=" + membership,
				"--sluice.cluster.warmup=1s", "--spring.grpc.server.ssl.bundle=" + SERVER_BUNDLE,
				"--spring.grpc.server.ssl.client-auth=REQUIRE",
				"--spring.ssl.bundle.pem." + SERVER_BUNDLE + ".keystore.certificate=file:" + serverPemPath,
				"--spring.ssl.bundle.pem." + SERVER_BUNDLE + ".keystore.private-key=file:" + serverKeyPemPath,
				"--spring.ssl.bundle.pem." + SERVER_BUNDLE + ".truststore.certificate=file:" + caPemPath };
		// @formatter:on
		String[] all = new String[args.length + extraArgs.length];
		System.arraycopy(args, 0, all, 0, args.length);
		System.arraycopy(extraArgs, 0, all, args.length, extraArgs.length);
		this.servers[index] = new SpringApplicationBuilder(SluiceServerApplication.class).run(all);
	}

	/**
	 * Starts a client that authenticates with the certificate of its own identity
	 * (1-based).
	 */
	private void startClient(int identity, String... extraArgs) {
		// @formatter:off
		String[] args = new String[] { "--sluice.server-url=grpcs://127.0.0.1:" + this.grpcPorts[0],
				"--sluice.tls-bundle=" + CLIENT_BUNDLE, "--sluice.client.id=mtls-it-client-" + identity,
				"--spring.ssl.bundle.pem." + CLIENT_BUNDLE + ".keystore.certificate=file:" + clientPemPaths[identity - 1],
				"--spring.ssl.bundle.pem." + CLIENT_BUNDLE + ".keystore.private-key=file:" + clientKeyPemPaths[identity - 1],
				"--spring.ssl.bundle.pem." + CLIENT_BUNDLE + ".truststore.certificate=file:" + caPemPath,
				"--sluice.token=" + TOKEN, "--server.port=0" };
		// @formatter:on
		String[] all = new String[args.length + extraArgs.length];
		System.arraycopy(args, 0, all, 0, args.length);
		System.arraycopy(extraArgs, 0, all, args.length, extraArgs.length);
		int slot = identity - 1;
		this.clients[slot] = new SpringApplicationBuilder(SluiceClientApplication.class).run(all);
	}

	/**
	 * A client context without the SSL bundle: trusts nobody, presents no certificate.
	 */
	private void startUnbundledClient(String... extraArgs) {
		// @formatter:off
		String[] args = new String[] { "--sluice.server-url=grpcs://127.0.0.1:" + this.grpcPorts[0],
				"--sluice.client.id=mtls-it-unbundled", "--sluice.token=" + TOKEN, "--server.port=0" };
		// @formatter:on
		String[] all = new String[args.length + extraArgs.length];
		System.arraycopy(args, 0, all, 0, args.length);
		System.arraycopy(extraArgs, 0, all, args.length, extraArgs.length);
		this.clients[0] = new SpringApplicationBuilder(SluiceClientApplication.class).run(all);
	}

	private static void closeContext(@org.jspecify.annotations.Nullable ConfigurableApplicationContext context) {
		if (context != null) {
			context.close();
		}
	}

	private long sessionCount(int index) {
		ConfigurableApplicationContext server = this.servers[index];
		return server == null ? -1 : server.getBean(SessionRegistry.class).count();
	}

	private TunnelClient tunnelClient(int identity) {
		return Objects.requireNonNull(this.clients[identity - 1]).getBean(TunnelClient.class);
	}

	/** One plain HTTP round trip through the data plane of the given node. */
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
	void bothNodesRequireAndAcceptTheClientCertificate() throws Exception {
		for (int i = 0; i < this.grpcPorts.length; i++) {
			this.grpcPorts[i] = TestPorts.freePort();
			this.dataPorts[i] = TestPorts.freePort();
		}
		int upstreamPort = this.startPlainUpstream("mtls-tunnel-ok");
		this.startServer(0);
		this.startServer(1);
		this.startClient(1, "--sluice.client.upstream[0].host=" + HOST,
				"--sluice.client.upstream[0].target=http://127.0.0.1:" + upstreamPort);
		this.awaitSessionOnAllNodes(2, Duration.ofSeconds(20));
		assertThat(this.httpRoundTrip(0, HOST)).contains("mtls-tunnel-ok");
		assertThat(this.httpRoundTrip(1, HOST)).contains("mtls-tunnel-ok");
		// node failure: the remaining node keeps serving over mTLS
		closeContext(this.servers[1]);
		this.servers[1] = null;
		assertThat(this.tunnelClient(1).isConnected()).isTrue();
		assertThat(this.httpRoundTrip(0, HOST)).contains("mtls-tunnel-ok");
	}

	@Test
	void clientsWithDistinctCertificatesTunnelSideBySide() throws Exception {
		this.grpcPorts[0] = TestPorts.freePort();
		this.dataPorts[0] = TestPorts.freePort();
		int upstream1 = this.startPlainUpstream("mtls-client1-ok");
		int upstream2 = this.startPlainUpstream("mtls-client2-ok");
		this.startServer(0);
		// each client authenticates with its own certificate
		this.startClient(1, "--sluice.client.upstream[0].host=client1." + HOST,
				"--sluice.client.upstream[0].target=http://127.0.0.1:" + upstream1);
		this.startClient(2, "--sluice.client.upstream[0].host=client2." + HOST,
				"--sluice.client.upstream[0].target=http://127.0.0.1:" + upstream2);
		Awaitility.await().atMost(Duration.ofSeconds(20)).until(() -> sessionCount(0) == 2);
		assertThat(this.httpRoundTrip(0, "client1." + HOST)).contains("mtls-client1-ok");
		assertThat(this.httpRoundTrip(0, "client2." + HOST)).contains("mtls-client2-ok");
		// one client leaving does not disturb the other
		closeContext(this.clients[0]);
		this.clients[0] = null;
		Awaitility.await().atMost(Duration.ofSeconds(20)).until(() -> sessionCount(0) == 1);
		assertThat(this.tunnelClient(2).isConnected()).isTrue();
		assertThat(this.httpRoundTrip(0, "client2." + HOST)).contains("mtls-client2-ok");
	}

	@Test
	void clientWithoutCertificateIsRejected() throws Exception {
		this.grpcPorts[0] = TestPorts.freePort();
		this.dataPorts[0] = TestPorts.freePort();
		this.startServer(0);
		this.startUnbundledClient("--sluice.client.upstream[0].host=" + HOST,
				"--sluice.client.upstream[0].target=http://127.0.0.1:1");
		// no truststore and no client certificate: the handshake against the REQUIRE
		// endpoint must not succeed within the window
		Awaitility.await()
			.during(3, TimeUnit.SECONDS)
			.atMost(10, TimeUnit.SECONDS)
			.until(() -> sessionCount(0) == 0 && !this.tunnelClient(1).isConnected());
	}

	@Test
	void dataPlaneTerminatesTlsOverTheMutualTlsTunnel() throws Exception {
		this.grpcPorts[0] = TestPorts.freePort();
		this.dataPorts[0] = TestPorts.freePort();
		int upstreamPort = this.startPlainUpstream("mtls-tls-termination-ok");
		// the data plane terminates TLS with the same server certificate
		this.startServer(0, "--sluice.data-tls-bundle=" + DATA_BUNDLE,
				"--spring.ssl.bundle.pem." + DATA_BUNDLE + ".keystore.certificate=file:" + serverPemPath,
				"--spring.ssl.bundle.pem." + DATA_BUNDLE + ".keystore.private-key=file:" + serverKeyPemPath);
		this.startClient(2, "--sluice.client.upstream[0].host=localhost",
				"--sluice.client.upstream[0].target=http://127.0.0.1:" + upstreamPort);
		Awaitility.await().atMost(Duration.ofSeconds(20)).until(() -> sessionCount(0) == 1);
		javax.net.ssl.SSLSocketFactory socketFactory = sslContext(null, trustManagerFactory(caStore))
			.getSocketFactory();
		try (SSLSocket socket = (SSLSocket) socketFactory.createSocket("127.0.0.1", this.dataPorts[0])) {
			socket.setSoTimeout(10_000);
			socket.startHandshake();
			socket.getOutputStream()
				.write("GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
					.getBytes(StandardCharsets.US_ASCII));
			socket.getOutputStream().flush();
			String response = new String(socket.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
			assertThat(response).startsWith("HTTP/1.1 200").contains("mtls-tls-termination-ok");
		}
	}

	@Test
	void dataPlanePassesTlsThroughToTheUpstreamOverTheMutualTlsTunnel() throws Exception {
		this.grpcPorts[0] = TestPorts.freePort();
		this.dataPorts[0] = TestPorts.freePort();
		this.startTlsUpstream("mtls-passthru:");
		this.startServer(0);
		this.startClient(1, "--sluice.client.upstream[0].host=" + PASSTHRU_HOST,
				"--sluice.client.upstream[0].target=tcp://127.0.0.1:" + this.tlsUpstreamPort,
				"--sluice.client.upstream[0].tls-passthrough=true");
		Awaitility.await().atMost(Duration.ofSeconds(20)).until(() -> sessionCount(0) == 1);
		// TLS is terminated by the upstream; the data plane only relays bytes. The
		// upstream certificate is validated, SNI selects the passthrough route.
		javax.net.ssl.SSLSocketFactory socketFactory = sslContext(null, trustManagerFactory(upstreamStore))
			.getSocketFactory();
		try (SSLSocket socket = (SSLSocket) socketFactory.createSocket("127.0.0.1", this.dataPorts[0])) {
			socket.setSoTimeout(10_000);
			SSLParameters parameters = socket.getSSLParameters();
			List<SNIServerName> serverNames = List.of(new SNIHostName(PASSTHRU_HOST));
			parameters.setServerNames(serverNames);
			socket.setSSLParameters(parameters);
			socket.startHandshake();
			socket.getOutputStream().write("ping\n".getBytes(StandardCharsets.US_ASCII));
			socket.getOutputStream().flush();
			String echoed = new String(socket.getInputStream().readNBytes("mtls-passthru:ping\n".length()),
					StandardCharsets.US_ASCII);
			assertThat(echoed).isEqualTo("mtls-passthru:ping\n");
		}
	}

}
