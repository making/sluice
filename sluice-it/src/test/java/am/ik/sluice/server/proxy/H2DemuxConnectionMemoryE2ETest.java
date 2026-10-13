package am.ik.sluice.server.proxy;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;

import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.client.config.Upstream;
import am.ik.sluice.client.tunnel.TunnelClient;
import am.ik.sluice.it.H2TestClient;
import am.ik.sluice.it.TestH2;
import am.ik.sluice.it.TestPorts;
import am.ik.sluice.it.WindowedH2Upstream;
import am.ik.sluice.server.SluiceServerApplication;
import am.ik.sluice.server.route.Router;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The bytes the data plane's connections reserve: with {@code connection-memory} holding
 * four h2 connections and one more connection's virtual connection, the four relays --
 * two whose clients do not read their downloads, two whose upstream does not read their
 * uploads -- hold no more than the queue budget and what their connections reserve
 * outside it, though without the windows withheld they would hold far more. A fifth h2
 * connection is admitted but not relayed as h2: in plaintext, or over TLS offering h2
 * alone, it gets a GOAWAY processing no stream; over TLS offering HTTP/1.1 too it gets
 * HTTP/1.1 and is served; a connection past what is left is answered 503. In the relay's
 * package for the relay's constants and the limits' accounts.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class H2DemuxConnectionMemoryE2ETest {

	private static final String DOWNLOAD_HOST = "memory.download.test";

	private static final String UPLOAD_HOST = "memory.upload.test";

	private static final String H1_HOST = "memory.h1.test";

	private static final String H1_BODY = "served-over-http1";

	/** h2 connections the memory holds, besides one more connection's own bytes. */
	private static final int H2_CONNECTIONS = 4;

	private static final long MEMORY = H2_CONNECTIONS
			* (ConnectionLimits.connectionBytes() + ConnectionLimits.h2RelayBytes())
			+ ConnectionLimits.connectionBytes();

	/** Far below what the connections hold without their windows withheld. */
	private static final long BUDGET = 2L * 1024 * 1024;

	/** Streams per connection, each moving a stream window: 32MiB in all. */
	private static final int STREAMS = 8;

	private static final int BODY = NettyHttp2DemuxRelay.STREAM_WINDOW;

	private static final int MAX_FRAME_SIZE = 16 * 1024;

	private static final Duration TIMEOUT = Duration.ofSeconds(10);

	private static final ExecutorService UPSTREAM_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

	private final List<AutoCloseable> closeables = new CopyOnWriteArrayList<>();

	private final List<TunnelClient> clients = new ArrayList<>();

	private static int grpcPort;

	private static int dataPort;

	private static @Nullable Path keystorePath;

	@Nullable
	@Autowired
	private Router router;

	@Nullable
	@Autowired
	private MeterRegistry meterRegistry;

	@Nullable
	@Autowired
	private ConnectionLimits limits;

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) throws Exception {
		grpcPort = TestPorts.freePort();
		dataPort = TestPorts.freePort();
		keystorePath = createKeystore();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(dataPort));
		registry.add("sluice.token", () -> "it-token");
		registry.add("sluice.access-log.rate-limit.enabled", () -> "false");
		registry.add("sluice.limits.h2-queue-budget", () -> BUDGET + "B");
		registry.add("sluice.limits.connection-memory", () -> MEMORY + "B");
		registry.add("server.port", () -> String.valueOf(TestPorts.freePort()));
		registry.add("spring.ssl.bundle.jks.data-plane.keystore.location",
				() -> "file:" + Objects.requireNonNull(keystorePath).toAbsolutePath());
		registry.add("spring.ssl.bundle.jks.data-plane.keystore.password", () -> "changeit");
		registry.add("sluice.data-tls-bundle", () -> "data-plane");
	}

	@BeforeAll
	void startClients() {
		startClient(DOWNLOAD_HOST, stub(H2DemuxConnectionMemoryE2ETest::serveResponses));
		startClient(UPLOAD_HOST, stub(H2DemuxConnectionMemoryE2ETest::holdRequests));
		startClient(H1_HOST, stub(H2DemuxConnectionMemoryE2ETest::serveH1));
	}

	@BeforeEach
	void awaitReleased() {
		Awaitility.await().atMost(TIMEOUT).untilAsserted(() -> {
			assertThat(limits().reserved()).as("reserved").isZero();
			assertThat(held()).as("held").isZero();
		});
	}

	@AfterAll
	void tearDown() {
		this.clients.forEach(TunnelClient::stop);
		this.closeables.forEach(H2DemuxConnectionMemoryE2ETest::close);
		UPSTREAM_EXECUTOR.shutdown();
	}

	/**
	 * Four h2 connections fill the memory: each reserves its virtual connection and its
	 * relay's part outside the budget. Two clients download eight stream windows each and
	 * grant no connection window; two upload eight stream windows each to an upstream
	 * granting no stream window. What the relays hold settles within the budget plus what
	 * the four reserve outside it -- the bound the derived limit rests on -- and past the
	 * budget, so the reservations are what bounds it. The connections past them: a fifth
	 * h2 one in plaintext and one over TLS offering h2 alone are refused with a GOAWAY
	 * naming no stream, one over TLS offering HTTP/1.1 too is served over HTTP/1.1 and,
	 * while it is open, one more is answered 503 by the overflow. Every byte is released
	 * once they closed.
	 */
	@Test
	void connectionsAtTheMemoryHoldNoMoreThanTheBudgetAndWhatTheyReserve() throws Exception {
		assertThat(limits().memory()).as("the configured memory took effect").isEqualTo(MEMORY);
		assertThat(limits().h2Budget().max()).as("the configured budget took effect").isEqualTo(BUDGET);
		double rejectedH2 = rejected("h2-limit");
		double rejectedLimit = rejected("limit");
		double declined = declined();
		List<H2TestClient> downloads = new ArrayList<>();
		List<Pump> uploads = new ArrayList<>();
		try {
			for (int i = 0; i < H2_CONNECTIONS / 2; i++) {
				downloads.add(download());
				uploads.add(upload());
			}
			Awaitility.await()
				.atMost(TIMEOUT)
				.untilAsserted(() -> assertThat(limits().reserved()).as("each connection reserved its relay")
					.isEqualTo(
							H2_CONNECTIONS * (ConnectionLimits.connectionBytes() + ConnectionLimits.h2RelayBytes())));
			uploads.forEach(Pump::start);
			long held = awaitStable(this::held);
			long bound = BUDGET + H2_CONNECTIONS * ConnectionLimits.h2RelayBytes();
			assertThat(held).as("held: past the budget, within the budget and what the relays reserve outside it")
				.isGreaterThan(BUDGET)
				.isLessThanOrEqualTo(bound);
			assertThat((long) H2_CONNECTIONS * STREAMS * BODY).as("what the streams move: twice the bound")
				.isGreaterThan(2 * bound);
			for (H2TestClient download : downloads) {
				assertThat(download.closed()).as("a download held, not ended").isFalse();
			}

			try (H2TestClient plaintext = H2TestClient.connect(dataPort)) {
				plaintext.headers(1, true, head(DOWNLOAD_HOST, "GET", "/"));
				assertRefused(plaintext);
				assertThat(plaintext.awaitTrue(plaintext::closed, TIMEOUT)).as("then closed").isTrue();
			}
			assertThat(rejected("h2-limit")).as("refused for the relay's bytes").isEqualTo(rejectedH2 + 1);
			try (SSLSocket socket = tls(DOWNLOAD_HOST, "h2")) {
				assertThat(socket.getApplicationProtocol()).as("h2 alone offered").isEqualTo("h2");
				H2TestClient h2Only = H2TestClient.over(socket);
				h2Only.headers(1, true, head(DOWNLOAD_HOST, "GET", "/"));
				assertRefused(h2Only);
			}
			assertThat(rejected("h2-limit")).isEqualTo(rejectedH2 + 2);
			assertThat(declined()).as("no HTTP/1.1 offered to fall back to").isEqualTo(declined);

			try (SSLSocket socket = tls(H1_HOST, "h2", "http/1.1")) {
				assertThat(socket.getApplicationProtocol()).as("h2 declined for HTTP/1.1").isEqualTo("http/1.1");
				assertThat(declined()).isEqualTo(declined + 1);
				Awaitility.await()
					.atMost(TIMEOUT)
					.untilAsserted(() -> assertThat(limits().reserved()).as("the memory is full").isEqualTo(MEMORY));
				try (Socket past = new Socket("127.0.0.1", dataPort)) {
					past.setSoTimeout((int) TIMEOUT.toMillis());
					past.getOutputStream()
						.write(("GET / HTTP/1.1\r\nHost: " + H1_HOST + "\r\n\r\n").getBytes(StandardCharsets.US_ASCII));
					String answer = new String(past.getInputStream().readNBytes(32), StandardCharsets.US_ASCII);
					assertThat(answer).as("answered by the overflow").startsWith("HTTP/1.1 503 Service Unavailable");
				}
				assertThat(rejected("limit")).isEqualTo(rejectedLimit + 1);
				OutputStream out = socket.getOutputStream();
				out.write(("GET / HTTP/1.1\r\nHost: " + H1_HOST + "\r\n\r\n").getBytes(StandardCharsets.US_ASCII));
				out.flush();
				String response = new String(socket.getInputStream().readAllBytes(), StandardCharsets.US_ASCII);
				assertThat(response).startsWith("HTTP/1.1 200 OK").endsWith(H1_BODY);
			}
			assertThat(rejected("h2-limit")).isEqualTo(rejectedH2 + 2);
			assertThat(limits().h2Budget().used()).as("the held budget").isPositive();
		}
		finally {
			uploads.forEach(Pump::close);
			downloads.forEach(H2DemuxConnectionMemoryE2ETest::close);
		}
		Awaitility.await().atMost(TIMEOUT).untilAsserted(() -> {
			assertThat(limits().reserved()).as("every reservation released").isZero();
			assertThat(limits().admitted()).as("every slot released").isZero();
			assertThat(held()).as("nothing held").isZero();
			assertThat(limits().h2Budget().used()).as("the budget released").isZero();
		});
	}

	/**
	 * While the bytes of a relay fit, a TLS connection offering h2 and HTTP/1.1 gets h2
	 * and reserves them: the HTTP/1.1 above was for want of them.
	 */
	@Test
	void aTlsConnectionOfferingBothGetsH2WhileItsRelayFits() throws Exception {
		double declined = declined();
		try (SSLSocket socket = tls(DOWNLOAD_HOST, "h2", "http/1.1")) {
			assertThat(socket.getApplicationProtocol()).isEqualTo("h2");
			assertThat(limits().reserved())
				.isEqualTo(ConnectionLimits.connectionBytes() + ConnectionLimits.h2RelayBytes());
		}
		assertThat(declined()).isEqualTo(declined);
	}

	/** A GOAWAY naming no stream processed, error code NO_ERROR, and no response. */
	private static void assertRefused(H2TestClient client) throws InterruptedException {
		assertThat(client.awaitTrue(() -> client.goAwayLastStreamId() != -1, TIMEOUT)).as("a GOAWAY").isTrue();
		assertThat(client.goAwayLastStreamId()).as("no stream processed").isZero();
		assertThat(client.goAwayCode()).as("NO_ERROR").isZero();
		assertThat(client.dataReceived()).isZero();
	}

	/** A client asking for {@link #STREAMS} downloads, granting no connection window. */
	private H2TestClient download() throws IOException {
		H2TestClient client = H2TestClient.connect(dataPort);
		client.withholdConnectionWindow();
		client.frame(TestH2.SETTINGS, 0, 0,
				WindowedH2Upstream.setting(WindowedH2Upstream.SETTINGS_INITIAL_WINDOW_SIZE, BODY));
		for (int i = 0; i < STREAMS; i++) {
			client.headers(streamId(i), true, head(DOWNLOAD_HOST, "GET", "/" + i));
		}
		return client;
	}

	/** A client opening {@link #STREAMS} uploads; the pump sends their bodies. */
	private Pump upload() throws IOException {
		H2TestClient client = H2TestClient.connect(dataPort);
		for (int i = 0; i < STREAMS; i++) {
			client.headers(streamId(i), false, head(UPLOAD_HOST, "POST", "/" + i));
		}
		return new Pump(client);
	}

	private static int streamId(int index) {
		return 2 * index + 1;
	}

	private static byte[] head(String host, String method, String path) {
		return TestH2.headerBlock(":authority=" + host, ":method=" + method, ":path=" + path);
	}

	/** Waits until the value has not grown for a second; returns its maximum. */
	private static long awaitStable(LongSupplier value) throws InterruptedException {
		long max = 0;
		long grownAt = System.nanoTime();
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
		while (System.nanoTime() - grownAt < TimeUnit.SECONDS.toNanos(1)) {
			assertThat(System.nanoTime()).as("the relays settle").isLessThan(deadline);
			long current = value.getAsLong();
			if (current > max) {
				max = current;
				grownAt = System.nanoTime();
			}
			Thread.sleep(10);
		}
		return max;
	}

	private ConnectionLimits limits() {
		return Objects.requireNonNull(this.limits);
	}

	private long held() {
		return Math
			.round(Objects.requireNonNull(this.meterRegistry).get(H2DemuxMeters.HELD_GAUGE_NAME).gauge().value());
	}

	private double rejected(String reason) {
		return Objects.requireNonNull(this.meterRegistry)
			.get("sluice.data.connections.rejected")
			.tag("reason", reason)
			.counter()
			.count();
	}

	private double declined() {
		return Objects.requireNonNull(this.meterRegistry).get("sluice.data.connections.h2.declined").counter().count();
	}

	/** One upload connection and the thread sending its bodies within the windows. */
	private static final class Pump implements AutoCloseable {

		private final H2TestClient client;

		private final long[] remaining = new long[STREAMS];

		private final AtomicBoolean stop = new AtomicBoolean();

		Pump(H2TestClient client) {
			this.client = client;
			Arrays.fill(this.remaining, BODY);
		}

		void start() {
			Thread.ofVirtual().name("e2e-memory-pump").start(this::pump);
		}

		private void pump() {
			try {
				while (!this.stop.get() && !this.client.closed()) {
					boolean progress = false;
					for (int i = 0; i < STREAMS; i++) {
						int id = streamId(i);
						long window = Math.min(this.client.sendable(id), this.client.connectionWindow());
						int n = (int) Math.min(Math.min(this.remaining[i], window), MAX_FRAME_SIZE);
						if (n > 0) {
							this.remaining[i] -= n;
							this.client.sendData(id, new byte[n], this.remaining[i] == 0 ? TestH2.FLAG_END_STREAM : 0);
							progress = true;
						}
					}
					if (!progress) {
						Thread.sleep(1);
					}
				}
			}
			catch (IOException | InterruptedException e) {
				// torn down
			}
		}

		@Override
		public void close() {
			this.stop.set(true);
			H2DemuxConnectionMemoryE2ETest.close(this.client);
		}

	}

	/**
	 * Answers every request with a head and a stream window of DATA, within the windows.
	 */
	private static void serveResponses(Socket socket) {
		try (socket) {
			WindowedH2Upstream upstream = WindowedH2Upstream.accept(socket);
			if (upstream == null) {
				return;
			}
			upstream.settings(null);
			TestH2.@Nullable Frame frame;
			while ((frame = upstream.next()) != null) {
				if (frame.type() == TestH2.HEADERS) {
					int streamId = frame.streamId();
					Thread.ofVirtual().name("e2e-memory-respond").start(() -> {
						try {
							upstream.headers(streamId, false);
							upstream.sendData(streamId, new byte[BODY], true);
						}
						catch (IOException | InterruptedException e) {
							// the relay went away
						}
					});
				}
			}
		}
		catch (IOException e) {
			// connection ended
		}
	}

	/** Holds every request body: a stream window of 0, read and never granted. */
	private static void holdRequests(Socket socket) {
		try (socket) {
			WindowedH2Upstream upstream = WindowedH2Upstream.accept(socket);
			if (upstream == null) {
				return;
			}
			upstream.settings(0);
			while (upstream.next() != null) {
				// read and dropped
			}
		}
		catch (IOException e) {
			// connection ended
		}
	}

	/** Answers one HTTP/1.1 request and closes. */
	private static void serveH1(Socket socket) {
		try (socket) {
			InputStream in = socket.getInputStream();
			int matched = 0;
			byte[] end = "\r\n\r\n".getBytes(StandardCharsets.US_ASCII);
			int b;
			while (matched < end.length && (b = in.read()) >= 0) {
				matched = b == end[matched] ? matched + 1 : b == end[0] ? 1 : 0;
			}
			byte[] body = H1_BODY.getBytes(StandardCharsets.US_ASCII);
			OutputStream out = socket.getOutputStream();
			out.write(("HTTP/1.1 200 OK\r\nContent-Length: " + body.length + "\r\nConnection: close\r\n\r\n")
				.getBytes(StandardCharsets.US_ASCII));
			out.write(body);
			out.flush();
		}
		catch (IOException e) {
			// connection ended
		}
	}

	/** A TLS connection to the data plane with the given SNI and ALPN offer. */
	private static SSLSocket tls(String sni, String... protocols) throws Exception {
		SSLSocket socket = (SSLSocket) clientSslContext().getSocketFactory().createSocket("127.0.0.1", dataPort);
		socket.setSoTimeout((int) TIMEOUT.toMillis());
		SSLParameters parameters = socket.getSSLParameters();
		parameters.setServerNames(List.of(new SNIHostName(sni)));
		parameters.setApplicationProtocols(protocols);
		socket.setSSLParameters(parameters);
		socket.startHandshake();
		return socket;
	}

	private static SSLContext clientSslContext() throws Exception {
		KeyStore keyStore = KeyStore.getInstance("PKCS12");
		try (InputStream in = Files.newInputStream(Objects.requireNonNull(keystorePath))) {
			keyStore.load(in, "changeit".toCharArray());
		}
		TrustManagerFactory trustManagerFactory = TrustManagerFactory
			.getInstance(TrustManagerFactory.getDefaultAlgorithm());
		trustManagerFactory.init(keyStore);
		SSLContext sslContext = SSLContext.getInstance("TLS");
		sslContext.init(null, trustManagerFactory.getTrustManagers(), null);
		return sslContext;
	}

	/** One certificate covering every host of the test. */
	private static Path createKeystore() throws Exception {
		Path keystore = Files.createTempFile("sluice-e2e-memory", ".p12");
		Files.delete(keystore);
		String keytool = Path.of(System.getProperty("java.home"), "bin", "keytool").toString();
		Process process = new ProcessBuilder(keytool, "-genkeypair", "-alias", "e2e", "-keyalg", "RSA", "-keysize",
				"2048", "-storetype", "PKCS12", "-storepass", "changeit", "-dname", "CN=" + DOWNLOAD_HOST, "-ext",
				"san=dns:" + DOWNLOAD_HOST + ",dns:" + H1_HOST, "-validity", "1", "-keystore",
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

	private void startClient(String host, ServerSocket upstream) {
		TunnelClient client = TunnelClient.builder()
			.properties(SluiceClientProperties.builder()
				.serverUrl("grpc://127.0.0.1:" + grpcPort)
				.upstream(Upstream.builder().host(host).target("tcp://127.0.0.1:" + upstream.getLocalPort()).build())
				.token("it-token")
				.build())
			.taskExecutor(task -> Thread.ofVirtual().name("e2e-memory").start(task))
			.meterRegistry(new SimpleMeterRegistry())
			.build();
		client.start();
		this.clients.add(client);
		Awaitility.await().atMost(TIMEOUT).until(() -> Objects.requireNonNull(this.router).lookup(host).isPresent());
	}

	private ServerSocket stub(Consumer<Socket> handler) {
		try {
			ServerSocket server = new ServerSocket();
			server.bind(new InetSocketAddress("127.0.0.1", 0), 128);
			this.closeables.add(server);
			Thread.ofVirtual().name("e2e-memory-accept").start(() -> {
				while (!server.isClosed()) {
					try {
						Socket socket = server.accept();
						this.closeables.add(socket);
						UPSTREAM_EXECUTOR.execute(() -> handler.accept(socket));
					}
					catch (IOException e) {
						return;
					}
				}
			});
			return server;
		}
		catch (IOException e) {
			throw new IllegalStateException(e);
		}
	}

	private static void close(AutoCloseable closeable) {
		try {
			closeable.close();
		}
		catch (Exception e) {
			// ignore
		}
	}

}
