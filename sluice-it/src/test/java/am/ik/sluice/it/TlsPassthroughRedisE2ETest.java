package am.ik.sluice.it;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.time.Duration;

import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLParameters;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.client.config.Upstream;
import am.ik.sluice.client.tunnel.TunnelClient;
import am.ik.sluice.server.SluiceServerApplication;
import am.ik.sluice.server.tunnel.SessionRegistry;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.SslOptions;
import io.lettuce.core.api.StatefulRedisConnection;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.task.TaskExecutor;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End to end through a real TLS-enabled Redis: the data plane runs in TLS passthrough
 * mode (no SSL bundle), routes the connection by the ClientHello SNI host name, and
 * relays the TLS records untouched; the tunnel client dials the TLS upstream and the
 * upstream terminates TLS. The Lettuce client connects by IP with an explicit SNI host
 * name (the redis-cli {@code --sni} equivalent) and validates the server certificate via
 * a truststore.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
@Testcontainers
class TlsPassthroughRedisE2ETest {

	private static final String HOST = "redis.sni.test";

	private static final String STORE_PASSWORD = "changeit";

	private static final TaskExecutor TASK_EXECUTOR = task -> Thread.ofVirtual()
		.name("e2e-sni-redis-worker")
		.start(task);

	/**
	 * Self-signed server certificate (SAN: DNS + loopback) and a JKS truststore for
	 * Lettuce.
	 */
	private static final Path TLS_DIR = generateCertificates();

	private static Path generateCertificates() {
		try {
			Path dir = Files.createTempDirectory("sluice-sni-redis");
			Path pkcs12 = dir.resolve("server.p12");
			exec("keytool", "-genkeypair", "-alias", "redis", "-keyalg", "RSA", "-keysize", "2048", "-dname",
					"CN=" + HOST, "-ext", "SAN=DNS:" + HOST + ",IP:127.0.0.1", "-keystore", pkcs12.toString(),
					"-storetype", "PKCS12", "-storepass", STORE_PASSWORD, "-validity", "2");
			exec("openssl", "pkcs12", "-in", pkcs12.toString(), "-passin", "pass:" + STORE_PASSWORD, "-nokeys",
					"-nodes", "-out", dir.resolve("cert.pem").toString());
			exec("openssl", "pkcs12", "-in", pkcs12.toString(), "-passin", "pass:" + STORE_PASSWORD, "-nocerts",
					"-nodes", "-out", dir.resolve("key.pem").toString());
			exec("keytool", "-importcert", "-noprompt", "-alias", "redis", "-file", dir.resolve("cert.pem").toString(),
					"-keystore", dir.resolve("truststore.jks").toString(), "-storepass", STORE_PASSWORD);
			// the container process (user redis) must read the mounted material
			for (String name : List.of("cert.pem", "key.pem")) {
				Files.setPosixFilePermissions(dir.resolve(name),
						java.nio.file.attribute.PosixFilePermissions.fromString("rw-r--r--"));
			}
			return dir;
		}
		catch (Exception e) {
			throw new IllegalStateException("failed to prepare TLS material", e);
		}
	}

	private static void exec(String... command) throws IOException, InterruptedException {
		Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
		if (!process.waitFor(30, java.util.concurrent.TimeUnit.SECONDS) || process.exitValue() != 0) {
			throw new IllegalStateException("command failed: %s%n%s".formatted(String.join(" ", command),
					new String(process.getInputStream().readAllBytes())));
		}
	}

	@Container
	@SuppressWarnings("resource")
	private static final GenericContainer<?> REDIS = new GenericContainer<>("redis:7-alpine").withExposedPorts(6379)
		.withCommand("redis-server", "--port", "0", "--tls-port", "6379", "--tls-cert-file", "/tls/cert.pem",
				"--tls-key-file", "/tls/key.pem", "--tls-ca-cert-file", "/tls/cert.pem", "--tls-auth-clients", "no")
		.withCopyFileToContainer(MountableFile.forHostPath(TLS_DIR.resolve("cert.pem")), "/tls/cert.pem")
		.withCopyFileToContainer(MountableFile.forHostPath(TLS_DIR.resolve("key.pem")), "/tls/key.pem");

	private static int grpcPort;

	private static int dataPort;

	private @Nullable TunnelClient client;

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) {
		grpcPort = freePort();
		dataPort = freePort();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(dataPort));
		registry.add("sluice.token", () -> "it-token");
		registry.add("server.port", () -> 0);
		// no sluice.data-tls-bundle: the data plane stays in TLS passthrough mode
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
	}

	/** The container is started by then; the client registers once. */
	private void startClient() {
		if (this.client != null) {
			return;
		}
		// @formatter:off
		SluiceClientProperties properties = SluiceClientProperties.builder()
			.serverUrl("grpc://127.0.0.1:" + grpcPort)
			// routed by the ClientHello SNI host name, not the catch-all
			// plain TCP dial: the TLS records are relayed end to end, the upstream
			// terminates TLS (the client-side peer is Lettuce with its own truststore)
			.upstream(Upstream.builder().host(HOST).target("tcp://127.0.0.1:" + REDIS.getMappedPort(6379)).tlsPassthrough(true).build())
			.token("it-token")
			.build();
		// @formatter:on
		TunnelClient started = new TunnelClient(properties, TASK_EXECUTOR, new SimpleMeterRegistry());
		started.start();
		this.client = started;
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(this.sessions::count, count -> count > 0);
	}

	/**
	 * Connects to the data port by IP with an explicit SNI host name, like redis-cli
	 * --sni.
	 */
	private <T> T redis(java.util.function.Function<io.lettuce.core.api.sync.RedisCommands<String, String>, T> work) {
		SslOptions sslOptions = SslOptions.builder()
			.jdkSslProvider()
			.truststore(TLS_DIR.resolve("truststore.jks").toFile(), STORE_PASSWORD)
			.sslParameters(() -> {
				SSLParameters parameters = new SSLParameters();
				parameters.setServerNames(List.of(new SNIHostName(HOST)));
				return parameters;
			})
			.build();
		RedisClient lettuce = RedisClient.create("rediss://127.0.0.1:" + dataPort);
		lettuce.setOptions(ClientOptions.builder().sslOptions(sslOptions).build());
		try (StatefulRedisConnection<String, String> connection = lettuce.connect()) {
			return work.apply(connection.sync());
		}
		finally {
			lettuce.shutdown();
		}
	}

	@Test
	void pingRoutesBySniThroughTunnel() {
		this.startClient();
		String pong = this.redis(cmds -> cmds.ping());
		assertThat(pong).isEqualTo("PONG");
	}

	@Test
	void setGetRoutesBySniThroughTunnel() {
		this.startClient();
		String ok = this.redis(cmds -> cmds.set("sluice", "sni"));
		assertThat(ok).isEqualTo("OK");
		String value = this.redis(cmds -> cmds.get("sluice"));
		assertThat(value).isEqualTo("sni");
	}

}
