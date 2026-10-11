package am.ik.sluice.it;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Semaphore;

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
import am.ik.sluice.server.config.SluiceServerProperties;
import am.ik.sluice.server.route.Router;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * {@code sluice.limits.half-close-probe-interval=0} turns the probe off: a client that
 * half-closed waits for its response with nothing before it, past the default interval.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class HalfCloseProbeDisabledE2ETest {

	private static final String HOST = "off.probe.test";

	private static final String OK = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";

	/** Past the default interval (1s), so a probe would have come. */
	private static final Duration QUIET = Duration.ofMillis(2500);

	private static int grpcPort;

	private static int dataPort;

	private final Semaphore gate = new Semaphore(0);

	private @Nullable ServerSocket upstream;

	private @Nullable TunnelClient client;

	@Nullable
	@Autowired
	private SluiceServerProperties properties;

	@Nullable
	@Autowired
	private Router router;

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) {
		grpcPort = TestPorts.freePort();
		dataPort = TestPorts.freePort();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(dataPort));
		registry.add("sluice.limits.half-close-probe-interval", () -> "0");
		registry.add("sluice.token", () -> "it-token");
		registry.add("server.port", () -> String.valueOf(TestPorts.freePort()));
	}

	@BeforeAll
	void startClient() throws IOException {
		ServerSocket server = new ServerSocket();
		server.bind(new InetSocketAddress("127.0.0.1", 0), 16);
		this.upstream = server;
		Thread.ofVirtual().name("e2e-probe-off-accept").start(() -> {
			while (!server.isClosed()) {
				try {
					Socket socket = server.accept();
					Thread.ofVirtual().name("e2e-probe-off-conn").start(() -> serve(socket));
				}
				catch (IOException e) {
					return;
				}
			}
		});
		TunnelClient started = TunnelClient.builder()
			.properties(SluiceClientProperties.builder()
				.serverUrl("grpc://127.0.0.1:" + grpcPort)
				.upstream(Upstream.builder().host(HOST).target("http://127.0.0.1:" + server.getLocalPort()).build())
				.token("it-token")
				.build())
			.taskExecutor(task -> Thread.ofVirtual().name("e2e-probe-off-worker").start(task))
			.meterRegistry(new SimpleMeterRegistry())
			.build();
		started.start();
		this.client = started;
		Awaitility.await()
			.atMost(Duration.ofSeconds(10))
			.until(() -> Objects.requireNonNull(this.router).lookup(HOST).isPresent());
	}

	@AfterAll
	void tearDown() throws IOException {
		this.gate.release(100);
		if (this.client != null) {
			this.client.stop();
		}
		if (this.upstream != null) {
			this.upstream.close();
		}
	}

	@Test
	void theProbeIsOff() {
		assertThat(Objects.requireNonNull(this.properties).limits().halfCloseProbeInterval()).isZero();
	}

	@Test
	void aClientThatHalfClosedGetsNoProbe() throws Exception {
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.getOutputStream()
				.write(("GET / HTTP/1.1\r\nHost: " + HOST + "\r\n\r\n").getBytes(StandardCharsets.US_ASCII));
			socket.getOutputStream().flush();
			socket.shutdownOutput();
			socket.setSoTimeout((int) QUIET.toMillis());
			assertThatExceptionOfType(SocketTimeoutException.class).isThrownBy(() -> socket.getInputStream().read());
			this.gate.release();
			socket.setSoTimeout(5000);
			assertThat(new String(socket.getInputStream().readAllBytes(), StandardCharsets.US_ASCII)).isEqualTo(OK);
		}
	}

	/** Answers each request with {@link #OK} once the gate opens; closes at EOF. */
	private void serve(Socket socket) {
		try (socket; InputStream in = socket.getInputStream(); OutputStream out = socket.getOutputStream()) {
			int matched = 0;
			int b;
			while ((b = in.read()) >= 0) {
				matched = (b == '\r' || b == '\n') ? matched + 1 : 0;
				if (matched == 4) {
					matched = 0;
					this.gate.acquire();
					out.write(OK.getBytes(StandardCharsets.US_ASCII));
					out.flush();
				}
			}
		}
		catch (IOException e) {
			// the peer went away
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

}
