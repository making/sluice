package am.ik.sluice.it;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;

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

import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.task.TaskExecutor;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End to end: with a connected client, closing the server context must not wait out the
 * gRPC shutdown grace; the server completes the tunnel streams itself after the drain
 * wait, so the shutdown takes drain-grace plus seconds, not the 30s gRPC grace.
 */
@TestInstance(Lifecycle.PER_CLASS)
class ServerShutdownE2ETest {

	private static final String HOST = "shutdown.local";

	private static final TaskExecutor TASK_EXECUTOR = task -> Thread.ofVirtual().name("e2e-shutdown").start(task);

	private static final Duration DRAIN_GRACE = Duration.ofSeconds(2);

	private static final long CLOSE_BUDGET_SECONDS = 15;

	private static @Nullable HttpServer upstream;

	private int grpcPort;

	private int dataPort;

	private @Nullable ConfigurableApplicationContext serverContext;

	private @Nullable TunnelClient client;

	@BeforeAll
	static void startUpstream() throws Exception {
		upstream = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		upstream.createContext("/", exchange -> {
			byte[] body = "it-ok".getBytes(StandardCharsets.UTF_8);
			exchange.getResponseHeaders().set("Content-Type", "text/plain");
			exchange.sendResponseHeaders(200, body.length);
			try (OutputStream out = exchange.getResponseBody()) {
				out.write(body);
			}
		});
		upstream.start();
	}

	@AfterAll
	void tearDown() {
		if (this.client != null) {
			this.client.stop();
		}
		if (upstream != null) {
			upstream.stop(0);
		}
	}

	private void startServer() {
		this.grpcPort = TestPorts.freePort();
		this.dataPort = TestPorts.freePort();
		this.serverContext = new SpringApplicationBuilder(SluiceServerApplication.class).run("--server.port=0",
				"--spring.grpc.server.port=" + this.grpcPort, "--sluice.data-port=" + this.dataPort,
				"--sluice.token=it-token", "--sluice.cluster.drain-grace=" + DRAIN_GRACE.getSeconds() + "s");
	}

	private void startClient() {
		SluiceClientProperties properties = SluiceClientProperties.builder()
			.serverUrl("grpc://127.0.0.1:" + this.grpcPort)
			.clientId("shutdown-client")
			.upstream(am.ik.sluice.client.config.Upstream.builder()
				.host(HOST)
				.target("http://127.0.0.1:" + upstream().getAddress().getPort())
				.build())
			.token("it-token")
			.build();
		TunnelClient started = TunnelClient.builder()
			.properties(properties)
			.taskExecutor(TASK_EXECUTOR)
			.meterRegistry(new SimpleMeterRegistry())
			.build();
		started.start();
		this.client = started;
		SessionRegistry registry = serverContext().getBean(SessionRegistry.class);
		Awaitility.await().atMost(Duration.ofSeconds(10)).until(registry::count, count -> count > 0);
	}

	private ConfigurableApplicationContext serverContext() {
		return Objects.requireNonNull(this.serverContext, "server not started");
	}

	private HttpServer upstream() {
		return Objects.requireNonNull(upstream, "upstream not started");
	}

	/** One round trip through the data plane; returns the upstream body. */
	private String roundTrip() throws Exception {
		try (Socket socket = new Socket("127.0.0.1", this.dataPort)) {
			socket.setSoTimeout(10_000);
			OutputStream out = socket.getOutputStream();
			out.write(("GET / HTTP/1.1\r\nHost: " + HOST + "\r\nConnection: close\r\n\r\n")
				.getBytes(StandardCharsets.US_ASCII));
			out.flush();
			InputStream in = socket.getInputStream();
			return new String(in.readAllBytes(), StandardCharsets.UTF_8);
		}
	}

	@Test
	void contextCloseDoesNotWaitOutTheGrpcShutdownGrace() throws Exception {
		startServer();
		startClient();
		assertThat(roundTrip()).contains("it-ok");
		// the client stays connected on purpose: the server must end the stream itself
		long start = System.nanoTime();
		serverContext().close();
		Duration elapsed = Duration.ofNanos(System.nanoTime() - start);
		assertThat(elapsed).isLessThan(Duration.ofSeconds(CLOSE_BUDGET_SECONDS));
	}

}
