package am.ik.sluice.it;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.client.tunnel.TunnelClient;
import am.ik.sluice.server.SluiceServerApplication;
import am.ik.sluice.server.tunnel.SessionRegistry;
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
 * End to end through a real Redis: a Redis container is the tunnel upstream and RESP
 * commands (PING, SET, GET) round trip through the data plane as raw relayed bytes.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
@Testcontainers
class RedisE2ETest {

	private static final TaskExecutor TASK_EXECUTOR = task -> Thread.ofVirtual().name("e2e-redis-worker").start(task);

	private static final String HOST = "redis.sluice.test";

	@Container
	@SuppressWarnings("resource")
	private static final GenericContainer<?> REDIS = new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);

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
		SluiceClientProperties properties = SluiceClientProperties.builder()
			.serverUrl("grpc://127.0.0.1:" + grpcPort)
			// RESP carries no host: the catch-all route (empty host) serves it
			.upstream("", "redis://127.0.0.1:" + REDIS.getMappedPort(6379))
			.token("it-token")
			.build();
		TunnelClient started = new TunnelClient(properties, TASK_EXECUTOR, new SimpleMeterRegistry());
		started.start();
		this.client = started;
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> this.sessions.count() > 0);
	}

	/** Sends a RESP command array and reads one line of the reply. */
	private @Nullable String redis(String... args) throws Exception {
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(10_000);
			OutputStream out = socket.getOutputStream();
			out.write(("*" + args.length + "\r\n").getBytes(StandardCharsets.US_ASCII));
			for (String arg : args) {
				byte[] bytes = arg.getBytes(StandardCharsets.US_ASCII);
				out.write(("$" + bytes.length + "\r\n").getBytes(StandardCharsets.US_ASCII));
				out.write(bytes);
				out.write("\r\n".getBytes(StandardCharsets.US_ASCII));
			}
			// a blank line terminates the connection head: RESP carries no host, so the
			// data plane routes this connection via the catch-all route
			out.write("\r\n".getBytes(StandardCharsets.US_ASCII));
			out.flush();
			java.io.InputStream raw = socket.getInputStream();
			byte[] buffer = new byte[64];
			int read = raw.read(buffer); // one read: the reply fits a single segment
			byte[] reply = read <= 0 ? new byte[0] : java.util.Arrays.copyOf(buffer, read);
			if (reply.length > 1 && reply[0] == '$') { // bulk string: skip the header
														// line
				int headerEnd = 0;
				while (reply[headerEnd++] != '\n') {
					// skip
				}
				return new String(reply, headerEnd,
						new String(reply, StandardCharsets.US_ASCII).indexOf("\r\n", headerEnd) - headerEnd,
						StandardCharsets.US_ASCII);
			}
			return reply.length == 0 ? null : new String(reply, StandardCharsets.US_ASCII).strip();
		}
	}

	@Test
	void pingRoundTripsThroughTunnel() throws Exception {
		this.startClient();
		assertThat(this.redis("PING")).isEqualTo("+PONG");
	}

	@Test
	void setGetRoundTripThroughTunnel() throws Exception {
		this.startClient();
		assertThat(this.redis("SET", "sluice", "tunnel")).isEqualTo("+OK");
		assertThat(this.redis("GET", "sluice")).isEqualTo("tunnel");
	}

}
