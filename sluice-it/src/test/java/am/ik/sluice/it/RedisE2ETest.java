package am.ik.sluice.it;

import java.net.ServerSocket;
import java.time.Duration;

import org.junit.jupiter.api.AfterAll;
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
import io.lettuce.core.RedisClient;
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
 * End to end through a real Redis: a Redis container is the tunnel upstream and a Lettuce
 * client drives RESP commands (PING, SET, GET) through the data plane as raw relayed
 * bytes.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
@Testcontainers
class RedisE2ETest {

	private static final TaskExecutor TASK_EXECUTOR = task -> Thread.ofVirtual().name("e2e-redis-worker").start(task);

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
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(this.sessions::count, count -> count > 0);
	}

	@AfterAll
	void tearDown() {
		if (this.client != null) {
			this.client.stop();
		}
	}

	/**
	 * Runs the given commands against the data port through a real Lettuce connection.
	 */
	private <T> T redis(java.util.function.Function<io.lettuce.core.api.sync.RedisCommands<String, String>, T> work) {
		RedisClient lettuce = RedisClient.create("redis://127.0.0.1:" + dataPort);
		try (StatefulRedisConnection<String, String> connection = lettuce.connect()) {
			return work.apply(connection.sync());
		}
		finally {
			lettuce.shutdown();
		}
	}

	@Test
	void pingRoundTripsThroughTunnel() {
		this.startClient();
		String pong = this.redis(cmds -> cmds.ping());
		assertThat(pong).isEqualTo("PONG");
	}

	@Test
	void setGetRoundTripThroughTunnel() {
		this.startClient();
		String ok = this.redis(cmds -> cmds.set("sluice", "tunnel"));
		assertThat(ok).isEqualTo("OK");
		String value = this.redis(cmds -> cmds.get("sluice"));
		assertThat(value).isEqualTo("tunnel");
	}

}
