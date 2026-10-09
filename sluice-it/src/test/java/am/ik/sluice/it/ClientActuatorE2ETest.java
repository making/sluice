package am.ik.sluice.it;

import java.time.Duration;
import java.util.Objects;

import org.awaitility.Awaitility;

import am.ik.sluice.client.SluiceClientApplication;
import am.ik.sluice.server.SluiceServerApplication;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.web.servlet.client.RestTestClient;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Full stack integration test: the client exposes the actuator on its HTTP port -- health
 * reporting the tunnel state (UP while a node stream lives, the per-node states as
 * details) and prometheus metrics. The security auto-configuration is excluded on the
 * client: it lands on the shared classpath through the server dependency, while the
 * standalone client deliberately ships none.
 */
@TestInstance(Lifecycle.PER_CLASS)
class ClientActuatorE2ETest {

	private static final String SECURITY_EXCLUDES = String.join(",",
			"org.springframework.boot.security.autoconfigure.SecurityAutoConfiguration",
			"org.springframework.boot.security.autoconfigure.UserDetailsServiceAutoConfiguration",
			"org.springframework.boot.security.autoconfigure.actuate.web.servlet.ManagementWebSecurityAutoConfiguration",
			"org.springframework.boot.security.autoconfigure.web.servlet.ServletWebSecurityAutoConfiguration",
			"org.springframework.boot.security.autoconfigure.web.servlet.SecurityFilterAutoConfiguration");

	private @org.jspecify.annotations.Nullable ConfigurableApplicationContext serverContext;

	private @org.jspecify.annotations.Nullable ConfigurableApplicationContext clientContext;

	private int clientPort;

	@AfterEach
	void stopApps() {
		if (this.clientContext != null) {
			this.clientContext.close();
			this.clientContext = null;
		}
		if (this.serverContext != null) {
			this.serverContext.close();
			this.serverContext = null;
		}
	}

	private void startApps() {
		int grpcPort = TestPorts.freePort();
		this.clientPort = TestPorts.freePort();
		this.serverContext = new SpringApplicationBuilder(SluiceServerApplication.class).run("--server.port=0",
				"--spring.grpc.server.port=" + grpcPort, "--sluice.data-port=" + TestPorts.freePort(),
				"--sluice.token=it-token", "--spring.grpc.server.shutdown.grace-period=2s");
		this.clientContext = new SpringApplicationBuilder(SluiceClientApplication.class).run(
				"--server.port=" + this.clientPort, "--sluice.server-url=grpc://127.0.0.1:" + grpcPort,
				"--sluice.client.upstream[0].host=actuator.local",
				"--sluice.client.upstream[0].target=http://127.0.0.1:1", "--sluice.token=it-token",
				"--spring.autoconfigure.exclude=" + SECURITY_EXCLUDES);
	}

	private @org.jspecify.annotations.Nullable String body(RestTestClient rest, String path) {
		return rest.get().uri(path).exchange().expectBody(String.class).returnResult().getResponseBody();
	}

	@Test
	void healthTracksTheTunnelAndPrometheusServesMetrics() {
		startApps();
		RestTestClient rest = RestTestClient.bindToServer().baseUrl("http://127.0.0.1:" + this.clientPort).build();
		// connected: UP with the per-node states as details
		Awaitility.await().atMost(Duration.ofSeconds(15)).until(() -> {
			String health = body(rest, "/actuator/health");
			return health != null && health.contains("\"status\":\"UP\"");
		});
		rest.get()
			.uri("/actuator/health")
			.exchange()
			.expectStatus()
			.isOk()
			.expectBody()
			.jsonPath("$.status")
			.isEqualTo("UP")
			.jsonPath("$.components.tunnel.details.tunnel")
			.isEqualTo("connected")
			.jsonPath("$.components.tunnel.details.nodes")
			.value(nodes -> assertThat(String.valueOf(nodes)).contains(":up"));
		// prometheus scrapes the tunnel metrics
		rest.get()
			.uri("/actuator/prometheus")
			.exchange()
			.expectStatus()
			.isOk()
			.expectBody(String.class)
			.consumeWith(result -> assertThat(result.getResponseBody()).contains("sluice_reconnect_total"));

		// server gone: the tunnel health goes DOWN (503)
		Objects.requireNonNull(this.serverContext).close();
		this.serverContext = null;
		Awaitility.await().atMost(Duration.ofSeconds(15)).until(() -> {
			String health = body(rest, "/actuator/health");
			return health != null && health.contains("\"status\":\"DOWN\"");
		});
		rest.get()
			.uri("/actuator/health")
			.exchange()
			.expectStatus()
			.isEqualTo(503)
			.expectBody()
			.jsonPath("$.status")
			.isEqualTo("DOWN")
			.jsonPath("$.components.tunnel.details.tunnel")
			.isEqualTo("not connected")
			.jsonPath("$.components.tunnel.details.nodes")
			.value(nodes -> assertThat(String.valueOf(nodes)).contains(":down"));
	}

}
