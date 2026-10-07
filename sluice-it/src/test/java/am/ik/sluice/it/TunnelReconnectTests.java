package am.ik.sluice.it;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;

import org.awaitility.Awaitility;

import am.ik.sluice.client.SluiceClientApplication;
import am.ik.sluice.client.tunnel.TunnelClient;
import am.ik.sluice.server.SluiceServerApplication;
import am.ik.sluice.server.tunnel.SessionRegistry;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Full stack integration test: the real server and client applications in one JVM. Covers
 * end to end proxying, reconnection after a server restart, and wrong-token rejection.
 */
@TestInstance(Lifecycle.PER_CLASS)
class TunnelReconnectTests {

	private static final String HOST = "it.local";

	private @org.jspecify.annotations.Nullable HttpServer upstream;

	private int upstreamPort;

	private int grpcPort;

	private int dataPort;

	private int webPort;

	private @org.jspecify.annotations.Nullable ConfigurableApplicationContext serverContext;

	private @org.jspecify.annotations.Nullable ConfigurableApplicationContext clientContext;

	@AfterEach
	void stopApps() {
		closeQuietly(this.clientContext);
		closeQuietly(this.serverContext);
		this.clientContext = null;
		this.serverContext = null;
	}

	private void startUpstream() throws Exception {
		this.upstream = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		this.upstream.createContext("/", exchange -> {
			byte[] body = "it-ok".getBytes(StandardCharsets.UTF_8);
			exchange.sendResponseHeaders(200, body.length);
			try (OutputStream out = exchange.getResponseBody()) {
				out.write(body);
			}
		});
		this.upstream.start();
		this.upstreamPort = this.upstream.getAddress().getPort();
	}

	private void startServer(String token) {
		ConfigurableApplicationContext context = new SpringApplicationBuilder(SluiceServerApplication.class).run(
				"--server.port=" + this.webPort, "--spring.grpc.server.port=" + this.grpcPort,
				"--sluice.data-port=" + this.dataPort, "--sluice.token=" + token,
				"--spring.grpc.server.shutdown.grace-period=2s");
		this.serverContext = context;
	}

	private void startClient(String token) {
		ConfigurableApplicationContext context = new SpringApplicationBuilder(SluiceClientApplication.class).run(
				"--sluice.server-url=grpc://127.0.0.1:" + this.grpcPort, "--sluice.client.upstream[0].host=" + HOST,
				"--sluice.client.upstream[0].target=http://127.0.0.1:" + this.upstreamPort, "--sluice.token=" + token,
				"--management.server.port=0");
		this.clientContext = context;
	}

	private void pickPorts() {
		this.grpcPort = TestPorts.freePort();
		this.dataPort = TestPorts.freePort();
		this.webPort = TestPorts.freePort();
	}

	private static void closeQuietly(@org.jspecify.annotations.Nullable ConfigurableApplicationContext context) {
		if (context != null) {
			context.close();
		}
	}

	private void awaitServerPortReleased() {
		Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> {
			try (ServerSocket ignored = new ServerSocket(this.dataPort)) {
				return true;
			}
			catch (Exception e) {
				return false;
			}
		});
	}

	private SessionRegistry serverRegistry() {
		return Objects.requireNonNull(this.serverContext, "server not started").getBean(SessionRegistry.class);
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
			byte[] buffer = in.readAllBytes();
			return new String(buffer, StandardCharsets.UTF_8);
		}
	}

	@Test
	void clientReconnectsAfterServerRestart() throws Exception {
		startUpstream();
		pickPorts();
		startServer("it-token");
		startClient("it-token");
		Awaitility.await().atMost(Duration.ofSeconds(15)).until(() -> serverRegistry().count() == 1);
		assertThat(roundTrip()).contains("it-ok");

		// restart the server; the client must re-register on its own
		ConfigurableApplicationContext client = Objects.requireNonNull(this.clientContext);
		Objects.requireNonNull(this.serverContext).close();
		this.serverContext = null;
		awaitServerPortReleased();
		startServer("it-token");
		Awaitility.await().atMost(Duration.ofSeconds(30)).until(() -> serverRegistry().count() == 1);
		assertThat(client.getBean(TunnelClient.class).isConnected()).isTrue();
		assertThat(roundTrip()).contains("it-ok");
	}

	@Test
	void wrongTokenIsRejectedAndClientKeepsRetrying() throws Exception {
		startUpstream();
		pickPorts();
		startServer("SECRET");
		startClient("WRONG");
		// no session ever registers with a wrong token, and the client keeps retrying
		Awaitility.await()
			.during(Duration.ofSeconds(4))
			.atMost(Duration.ofSeconds(6))
			.until(() -> serverRegistry().count() == 0);
		assertThat(Objects.requireNonNull(this.clientContext).getBean(TunnelClient.class).isRunning()).isTrue();
	}

}
