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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Full stack cluster test: two/three server nodes plus one client in one JVM. The client
 * fans out one tunnel stream per node, every node serves the data plane, a stopped node
 * is failed over, a started node joins via the membership push, and drain lets in-flight
 * connections finish.
 */
@TestInstance(Lifecycle.PER_CLASS)
class ClusterFanoutE2ETests {

	private static final String HOST = "cluster.local";

	private static final String TOKEN = "cluster-it-token";

	private static final int TCP_PORT = 19123;

	private @org.jspecify.annotations.Nullable HttpServer httpUpstream;

	private @org.jspecify.annotations.Nullable ServerSocket echoUpstream;

	private @org.jspecify.annotations.Nullable Thread echoLoop;

	private int httpPort;

	private final int[] grpcPorts = new int[3];

	private final int[] dataPorts = new int[3];

	private final @org.jspecify.annotations.Nullable ConfigurableApplicationContext[] servers = new ConfigurableApplicationContext[3];

	private @org.jspecify.annotations.Nullable ConfigurableApplicationContext clientContext;

	private final String[] nodeNames = { "node-1", "node-2", "node-3" };

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
		if (this.echoLoop != null) {
			this.echoLoop.interrupt();
		}
		closeSocket(this.echoUpstream);
		closeServer(this.httpUpstream);
	}

	private void startUpstreams() throws Exception {
		this.httpUpstream = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		this.httpUpstream.createContext("/", exchange -> {
			byte[] body = "cluster-ok".getBytes(StandardCharsets.UTF_8);
			exchange.sendResponseHeaders(200, body.length);
			try (OutputStream out = exchange.getResponseBody()) {
				out.write(body);
			}
		});
		// slow variant: the response takes a while, so a request against it is really
		// in-flight while the server drains
		this.httpUpstream.createContext("/slow", exchange -> {
			try {
				Thread.sleep(1000);
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			byte[] body = "cluster-ok".getBytes(StandardCharsets.UTF_8);
			exchange.sendResponseHeaders(200, body.length);
			try (OutputStream out = exchange.getResponseBody()) {
				out.write(body);
			}
		});
		this.httpUpstream.start();
		this.httpPort = this.httpUpstream.getAddress().getPort();

		this.echoUpstream = new ServerSocket(0, 128, java.net.InetAddress.getByName("127.0.0.1"));
		this.echoLoop = Thread.ofVirtual().name("it-echo-accept").start(this::acceptEchoes);
	}

	private void acceptEchoes() {
		while (this.echoUpstream != null && !this.echoUpstream.isClosed()) {
			try {
				Socket socket = Objects.requireNonNull(this.echoUpstream).accept();
				Thread.ofVirtual().start(() -> {
					try (socket) {
						InputStream in = socket.getInputStream();
						OutputStream out = socket.getOutputStream();
						out.write(in.readAllBytes());
						out.flush();
					}
					catch (Exception ignored) {
					}
				});
			}
			catch (Exception e) {
				return;
			}
		}
	}

	private void startServer(int index) {
		String membership = this.nodeNames[0] + "=grpc://127.0.0.1:" + this.grpcPorts[0] + "," + this.nodeNames[1]
				+ "=grpc://127.0.0.1:" + this.grpcPorts[1] + "," + this.nodeNames[2] + "=grpc://127.0.0.1:"
				+ this.grpcPorts[2];
		ConfigurableApplicationContext context = new SpringApplicationBuilder(SluiceServerApplication.class).run(
				"--server.port=" + TestPorts.freePort(), "--spring.grpc.server.port=" + this.grpcPorts[index],
				"--sluice.data-port=" + this.dataPorts[index], "--sluice.token=" + TOKEN,
				"--sluice.node.id=" + this.nodeNames[index], "--sluice.cluster.nodes=" + membership,
				"--sluice.cluster.warmup=1s", "--sluice.cluster.drain-grace=10s", "--sluice.tcp-port-range=" + TCP_PORT,
				"--spring.grpc.server.shutdown.grace-period=2s");
		this.servers[index] = context;
	}

	private void startClient() {
		ConfigurableApplicationContext context = new SpringApplicationBuilder(SluiceClientApplication.class).run(
				"--sluice.server-url=grpc://127.0.0.1:" + this.grpcPorts[0], "--sluice.client.id=it-client",
				"--sluice.client.upstream[0].host=" + HOST,
				"--sluice.client.upstream[0].target=http://127.0.0.1:" + this.httpPort,
				"--sluice.client.upstream[1].host=",
				"--sluice.client.upstream[1].target=tcp://127.0.0.1:"
						+ Objects.requireNonNull(this.echoUpstream).getLocalPort(),
				"--sluice.client.upstream[1].listen-port=" + TCP_PORT, "--sluice.token=" + TOKEN, "--server.port=0");
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

	private static void closeSocket(@org.jspecify.annotations.Nullable ServerSocket socket) {
		if (socket != null && !socket.isClosed()) {
			try {
				socket.close();
			}
			catch (Exception ignored) {
			}
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

	/** One echo round trip through the tcp route of the given node. */
	private String echoRoundTrip(int index, String payload) throws Exception {
		try (Socket socket = new Socket("127.0.0.1", TCP_PORT)) {
			socket.setSoTimeout(10_000);
			socket.getOutputStream().write(payload.getBytes(StandardCharsets.UTF_8));
			socket.getOutputStream().flush();
			socket.shutdownOutput();
			return new String(socket.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
		}
	}

	/*
	 * The tcp route listener binds on the data host with the listen port; it is a
	 * different socket from the HTTP data plane port.
	 */
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
	void bothNodesServeAndClientFailsOver() throws Exception {
		startUpstreams();
		for (int i = 0; i < 3; i++) {
			this.grpcPorts[i] = TestPorts.freePort();
			this.dataPorts[i] = TestPorts.freePort();
		}
		startServer(0);
		startServer(1);
		startClient();
		this.awaitSessionOnAllNodes(2, Duration.ofSeconds(20));

		// the same routes serve through both nodes
		assertThat(this.httpRoundTrip(0)).contains("cluster-ok");
		assertThat(this.httpRoundTrip(1)).contains("cluster-ok");
		assertThat(this.echoRoundTrip(0, "ping-a")).isEqualTo("ping-a");
		assertThat(this.echoRoundTrip(1, "ping-b")).isEqualTo("ping-b");

		// node failure: the remaining node keeps serving, the client stays up
		closeContext(this.servers[1]);
		this.servers[1] = null;
		// nullable write is fine: the array is scanned for null entries everywhere
		assertThat(this.tunnelClient().isConnected()).isTrue();
		assertThat(this.httpRoundTrip(0)).contains("cluster-ok");
	}

	@Test
	void addedNodeIsPickedUpThroughTheMembershipPush() throws Exception {
		startUpstreams();
		for (int i = 0; i < 3; i++) {
			this.grpcPorts[i] = TestPorts.freePort();
			this.dataPorts[i] = TestPorts.freePort();
		}
		startServer(0);
		startServer(1);
		startClient();
		this.awaitSessionOnAllNodes(2, Duration.ofSeconds(20));

		// the third node was in the membership from the start but only now appears
		startServer(2);
		Awaitility.await().atMost(Duration.ofSeconds(30)).until(() -> sessionCount(2) == 1);
		assertThat(this.tunnelClient().isConnected()).isTrue();
		assertThat(this.httpRoundTrip(2)).contains("cluster-ok");
	}

	@Test
	void drainLetsInFlightConnectionFinish() throws Exception {
		startUpstreams();
		for (int i = 0; i < 3; i++) {
			this.grpcPorts[i] = TestPorts.freePort();
			this.dataPorts[i] = TestPorts.freePort();
		}
		startServer(0);
		startServer(1);
		startClient();
		this.awaitSessionOnAllNodes(2, Duration.ofSeconds(20));

		// send the request against the slow upstream and give the relay time to reach
		// it: the response is still pending when the server starts draining
		Socket socket = new Socket("127.0.0.1", this.dataPorts[0]);
		socket.setSoTimeout(20_000);
		OutputStream out = socket.getOutputStream();
		out.write(("GET /slow HTTP/1.1\r\nHost: " + HOST + "\r\nConnection: close\r\n\r\n")
			.getBytes(StandardCharsets.US_ASCII));
		out.flush();
		Thread.sleep(300);

		ch.qos.logback.classic.Logger lifecycleLogger = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory
			.getLogger(am.ik.sluice.server.tunnel.ClusterLifecycle.class);
		ch.qos.logback.classic.Logger grpcLogger = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory
			.getLogger(org.springframework.grpc.server.lifecycle.GrpcServerLifecycle.class);
		ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent> events = new ch.qos.logback.core.read.ListAppender<>();
		events.start();
		lifecycleLogger.addAppender(events);
		grpcLogger.addAppender(events);
		try {
			closeContext(this.servers[0]);
		}
		finally {
			lifecycleLogger.detachAppender(events);
			grpcLogger.detachAppender(events);
		}
		this.servers[0] = null;
		// the drain must run before the gRPC server shuts down, or the in-flight
		// connection would be cut with the stream
		assertThat(events.list).extracting(ch.qos.logback.classic.spi.ILoggingEvent::getFormattedMessage)
			.containsSubsequence("draining: notifying 1 client session(s)", "Completed gRPC server shutdown");

		String response = new String(socket.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
		socket.close();
		assertThat(response).contains("cluster-ok");
		// and the client failed over to the remaining node
		Awaitility.await().atMost(Duration.ofSeconds(15)).until(() -> this.tunnelClient().isConnected());
		assertThat(sessionCount(1)).isOne();
	}

}
