package am.ik.sluice.it;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.client.config.Upstream;
import am.ik.sluice.client.tunnel.TunnelClient;
import am.ik.sluice.server.SluiceServerApplication;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * When the tunnel stream ends -- the server vanishes -- the client must close its relayed
 * virtual connections: the upstream socket sees EOF instead of being held open by a
 * leaked relay, whether the relay idles or waits for send credit with its queue full.
 */
class ClientStreamEndCleanupE2ETest {

	private final CountDownLatch upstreamClosed = new CountDownLatch(1);

	private final List<AutoCloseable> held = new CopyOnWriteArrayList<>();

	private final int grpcPort = TestPorts.freePort();

	private final int listenPort = TestPorts.freePort();

	private @Nullable ConfigurableApplicationContext serverContext;

	private @Nullable TunnelClient client;

	@AfterEach
	void tearDown() throws Exception {
		TunnelClient client = this.client;
		if (client != null) {
			// a client whose relay is parked must not hold up the teardown
			Thread.ofVirtual().start(client::stop);
		}
		if (this.serverContext != null) {
			this.serverContext.close();
		}
		for (AutoCloseable closeable : this.held) {
			closeable.close();
		}
	}

	@Test
	void upstreamSeesEofWhenServerVanishes() throws Exception {
		startClient(upstream(this::echoUntilEof));
		// relay one round trip so the end-to-end path is up, keep the peer open, then
		// kill the server: the only remaining EOF source for the upstream is the
		// client-side cleanup of the relayed connection
		try (Socket peer = new Socket("127.0.0.1", this.listenPort)) {
			peer.setSoTimeout(10_000);
			OutputStream out = peer.getOutputStream();
			out.write("hello".getBytes(StandardCharsets.US_ASCII));
			out.flush();
			byte[] echoed = peer.getInputStream().readNBytes(5);
			assertThat(new String(echoed, StandardCharsets.US_ASCII)).isEqualTo("hello");

			stopServer();

			assertThat(this.upstreamClosed.await(5, TimeUnit.SECONDS))
				.as("upstream socket closed after the tunnel stream ended")
				.isTrue();
		}
	}

	/**
	 * The client -> server direction is stalled (the public peer never reads), so the
	 * client relay waits for send credit; the teardown must still complete and release
	 * the upstream socket instead of leaving the relay parked.
	 */
	@Test
	void clientTeardownCompletesWithFullSenderQueue() throws Exception {
		startClient(upstream(this::flood));
		Socket peer = new Socket("127.0.0.1", this.listenPort);
		this.held.add(peer);
		peer.getOutputStream().write("hello".getBytes(StandardCharsets.US_ASCII));
		peer.getOutputStream().flush();
		// precondition: the client relay waits for the credit the stalled server
		// withholds
		Awaitility.await()
			.atMost(Duration.ofSeconds(30))
			.alias("client relay parked in VirtualConnection.awaitCredit")
			.until(ThreadDumps::anyThreadWaitingForCredit);

		stopServer();

		boolean closed = this.upstreamClosed.await(10, TimeUnit.SECONDS);
		boolean stuck = ThreadDumps.anyThreadIn("am.ik.sluice.client.tunnel.NodeConnection", "closeConnections");
		assertThat(stuck).as("no thread stuck in NodeConnection.closeConnections").isFalse();
		assertThat(closed).as("upstream socket closed after the tunnel stream ended").isTrue();
	}

	/**
	 * Starts the server and a client with a tcp route on the listen port to the upstream.
	 */
	private void startClient(int upstreamPort) {
		this.serverContext = new SpringApplicationBuilder(SluiceServerApplication.class).run("--server.port=0",
				"--spring.grpc.server.port=" + this.grpcPort, "--sluice.data-port=" + TestPorts.freePort(),
				"--sluice.tcp-port-range=" + this.listenPort, "--sluice.token=it-token",
				"--spring.grpc.server.shutdown.grace-period=1s",
				// the server ends the stream at once rather than waiting out the drain
				// for the connection held open
				"--sluice.cluster.drain-grace=0s");
		TunnelClient started = TunnelClient.builder()
			.properties(SluiceClientProperties.builder()
				.serverUrl("grpc://127.0.0.1:" + this.grpcPort)
				.upstream(Upstream.builder()
					.host("eof.local")
					.target("tcp://127.0.0.1:" + upstreamPort)
					.listenPort(this.listenPort)
					.build())
				.token("it-token")
				.build())
			.taskExecutor(task -> Thread.ofVirtual().name("e2e-eof-worker").start(task))
			.meterRegistry(new SimpleMeterRegistry())
			.build();
		started.start();
		this.client = started;
		Awaitility.await().atMost(Duration.ofSeconds(10)).until(started::isConnected);
	}

	private void stopServer() {
		ConfigurableApplicationContext context = this.serverContext;
		if (context != null) {
			context.close();
			this.serverContext = null;
		}
	}

	/** An upstream serving every accepted connection with {@code handler}. */
	private int upstream(Consumer<Socket> handler) throws Exception {
		ServerSocket server = new ServerSocket();
		server.setReuseAddress(true);
		server.bind(new InetSocketAddress("127.0.0.1", 0), 16);
		this.held.add(server);
		Thread.ofVirtual().name("e2e-eof-accept").start(() -> {
			while (!server.isClosed()) {
				try {
					Socket socket = server.accept();
					this.held.add(socket);
					Thread.ofVirtual().name("e2e-eof-conn").start(() -> handler.accept(socket));
				}
				catch (Exception e) {
					return;
				}
			}
		});
		return server.getLocalPort();
	}

	/** Echoes until EOF; latches when the peer disappears. */
	private void echoUntilEof(Socket socket) {
		try (socket; InputStream in = socket.getInputStream(); OutputStream out = socket.getOutputStream()) {
			byte[] buffer = new byte[1024];
			int n;
			while ((n = in.read(buffer)) > 0) {
				out.write(buffer, 0, n);
				out.flush();
			}
		}
		catch (Exception e) {
			// the peer went away
		}
		this.upstreamClosed.countDown();
	}

	/** Writes as fast as it can; latches when the write fails. */
	private void flood(Socket socket) {
		byte[] chunk = new byte[64 * 1024];
		try {
			OutputStream out = socket.getOutputStream();
			while (true) {
				out.write(chunk);
			}
		}
		catch (Exception e) {
			this.upstreamClosed.countDown();
		}
	}

}
