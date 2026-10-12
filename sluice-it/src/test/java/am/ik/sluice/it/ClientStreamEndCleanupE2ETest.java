package am.ik.sluice.it;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

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
 * When the tunnel stream ends, the client must close its relayed virtual connections: the
 * upstream socket sees EOF instead of being held open by a leaked relay.
 */
@TestInstance(Lifecycle.PER_CLASS)
class ClientStreamEndCleanupE2ETest {

	private static final String HOST = "eof.local";

	private int grpcPort;

	private int dataPort;

	private int listenPort;

	private int upstreamPort;

	private final CountDownLatch upstreamSawEof = new CountDownLatch(1);

	private @Nullable ServerSocket upstreamServer;

	private @Nullable ConfigurableApplicationContext serverContext;

	private @Nullable TunnelClient client;

	@AfterEach
	void tearDown() {
		TunnelClient client = this.client;
		if (client != null) {
			client.stop();
			this.client = null;
		}
		ConfigurableApplicationContext serverContext = this.serverContext;
		if (serverContext != null) {
			serverContext.close();
			this.serverContext = null;
		}
		ServerSocket upstreamServer = this.upstreamServer;
		if (upstreamServer != null) {
			try {
				upstreamServer.close();
			}
			catch (Exception e) {
				// already closed
			}
			this.upstreamServer = null;
		}
	}

	@Test
	void upstreamSeesEofWhenServerVanishes() throws Exception {
		startUpstream();
		this.grpcPort = TestPorts.freePort();
		this.dataPort = TestPorts.freePort();
		this.listenPort = TestPorts.freePort();
		this.serverContext = new SpringApplicationBuilder(SluiceServerApplication.class).run("--server.port=0",
				"--spring.grpc.server.port=" + this.grpcPort, "--sluice.data-port=" + this.dataPort,
				"--sluice.tcp-port-range=" + this.listenPort, "--sluice.token=it-token",
				"--spring.grpc.server.shutdown.grace-period=1s",
				// the server ends the stream at once rather than waiting out the drain
				// for the connection held open
				"--sluice.cluster.drain-grace=0s");
		SluiceClientProperties properties = SluiceClientProperties.builder()
			.serverUrl("grpc://127.0.0.1:" + this.grpcPort)
			.upstream(Upstream.builder()
				.host(HOST)
				.target("tcp://127.0.0.1:" + this.upstreamPort)
				.listenPort(this.listenPort)
				.build())
			.token("it-token")
			.build();
		TunnelClient started = TunnelClient.builder()
			.properties(properties)
			.taskExecutor(task -> Thread.ofVirtual().name("e2e-eof-worker").start(task))
			.meterRegistry(new SimpleMeterRegistry())
			.build();
		started.start();
		this.client = started;
		Awaitility.await().atMost(Duration.ofSeconds(10)).until(started::isConnected);

		// relay one round trip so the end-to-end path is up, keep the peer open, then
		// kill
		// the server: the only remaining EOF source for the upstream is the client-side
		// cleanup of the relayed connection
		try (Socket peer = new Socket("127.0.0.1", this.listenPort)) {
			peer.setSoTimeout(10_000);
			OutputStream out = peer.getOutputStream();
			out.write("hello".getBytes(StandardCharsets.US_ASCII));
			out.flush();
			byte[] echoed = peer.getInputStream().readNBytes(5);
			assertThat(new String(echoed, StandardCharsets.US_ASCII)).isEqualTo("hello");

			this.serverContext.close();
			this.serverContext = null;

			// the client-side relay of the connection must end and close the upstream
			// socket
			assertThat(this.upstreamSawEof.await(5, TimeUnit.SECONDS))
				.as("upstream socket closed after the tunnel stream ended")
				.isTrue();
		}
	}

	/** Single-connection echo upstream; latches when the peer disappears. */
	private void startUpstream() throws Exception {
		ServerSocket server = new ServerSocket();
		server.setReuseAddress(true);
		server.bind(new InetSocketAddress("127.0.0.1", 0), 1);
		this.upstreamServer = server;
		this.upstreamPort = server.getLocalPort();
		Thread.ofVirtual().name("e2e-eof-accept").start(() -> {
			while (!server.isClosed()) {
				try {
					Socket socket = server.accept();
					Thread.ofVirtual().name("e2e-eof-conn").start(() -> echoUntilEof(socket));
				}
				catch (Exception e) {
					return;
				}
			}
		});
	}

	private void echoUntilEof(Socket socket) {
		try (socket; InputStream in = socket.getInputStream(); OutputStream out = socket.getOutputStream()) {
			byte[] buffer = new byte[1024];
			int n;
			while ((n = in.read(buffer)) > 0) {
				out.write(buffer, 0, n);
				out.flush();
			}
			this.upstreamSawEof.countDown();
		}
		catch (Exception e) {
			this.upstreamSawEof.countDown();
		}
	}

}
