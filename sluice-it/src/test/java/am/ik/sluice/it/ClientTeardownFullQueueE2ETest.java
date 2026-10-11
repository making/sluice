package am.ik.sluice.it;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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
 * Repro: the client -> server direction is stalled (the public peer never reads), so the
 * client sender queue is full and the client relay is parked in SessionSender.enqueue.
 * When the server vanishes, NodeConnection#closeConnections closes each connection BEFORE
 * the sender, and VirtualConnection#close -> sendClose parks on the full queue.
 */
class ClientTeardownFullQueueE2ETest {

	private final CountDownLatch upstreamClosed = new CountDownLatch(1);

	private final List<Socket> held = new CopyOnWriteArrayList<>();

	private @Nullable ConfigurableApplicationContext serverContext;

	private @Nullable TunnelClient client;

	private @Nullable ServerSocket upstreamServer;

	@AfterEach
	void tearDown() throws Exception {
		if (this.client != null) {
			Thread.ofVirtual().start(this.client::stop);
		}
		if (this.serverContext != null) {
			this.serverContext.close();
		}
		if (this.upstreamServer != null) {
			this.upstreamServer.close();
		}
		for (Socket socket : this.held) {
			socket.close();
		}
	}

	@Test
	void clientTeardownCompletesWithFullSenderQueue() throws Exception {
		int upstreamPort = startFloodingUpstream();
		int grpcPort = TestPorts.freePort();
		int dataPort = TestPorts.freePort();
		int listenPort = TestPorts.freePort();
		this.serverContext = new SpringApplicationBuilder(SluiceServerApplication.class).run("--server.port=0",
				"--spring.grpc.server.port=" + grpcPort, "--sluice.data-port=" + dataPort,
				"--sluice.tcp-port-range=" + listenPort, "--sluice.token=it-token",
				"--spring.grpc.server.shutdown.grace-period=1s");
		TunnelClient started = TunnelClient.builder()
			.properties(SluiceClientProperties.builder()
				.serverUrl("grpc://127.0.0.1:" + grpcPort)
				.upstream(Upstream.builder()
					.host("flood.local")
					.target("tcp://127.0.0.1:" + upstreamPort)
					.listenPort(listenPort)
					.build())
				.token("it-token")
				.build())
			.taskExecutor(task -> Thread.ofVirtual().name("repro-worker").start(task))
			.meterRegistry(new SimpleMeterRegistry())
			.build();
		started.start();
		this.client = started;
		Awaitility.await().atMost(Duration.ofSeconds(10)).until(started::isConnected);

		// public peer that never reads
		Socket peer = new Socket("127.0.0.1", listenPort);
		this.held.add(peer);
		peer.getOutputStream().write("hello".getBytes());
		peer.getOutputStream().flush();

		// precondition: the client relay is parked on the full client sender queue
		Awaitility.await()
			.atMost(Duration.ofSeconds(30))
			.alias("client relay parked in SessionSender.enqueue")
			.until(() -> anyThreadIn("am.ik.sluice.tunnel.SessionSender", "enqueueData"));

		this.serverContext.close();
		this.serverContext = null;

		boolean closed = this.upstreamClosed.await(10, TimeUnit.SECONDS);
		boolean stuck = anyThreadIn("am.ik.sluice.client.tunnel.NodeConnection", "closeConnections");
		assertThat(stuck).as("no thread stuck in NodeConnection.closeConnections").isFalse();
		assertThat(closed).as("upstream socket closed after the tunnel stream ended").isTrue();
	}

	/** Includes virtual threads, unlike Thread.getAllStackTraces. */
	static boolean anyThreadIn(String className, String methodName) throws Exception {
		java.nio.file.Path file = java.nio.file.Files.createTempDirectory("dump").resolve("threads.txt");
		java.lang.management.ManagementFactory.getPlatformMXBean(com.sun.management.HotSpotDiagnosticMXBean.class)
			.dumpThreads(file.toString(), com.sun.management.HotSpotDiagnosticMXBean.ThreadDumpFormat.TEXT_PLAIN);
		return java.nio.file.Files.readString(file).contains(className + "." + methodName + "(");
	}

	/** Upstream that writes as fast as it can; latches when the write fails. */
	private int startFloodingUpstream() throws Exception {
		ServerSocket server = new ServerSocket();
		server.setReuseAddress(true);
		server.bind(new InetSocketAddress("127.0.0.1", 0), 16);
		this.upstreamServer = server;
		Thread.ofPlatform().daemon().name("repro-accept").start(() -> {
			while (!server.isClosed()) {
				try {
					Socket socket = server.accept();
					this.held.add(socket);
					Thread.ofPlatform().daemon().name("repro-flood").start(() -> {
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
					});
				}
				catch (Exception e) {
					return;
				}
			}
		});
		return server.getLocalPort();
	}

}
