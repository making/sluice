package am.ik.sluice.it;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import com.sun.management.HotSpotDiagnosticMXBean;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.task.TaskExecutor;

import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.client.config.Upstream;
import am.ik.sluice.client.tunnel.TunnelClient;
import am.ik.sluice.server.SluiceServerApplication;
import am.ik.sluice.server.tunnel.SessionRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The server -> client direction is bounded (the gRPC send path honors the peer's flow
 * control), and a tunnel stream dying with the sender queue full still tears the session
 * and its virtual connections down.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class TunnelBackpressureE2ETest {

	private static final TaskExecutor TASK_EXECUTOR = task -> Thread.ofVirtual().name("e2e-bp-worker").start(task);

	private static int grpcPort;

	private static int dataPort;

	private static int sinkRoutePort;

	private static int stallRoutePort;

	private static int echoRoutePort;

	/** The never-read sockets of the current stall test, closed to end the local side. */
	private static final List<Socket> stallUpstreams = new java.util.concurrent.CopyOnWriteArrayList<>();

	private static final AtomicLong sunk = new AtomicLong();

	/**
	 * Keeps never-read upstream sockets reachable: an unreachable socket is closed by GC.
	 */
	private static final List<Socket> held = new java.util.concurrent.CopyOnWriteArrayList<>();

	@Autowired
	private SessionRegistry registry;

	private @Nullable TunnelClient client;

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry properties) {
		grpcPort = TestPorts.freePort();
		dataPort = TestPorts.freePort();
		sinkRoutePort = TestPorts.freePort();
		stallRoutePort = TestPorts.freePort();
		echoRoutePort = TestPorts.freePort();
		properties.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		properties.add("sluice.data-port", () -> String.valueOf(dataPort));
		properties.add("sluice.tcp-port-range", () -> sinkRoutePort + "," + stallRoutePort + "," + echoRoutePort);
		properties.add("sluice.token", () -> "it-token");
		properties.add("server.port", () -> 0);
	}

	@AfterEach
	void tearDown() {
		TunnelClient client = this.client;
		if (client != null) {
			client.stop();
			this.client = null;
		}
	}

	/**
	 * With the client stalled (its upstream never reads), the public peer pushing into
	 * the tunnel must be backpressured by the bounded server -> client path instead of
	 * being buffered without limit.
	 */
	@Test
	void serverToClientDirectionIsBackpressured() throws Exception {
		int sink = upstream(TunnelBackpressureE2ETest::neverRead);
		startClient(List.of(route("sink.local", sink, sinkRoutePort)));
		sunk.set(0);
		try (Socket victim = new Socket("127.0.0.1", sinkRoutePort)) {
			Thread.ofVirtual().name("e2e-bp-pusher").start(() -> push(victim));
			Thread.sleep(10_000);
			long pushed = sunk.get();
			assertThat(pushed).as("bytes accepted from a peer whose upstream never reads")
				.isLessThan(128L * 1024 * 1024);
		}
	}

	/**
	 * A client vanishing while the server sender queue is full must not leave threads
	 * parked in {@code SessionSender.enqueueData} forever: the session leaves the
	 * registry, no relay stays parked, and the public peer socket is released (EOF /
	 * reset).
	 */
	@Test
	void deadStreamReleasesSessionConnectionsAndThreads() throws Exception {
		int sink = upstream(TunnelBackpressureE2ETest::neverRead);
		Instant before = Instant.now();
		TunnelClient started = newClient(List.of(route("sink.local", sink, sinkRoutePort)));
		started.start();
		this.client = started;
		Awaitility.await().atMost(Duration.ofSeconds(10)).until(started::isConnected);
		Awaitility.await()
			.atMost(Duration.ofSeconds(10))
			.until(() -> registry.all().stream().anyMatch(session -> session.connectedAt().isAfter(before)));
		sunk.set(0);
		try (Socket victim = new Socket("127.0.0.1", sinkRoutePort)) {
			Thread.ofVirtual().name("e2e-bp-pusher").start(() -> push(victim));
			AtomicBoolean peerReleased = new AtomicBoolean();
			Thread peerReader = Thread.ofVirtual().name("e2e-bp-peer-reader").start(() -> {
				try {
					while (victim.getInputStream().read() != -1) {
						// drain until the tunnel teardown reaches the peer socket
					}
					peerReleased.set(true);
				}
				catch (IOException e) {
					peerReleased.set(true); // reset counts as released
				}
			});
			// prove the relay is flowing, then keep it under load for a while: the
			// teardown below must work with frames in flight and queues full
			Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> sunk.get() > 1024L * 1024);
			Thread.sleep(5000);
			// precondition: a relay is parked on the full sender queue; the dump includes
			// virtual threads, unlike Thread.getAllStackTraces
			Awaitility.await()
				.atMost(Duration.ofSeconds(10))
				.alias("server relay parked in SessionSender.enqueueData")
				.until(() -> anyThreadIn("am.ik.sluice.tunnel.SessionSender", "enqueueData"));
			started.stop();
			Awaitility.await()
				.atMost(Duration.ofSeconds(5))
				.alias("session leaves the registry")
				.until(() -> registry.count() == 0);
			Awaitility.await()
				.atMost(Duration.ofSeconds(5))
				.alias("no thread parked in SessionSender.enqueueData")
				.until(() -> !anyThreadIn("am.ik.sluice.tunnel.SessionSender", "enqueueData"));
			Awaitility.await()
				.atMost(Duration.ofSeconds(5))
				.alias("public peer socket released (EOF / reset)")
				.untilTrue(peerReleased);
			peerReader.join(TimeUnit.SECONDS.toMillis(1));
		}
	}

	/**
	 * A connection whose local side ends while the remote keeps pushing must not leave
	 * the session's receive path parked on the full inbound bound: the producer waiting
	 * in {@code acceptData} wakes on close and another connection on the same stream
	 * keeps working.
	 */
	@Test
	void localCloseWhileReceivingKeepsOtherConnectionsWorking() throws Exception {
		int stall = upstream(TunnelBackpressureE2ETest::holdUnread);
		int echo = upstream(TunnelBackpressureE2ETest::echo);
		startClient(List.of(route("stall.local", stall, stallRoutePort), route("echo.local", echo, echoRoutePort)));
		stallUpstreams.clear();
		try (Socket victim = new Socket("127.0.0.1", stallRoutePort)) {
			Thread.ofVirtual().name("e2e-bp-stall-pusher").start(() -> push(victim));
			// the stalled upstream never reads, the inbound bound fills and the producer
			// parks inside acceptData; sightings on two consecutive dumps pin the park
			Awaitility.await()
				.atMost(Duration.ofSeconds(30))
				.alias("producer parked in VirtualConnection.acceptData")
				.until(() -> anyThreadIn("am.ik.sluice.tunnel.VirtualConnection", "acceptData") && parkedAgain());
			// the local side ends: the relay's writes to the upstream fail and it closes
			// the connection while the remote keeps pushing
			for (Socket socket : stallUpstreams) {
				socket.close();
			}
			try (Socket peer = new Socket("127.0.0.1", echoRoutePort)) {
				peer.setSoTimeout(10_000);
				OutputStream out = peer.getOutputStream();
				out.write("ping".getBytes());
				out.flush();
				byte[] echoed = new byte[4];
				InputStream in = peer.getInputStream();
				int read = 0;
				while (read < echoed.length) {
					int n = in.read(echoed, read, echoed.length - read);
					assertThat(n).as("echo reply arrives").isPositive();
					read += n;
				}
				assertThat(new String(echoed, java.nio.charset.StandardCharsets.UTF_8)).isEqualTo("ping");
			}
		}
	}

	/** Second park sighting, after a pause, for the stability check above. */
	private static boolean parkedAgain() throws Exception {
		Thread.sleep(500);
		return anyThreadIn("am.ik.sluice.tunnel.VirtualConnection", "acceptData");
	}

	/** Includes virtual threads, unlike Thread.getAllStackTraces. */
	private static boolean anyThreadIn(String className, String methodName) throws Exception {
		Path dump = Files.createTempDirectory("sluice-thread-dump").resolve("threads.txt");
		try {
			// dumpThreads refuses an existing path, hence the fresh directory per call
			ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class)
				.dumpThreads(dump.toString(), HotSpotDiagnosticMXBean.ThreadDumpFormat.TEXT_PLAIN);
			return Files.readString(dump).contains(className + "." + methodName + "(");
		}
		finally {
			Files.deleteIfExists(dump);
			Files.deleteIfExists(dump.getParent());
		}
	}

	private static void push(Socket socket) {
		byte[] chunk = new byte[64 * 1024];
		try {
			java.io.OutputStream out = socket.getOutputStream();
			while (true) {
				out.write(chunk);
				sunk.addAndGet(chunk.length);
			}
		}
		catch (Exception e) {
			// torn down
		}
	}

	private void startClient(List<Upstream> upstreams) {
		TunnelClient started = newClient(upstreams);
		started.start();
		this.client = started;
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(started::isConnected);
	}

	private static TunnelClient newClient(List<Upstream> upstreams) {
		SluiceClientProperties.Builder properties = SluiceClientProperties.builder()
			.serverUrl("grpc://127.0.0.1:" + grpcPort);
		upstreams.forEach(properties::upstream);
		return TunnelClient.builder()
			.properties(properties.token("it-token").build())
			.taskExecutor(TASK_EXECUTOR)
			.meterRegistry(new SimpleMeterRegistry())
			.build();
	}

	private static Upstream route(String host, int upstreamPort, int listenPort) {
		return Upstream.builder().host(host).target("tcp://127.0.0.1:" + upstreamPort).listenPort(listenPort).build();
	}

	/** Starts a raw upstream serving every accepted socket with the handler. */
	private static int upstream(Consumer<Socket> handler) throws Exception {
		ServerSocket server = new ServerSocket();
		server.setReuseAddress(true);
		server.bind(new InetSocketAddress("127.0.0.1", 0), 128);
		Thread.ofVirtual().name("e2e-bp-accept").start(() -> {
			while (!server.isClosed()) {
				try {
					Socket socket = server.accept();
					Thread.ofVirtual().name("e2e-bp-conn").start(() -> handler.accept(socket));
				}
				catch (Exception e) {
					return;
				}
			}
		});
		return server.getLocalPort();
	}

	/** Accepts and never reads. */
	private static void neverRead(Socket socket) {
		held.add(socket);
		try {
			Thread.sleep(Duration.ofMinutes(5));
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	/** Accepts, keeps the socket closeable by the test, and never reads. */
	private static void holdUnread(Socket socket) {
		held.add(socket);
		stallUpstreams.add(socket);
		try {
			Thread.sleep(Duration.ofMinutes(5));
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	/** Echoes every chunk back. */
	private static void echo(Socket socket) {
		try (socket) {
			byte[] buf = new byte[8 * 1024];
			InputStream in = socket.getInputStream();
			OutputStream out = socket.getOutputStream();
			int n;
			while ((n = in.read(buf)) != -1) {
				out.write(buf, 0, n);
				out.flush();
			}
		}
		catch (Exception e) {
			// torn down
		}
	}

}
