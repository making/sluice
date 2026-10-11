package am.ik.sluice.it;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

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
import am.ik.sluice.server.tunnel.TunnelSession;
import am.ik.sluice.tunnel.SessionSender;
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
		properties.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		properties.add("sluice.data-port", () -> String.valueOf(dataPort));
		properties.add("sluice.tcp-port-range", () -> String.valueOf(sinkRoutePort));
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
	 * parked in {@code SessionSender.enqueue} forever: the session leaves the registry
	 * and every virtual connection closes within 5 seconds.
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
		TunnelSession session = registry.all()
			.stream()
			.filter(candidate -> candidate.connectedAt().isAfter(before))
			.max(java.util.Comparator.comparing(TunnelSession::connectedAt))
			.orElseThrow();
		sunk.set(0);
		try (Socket victim = new Socket("127.0.0.1", sinkRoutePort)) {
			Thread.ofVirtual().name("e2e-bp-pusher").start(() -> push(victim));
			// prove the relay is flowing, then keep it under load for a while: the
			// teardown below must work with frames in flight and queues filling up
			Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> sunk.get() > 1024L * 1024);
			Thread.sleep(5000);
			started.stop();
			Awaitility.await()
				.atMost(Duration.ofSeconds(5))
				.alias("session leaves the registry")
				.until(() -> registry.count() == 0);
			Awaitility.await()
				.atMost(Duration.ofSeconds(5))
				.alias("virtual connections closed")
				.until(() -> session.connectionCount() == 0);
			Awaitility.await()
				.atMost(Duration.ofSeconds(5))
				.alias("no thread parked in SessionSender.enqueue")
				.until(() -> !anyThreadInSessionSenderEnqueue());
		}
	}

	private static boolean anyThreadInSessionSenderEnqueue() {
		for (Map.Entry<Thread, StackTraceElement[]> entry : Thread.getAllStackTraces().entrySet()) {
			for (StackTraceElement frame : entry.getValue()) {
				if (frame.getClassName().equals(SessionSender.class.getName())
						&& frame.getMethodName().equals("enqueue")) {
					return true;
				}
			}
		}
		return false;
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

}
