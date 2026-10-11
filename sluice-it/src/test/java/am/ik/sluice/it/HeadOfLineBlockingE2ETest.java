package am.ik.sluice.it;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import org.springframework.core.task.TaskExecutor;

import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.client.config.Upstream;
import am.ik.sluice.client.tunnel.TunnelClient;
import am.ik.sluice.server.SluiceServerApplication;
import am.ik.sluice.tunnel.VirtualConnection;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * One stalled virtual connection must not stall the other virtual connections of the same
 * tunnel stream: per-connection credit flow control holds the stalled connection's writer
 * back on its own thread instead of blocking the shared receive path.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class HeadOfLineBlockingE2ETest {

	private static final TaskExecutor TASK_EXECUTOR = task -> Thread.ofVirtual().name("e2e-hol-worker").start(task);

	/** Moves more than a window each way, so both sides grant credit back. */
	private static final int WARM_UP_BYTES = 4 * VirtualConnection.DEFAULT_WINDOW;

	private static int grpcPort;

	private static int echoRoutePort;

	private static int floodRoutePort;

	private static int sinkRoutePort;

	private static final AtomicLong flooded = new AtomicLong();

	private static final AtomicLong sunk = new AtomicLong();

	/**
	 * Keeps never-read upstream sockets reachable: an unreachable socket is closed by GC.
	 */
	private static final List<Socket> held = new CopyOnWriteArrayList<>();

	private final MeterRegistry clientMeters = new SimpleMeterRegistry();

	private @Nullable TunnelClient client;

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) {
		grpcPort = TestPorts.freePort();
		echoRoutePort = TestPorts.freePort();
		floodRoutePort = TestPorts.freePort();
		sinkRoutePort = TestPorts.freePort();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(TestPorts.freePort()));
		registry.add("sluice.tcp-port-range", () -> echoRoutePort + "," + floodRoutePort + "," + sinkRoutePort);
		registry.add("sluice.token", () -> "it-token");
		registry.add("server.port", () -> 0);
	}

	@AfterEach
	void tearDown() throws Exception {
		TunnelClient client = this.client;
		if (client != null) {
			client.stop();
			this.client = null;
		}
		for (Socket socket : held) {
			socket.close();
		}
		held.clear();
	}

	/**
	 * Server side: a public peer that never reads a large response. The client's writer
	 * for that connection waits for credit; the server's receive path stays free.
	 */
	@Test
	void slowPublicReaderDoesNotStallOtherConnections() throws Exception {
		int flood = upstream(HeadOfLineBlockingE2ETest::flood);
		int echo = upstream(HeadOfLineBlockingE2ETest::echo);
		startClient(List.of(route("flood.local", flood, floodRoutePort), route("echo.local", echo, echoRoutePort)));
		assertWindowedStream();
		flooded.set(0);
		try (Socket victim = new Socket()) {
			victim.setReceiveBufferSize(4096);
			victim.connect(new InetSocketAddress("127.0.0.1", floodRoutePort));
			victim.getOutputStream().write('x'); // never reads
			awaitPlateau(flooded);
			assertThat(flooded.get()).as("flooded past one window: the stall is held by the credit")
				.isGreaterThan(VirtualConnection.DEFAULT_WINDOW);
			assertThat(ThreadDumps.anyThreadWaitingForCredit()).as("a writer waits for credit").isTrue();
			assertThat(echoes(Duration.ofSeconds(5))).as("echo while another connection stalls").isTrue();
		}
	}

	/**
	 * Client side: an upstream that never reads a large request body. The server's writer
	 * for that connection waits for credit; the client's receive path, which also
	 * delivers the CONNECT of every other connection, stays free.
	 */
	@Test
	void slowUpstreamReaderDoesNotStallOtherConnections() throws Exception {
		int sink = upstream(HeadOfLineBlockingE2ETest::neverRead);
		int echo = upstream(HeadOfLineBlockingE2ETest::echo);
		startClient(List.of(route("sink.local", sink, sinkRoutePort), route("echo.local", echo, echoRoutePort)));
		assertWindowedStream();
		sunk.set(0);
		try (Socket victim = new Socket("127.0.0.1", sinkRoutePort)) {
			Thread.ofVirtual().name("e2e-hol-pusher").start(() -> push(victim));
			awaitPlateau(sunk);
			assertThat(sunk.get()).as("pushed past one window: the stall is held by the credit")
				.isGreaterThan(VirtualConnection.DEFAULT_WINDOW);
			assertThat(ThreadDumps.anyThreadWaitingForCredit()).as("a writer waits for credit").isTrue();
			assertThat(echoes(Duration.ofSeconds(5))).as("echo while another connection stalls").isTrue();
		}
	}

	/**
	 * Pins the windowed mode of the stream: a bulk echo makes both sides consume more
	 * than half a window, and the client counts grants both ways.
	 */
	private void assertWindowedStream() throws Exception {
		try (Socket socket = new Socket("127.0.0.1", echoRoutePort)) {
			socket.setSoTimeout(10_000);
			Thread.ofVirtual().name("e2e-hol-warm-up").start(() -> {
				try {
					OutputStream out = socket.getOutputStream();
					out.write(new byte[WARM_UP_BYTES]);
					out.flush();
				}
				catch (Exception e) {
					// the read below fails then
				}
			});
			assertThat(socket.getInputStream().readNBytes(WARM_UP_BYTES)).hasSize(WARM_UP_BYTES);
		}
		assertThat(windowUpdates("sent")).as("client grants sent").isPositive();
		assertThat(windowUpdates("received")).as("client grants received").isPositive();
	}

	private double windowUpdates(String direction) {
		return this.clientMeters.find("sluice.tunnel.window.updates")
			.tag("direction", direction)
			.counters()
			.stream()
			.mapToDouble(Counter::count)
			.sum();
	}

	private static void push(Socket socket) {
		byte[] chunk = new byte[64 * 1024];
		try {
			OutputStream out = socket.getOutputStream();
			while (true) {
				out.write(chunk);
				sunk.addAndGet(chunk.length);
			}
		}
		catch (Exception e) {
			// torn down
		}
	}

	/** Waits until the counter stops growing for a second: the pipeline is full. */
	private static void awaitPlateau(AtomicLong counter) throws InterruptedException {
		long last = -1;
		long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
		while (System.nanoTime() < deadline) {
			long now = counter.get();
			if (now == last && now > 0) {
				return;
			}
			last = now;
			Thread.sleep(1000);
		}
		throw new AssertionError("counter never plateaued: " + counter.get());
	}

	private static boolean echoes(Duration timeout) throws Exception {
		try (Socket socket = new Socket("127.0.0.1", echoRoutePort)) {
			socket.setSoTimeout((int) timeout.toMillis());
			socket.getOutputStream().write("ping".getBytes(StandardCharsets.US_ASCII));
			socket.getOutputStream().flush();
			byte[] echoed = socket.getInputStream().readNBytes(4);
			return "ping".equals(new String(echoed, StandardCharsets.US_ASCII));
		}
		catch (SocketTimeoutException e) {
			return false;
		}
	}

	private void startClient(List<Upstream> upstreams) {
		SluiceClientProperties.Builder properties = SluiceClientProperties.builder()
			.serverUrl("grpc://127.0.0.1:" + grpcPort);
		upstreams.forEach(properties::upstream);
		TunnelClient started = TunnelClient.builder()
			.properties(properties.token("it-token").build())
			.taskExecutor(TASK_EXECUTOR)
			.meterRegistry(this.clientMeters)
			.build();
		started.start();
		this.client = started;
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(started::isConnected);
	}

	private static Upstream route(String host, int upstreamPort, int listenPort) {
		return Upstream.builder().host(host).target("tcp://127.0.0.1:" + upstreamPort).listenPort(listenPort).build();
	}

	/** Starts a raw upstream serving every accepted socket with the handler. */
	private static int upstream(Consumer<Socket> handler) throws Exception {
		ServerSocket server = new ServerSocket();
		server.setReuseAddress(true);
		server.bind(new InetSocketAddress("127.0.0.1", 0), 128);
		Thread.ofVirtual().name("e2e-hol-accept").start(() -> {
			while (!server.isClosed()) {
				try {
					Socket socket = server.accept();
					Thread.ofVirtual().name("e2e-hol-conn").start(() -> handler.accept(socket));
				}
				catch (Exception e) {
					return;
				}
			}
		});
		return server.getLocalPort();
	}

	/** Writes as fast as the tunnel takes it. */
	private static void flood(Socket socket) {
		held.add(socket);
		byte[] chunk = new byte[64 * 1024];
		try (socket) {
			OutputStream out = socket.getOutputStream();
			while (true) {
				out.write(chunk);
				flooded.addAndGet(chunk.length);
			}
		}
		catch (Exception e) {
			// torn down
		}
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

	private static void echo(Socket socket) {
		try (socket; InputStream in = socket.getInputStream(); OutputStream out = socket.getOutputStream()) {
			byte[] buffer = new byte[64 * 1024];
			int n;
			while ((n = in.read(buffer)) > 0) {
				out.write(buffer, 0, n);
				out.flush();
			}
		}
		catch (Exception e) {
			// torn down
		}
	}

}
