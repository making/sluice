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
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * One stalled virtual connection must not stall the other virtual connections of the
 * same tunnel stream.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class HeadOfLineBlockingE2ETest {

	private static final TaskExecutor TASK_EXECUTOR = task -> Thread.ofVirtual().name("e2e-hol-worker").start(task);

	private static int grpcPort;

	private static int dataPort;

	private static int echoRoutePort;

	private static int floodRoutePort;

	private static int sinkRoutePort;

	private static final AtomicLong flooded = new AtomicLong();

	private static final AtomicLong sunk = new AtomicLong();

	/** Keeps never-read upstream sockets reachable: an unreachable socket is closed by GC. */
	private static final List<Socket> held = new java.util.concurrent.CopyOnWriteArrayList<>();

	private @Nullable TunnelClient client;

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) {
		grpcPort = TestPorts.freePort();
		dataPort = TestPorts.freePort();
		echoRoutePort = TestPorts.freePort();
		floodRoutePort = TestPorts.freePort();
		sinkRoutePort = TestPorts.freePort();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(dataPort));
		registry.add("sluice.tcp-port-range", () -> echoRoutePort + "," + floodRoutePort + "," + sinkRoutePort);
		registry.add("sluice.token", () -> "it-token");
		registry.add("server.port", () -> 0);
	}

	@AfterEach
	void tearDown() {
		if (this.client != null) {
			this.client.stop();
		}
	}

	/**
	 * Server side: a public peer that never reads a large response fills its virtual
	 * connection's inbound queue; the gRPC receive callback then blocks in acceptData.
	 */
	@Test
	void slowPublicReaderDoesNotStallOtherConnections() throws Exception {
		int flood = upstream(HeadOfLineBlockingE2ETest::flood);
		int echo = upstream(HeadOfLineBlockingE2ETest::echo);
		startClient(List.of(route("flood.local", flood, floodRoutePort), route("echo.local", echo, echoRoutePort)));
		assertThat(echoes(Duration.ofSeconds(5))).as("echo works before the stall").isTrue();
		try (Socket victim = new Socket()) {
			victim.setReceiveBufferSize(4096);
			victim.connect(new InetSocketAddress("127.0.0.1", floodRoutePort));
			victim.getOutputStream().write('x'); // never reads
			awaitPlateau(flooded);
			// the server-side inbound queue alone holds 128 x 64KiB
			assertThat(flooded.get()).as("flooded bytes").isGreaterThan(8L * 1024 * 1024);
			dumpBlocked();
			assertThat(echoes(Duration.ofSeconds(5))).as("echo while another connection stalls").isTrue();
		}
	}

	/**
	 * Client side: an upstream that never reads fills the client-side inbound queue of its
	 * virtual connection; the client's gRPC receive callback then blocks, and CONNECT
	 * frames for other connections are no longer delivered.
	 */
	@Test
	void slowUpstreamReaderDoesNotStallOtherConnections() throws Exception {
		int sink = upstream(HeadOfLineBlockingE2ETest::neverRead);
		int echo = upstream(HeadOfLineBlockingE2ETest::echo);
		startClient(List.of(route("sink.local", sink, sinkRoutePort), route("echo.local", echo, echoRoutePort)));
		assertThat(echoes(Duration.ofSeconds(5))).as("echo works before the stall").isTrue();
		sunk.set(0);
		try (Socket victim = new Socket("127.0.0.1", sinkRoutePort)) {
			Thread.ofVirtual().name("e2e-hol-pusher").start(() -> push(victim));
			// well past every bounded buffer on the path (sender queue 16MiB, inbound queue
			// 8MiB, gRPC windows, socket buffers)
			Awaitility.await().atMost(Duration.ofSeconds(30)).until(() -> sunk.get() > 64L * 1024 * 1024);
			dumpBlocked();
			assertThat(echoes(Duration.ofSeconds(5))).as("echo while another connection stalls").isTrue();
		}
	}

	/**
	 * The server -> client direction is bounded: with the client stalled, the public peer
	 * pushing into the tunnel must be backpressured instead of buffered without limit.
	 */
	@Test
	void serverToClientDirectionIsBackpressured() throws Exception {
		int sink = upstream(HeadOfLineBlockingE2ETest::neverRead);
		startClient(List.of(route("sink.local", sink, sinkRoutePort)));
		sunk.set(0);
		long directBefore = io.netty.util.internal.PlatformDependent.usedDirectMemory();
		try (Socket victim = new Socket("127.0.0.1", sinkRoutePort)) {
			Thread.ofVirtual().name("e2e-hol-pusher").start(() -> push(victim));
			Thread.sleep(10_000);
			long pushed = sunk.get();
			long direct = io.netty.util.internal.PlatformDependent.usedDirectMemory() - directBefore;
			System.out.println("HOLDUMP pushed=" + pushed / (1024 * 1024) + "MiB nettyDirectDelta="
					+ direct / (1024 * 1024) + "MiB heapUsed="
					+ (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / (1024 * 1024) + "MiB");
			assertThat(pushed).as("bytes accepted from a peer whose upstream never reads")
				.isLessThan(128L * 1024 * 1024);
		}
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

	private static void dumpBlocked() {
		Thread.getAllStackTraces().forEach((thread, stack) -> {
			for (StackTraceElement frame : stack) {
				if (frame.getMethodName().equals("acceptData") || frame.getMethodName().equals("awaitReady")
						|| frame.getMethodName().equals("enqueue")) {
					System.out.println("HOLDUMP thread=" + thread.getName() + " at " + frame);
					for (int i = 0; i < Math.min(stack.length, 14); i++) {
						System.out.println("HOLDUMP   " + stack[i]);
					}
					break;
				}
			}
		});
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
			.meterRegistry(new SimpleMeterRegistry())
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
			byte[] buffer = new byte[1024];
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
