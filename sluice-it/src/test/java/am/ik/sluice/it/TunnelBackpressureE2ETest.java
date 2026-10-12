package am.ik.sluice.it;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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

	private static int answerRoutePort;

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
		answerRoutePort = TestPorts.freePort();
		properties.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		properties.add("sluice.data-port", () -> String.valueOf(dataPort));
		properties.add("sluice.tcp-port-range",
				() -> sinkRoutePort + "," + stallRoutePort + "," + echoRoutePort + "," + answerRoutePort);
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
	 * being buffered without limit: its writes stop being accepted, the connection still
	 * open, well before 128MiB.
	 */
	@Test
	void serverToClientDirectionIsBackpressured() throws Exception {
		int sink = upstream(TunnelBackpressureE2ETest::neverRead);
		startClient(List.of(route("sink.local", sink, sinkRoutePort)));
		sunk.set(0);
		try (Socket victim = new Socket("127.0.0.1", sinkRoutePort)) {
			Thread pusher = Thread.ofVirtual().name("e2e-bp-pusher").start(() -> push(victim));
			Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> sunk.get() > 1024L * 1024);
			long pushed = awaitNoGrowth(sunk, Duration.ofSeconds(1), Duration.ofSeconds(10));
			assertThat(pusher.isAlive()).as("the pusher is blocked in a write, not torn down").isTrue();
			assertThat(pushed).as("bytes accepted from a peer whose upstream never reads")
				.isLessThan(128L * 1024 * 1024);
		}
	}

	/**
	 * Waits until {@code counter} did not grow for {@code quiet}, failing after
	 * {@code atMost}; its value then.
	 */
	private static long awaitNoGrowth(AtomicLong counter, Duration quiet, Duration atMost) {
		AtomicLong last = new AtomicLong(counter.get());
		AtomicLong changedAt = new AtomicLong(System.nanoTime());
		Awaitility.await().pollInterval(Duration.ofMillis(50)).atMost(atMost).until(() -> {
			long now = counter.get();
			if (now != last.getAndSet(now)) {
				changedAt.set(System.nanoTime());
			}
			return System.nanoTime() - changedAt.get() >= quiet.toNanos();
		});
		return last.get();
	}

	/**
	 * A client vanishing while the server relay waits for send credit must not leave it
	 * parked forever: the session leaves the registry, no relay stays parked, and the
	 * public peer socket is released (EOF / reset).
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
			// prove the relay is flowing, then let it fill up: the teardown below must
			// work with frames in flight and queues full, which holds the pusher back
			Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> sunk.get() > 1024L * 1024);
			awaitNoGrowth(sunk, Duration.ofSeconds(1), Duration.ofSeconds(10));
			// precondition: the relay waits for the credit the stalled client withholds
			Awaitility.await()
				.atMost(Duration.ofSeconds(10))
				.alias("server relay parked in VirtualConnection.awaitCredit")
				.until(ThreadDumps::anyThreadWaitingForCredit);
			started.stop();
			Awaitility.await()
				.atMost(Duration.ofSeconds(5))
				.alias("session leaves the registry")
				.until(() -> registry.count() == 0);
			Awaitility.await()
				.atMost(Duration.ofSeconds(5))
				.alias("no thread parked in VirtualConnection.awaitCredit")
				.until(() -> !ThreadDumps.anyThreadWaitingForCredit());
			Awaitility.await()
				.atMost(Duration.ofSeconds(5))
				.alias("public peer socket released (EOF / reset)")
				.untilTrue(peerReleased);
			peerReader.join(TimeUnit.SECONDS.toMillis(1));
		}
	}

	/**
	 * A connection whose local side ends while the remote keeps pushing makes the
	 * remote's writer, waiting for credit that the closed side will never grant, give up;
	 * another connection on the same stream keeps working.
	 */
	@Test
	void localCloseWhileReceivingReleasesTheRemoteWriter() throws Exception {
		int stall = upstream(TunnelBackpressureE2ETest::holdUnread);
		int echo = upstream(TunnelBackpressureE2ETest::echo);
		startClient(List.of(route("stall.local", stall, stallRoutePort), route("echo.local", echo, echoRoutePort)));
		stallUpstreams.clear();
		try (Socket victim = new Socket("127.0.0.1", stallRoutePort)) {
			Thread.ofVirtual().name("e2e-bp-stall-pusher").start(() -> push(victim));
			// the stalled upstream never reads, the client stops granting and the server
			// relay parks waiting for credit; sightings on two consecutive dumps pin it
			Awaitility.await()
				.atMost(Duration.ofSeconds(30))
				.alias("server relay parked in VirtualConnection.awaitCredit")
				.until(() -> ThreadDumps.anyThreadWaitingForCredit() && parkedAgain());
			// the local side ends: the relay's writes to the upstream fail and it closes
			// the connection while the remote keeps pushing
			for (Socket socket : stallUpstreams) {
				socket.close();
			}
			Awaitility.await()
				.atMost(Duration.ofSeconds(5))
				.alias("the server relay leaves the credit wait")
				.until(() -> !ThreadDumps.anyThreadWaitingForCredit());
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

	/**
	 * An upstream that answers and closes without reading the upload (a 413, say) must
	 * not leave the server's writer waiting for credit: the client's side of the
	 * connection is gone, so it tells the server with an ERROR, and the public peer gets
	 * the answer and the end of the connection.
	 */
	@Test
	void upstreamClosingWithoutReadingReleasesTheUploadingPeer() throws Exception {
		CountDownLatch uploadStalled = new CountDownLatch(1);
		CountDownLatch answer = new CountDownLatch(1);
		int answering = upstream(socket -> answerWithoutReading(socket, uploadStalled, answer));
		startClient(List.of(route("answer.local", answering, answerRoutePort)));
		try (Socket peer = new Socket("127.0.0.1", answerRoutePort)) {
			peer.setSoTimeout(10_000);
			Thread.ofVirtual().name("e2e-bp-uploader").start(() -> push(peer));
			uploadStalled.countDown();
			// precondition: the upload is held by the credit the client withholds
			Awaitility.await()
				.atMost(Duration.ofSeconds(30))
				.alias("server relay parked in VirtualConnection.awaitCredit")
				.until(() -> ThreadDumps.anyThreadWaitingForCredit() && parkedAgain());
			answer.countDown();
			byte[] response = peer.getInputStream().readNBytes(ANSWER.length);
			assertThat(new String(response, java.nio.charset.StandardCharsets.US_ASCII))
				.isEqualTo(new String(ANSWER, java.nio.charset.StandardCharsets.US_ASCII));
			Awaitility.await()
				.atMost(Duration.ofSeconds(5))
				.alias("the server relay leaves the credit wait")
				.until(() -> !ThreadDumps.anyThreadWaitingForCredit());
			try {
				assertThat(peer.getInputStream().read()).as("the peer sees the end").isEqualTo(-1);
			}
			catch (IOException e) {
				// a reset ends the connection too
			}
		}
	}

	private static final byte[] ANSWER = "HTTP/1.1 413 Content Too Large\r\ncontent-length: 0\r\n\r\n"
		.getBytes(java.nio.charset.StandardCharsets.US_ASCII);

	/**
	 * Never reads; once told to, answers, half-closes (the client sends CLOSE) and then
	 * closes with the upload unread.
	 */
	private static void answerWithoutReading(Socket socket, CountDownLatch uploadStalled, CountDownLatch answer) {
		try (socket) {
			uploadStalled.await();
			answer.await();
			OutputStream out = socket.getOutputStream();
			out.write(ANSWER);
			out.flush();
			socket.shutdownOutput();
			Thread.sleep(500);
		}
		catch (Exception e) {
			// torn down
		}
	}

	/** Second park sighting, after a pause, for the stability check above. */
	private static boolean parkedAgain() throws Exception {
		Thread.sleep(500);
		return ThreadDumps.anyThreadWaitingForCredit();
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
