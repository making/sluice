package am.ik.sluice.it;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.DoubleSupplier;

import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.Timeout;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.util.unit.DataSize;

import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.client.config.Upstream;
import am.ik.sluice.client.tunnel.TunnelClient;
import am.ik.sluice.server.SluiceServerApplication;
import am.ik.sluice.server.route.Router;
import am.ik.sluice.tunnel.VirtualConnection;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The tunnel windows grow against a node-wide budget, on the server and on the client: a
 * connection whose reader stops holds its announced window and what it took of the
 * budget, so many of them hold what they buffer within their allowances and the budget,
 * not a full window each; a connection whose window bounds it alone still grows to the
 * full window; and every exit -- the public client gone, the upstream's reset, an idle
 * timeout -- releases what it took.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
@Timeout(90)
class TunnelBudgetE2ETest {

	private static final long KIB = 1024;

	private static final long MIB = 1024 * KIB;

	/** The budget of the server and of the client. */
	private static final long BUDGET = 2 * MIB;

	/** Connections whose reader stops: unbudgeted, they would hold a window each. */
	private static final int SLOW = 8;

	/** Far more than the tunnel, the relays and the sockets absorb. */
	private static final long BODY = 64 * MIB;

	/** What the connections may buffer: their announced windows and the budget. */
	private static final long BOUND = SLOW * VirtualConnection.INITIAL_WINDOW + BUDGET;

	private static final Duration TIMEOUT = Duration.ofSeconds(30);

	private static int grpcPort;

	private static int dataPort;

	private static int idleRoutePort;

	@Autowired
	private @Nullable MeterRegistry serverMeters;

	@Autowired
	private @Nullable Router router;

	private final List<AutoCloseable> closeables = new CopyOnWriteArrayList<>();

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) {
		grpcPort = TestPorts.freePort();
		dataPort = TestPorts.freePort();
		idleRoutePort = TestPorts.freePort();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(dataPort));
		registry.add("sluice.tcp-port-range", () -> String.valueOf(idleRoutePort));
		registry.add("sluice.limits.tunnel-budget", () -> BUDGET + "B");
		registry.add("sluice.token", () -> "it-token");
		registry.add("server.port", () -> String.valueOf(TestPorts.freePort()));
	}

	@AfterEach
	void tearDown() {
		for (int i = this.closeables.size() - 1; i >= 0; i--) {
			try {
				this.closeables.get(i).close();
			}
			catch (Exception e) {
				// ignore
			}
		}
		this.closeables.clear();
		// what one test's connections took is back before the next starts
		Awaitility.await().atMost(TIMEOUT).until(() -> server("sluice.tunnel.budget.used.bytes") == 0);
	}

	/**
	 * Downloads whose public clients stop reading: the server's windows hold what their
	 * relays have not written, within the announced windows and the budget at every
	 * sample, though they grew while the sockets absorbed; once the clients are gone
	 * everything is released.
	 */
	@Test
	void slowReadingDownloadsBufferWithinTheAllowancesAndTheBudget() throws Exception {
		String host = "slow.download.budget.test";
		startClient(Upstream.builder().host(host).target("tcp://127.0.0.1:" + download(BODY)), grpcPort,
				new SimpleMeterRegistry());
		List<Socket> readers = new ArrayList<>();
		for (int i = 0; i < SLOW; i++) {
			readers.add(stalledReader(host));
		}
		DoubleSupplier buffered = () -> server("sluice.tunnel.buffered.bytes");
		long held = awaitPlateau(buffered, () -> server("sluice.tunnel.budget.used.bytes"));
		System.out.printf("slow downloads: %d connections buffer %d (bound %d), budget used %.0f of %d%n", SLOW, held,
				BOUND, server("sluice.tunnel.budget.used.bytes"), BUDGET);
		assertThat(server("sluice.tunnel.budget.bytes")).as("precondition: the configured budget").isEqualTo(BUDGET);
		assertThat(held).as("precondition: the stalled connections hold their windows")
			.isGreaterThan(SLOW * VirtualConnection.INITIAL_WINDOW / 2);
		assertThat(server("sluice.tunnel.budget.used.bytes")).as("the windows grew into the budget")
			.isPositive()
			.isLessThanOrEqualTo(BUDGET);
		for (Socket reader : readers) {
			reader.close();
		}
		Awaitility.await()
			.atMost(TIMEOUT)
			.alias("released once the clients are gone")
			.until(() -> server("sluice.tunnel.budget.used.bytes") == 0 && buffered.getAsDouble() == 0);
	}

	/**
	 * Uploads to upstreams that stop reading: the client's windows hold what its relays
	 * have not written, within the announced windows and the client's budget.
	 */
	@Test
	void uploadsToStalledUpstreamsBufferWithinTheClientsBudget() throws Exception {
		String host = "slow.upload.budget.test";
		SimpleMeterRegistry clientMeters = new SimpleMeterRegistry();
		startClient(Upstream.builder().host(host).target("tcp://127.0.0.1:" + stalledUpstream()), grpcPort,
				clientMeters);
		for (int i = 0; i < SLOW; i++) {
			upload(host);
		}
		DoubleSupplier buffered = () -> clientMeters.get("sluice.tunnel.buffered.bytes").gauge().value();
		DoubleSupplier used = () -> clientMeters.get("sluice.tunnel.budget.used.bytes").gauge().value();
		long held = awaitPlateau(buffered, used);
		System.out.printf("stalled uploads: %d connections buffer %d on the client (bound %d), budget used %.0f%n",
				SLOW, held, BOUND, used.getAsDouble());
		assertThat(held).as("precondition: the stalled connections hold their windows")
			.isGreaterThan(SLOW * VirtualConnection.INITIAL_WINDOW / 2);
		assertThat(used.getAsDouble()).as("the windows grew into the budget").isPositive().isLessThanOrEqualTo(BUDGET);
	}

	/**
	 * The public client gone, the upstream's reset and the idle timeout of a connection
	 * relayed verbatim each release what its window took.
	 */
	@Test
	void everyExitReleasesTheWindow() throws Exception {
		String host = "exit.budget.test";
		Socket[] upstreamSide = new Socket[1];
		int upstream = download(BODY, socket -> upstreamSide[0] = socket);
		startClient(Upstream.builder().host(host).target("tcp://127.0.0.1:" + upstream), grpcPort,
				new SimpleMeterRegistry());
		startClient(Upstream.builder()
			.host("idle.budget.test")
			.target("tcp://127.0.0.1:" + download(BODY))
			.listenPort(idleRoutePort)
			.verbatimIdleTimeout(Duration.ofSeconds(1)), grpcPort, new SimpleMeterRegistry());
		// the upstream resets mid-transfer: its client fails the virtual connection
		Socket reset = remember(new Socket("127.0.0.1", dataPort));
		reset.getOutputStream().write(get(host));
		Thread.ofVirtual().name("e2e-budget-drain").start(() -> {
			try {
				reset.getInputStream().transferTo(OutputStream.nullOutputStream());
			}
			catch (IOException e) {
				// torn down
			}
		});
		awaitCharged();
		Socket upstreamSocket = Objects.requireNonNull(upstreamSide[0]);
		upstreamSocket.setSoLinger(true, 0);
		upstreamSocket.close();
		awaitReleased("the upstream's reset");
		reset.close();
		// the public client goes away
		Socket gone = stalledReader(host);
		awaitCharged();
		gone.close();
		awaitReleased("the client gone");
		// nothing moves for the route's idle timeout
		Socket idle = remember(new Socket());
		idle.setReceiveBufferSize(16 * 1024);
		idle.connect(new InetSocketAddress("127.0.0.1", idleRoutePort));
		idle.getOutputStream().write(get("idle.budget.test"));
		awaitCharged();
		awaitReleased("the idle timeout");
		assertThat(serverCounter("sluice.data.connections.verbatim.idle.closed")).isPositive();
	}

	/**
	 * Behind a 20ms tunnel round trip and with the budget's room, one download and one
	 * upload grow to the full window: the writer waits about once per round trip of the
	 * window, so a few times per full window and once per doubling, where the announced
	 * window alone would take a wait per half of it.
	 */
	@Test
	void oneConnectionWithRoomGrowsToTheFullWindow() throws Exception {
		long bytes = 8 * MIB;
		long maxWaits = 4 * bytes / VirtualConnection.DEFAULT_WINDOW + 2L
				* Integer.numberOfTrailingZeros(VirtualConnection.DEFAULT_WINDOW / VirtualConnection.INITIAL_WINDOW);
		long initialWindowWaits = bytes / (VirtualConnection.INITIAL_WINDOW / 2);
		DelayingProxy tunnel = remember(DelayingProxy.start(grpcPort, Duration.ofMillis(10)));
		String downloadHost = "download.window.budget.test";
		SimpleMeterRegistry clientMeters = new SimpleMeterRegistry();
		startClient(Upstream.builder().host(downloadHost).target("tcp://127.0.0.1:" + download(bytes)), tunnel.port(),
				clientMeters);
		long start = System.nanoTime();
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.getOutputStream().write(get(downloadHost));
			assertThat(drain(socket.getInputStream(), bytes)).isEqualTo(bytes);
		}
		long downloadWaits = (long) clientMeters.get("sluice.tunnel.credit.waits").counter().count();
		System.out.printf("download of %dMiB through a 20ms tunnel round trip: %d credit waits in %dms%n", bytes / MIB,
				downloadWaits, Duration.ofNanos(System.nanoTime() - start).toMillis());
		assertThat(tunnel.relayed()).as("precondition: through the delay").isGreaterThan(bytes);
		assertThat(downloadWaits).isPositive().isLessThanOrEqualTo(maxWaits).isLessThan(initialWindowWaits / 4);

		String uploadHost = "upload.window.budget.test";
		AtomicLong received = new AtomicLong();
		startClient(Upstream.builder().host(uploadHost).target("tcp://127.0.0.1:" + sink(received)), tunnel.port(),
				new SimpleMeterRegistry());
		long serverWaitsBefore = serverCounter("sluice.tunnel.credit.waits");
		start = System.nanoTime();
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			OutputStream out = socket.getOutputStream();
			out.write(("POST / HTTP/1.1\r\nHost: " + uploadHost + "\r\nContent-Length: " + bytes + "\r\n\r\n")
				.getBytes(StandardCharsets.US_ASCII));
			byte[] chunk = new byte[64 * 1024];
			for (long sent = 0; sent < bytes; sent += chunk.length) {
				out.write(chunk);
			}
			Awaitility.await().atMost(TIMEOUT).until(() -> received.get() >= bytes);
		}
		long uploadWaits = serverCounter("sluice.tunnel.credit.waits") - serverWaitsBefore;
		System.out.printf("upload of %dMiB through a 20ms tunnel round trip: %d credit waits in %dms%n", bytes / MIB,
				uploadWaits, Duration.ofNanos(System.nanoTime() - start).toMillis());
		assertThat(uploadWaits).isPositive().isLessThanOrEqualTo(maxWaits).isLessThan(initialWindowWaits / 4);
	}

	/**
	 * Samples what is buffered until it held still for a second, asserting at every
	 * sample that it stays within the allowances and the budget and that the budget is
	 * never passed; what it settled at.
	 */
	private static long awaitPlateau(DoubleSupplier buffered, DoubleSupplier used) {
		AtomicLong last = new AtomicLong(-1);
		AtomicLong since = new AtomicLong(System.nanoTime());
		Awaitility.await().atMost(TIMEOUT).pollInterval(Duration.ofMillis(10)).until(() -> {
			long now = (long) buffered.getAsDouble();
			assertThat(now).as("buffered within the allowances and the budget").isLessThanOrEqualTo(BOUND);
			assertThat(used.getAsDouble()).as("the budget is never passed").isLessThanOrEqualTo(BUDGET);
			if (now != last.getAndSet(now)) {
				since.set(System.nanoTime());
			}
			return now > 0 && Duration.ofNanos(System.nanoTime() - since.get()).toMillis() >= 1000;
		});
		return last.get();
	}

	private void awaitCharged() {
		Awaitility.await()
			.atMost(TIMEOUT)
			.alias("the window grew into the budget")
			.until(() -> server("sluice.tunnel.budget.used.bytes") > 0);
	}

	private void awaitReleased(String exit) {
		Awaitility.await()
			.atMost(TIMEOUT)
			.alias("released on " + exit)
			.until(() -> server("sluice.tunnel.budget.used.bytes") == 0 && server("sluice.tunnel.buffered.bytes") == 0);
	}

	private double server(String gauge) {
		return Objects.requireNonNull(this.serverMeters).get(gauge).gauge().value();
	}

	private long serverCounter(String counter) {
		return (long) Objects.requireNonNull(this.serverMeters).get(counter).counter().count();
	}

	/** A download whose client reads nothing; a small receive buffer absorbs little. */
	private Socket stalledReader(String host) throws IOException {
		Socket socket = remember(new Socket());
		socket.setReceiveBufferSize(16 * 1024);
		socket.connect(new InetSocketAddress("127.0.0.1", dataPort));
		socket.getOutputStream().write(get(host));
		return socket;
	}

	/** An upload of {@link #BODY} sent on a thread of its own until torn down. */
	private void upload(String host) throws IOException {
		Socket socket = remember(new Socket("127.0.0.1", dataPort));
		Thread.ofVirtual().name("e2e-budget-upload").start(() -> {
			try {
				OutputStream out = socket.getOutputStream();
				out.write(("POST / HTTP/1.1\r\nHost: " + host + "\r\nContent-Length: " + BODY + "\r\n\r\n")
					.getBytes(StandardCharsets.US_ASCII));
				byte[] chunk = new byte[64 * 1024];
				for (long sent = 0; sent < BODY; sent += chunk.length) {
					out.write(chunk);
				}
			}
			catch (IOException e) {
				// torn down
			}
		});
	}

	private static byte[] get(String host) {
		return ("GET / HTTP/1.1\r\nHost: " + host + "\r\n\r\n").getBytes(StandardCharsets.US_ASCII);
	}

	/** Reads the response head and {@code body} bytes after it; the body bytes read. */
	private static long drain(InputStream in, long body) throws IOException {
		int matched = 0;
		while (matched < 4) {
			int b = in.read();
			if (b < 0) {
				return 0;
			}
			matched = (b == "\r\n\r\n".charAt(matched)) ? matched + 1 : (b == '\r' ? 1 : 0);
		}
		byte[] buffer = new byte[64 * 1024];
		long read = 0;
		while (read < body) {
			int n = in.read(buffer, 0, (int) Math.min(buffer.length, body - read));
			if (n < 0) {
				break;
			}
			read += n;
		}
		return read;
	}

	private int download(long body) throws IOException {
		return download(body, socket -> {
		});
	}

	/**
	 * An upstream answering every request head with {@code body} bytes; each accepted
	 * socket is handed to {@code accepted} first.
	 */
	private int download(long body, Consumer<Socket> accepted) throws IOException {
		return serve(socket -> {
			accepted.accept(socket);
			InputStream in = socket.getInputStream();
			int matched = 0;
			while (matched < 4) {
				int b = in.read();
				if (b < 0) {
					return;
				}
				matched = (b == "\r\n\r\n".charAt(matched)) ? matched + 1 : (b == '\r' ? 1 : 0);
			}
			OutputStream out = socket.getOutputStream();
			out.write(("HTTP/1.1 200 OK\r\nContent-Length: " + body + "\r\n\r\n").getBytes(StandardCharsets.US_ASCII));
			byte[] chunk = new byte[64 * 1024];
			for (long sent = 0; sent < body; sent += chunk.length) {
				out.write(chunk, 0, (int) Math.min(chunk.length, body - sent));
			}
			out.flush();
			in.read();
		});
	}

	/**
	 * An upstream that accepts and never reads; a small receive buffer absorbs little.
	 */
	private int stalledUpstream() throws IOException {
		ServerSocket server = remember(new ServerSocket());
		server.setReceiveBufferSize(16 * 1024);
		server.bind(new InetSocketAddress("127.0.0.1", 0));
		Thread.ofVirtual().name("e2e-budget-stalled-accept").start(() -> {
			while (!server.isClosed()) {
				try {
					remember(server.accept());
				}
				catch (IOException e) {
					return;
				}
			}
		});
		return server.getLocalPort();
	}

	/** An upstream counting what it reads into {@code received}. */
	private int sink(AtomicLong received) throws IOException {
		return serve(socket -> {
			InputStream in = socket.getInputStream();
			byte[] buffer = new byte[64 * 1024];
			int n;
			while ((n = in.read(buffer)) > 0) {
				received.addAndGet(n);
			}
		});
	}

	private int serve(Handler handler) throws IOException {
		ServerSocket server = remember(new ServerSocket());
		server.bind(new InetSocketAddress("127.0.0.1", 0));
		Thread.ofVirtual().name("e2e-budget-accept").start(() -> {
			while (!server.isClosed()) {
				try {
					Socket socket = remember(server.accept());
					Thread.ofVirtual().name("e2e-budget-upstream").start(() -> {
						try (socket) {
							handler.handle(socket);
						}
						catch (IOException e) {
							// torn down
						}
					});
				}
				catch (IOException e) {
					return;
				}
			}
		});
		return server.getLocalPort();
	}

	private void startClient(Upstream.Builder upstream, int serverPort, MeterRegistry meters) {
		Upstream built = upstream.build();
		TunnelClient client = TunnelClient.builder()
			.properties(SluiceClientProperties.builder()
				.serverUrl("grpc://127.0.0.1:" + serverPort)
				.upstream(built)
				.token("it-token")
				.tunnelBudget(DataSize.ofBytes(BUDGET))
				.build())
			.taskExecutor(task -> Thread.ofVirtual().name("e2e-budget-worker").start(task))
			.meterRegistry(meters)
			.build();
		client.start();
		remember(client::stop);
		Awaitility.await()
			.atMost(TIMEOUT)
			.until(() -> built.listenPort() > 0 ? client.isConnected()
					: Objects.requireNonNull(this.router).lookup(built.host()).isPresent());
	}

	private <T extends AutoCloseable> T remember(T closeable) {
		this.closeables.add(closeable);
		return closeable;
	}

	@FunctionalInterface
	private interface Handler {

		void handle(Socket socket) throws IOException;

	}

}
