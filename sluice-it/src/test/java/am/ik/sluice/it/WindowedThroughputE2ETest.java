package am.ik.sluice.it;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.Timeout;

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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A large transfer over one windowed connection completes in either direction: the credit
 * keeps being granted back (a credit that got stuck would stall it).
 * <p>
 * The grant count is bounded, not exact: a reader decides its window once it consumed
 * half of it, so a grant overshoots half the window by up to one read, and the read sizes
 * follow how the bytes arrived, which differs between platforms. The window starts at the
 * initial 64KiB and grows to 1MiB while it bounds the transfer: a grant is never larger
 * than the largest window, and completing the transfer needs all but one window of it
 * granted back. A grant is at least half the initial window, and most are far larger: the
 * transfer takes under a quarter of the grants the initial window would.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class WindowedThroughputE2ETest {

	private static final long TRANSFER_BYTES = 256L * 1024 * 1024;

	/** Every grant covers at most the window. */
	private static final long MIN_GRANTS = (TRANSFER_BYTES - VirtualConnection.DEFAULT_WINDOW)
			/ VirtualConnection.DEFAULT_WINDOW;

	/**
	 * A quarter of what grants of half the initial window would take: the window grew.
	 */
	private static final long MAX_GRANTS = TRANSFER_BYTES / (VirtualConnection.INITIAL_WINDOW / 2) / 4;

	private static int grpcPort;

	private static int uploadRoutePort;

	private static int downloadRoutePort;

	private static final AtomicLong uploaded = new AtomicLong();

	private final MeterRegistry clientMeters = new SimpleMeterRegistry();

	@Autowired
	private MeterRegistry serverMeters;

	private @Nullable TunnelClient client;

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) {
		grpcPort = TestPorts.freePort();
		uploadRoutePort = TestPorts.freePort();
		downloadRoutePort = TestPorts.freePort();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(TestPorts.freePort()));
		registry.add("sluice.tcp-port-range", () -> uploadRoutePort + "," + downloadRoutePort);
		registry.add("sluice.token", () -> "it-token");
		registry.add("server.port", () -> 0);
	}

	@AfterEach
	void tearDown() {
		TunnelClient client = this.client;
		if (client != null) {
			client.stop();
			this.client = null;
		}
	}

	/** Public peer -> server -> client -> upstream; the client grants. */
	@Test
	@Timeout(120)
	void uploadOverOneWindowedConnectionCompletes() throws Exception {
		int sink = upstream(WindowedThroughputE2ETest::count);
		startClient(Upstream.builder().host("up.local").target("tcp://127.0.0.1:" + sink).listenPort(uploadRoutePort));
		uploaded.set(0);
		long serverReceivedBefore = windowUpdates(this.serverMeters, "received");
		long start = System.nanoTime();
		try (Socket peer = new Socket("127.0.0.1", uploadRoutePort)) {
			OutputStream out = peer.getOutputStream();
			byte[] chunk = new byte[64 * 1024];
			for (long sent = 0; sent < TRANSFER_BYTES; sent += chunk.length) {
				out.write(chunk);
			}
			out.flush();
			Awaitility.await().atMost(Duration.ofSeconds(60)).until(() -> uploaded.get() >= TRANSFER_BYTES);
		}
		report("upload", start);
		assertThat(windowUpdates(this.clientMeters, "sent")).as("grants sent by the client")
			.isBetween(MIN_GRANTS, MAX_GRANTS);
		assertThat(windowUpdates(this.serverMeters, "received") - serverReceivedBefore)
			.as("grants received by the server")
			.isBetween(MIN_GRANTS, MAX_GRANTS);
	}

	/** Upstream -> client -> server -> public peer; the server grants. */
	@Test
	@Timeout(120)
	void downloadOverOneWindowedConnectionCompletes() throws Exception {
		int source = upstream(WindowedThroughputE2ETest::produce);
		startClient(Upstream.builder()
			.host("down.local")
			.target("tcp://127.0.0.1:" + source)
			.listenPort(downloadRoutePort));
		long serverSentBefore = windowUpdates(this.serverMeters, "sent");
		long start = System.nanoTime();
		long received = 0;
		try (Socket peer = new Socket("127.0.0.1", downloadRoutePort)) {
			peer.setSoTimeout(10_000);
			InputStream in = peer.getInputStream();
			byte[] buffer = new byte[64 * 1024];
			int n;
			while ((n = in.read(buffer)) > 0) {
				received += n;
			}
		}
		report("download", start);
		assertThat(received).isEqualTo(TRANSFER_BYTES);
		assertThat(windowUpdates(this.clientMeters, "received")).as("grants received by the client")
			.isBetween(MIN_GRANTS, MAX_GRANTS);
		assertThat(windowUpdates(this.serverMeters, "sent") - serverSentBefore).as("grants sent by the server")
			.isBetween(MIN_GRANTS, MAX_GRANTS);
	}

	private static void report(String direction, long startNanos) {
		double seconds = (System.nanoTime() - startNanos) / 1e9;
		System.out.printf("windowed %s: %d MiB in %.2fs (%.0f MiB/s)%n", direction, TRANSFER_BYTES >> 20, seconds,
				(TRANSFER_BYTES >> 20) / seconds);
	}

	private static long windowUpdates(MeterRegistry meters, String direction) {
		return (long) meters.find("sluice.tunnel.window.updates")
			.tag("direction", direction)
			.counters()
			.stream()
			.mapToDouble(Counter::count)
			.sum();
	}

	private void startClient(Upstream.Builder upstream) {
		TunnelClient started = TunnelClient.builder()
			.properties(SluiceClientProperties.builder()
				.serverUrl("grpc://127.0.0.1:" + grpcPort)
				.upstream(upstream.build())
				.token("it-token")
				.build())
			.taskExecutor(task -> Thread.ofVirtual().name("e2e-throughput-worker").start(task))
			.meterRegistry(this.clientMeters)
			.build();
		started.start();
		this.client = started;
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(started::isConnected);
	}

	private static int upstream(Consumer<Socket> handler) throws Exception {
		ServerSocket server = new ServerSocket();
		server.bind(new InetSocketAddress("127.0.0.1", 0));
		Thread.ofVirtual().name("e2e-throughput-accept").start(() -> {
			try (server) {
				Socket socket = server.accept();
				handler.accept(socket);
			}
			catch (Exception e) {
				// torn down
			}
		});
		return server.getLocalPort();
	}

	/** Reads and counts everything. */
	private static void count(Socket socket) {
		try (socket; InputStream in = socket.getInputStream()) {
			byte[] buffer = new byte[64 * 1024];
			int n;
			while ((n = in.read(buffer)) > 0) {
				uploaded.addAndGet(n);
			}
		}
		catch (Exception e) {
			// torn down
		}
	}

	/** Writes the transfer, then closes. */
	private static void produce(Socket socket) {
		try (socket; OutputStream out = socket.getOutputStream()) {
			byte[] chunk = new byte[64 * 1024];
			for (long sent = 0; sent < TRANSFER_BYTES; sent += chunk.length) {
				out.write(chunk);
			}
		}
		catch (Exception e) {
			// torn down
		}
	}

}
