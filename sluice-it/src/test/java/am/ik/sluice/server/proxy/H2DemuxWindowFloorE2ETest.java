package am.ik.sluice.server.proxy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.client.config.Upstream;
import am.ik.sluice.client.tunnel.TunnelClient;
import am.ik.sluice.it.DelayingProxy;
import am.ik.sluice.it.H2TestClient;
import am.ik.sluice.it.TestH2;
import am.ik.sluice.it.TestPorts;
import am.ik.sluice.it.WindowedH2Upstream;
import am.ik.sluice.server.SluiceServerApplication;
import am.ik.sluice.server.route.Router;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A relay whose queue budget has no room for a connection window past RFC 9113's still
 * relays its streams, at that window: a download through a delayed tunnel and an upload
 * from a delayed client each complete, their sender never granted more than 65535 bytes
 * of connection window, so they move at most that per round trip. The waits and the time
 * are printed for the throughput; the time is bounded below by the window, the waits
 * above by a refill per half window.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class H2DemuxWindowFloorE2ETest {

	private static final int MIB = 1024 * 1024;

	private static final int BODY = 4 * MIB;

	/** No room for a byte of credit past the floor. */
	private static final long BUDGET = 1;

	/** Each way: a 20ms round trip. */
	private static final Duration DELAY = Duration.ofMillis(10);

	/** The least a body takes: a floor's window per round trip. */
	private static final Duration LEAST = DELAY.multipliedBy(2).multipliedBy(BODY / HopWindow.FLOOR - 1);

	/** A wait per refill at most, a refill per half floor. */
	private static final long MAX_WAITS = 2L * BODY / HopWindow.FLOOR + 2;

	/** The receivers' windows: far above the relay's, so only the relay's bind. */
	private static final int PEER_WINDOW = 16 * MIB;

	private static final int SETTINGS_INITIAL_WINDOW_SIZE = 0x4;

	private static final Duration TIMEOUT = Duration.ofSeconds(30);

	private static int grpcPort;

	private static int dataPort;

	private final List<AutoCloseable> closeables = new CopyOnWriteArrayList<>();

	@Nullable
	@Autowired
	private Router router;

	@Nullable
	@Autowired
	private MeterRegistry meterRegistry;

	@Nullable
	@Autowired
	private ConnectionLimits limits;

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) {
		grpcPort = TestPorts.freePort();
		dataPort = TestPorts.freePort();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(dataPort));
		registry.add("sluice.token", () -> "it-token");
		registry.add("sluice.limits.h2-queue-budget", () -> BUDGET + "B");
		registry.add("server.port", () -> String.valueOf(TestPorts.freePort()));
	}

	@AfterEach
	void tearDown() {
		for (int i = this.closeables.size() - 1; i >= 0; i--) {
			close(this.closeables.get(i));
		}
		this.closeables.clear();
	}

	@Test
	void aDownloadWhoseLargerWindowDoesNotFitCompletesAtRfc9113sWindow() throws Exception {
		assertThat(limits().h2Budget().max()).as("the configured budget took effect").isEqualTo(BUDGET);
		String route = "download.window-floor.test";
		AtomicReference<@Nullable WindowedH2Upstream> seen = new AtomicReference<>();
		ServerSocket upstream = upstream(seen, peer -> {
			peer.settings(null);
			TestH2.@Nullable Frame frame;
			while ((frame = peer.next()) != null) {
				if (frame.type() == TestH2.HEADERS) {
					int streamId = frame.streamId();
					Thread.ofVirtual().start(() -> {
						try {
							peer.headers(streamId, false);
							peer.sendData(streamId, new byte[BODY], true);
						}
						catch (IOException | InterruptedException e) {
							// the relay went away
						}
					});
				}
			}
		});
		DelayingProxy tunnel = remember(DelayingProxy.start(grpcPort, DELAY));
		startClient(route, upstream, tunnel.port());
		double declined = declined();
		try (H2TestClient client = H2TestClient.connect(dataPort)) {
			client.frame(TestH2.SETTINGS, 0, 0, WindowedH2Upstream.setting(SETTINGS_INITIAL_WINDOW_SIZE, PEER_WINDOW));
			long start = System.nanoTime();
			client.headers(1, true, TestH2.headerBlock(":authority=" + route, ":method=GET", ":path=/"));
			assertThat(client.awaitEnd(1, TIMEOUT)).as("the response ended").isTrue();
			Duration took = Duration.ofNanos(System.nanoTime() - start);
			WindowedH2Upstream peer = Objects.requireNonNull(seen.get());
			report("download", peer.windowWaits(), took);
			assertThat(client.dataReceived(1)).isEqualTo(BODY);
			assertThat(tunnel.relayed()).as("the tunnel went through the delay").isGreaterThan(BODY);
			assertThat(declined()).as("a larger window was wanted and declined").isGreaterThan(declined);
			assertThat(peer.maxSendConnectionWindow()).as("the leg's connection window stayed at the floor")
				.isEqualTo(HopWindow.FLOOR);
			assertThat(peer.windowWaits()).as("waits of a %dMiB download in %s", BODY / MIB, took)
				.isPositive()
				.isLessThanOrEqualTo(MAX_WAITS);
			assertThat(took).as("a floor per round trip at most").isGreaterThanOrEqualTo(LEAST);
		}
	}

	@Test
	void anUploadWhoseLargerWindowDoesNotFitCompletesAtRfc9113sWindow() throws Exception {
		String route = "upload.window-floor.test";
		AtomicReference<@Nullable WindowedH2Upstream> seen = new AtomicReference<>();
		ServerSocket upstream = upstream(seen, peer -> {
			peer.settings(PEER_WINDOW);
			peer.grant(0, PEER_WINDOW - WindowedH2Upstream.DEFAULT_WINDOW);
			TestH2.@Nullable Frame frame;
			while ((frame = peer.next()) != null) {
				if (frame.type() == TestH2.DATA) {
					int length = frame.payload().length;
					if (length > 0) {
						peer.grant(0, length);
						peer.grant(frame.streamId(), length);
					}
					if ((frame.flags() & TestH2.FLAG_END_STREAM) != 0) {
						peer.headers(frame.streamId(), true);
					}
				}
			}
		});
		startClient(route, upstream, grpcPort);
		DelayingProxy link = remember(DelayingProxy.start(dataPort, DELAY));
		double declined = declined();
		try (H2TestClient client = H2TestClient.connect(link.port())) {
			long start = System.nanoTime();
			client.headers(1, false, TestH2.headerBlock(":authority=" + route, ":method=POST", ":path=/"));
			assertThat(client.data(1, BODY, true, TIMEOUT)).as("the body went out").isTrue();
			assertThat(client.awaitEnd(1, TIMEOUT)).as("the response ended").isTrue();
			Duration took = Duration.ofNanos(System.nanoTime() - start);
			WindowedH2Upstream peer = Objects.requireNonNull(seen.get());
			report("upload", client.windowWaits(), took);
			assertThat(peer.received(1)).isEqualTo(BODY);
			assertThat(link.relayed()).as("the client went through the delay").isGreaterThan(BODY);
			assertThat(declined()).as("a larger window was wanted and declined").isGreaterThan(declined);
			assertThat(client.maxConnectionWindow()).as("the client's connection window stayed at the floor")
				.isEqualTo(HopWindow.FLOOR);
			assertThat(client.windowWaits()).as("waits of a %dMiB upload in %s", BODY / MIB, took)
				.isPositive()
				.isLessThanOrEqualTo(MAX_WAITS);
			assertThat(took).as("a floor per round trip at most").isGreaterThanOrEqualTo(LEAST);
		}
	}

	private static void report(String direction, long waits, Duration took) {
		System.out.printf("window floor: %s of %dMiB with a %dms round trip: %d waits in %dms%n", direction, BODY / MIB,
				2 * DELAY.toMillis(), waits, took.toMillis());
	}

	private ConnectionLimits limits() {
		return Objects.requireNonNull(this.limits);
	}

	private double declined() {
		return Objects.requireNonNull(this.meterRegistry).get("sluice.h2demux.window.declined").counter().count();
	}

	private <T extends AutoCloseable> T remember(T closeable) {
		this.closeables.add(closeable);
		return closeable;
	}

	private void startClient(String host, ServerSocket upstream, int serverPort) {
		TunnelClient client = TunnelClient.builder()
			.properties(SluiceClientProperties.builder()
				.serverUrl("grpc://127.0.0.1:" + serverPort)
				.upstream(Upstream.builder().host(host).target("tcp://127.0.0.1:" + upstream.getLocalPort()).build())
				.token("it-token")
				.build())
			.taskExecutor(task -> Thread.ofVirtual().name("e2e-window-floor").start(task))
			.meterRegistry(new SimpleMeterRegistry())
			.build();
		client.start();
		remember(client::stop);
		Awaitility.await().atMost(TIMEOUT).until(() -> Objects.requireNonNull(this.router).lookup(host).isPresent());
	}

	/** A test upstream serving every accepted connection; the first is published. */
	private ServerSocket upstream(AtomicReference<@Nullable WindowedH2Upstream> first, Handler handler)
			throws IOException {
		ServerSocket server = new ServerSocket();
		server.bind(new InetSocketAddress("127.0.0.1", 0), 16);
		remember(server);
		Thread.ofVirtual().name("e2e-window-floor-accept").start(() -> {
			while (!server.isClosed()) {
				try {
					Socket socket = server.accept();
					remember(socket);
					Thread.ofVirtual().start(() -> serve(socket, first, handler));
				}
				catch (IOException e) {
					return;
				}
			}
		});
		return server;
	}

	private static void serve(Socket socket, AtomicReference<@Nullable WindowedH2Upstream> first, Handler handler) {
		try (socket) {
			WindowedH2Upstream upstream = WindowedH2Upstream.accept(socket);
			if (upstream == null) {
				return;
			}
			first.compareAndSet(null, upstream);
			handler.serve(upstream);
		}
		catch (Exception e) {
			// connection ended
		}
	}

	private static void close(AutoCloseable closeable) {
		try {
			closeable.close();
		}
		catch (Exception e) {
			// ignore
		}
	}

	/** A test upstream's connection handler. */
	@FunctionalInterface
	private interface Handler {

		void serve(WindowedH2Upstream upstream) throws Exception;

	}

}
