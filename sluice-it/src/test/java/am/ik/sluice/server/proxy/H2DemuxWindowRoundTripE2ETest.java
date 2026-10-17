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
import am.ik.sluice.tunnel.VirtualConnection;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The windows the relay grants bound how much of a stream moves per round trip of each
 * hop: an upstream behind a delayed tunnel downloads, and a delayed client uploads, a
 * body in a few round trips of the relay's windows. Each sender counts the times it ran
 * out of window and waited for a grant; with the hop's round trip far above the time it
 * takes to send a window, that is one per round trip. The windows' grants are each at
 * least half a window (netty's update ratio for the stream windows, the relay's refill
 * for the connection windows), so a body moves in at most four waits per window of it,
 * plus two per doubling of the connection window from RFC 9113's 65535 bytes, each
 * doubling taking a round trip of the PING measuring the sender's; a 64KiB window could
 * not move it in fewer than one round trip per 64KiB.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class H2DemuxWindowRoundTripE2ETest {

	private static final int MIB = 1024 * 1024;

	private static final int BODY = 4 * MIB;

	/**
	 * What the relay's windows allow: four grants per window of the body, plus one, plus
	 * two per doubling of the connection window from the floor.
	 */
	private static final long MAX_WAITS = 4L * BODY / NettyHttp2DemuxRelay.STREAM_WINDOW + 1
			+ 2L * Integer.numberOfTrailingZeros(NettyHttp2DemuxRelay.CONNECTION_WINDOW / (HopWindow.FLOOR + 1));

	/**
	 * What the leg's virtual connection allows: a few waits per full window of the body,
	 * and one per doubling from the initial window.
	 */
	private static final long MAX_CREDIT_WAITS = 4L * BODY / VirtualConnection.DEFAULT_WINDOW
			+ 2L * Integer.numberOfTrailingZeros(VirtualConnection.DEFAULT_WINDOW / VirtualConnection.INITIAL_WINDOW);

	/** The round trips a body takes at least with the 64KiB windows of RFC 9113. */
	private static final long SPEC_WINDOW_ROUND_TRIPS = BODY / 65535;

	/** Each way: a 20ms round trip, far above the time to send a window on loopback. */
	private static final Duration DELAY = Duration.ofMillis(10);

	/** The receivers' windows: far above the relay's, so only the relay's bind. */
	private static final int PEER_WINDOW = 16 * MIB;

	private static final int SETTINGS_INITIAL_WINDOW_SIZE = 0x4;

	private static final Duration TIMEOUT = Duration.ofSeconds(30);

	private static int grpcPort;

	private static int dataPort;

	private final List<AutoCloseable> closeables = new CopyOnWriteArrayList<>();

	private final SimpleMeterRegistry clientMeters = new SimpleMeterRegistry();

	@Nullable
	@Autowired
	private Router router;

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) {
		grpcPort = TestPorts.freePort();
		dataPort = TestPorts.freePort();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(dataPort));
		registry.add("sluice.token", () -> "it-token");
		registry.add("server.port", () -> String.valueOf(TestPorts.freePort()));
	}

	@AfterEach
	void tearDown() {
		// in reverse: the tunnel clients before the proxies and the upstreams
		for (int i = this.closeables.size() - 1; i >= 0; i--) {
			close(this.closeables.get(i));
		}
		this.closeables.clear();
	}

	/**
	 * Upstream -> delayed tunnel -> relay -> client: the upstream sends within the leg's
	 * windows, so it waits once per tunnel round trip its window bounds.
	 */
	@Test
	void aDownloadThroughADelayedTunnelTakesAFewRoundTripsOfTheRelaysWindows() throws Exception {
		String route = "download.window-round-trip.test";
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
		try (H2TestClient client = H2TestClient.connect(dataPort)) {
			client.frame(TestH2.SETTINGS, 0, 0, WindowedH2Upstream.setting(SETTINGS_INITIAL_WINDOW_SIZE, PEER_WINDOW));
			long start = System.nanoTime();
			client.headers(1, true, TestH2.headerBlock(":authority=" + route, ":method=GET", ":path=/"));
			assertThat(client.awaitEnd(1, TIMEOUT)).as("the response ended").isTrue();
			Duration took = Duration.ofNanos(System.nanoTime() - start);
			WindowedH2Upstream peer = Objects.requireNonNull(seen.get());
			report("download", peer.windowWaits(), took);
			assertThat(client.dataReceived(1)).isEqualTo(BODY);
			assertThat(peer.sendInitialWindow()).as("the stream window the leg announced")
				.isEqualTo(NettyHttp2DemuxRelay.LEG_STREAM_WINDOW);
			assertThat(tunnel.relayed()).as("the tunnel went through the delay").isGreaterThan(BODY);
			// past the client's largest window, half the leg's: only a window grown to
			// the
			// leg's largest grants that much
			assertThat(peer.maxSendConnectionWindow()).as("the leg's connection window grown to its largest")
				.isGreaterThan(NettyHttp2DemuxRelay.CONNECTION_WINDOW);
			assertThat(peer.windowWaits()).as("round trips of a %dMiB download in %s", BODY / MIB, took)
				.isPositive()
				.isLessThanOrEqualTo(MAX_WAITS)
				.isLessThan(SPEC_WINDOW_ROUND_TRIPS);
			// the leg's virtual connection grew from its initial window to the full one:
			// its writer, the client, waited a few times per full window and once per
			// doubling
			long creditWaits = (long) this.clientMeters.get("sluice.tunnel.credit.waits")
				.tag("node", "grpc://127.0.0.1:" + tunnel.port())
				.counter()
				.count();
			System.out.printf("window round trips: the leg's tunnel connection waited for credit %d times%n",
					creditWaits);
			assertThat(creditWaits).as("the leg's credit waits").isLessThanOrEqualTo(MAX_CREDIT_WAITS);
		}
	}

	/**
	 * Delayed client -> relay -> tunnel -> upstream: the client sends within the windows
	 * of the relay's client face, so it waits once per client round trip they bound.
	 */
	@Test
	void anUploadFromADelayedClientTakesAFewRoundTripsOfTheRelaysWindows() throws Exception {
		String route = "upload.window-round-trip.test";
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
		try (H2TestClient client = H2TestClient.connect(link.port())) {
			long start = System.nanoTime();
			client.headers(1, false, TestH2.headerBlock(":authority=" + route, ":method=POST", ":path=/"));
			assertThat(client.data(1, BODY, true, TIMEOUT)).as("the body went out").isTrue();
			assertThat(client.awaitEnd(1, TIMEOUT)).as("the response ended").isTrue();
			Duration took = Duration.ofNanos(System.nanoTime() - start);
			WindowedH2Upstream peer = Objects.requireNonNull(seen.get());
			report("upload", client.windowWaits(), took);
			assertThat(peer.received(1)).isEqualTo(BODY);
			assertThat(client.setting(SETTINGS_INITIAL_WINDOW_SIZE)).as("the stream window the relay announced")
				.isEqualTo(NettyHttp2DemuxRelay.STREAM_WINDOW);
			assertThat(link.relayed()).as("the client went through the delay").isGreaterThan(BODY);
			assertThat(client.maxConnectionWindow()).as("the client's connection window grown to its largest")
				.isGreaterThan(NettyHttp2DemuxRelay.CONNECTION_WINDOW / 2);
			assertThat(client.windowWaits()).as("round trips of a %dMiB upload in %s", BODY / MIB, took)
				.isPositive()
				.isLessThanOrEqualTo(MAX_WAITS)
				.isLessThan(SPEC_WINDOW_ROUND_TRIPS);
		}
	}

	private static void report(String direction, long waits, Duration took) {
		System.out.printf("window round trips: %s of %dMiB with a %dms round trip: %d waits in %dms%n", direction,
				BODY / MIB, 2 * DELAY.toMillis(), waits, took.toMillis());
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
			.taskExecutor(task -> Thread.ofVirtual().name("e2e-window-round-trip").start(task))
			.meterRegistry(this.clientMeters)
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
		Thread.ofVirtual().name("e2e-window-round-trip-accept").start(() -> {
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
