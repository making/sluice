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
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The windows the relay grants a sender they bound: an upstream behind a delayed tunnel
 * downloading, and a delayed client uploading, a body of several windows each get the
 * relay's stream window and a connection window grown far past RFC 9113's 65535 bytes --
 * the hop's round trip is far above the time it takes to send a window, so the sender
 * runs out of window and waits for grants. How many round trips the body takes depends on
 * the machine and is not asserted.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class H2DemuxWindowRoundTripE2ETest {

	private static final int MIB = 1024 * 1024;

	private static final int BODY = 4 * MIB;

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
	void aDownloadThroughADelayedTunnelGrowsTheLegsConnectionWindow() throws Exception {
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
			client.headers(1, true, TestH2.headerBlock(":authority=" + route, ":method=GET", ":path=/"));
			assertThat(client.awaitEnd(1, TIMEOUT)).as("the response ended").isTrue();
			WindowedH2Upstream peer = Objects.requireNonNull(seen.get());
			assertThat(client.dataReceived(1)).isEqualTo(BODY);
			assertThat(peer.sendInitialWindow()).as("the stream window the leg announced")
				.isEqualTo(NettyHttp2DemuxRelay.LEG_STREAM_WINDOW);
			assertThat(tunnel.relayed()).as("the tunnel went through the delay").isGreaterThan(BODY);
			// past the client's largest window, half the leg's: only a window grown to
			// the
			// leg's largest grants that much
			assertThat(peer.maxSendConnectionWindow()).as("the leg's connection window grown to its largest")
				.isGreaterThan(NettyHttp2DemuxRelay.CONNECTION_WINDOW);
			assertThat(peer.windowWaits()).as("the upstream ran out of window: the windows bound it").isPositive();
		}
	}

	/**
	 * Delayed client -> relay -> tunnel -> upstream: the client sends within the windows
	 * of the relay's client face, so it waits once per client round trip they bound.
	 */
	@Test
	void anUploadFromADelayedClientGrowsTheClientsConnectionWindow() throws Exception {
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
			client.headers(1, false, TestH2.headerBlock(":authority=" + route, ":method=POST", ":path=/"));
			assertThat(client.data(1, BODY, true, TIMEOUT)).as("the body went out").isTrue();
			assertThat(client.awaitEnd(1, TIMEOUT)).as("the response ended").isTrue();
			WindowedH2Upstream peer = Objects.requireNonNull(seen.get());
			assertThat(peer.received(1)).isEqualTo(BODY);
			assertThat(client.setting(SETTINGS_INITIAL_WINDOW_SIZE)).as("the stream window the relay announced")
				.isEqualTo(NettyHttp2DemuxRelay.STREAM_WINDOW);
			assertThat(link.relayed()).as("the client went through the delay").isGreaterThan(BODY);
			assertThat(client.maxConnectionWindow()).as("the client's connection window grown to its largest")
				.isGreaterThan(NettyHttp2DemuxRelay.CONNECTION_WINDOW / 2);
			assertThat(client.windowWaits()).as("the client ran out of window: the windows bound it").isPositive();
		}
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
