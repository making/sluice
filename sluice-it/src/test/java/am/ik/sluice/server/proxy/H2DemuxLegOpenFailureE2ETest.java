package am.ik.sluice.server.proxy;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;

import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.client.config.Upstream;
import am.ik.sluice.client.tunnel.TunnelClient;
import am.ik.sluice.it.H2TestClient;
import am.ik.sluice.it.TestH2;
import am.ik.sluice.it.TestPorts;
import am.ik.sluice.server.SluiceServerApplication;
import am.ik.sluice.server.proxy.hop.Hop;
import am.ik.sluice.server.route.Router;
import am.ik.sluice.server.tunnel.SessionRegistry;
import am.ik.sluice.server.tunnel.TunnelSession;
import am.ik.sluice.tunnel.VirtualConnection;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.TestInstance;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * An h2 stream whose leg fails to open -- the session cannot open the connection, or the
 * leg's hop cannot be configured -- is refused (REFUSED_STREAM): the client's connection
 * goes on and serves the next stream, the tunnel session holds no connection of the
 * failed leg, and the slot the leg took comes back. In the relay's package for its seam.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class H2DemuxLegOpenFailureE2ETest {

	private static final ExecutorService UPSTREAM_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

	private final List<AutoCloseable> closeables = new CopyOnWriteArrayList<>();

	private final List<TunnelClient> clients = new ArrayList<>();

	private static int grpcPort;

	private static int dataPort;

	@Nullable
	@Autowired
	private Router router;

	@Nullable
	@Autowired
	private MeterRegistry meterRegistry;

	@Nullable
	@Autowired
	private SessionRegistry sessions;

	@Nullable
	@Autowired
	private DataProxyServer dataProxyServer;

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) {
		grpcPort = TestPorts.freePort();
		dataPort = TestPorts.freePort();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(dataPort));
		registry.add("sluice.token", () -> "it-token");
		registry.add("sluice.access-log.rate-limit.enabled", () -> "false");
		registry.add("server.port", () -> String.valueOf(TestPorts.freePort()));
	}

	@AfterEach
	void resetSeam() {
		Objects.requireNonNull(this.dataProxyServer).legSeam(NettyHttp2DemuxRelay.LegSeam.NONE);
	}

	@AfterAll
	void tearDown() {
		this.clients.forEach(TunnelClient::stop);
		this.closeables.forEach(H2DemuxLegOpenFailureE2ETest::close);
		UPSTREAM_EXECUTOR.shutdown();
	}

	/**
	 * The session throws opening the leg: nothing was opened, and both the connection's
	 * own slot (first leg) and a further leg's permit are given back.
	 */
	@Test
	void legTheSessionCannotOpenRefusesItsStream() throws Exception {
		AtomicBoolean fail = new AtomicBoolean();
		Objects.requireNonNull(this.dataProxyServer).legSeam(new NettyHttp2DemuxRelay.LegSeam() {
			@Override
			public VirtualConnection open(TunnelSession session, String address) {
				if (fail.get()) {
					throw new IllegalStateException("the session cannot open");
				}
				return session.open(address);
			}
		});
		assertFailedLegsAreGivenBack(fail, "open");
	}

	/**
	 * The leg's hop is built but cannot be configured: it is aborted and its connection
	 * leaves the session, and both kinds of slot are given back once its frames left.
	 */
	@Test
	void legWhoseHopCannotBeConfiguredRefusesItsStream() throws Exception {
		AtomicBoolean fail = new AtomicBoolean();
		Objects.requireNonNull(this.dataProxyServer).legSeam(new NettyHttp2DemuxRelay.LegSeam() {
			@Override
			public void built(Hop hop) {
				if (fail.get()) {
					throw new IllegalStateException("the hop cannot be configured");
				}
			}
		});
		assertFailedLegsAreGivenBack(fail, "configure");
	}

	/** The hosts carry the tag: the tests share one server and its routes. */
	private void assertFailedLegsAreGivenBack(AtomicBoolean fail, String tag) throws Exception {
		String first = "lo." + tag + ".first.test";
		String second = "lo." + tag + ".second.test";
		startClient(first, stub(H2DemuxLegOpenFailureE2ETest::answerEndedRequests));
		startClient(second, stub(H2DemuxLegOpenFailureE2ETest::answerEndedRequests));
		assertThat(admitted()).isZero();
		try (H2TestClient client = H2TestClient.connect(dataPort)) {
			// the connection's own slot: the first leg fails
			fail.set(true);
			assertThat(refused(client, 1, first)).isEqualTo(TestH2.REFUSED_STREAM);
			Awaitility.await()
				.atMost(Duration.ofSeconds(5))
				.untilAsserted(
						() -> assertThat(connections(first)).as("the session holds no connection of the failed leg")
							.isZero());
			assertThat(admitted()).as("the connection holds its own slot only").isEqualTo(1);
			fail.set(false);
			assertThat(get(client, 3, first)).as("the connection serves the next stream").isTrue();
			assertThat(admitted()).as("the leg took the own slot again, not a permit").isEqualTo(1);

			// a further leg's permit
			fail.set(true);
			assertThat(refused(client, 5, second)).isEqualTo(TestH2.REFUSED_STREAM);
			Awaitility.await()
				.atMost(Duration.ofSeconds(5))
				.untilAsserted(() -> assertThat(connections(second)).isZero());
			Awaitility.await()
				.atMost(Duration.ofSeconds(5))
				.untilAsserted(() -> assertThat(admitted()).as("the permit is given back").isEqualTo(1));
			fail.set(false);
			assertThat(get(client, 7, second)).as("the route opens its leg afterwards").isTrue();
			assertThat(admitted()).isEqualTo(2);
			assertThat(client.closed()).isFalse();
		}
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> admitted() == 0);
	}

	/** Sends a GET on the stream; the code it was reset with. */
	private static @Nullable Integer refused(H2TestClient client, int streamId, String host) throws Exception {
		client.headers(streamId, true, TestH2.headerBlock(":authority=" + host, ":method=GET", ":path=/"));
		assertThat(client.awaitTrue(() -> client.reset(streamId) != null, Duration.ofSeconds(5))).isTrue();
		return client.reset(streamId);
	}

	private static boolean get(H2TestClient client, int streamId, String host) throws Exception {
		client.headers(streamId, true, TestH2.headerBlock(":authority=" + host, ":method=GET", ":path=/"));
		return client.awaitEnd(streamId, Duration.ofSeconds(5));
	}

	/** The connections the tunnel session of the host's route holds. */
	private int connections(String host) {
		String clientId = Objects.requireNonNull(this.router).lookup(host).orElseThrow().clientId();
		return Objects.requireNonNull(this.sessions).find(clientId).orElseThrow().connectionCount();
	}

	private int admitted() {
		return (int) Objects.requireNonNull(this.meterRegistry).get("sluice.data.connections.admitted").gauge().value();
	}

	private static void answerEndedRequests(Socket socket) {
		try (socket) {
			InputStream in = socket.getInputStream();
			OutputStream out = socket.getOutputStream();
			if (in.readNBytes(TestH2.PREFACE.length).length < TestH2.PREFACE.length) {
				return;
			}
			TestH2.writeFrame(out, TestH2.SETTINGS, 0, 0, new byte[0]);
			TestH2.@Nullable Frame frame;
			while ((frame = TestH2.readFrame(in)) != null) {
				boolean ended = frame.streamId() != 0 && (frame.type() == TestH2.HEADERS || frame.type() == TestH2.DATA)
						&& (frame.flags() & TestH2.FLAG_END_STREAM) != 0;
				if (ended) {
					out.write(TestH2.cannedResponse(frame.streamId(), "ok"));
					out.flush();
				}
			}
		}
		catch (IOException e) {
			// connection ended
		}
	}

	private void startClient(String host, ServerSocket upstream) {
		TunnelClient client = TunnelClient.builder()
			.properties(SluiceClientProperties.builder()
				.serverUrl("grpc://127.0.0.1:" + grpcPort)
				.upstream(Upstream.builder().host(host).target("tcp://127.0.0.1:" + upstream.getLocalPort()).build())
				.token("it-token")
				.build())
			.taskExecutor(task -> Thread.ofVirtual().name("e2e-lo").start(task))
			.meterRegistry(new SimpleMeterRegistry())
			.build();
		client.start();
		this.clients.add(client);
		Awaitility.await()
			.atMost(Duration.ofSeconds(10))
			.until(() -> Objects.requireNonNull(this.router).lookup(host).isPresent());
	}

	private ServerSocket stub(Consumer<Socket> handler) {
		try {
			ServerSocket server = new ServerSocket();
			server.bind(new InetSocketAddress("127.0.0.1", 0), 128);
			this.closeables.add(server);
			Thread.ofVirtual().name("e2e-lo-accept").start(() -> {
				while (!server.isClosed()) {
					try {
						Socket socket = server.accept();
						this.closeables.add(socket);
						UPSTREAM_EXECUTOR.execute(() -> handler.accept(socket));
					}
					catch (IOException e) {
						return;
					}
				}
			});
			return server;
		}
		catch (IOException e) {
			throw new IllegalStateException(e);
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

}
