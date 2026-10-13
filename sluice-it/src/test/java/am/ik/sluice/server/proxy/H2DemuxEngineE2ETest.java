package am.ik.sluice.server.proxy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.client.config.Upstream;
import am.ik.sluice.client.tunnel.TunnelClient;
import am.ik.sluice.it.H2TestClient;
import am.ik.sluice.it.TestH2;
import am.ik.sluice.it.TestPorts;
import am.ik.sluice.it.WindowedH2Upstream;
import am.ik.sluice.server.SluiceServerApplication;
import am.ik.sluice.server.config.SluiceServerProperties;
import am.ik.sluice.server.config.SluiceServerProperties.H2DemuxEngine;
import am.ik.sluice.server.route.Router;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code sluice.h2-demux-engine} selects the relay of every h2 connection: with
 * {@code netty}, the connection is counted for the netty engine and served by netty's
 * codec, whose SETTINGS announce the 64KiB header list limit the legacy relay never
 * announces. The test seam switches it back to the legacy relay, told apart the same way.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class H2DemuxEngineE2ETest {

	private static final String HOST = "engine.h2demux.test";

	private static final int SETTINGS_MAX_HEADER_LIST_SIZE = 0x6;

	private static final Duration TIMEOUT = Duration.ofSeconds(10);

	private static final ExecutorService UPSTREAM_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

	private static int grpcPort;

	private static int dataPort;

	private final List<AutoCloseable> closeables = new CopyOnWriteArrayList<>();

	private @Nullable TunnelClient client;

	@Nullable
	@Autowired
	private DataProxyServer dataProxyServer;

	@Nullable
	@Autowired
	private SluiceServerProperties properties;

	@Nullable
	@Autowired
	private Router router;

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) {
		grpcPort = TestPorts.freePort();
		dataPort = TestPorts.freePort();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(dataPort));
		registry.add("sluice.h2-demux-engine", () -> "netty");
		registry.add("sluice.token", () -> "it-token");
		registry.add("server.port", () -> String.valueOf(TestPorts.freePort()));
	}

	@BeforeAll
	void startClient() throws IOException {
		ServerSocket upstream = new ServerSocket();
		upstream.bind(new InetSocketAddress("127.0.0.1", 0), 128);
		this.closeables.add(upstream);
		Thread.ofVirtual().name("e2e-h2engine-accept").start(() -> {
			while (!upstream.isClosed()) {
				try {
					Socket socket = upstream.accept();
					this.closeables.add(socket);
					UPSTREAM_EXECUTOR.execute(() -> serve(socket));
				}
				catch (IOException e) {
					return;
				}
			}
		});
		TunnelClient started = TunnelClient.builder()
			.properties(SluiceClientProperties.builder()
				.serverUrl("grpc://127.0.0.1:" + grpcPort)
				.token("it-token")
				.upstream(Upstream.builder().host(HOST).target("tcp://127.0.0.1:" + upstream.getLocalPort()).build())
				.build())
			.taskExecutor(task -> Thread.ofVirtual().name("e2e-h2engine-worker").start(task))
			.meterRegistry(new SimpleMeterRegistry())
			.build();
		started.start();
		this.client = started;
		Awaitility.await().atMost(TIMEOUT).until(() -> router().lookup(HOST).isPresent());
	}

	@AfterEach
	void restoreEngine() {
		server().h2DemuxEngine(Objects.requireNonNull(this.properties).h2DemuxEngine());
	}

	@AfterAll
	void tearDown() {
		if (this.client != null) {
			this.client.stop();
		}
		this.closeables.forEach(H2DemuxEngineE2ETest::close);
		UPSTREAM_EXECUTOR.shutdown();
	}

	@Test
	void theSettingSelectsTheNettyRelay() throws Exception {
		assertThat(Objects.requireNonNull(this.properties).h2DemuxEngine()).isEqualTo(H2DemuxEngine.NETTY);
		double netty = server().h2DemuxMeters().demuxedBy(H2DemuxEngine.NETTY);
		double legacy = server().h2DemuxMeters().demuxedBy(H2DemuxEngine.LEGACY);
		try (H2TestClient h2 = request()) {
			assertThat(h2.setting(SETTINGS_MAX_HEADER_LIST_SIZE)).as("netty's codec announces the header list limit")
				.isEqualTo(64L * 1024);
		}
		assertThat(server().h2DemuxMeters().demuxedBy(H2DemuxEngine.NETTY)).isEqualTo(netty + 1);
		assertThat(server().h2DemuxMeters().demuxedBy(H2DemuxEngine.LEGACY)).isEqualTo(legacy);
	}

	@Test
	void theSeamSelectsTheLegacyRelay() throws Exception {
		server().h2DemuxEngine(H2DemuxEngine.LEGACY);
		double netty = server().h2DemuxMeters().demuxedBy(H2DemuxEngine.NETTY);
		double legacy = server().h2DemuxMeters().demuxedBy(H2DemuxEngine.LEGACY);
		try (H2TestClient h2 = request()) {
			assertThat(h2.setting(SETTINGS_MAX_HEADER_LIST_SIZE)).as("the legacy relay announces no header list limit")
				.isNull();
		}
		assertThat(server().h2DemuxMeters().demuxedBy(H2DemuxEngine.LEGACY)).isEqualTo(legacy + 1);
		assertThat(server().h2DemuxMeters().demuxedBy(H2DemuxEngine.NETTY)).isEqualTo(netty);
	}

	/** A request answered by the upstream, the relay's SETTINGS read before it. */
	private H2TestClient request() throws Exception {
		H2TestClient h2 = H2TestClient.connect(dataPort, 1);
		try {
			h2.headers(1, true, TestH2.headerBlock(":authority=" + HOST, ":method=GET", ":scheme=http", ":path=/"));
			assertThat(h2.awaitEnd(1, TIMEOUT)).as("answered").isTrue();
			assertThat(TestH2.bodyOf(h2.frames(1))).isEqualTo("ok");
			assertThat(h2.awaitSettings(TIMEOUT)).isTrue();
			return h2;
		}
		catch (Exception | Error e) {
			h2.close();
			throw e;
		}
	}

	/** Answers every ended request with {@code ok}. */
	private static void serve(Socket socket) {
		try (socket) {
			WindowedH2Upstream upstream = WindowedH2Upstream.accept(socket);
			if (upstream == null) {
				return;
			}
			upstream.settings(null);
			TestH2.@Nullable Frame frame;
			while ((frame = upstream.next()) != null) {
				boolean request = frame.type() == TestH2.HEADERS || frame.type() == TestH2.DATA;
				if (request && (frame.flags() & TestH2.FLAG_END_STREAM) != 0) {
					upstream.headers(frame.streamId(), false);
					upstream.sendData(frame.streamId(), "ok".getBytes(StandardCharsets.UTF_8), true);
				}
			}
		}
		catch (Exception e) {
			// connection ended
		}
	}

	private DataProxyServer server() {
		return Objects.requireNonNull(this.dataProxyServer, "dataProxyServer");
	}

	private Router router() {
		return Objects.requireNonNull(this.router, "router");
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
