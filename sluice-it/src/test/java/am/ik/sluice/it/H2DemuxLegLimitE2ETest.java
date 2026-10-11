package am.ik.sluice.it;

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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;

import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.client.config.Upstream;
import am.ik.sluice.client.tunnel.TunnelClient;
import am.ik.sluice.server.SluiceServerApplication;
import am.ik.sluice.server.route.Router;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * An h2 connection takes a slot of the connection limit for every leg beyond the first: a
 * stream whose route needs a leg while no slot is free is refused (REFUSED_STREAM), a
 * plain connection meanwhile is closed unread, and every slot comes back -- when a leg
 * retires, and when the connection ends.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class H2DemuxLegLimitE2ETest {

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

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) {
		grpcPort = TestPorts.freePort();
		dataPort = TestPorts.freePort();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(dataPort));
		registry.add("sluice.token", () -> "it-token");
		registry.add("sluice.access-log.rate-limit.enabled", () -> "false");
		registry.add("sluice.limits.max-connections", () -> "3");
		registry.add("server.port", () -> String.valueOf(TestPorts.freePort()));
	}

	@AfterAll
	void tearDown() {
		this.clients.forEach(TunnelClient::stop);
		this.closeables.forEach(H2DemuxLegLimitE2ETest::close);
		UPSTREAM_EXECUTOR.shutdown();
	}

	@Test
	void legsBeyondTheFirstTakeSlotsOfTheConnectionLimit() throws Exception {
		startClient("leg1.test", stub(H2DemuxLegLimitE2ETest::answerEndedRequests));
		CountDownLatch retire = new CountDownLatch(1);
		startClient("leg2.test", stub(socket -> answerThenClose(socket, retire)));
		startClient("leg3.test", stub(H2DemuxLegLimitE2ETest::answerEndedRequests));
		startClient("leg4.test", stub(H2DemuxLegLimitE2ETest::answerEndedRequests));
		assertThat(admitted()).isZero();
		double refusedBefore = refusedForLegLimit();
		double rejectedBefore = rejected();
		try (H2TestClient client = H2TestClient.connect(dataPort)) {
			assertThat(get(client, 1, "leg1.test")).as("the first leg rides on the connection's slot").isTrue();
			assertThat(admitted()).isEqualTo(1);
			assertThat(get(client, 3, "leg2.test")).isTrue();
			assertThat(get(client, 5, "leg3.test")).isTrue();
			assertThat(admitted()).as("one slot per further leg").isEqualTo(3);

			client.headers(7, true, TestH2.headerBlock(":authority=leg4.test", ":method=GET", ":path=/"));
			assertThat(client.awaitTrue(() -> client.reset(7) != null, Duration.ofSeconds(5))).isTrue();
			assertThat(client.reset(7)).isEqualTo(TestH2.REFUSED_STREAM);
			assertThat(refusedForLegLimit()).isEqualTo(refusedBefore + 1);
			assertThat(get(client, 9, "leg1.test")).as("a route with a leg is still served").isTrue();

			try (Socket plain = new Socket("127.0.0.1", dataPort)) {
				assertThat(TestSockets.awaitClosed(plain, Duration.ofSeconds(5))).as("closed unread")
					.isLessThan(Duration.ofSeconds(5));
			}
			Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> rejected() == rejectedBefore + 1);

			// the upstream of leg2 closes with no response owed: the idle leg retires
			retire.countDown();
			Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> admitted() == 2);
			assertThat(get(client, 11, "leg4.test")).as("the freed slot takes the new leg").isTrue();
			assertThat(admitted()).isEqualTo(3);
			assertThat(client.closed()).isFalse();
		}
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> admitted() == 0);
	}

	/** Sends a GET on the stream; whether its answer ended it. */
	private static boolean get(H2TestClient client, int streamId, String host) throws Exception {
		client.headers(streamId, true, TestH2.headerBlock(":authority=" + host, ":method=GET", ":path=/"));
		return client.awaitEnd(streamId, Duration.ofSeconds(5));
	}

	/** An h2 stub answering every ended request. */
	private static void answerEndedRequests(Socket socket) {
		answer(socket, null);
	}

	/**
	 * An h2 stub answering the first ended request, then ending its side of the
	 * connection once the latch opens.
	 */
	private static void answerThenClose(Socket socket, CountDownLatch close) {
		answer(socket, close);
	}

	private static void answer(Socket socket, @Nullable CountDownLatch close) {
		try (socket) {
			InputStream in = socket.getInputStream();
			OutputStream out = socket.getOutputStream();
			if (in.readNBytes(TestH2.PREFACE.length).length < TestH2.PREFACE.length) {
				return;
			}
			TestH2.@Nullable Frame frame;
			while ((frame = TestH2.readFrame(in)) != null) {
				boolean ended = frame.streamId() != 0 && (frame.type() == TestH2.HEADERS || frame.type() == TestH2.DATA)
						&& (frame.flags() & TestH2.FLAG_END_STREAM) != 0;
				if (ended) {
					out.write(TestH2.cannedResponse(frame.streamId(), "ok"));
					out.flush();
					if (close != null) {
						close.await();
						// half-closed, still reading: a close with unread bytes would
						// reset
						socket.shutdownOutput();
						while (in.read() != -1) {
							// drain
						}
						return;
					}
				}
			}
		}
		catch (IOException e) {
			// connection ended
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	private int admitted() {
		return (int) Objects.requireNonNull(this.meterRegistry).get("sluice.data.connections.admitted").gauge().value();
	}

	private double refusedForLegLimit() {
		return Objects.requireNonNull(this.meterRegistry)
			.get("sluice.h2demux.streams.refused")
			.tag("reason", "leg-limit")
			.counter()
			.count();
	}

	private double rejected() {
		return Objects.requireNonNull(this.meterRegistry)
			.get("sluice.data.connections.rejected")
			.tag("reason", "limit")
			.counter()
			.count();
	}

	private void startClient(String host, ServerSocket upstream) {
		TunnelClient client = TunnelClient.builder()
			.properties(SluiceClientProperties.builder()
				.serverUrl("grpc://127.0.0.1:" + grpcPort)
				.upstream(Upstream.builder().host(host).target("tcp://127.0.0.1:" + upstream.getLocalPort()).build())
				.token("it-token")
				.build())
			.taskExecutor(task -> Thread.ofVirtual().name("e2e-leg").start(task))
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
			Thread.ofVirtual().name("e2e-leg-accept").start(() -> {
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
