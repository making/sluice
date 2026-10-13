package am.ik.sluice.server.proxy;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.client.config.Upstream;
import am.ik.sluice.client.tunnel.TunnelClient;
import am.ik.sluice.it.H2TestClient;
import am.ik.sluice.it.TestH2;
import am.ik.sluice.it.TestPorts;
import am.ik.sluice.server.SluiceServerApplication;
import am.ik.sluice.server.route.Router;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * An h2 connection outlives {@link NettyHttp2DemuxRelay#HOP_DONE_TIMEOUT}, the time its
 * hops' threads get to end once a channel closed: a stream in flight across it gets its
 * response, the connection stays open and takes new streams. In the relay's package for
 * that constant.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class H2DemuxLongLivedConnectionE2ETest {

	private static final String ROUTE = "long.test";

	/** The stream the upstream answers only once released; the leg keeps its id. */
	private static final int HELD_STREAM = 3;

	private static int grpcPort;

	private static int dataPort;

	private final List<AutoCloseable> closeables = new CopyOnWriteArrayList<>();

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

	@AfterAll
	void tearDown() {
		this.closeables.forEach(H2DemuxLongLivedConnectionE2ETest::close);
	}

	@Test
	void connectionOutlivesTheHopDoneTimeout() throws Exception {
		CountDownLatch release = new CountDownLatch(1);
		startClient(stub(socket -> answer(socket, release)));
		try (H2TestClient client = H2TestClient.connect(dataPort)) {
			long opened = System.nanoTime();
			client.headers(1, true, TestH2.headerBlock(":authority=" + ROUTE, ":method=GET", ":path=/first"));
			assertThat(client.awaitEnd(1, Duration.ofSeconds(10))).as("the first stream").isTrue();
			client.headers(HELD_STREAM, true, TestH2.headerBlock(":authority=" + ROUTE, ":method=GET", ":path=/held"));
			Duration past = NettyHttp2DemuxRelay.HOP_DONE_TIMEOUT.plusSeconds(1);
			Thread.sleep(Math.max(0, past.toMillis() - Duration.ofNanos(System.nanoTime() - opened).toMillis()));
			assertThat(client.closed()).as("open past the hop done timeout").isFalse();
			release.countDown();
			assertThat(client.awaitEnd(HELD_STREAM, Duration.ofSeconds(10))).as("the stream in flight across it")
				.isTrue();
			client.headers(5, true, TestH2.headerBlock(":authority=" + ROUTE, ":method=GET", ":path=/later"));
			assertThat(client.awaitEnd(5, Duration.ofSeconds(10))).as("a new stream after it").isTrue();
			assertThat(client.resets()).isEmpty();
			assertThat(client.goAwayCode()).isEqualTo(-1);
		}
	}

	/**
	 * Answers every request that ends with its head, the one of {@link #HELD_STREAM} once
	 * {@code release} opened.
	 */
	private static void answer(Socket socket, CountDownLatch release) {
		try (socket) {
			InputStream in = socket.getInputStream();
			OutputStream out = socket.getOutputStream();
			if (in.readNBytes(TestH2.PREFACE.length).length < TestH2.PREFACE.length) {
				return;
			}
			TestH2.writeFrame(out, TestH2.SETTINGS, 0, 0, new byte[0]);
			TestH2.@Nullable Frame frame;
			while ((frame = TestH2.readFrame(in)) != null) {
				if (frame.type() != TestH2.HEADERS || (frame.flags() & TestH2.FLAG_END_STREAM) == 0) {
					continue;
				}
				int streamId = frame.streamId();
				if (streamId == HELD_STREAM) {
					Thread.ofVirtual().name("e2e-long-held").start(() -> {
						try {
							release.await();
							synchronized (out) {
								out.write(TestH2.cannedResponse(streamId, "ok"));
								out.flush();
							}
						}
						catch (IOException e) {
							// connection ended
						}
						catch (InterruptedException e) {
							Thread.currentThread().interrupt();
						}
					});
					continue;
				}
				synchronized (out) {
					out.write(TestH2.cannedResponse(streamId, "ok"));
					out.flush();
				}
			}
		}
		catch (IOException e) {
			// connection ended
		}
	}

	private void startClient(ServerSocket upstream) {
		TunnelClient client = TunnelClient.builder()
			.properties(SluiceClientProperties.builder()
				.serverUrl("grpc://127.0.0.1:" + grpcPort)
				.upstream(Upstream.builder().host(ROUTE).target("tcp://127.0.0.1:" + upstream.getLocalPort()).build())
				.token("it-token")
				.build())
			.taskExecutor(task -> Thread.ofVirtual().name("e2e-long").start(task))
			.meterRegistry(new SimpleMeterRegistry())
			.build();
		client.start();
		this.closeables.add(client::stop);
		Awaitility.await()
			.atMost(Duration.ofSeconds(10))
			.until(() -> Objects.requireNonNull(this.router).lookup(ROUTE).isPresent());
	}

	private ServerSocket stub(Consumer<Socket> handler) throws IOException {
		ServerSocket server = new ServerSocket();
		server.bind(new InetSocketAddress("127.0.0.1", 0));
		this.closeables.add(server);
		Thread.ofVirtual().name("e2e-long-accept").start(() -> {
			while (!server.isClosed()) {
				try {
					Socket socket = server.accept();
					this.closeables.add(socket);
					Thread.ofVirtual().name("e2e-long-upstream").start(() -> handler.accept(socket));
				}
				catch (IOException e) {
					return;
				}
			}
		});
		return server;
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
