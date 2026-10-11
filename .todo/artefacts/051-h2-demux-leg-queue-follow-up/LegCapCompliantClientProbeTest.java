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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;

import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.client.config.Upstream;
import am.ik.sluice.client.tunnel.TunnelClient;
import am.ik.sluice.server.SluiceServerApplication;
import am.ik.sluice.server.route.Router;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/** Review probe: a client honoring every window must not be torn down by the leg cap. */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class LegCapCompliantClientProbeTest {

	private static final int STREAMS = 200;

	private static final int STREAM_WINDOW = 65535;

	private static final List<Socket> held = new ArrayList<>();

	private static final List<TunnelClient> clients = new ArrayList<>();

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
		registry.add("server.port", () -> String.valueOf(TestPorts.freePort()));
	}

	@AfterAll
	static void tearDown() {
		clients.forEach(TunnelClient::stop);
		synchronized (held) {
			held.forEach(s -> {
				try {
					s.close();
				}
				catch (IOException e) {
					// ignore
				}
			});
		}
	}

	@Test
	void compliantClientWithManyStreamsIsNotTornDown() throws Exception {
		ServerSocket upstream = new ServerSocket();
		upstream.bind(new InetSocketAddress("127.0.0.1", 0));
		Thread.ofVirtual().start(() -> {
			while (true) {
				try {
					Socket s = upstream.accept();
					synchronized (held) {
						held.add(s); // accepts, never reads
					}
				}
				catch (IOException e) {
					return;
				}
			}
		});
		String host = "h2d.review.test";
		TunnelClient client = TunnelClient.builder()
			.properties(SluiceClientProperties.builder()
				.serverUrl("grpc://127.0.0.1:" + grpcPort)
				.upstream(Upstream.builder().host(host).target("tcp://127.0.0.1:" + upstream.getLocalPort()).build())
				.token("it-token")
				.build())
			.taskExecutor(task -> Thread.ofVirtual().start(task))
			.meterRegistry(new SimpleMeterRegistry())
			.build();
		client.start();
		clients.add(client);
		Awaitility.await()
			.atMost(Duration.ofSeconds(10))
			.until(() -> Objects.requireNonNull(this.router).lookup(host).isPresent());
		AtomicBoolean closed = new AtomicBoolean();
		// the client's connection send window: the spec initial plus the relay's grants
		AtomicLong connectionWindow = new AtomicLong(STREAM_WINDOW);
		AtomicLong maxQueued = new AtomicLong();
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			OutputStream out = socket.getOutputStream();
			Thread.ofVirtual().start(() -> {
				try {
					InputStream in = socket.getInputStream();
					TestH2.@Nullable Frame frame;
					while ((frame = TestH2.readFrame(in)) != null) {
						if (frame.type() == 0x8 && frame.streamId() == 0) {
							byte[] p = frame.payload();
							connectionWindow.addAndGet((p[0] & 0x7f) << 24 | (p[1] & 0xff) << 16 | (p[2] & 0xff) << 8
									| (p[3] & 0xff));
						}
					}
				}
				catch (IOException e) {
					// reset
				}
				closed.set(true);
			});
			out.write(TestH2.PREFACE);
			TestH2.writeFrame(out, TestH2.SETTINGS, 0, 0, new byte[0]);
			long sent = 0;
			int chunk = 16 * 1024;
			try {
				for (int i = 0; i < STREAMS; i++) {
					int streamId = 1 + 2 * i;
					TestH2.writeFrame(out, TestH2.HEADERS, TestH2.FLAG_END_HEADERS, streamId,
							TestH2.headerBlock(":authority=" + host, ":method=POST", ":path=/up" + i));
					// exactly the stream's initial window, never more
					for (int left = STREAM_WINDOW; left > 0; left -= chunk) {
						int n = Math.min(chunk, left);
						long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
						while (connectionWindow.get() - sent < n) {
							assertThat(System.nanoTime()).as("connection window granted").isLessThan(deadline);
							assertThat(closed.get()).as("closed at stream %d, sent %d", i, sent).isFalse();
							Thread.sleep(1);
						}
						TestH2.writeFrame(out, TestH2.DATA, 0, streamId, new byte[n]);
						sent += n;
						maxQueued.accumulateAndGet(queued(host), Math::max);
					}
				}
			}
			catch (IOException e) {
				// the relay closed the connection
			}
			Thread.sleep(3000);
			maxQueued.accumulateAndGet(queued(host), Math::max);
			System.out.printf("REVIEW sent=%d maxQueued=%d closed=%s%n", sent, maxQueued.get(), closed.get());
			assertThat(closed.get()).as("a compliant client (sent %d, every stream within its window) is not torn down",
					sent)
				.isFalse();
		}
	}

	private long queued(String route) {
		return Math.round(Objects.requireNonNull(this.meterRegistry)
			.find("sluice.h2demux.queued.bytes")
			.tags("route", route)
			.gauges()
			.stream()
			.mapToDouble(Gauge::value)
			.max()
			.orElse(0));
	}

}
