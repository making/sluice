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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;

import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.client.config.Upstream;
import am.ik.sluice.client.tunnel.TunnelClient;
import am.ik.sluice.server.SluiceServerApplication;
import am.ik.sluice.server.route.Router;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Review probe: a header block split into HEADERS + CONTINUATION must reach the upstream
 * contiguous (RFC 9113 6.10) while the leg's inbound side emits connection-level window
 * updates concurrently.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class HeaderBlockInterleaveProbeTest {

	private static final int REQUESTS = 1500;

	private static final List<TunnelClient> clients = new ArrayList<>();

	private static int grpcPort;

	private static int dataPort;

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
		registry.add("sluice.access-log.rate-limit.enabled", () -> "false");
		registry.add("server.port", () -> String.valueOf(TestPorts.freePort()));
	}

	@AfterAll
	static void tearDown() {
		clients.forEach(TunnelClient::stop);
	}

	@Test
	void continuationIsNeverInterleaved() throws Exception {
		AtomicReference<String> violation = new AtomicReference<>("");
		AtomicInteger heads = new AtomicInteger();
		ServerSocket upstream = new ServerSocket();
		upstream.bind(new InetSocketAddress("127.0.0.1", 0));
		Thread.ofVirtual().start(() -> {
			try {
				Socket socket = upstream.accept();
				InputStream in = socket.getInputStream();
				OutputStream out = socket.getOutputStream();
				in.readNBytes(TestH2.PREFACE.length);
				int expectContinuationOf = -1;
				boolean streaming = false;
				TestH2.@Nullable Frame frame;
				while ((frame = TestH2.readFrame(in)) != null) {
					if (expectContinuationOf != -1
							&& (frame.type() != TestH2.CONTINUATION || frame.streamId() != expectContinuationOf)) {
						violation.compareAndSet("", "type=%d stream=%d inside the header block of stream %d"
							.formatted(frame.type(), frame.streamId(), expectContinuationOf));
					}
					if (frame.type() == TestH2.HEADERS || frame.type() == TestH2.CONTINUATION) {
						boolean endHeaders = (frame.flags() & TestH2.FLAG_END_HEADERS) != 0;
						expectContinuationOf = endHeaders ? -1 : frame.streamId();
						if (endHeaders) {
							heads.incrementAndGet();
						}
					}
					if (!streaming && frame.type() == TestH2.HEADERS && frame.streamId() == 1) {
						streaming = true;
						// an endless response on stream 1: every relay read of it queues a
						// connection-level WINDOW_UPDATE towards this upstream
						Thread.ofVirtual().start(() -> {
							try {
								TestH2.writeFrame(out, TestH2.SETTINGS, 0, 0, new byte[0]);
								TestH2.writeFrame(out, TestH2.HEADERS, TestH2.FLAG_END_HEADERS, 1,
										new byte[] { (byte) 0x88 });
								byte[] chunk = new byte[1024];
								while (true) {
									TestH2.writeFrame(out, TestH2.DATA, 0, 1, chunk);
								}
							}
							catch (IOException e) {
								// ended
							}
						});
					}
				}
			}
			catch (IOException e) {
				// ended
			}
		});
		String host = "h2d.interleave.test";
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
		// ~40KiB of headers: re-encoded by the relay into HEADERS + 2 CONTINUATION
		List<String> big = new ArrayList<>(List.of(":authority=" + host, ":method=GET", ":path=/big"));
		for (int i = 0; i < 320; i++) {
			big.add("x-h" + i + "=" + "v".repeat(120));
		}
		byte[] bigBlock = TestH2.headerBlock(big.toArray(String[]::new));
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			OutputStream out = socket.getOutputStream();
			Thread.ofVirtual().start(() -> {
				try {
					socket.getInputStream().transferTo(OutputStream.nullOutputStream());
				}
				catch (IOException e) {
					// ended
				}
			});
			out.write(TestH2.PREFACE);
			TestH2.writeFrame(out, TestH2.SETTINGS, 0, 0, new byte[0]);
			TestH2.writeFrame(out, TestH2.HEADERS, TestH2.FLAG_END_HEADERS | TestH2.FLAG_END_STREAM, 1,
					TestH2.headerBlock(":authority=" + host, ":method=GET", ":path=/stream"));
			for (int i = 0; i < REQUESTS && violation.get().isEmpty(); i++) {
				TestH2.writeFrame(out, TestH2.HEADERS, TestH2.FLAG_END_HEADERS | TestH2.FLAG_END_STREAM, 3 + 2 * i,
						bigBlock);
			}
			Awaitility.await()
				.atMost(Duration.ofSeconds(30))
				.until(() -> !violation.get().isEmpty() || heads.get() >= REQUESTS + 1);
			System.out.printf("REVIEW heads=%d violation=%s%n", heads.get(), violation.get());
			assertThat(violation.get()).as("frames interleaved into a header block").isEmpty();
		}
	}

}
