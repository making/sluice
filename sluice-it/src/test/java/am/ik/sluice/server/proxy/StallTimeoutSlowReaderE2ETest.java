package am.ik.sluice.server.proxy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.client.config.Upstream;
import am.ik.sluice.client.tunnel.TunnelClient;
import am.ik.sluice.it.TestPorts;
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
 * A client reading a large HTTP/1.1 response slowly but steadily -- one 64KiB read per
 * second -- is not cut by the stall timeout, though the relay's writes to it block most
 * of the time. Its own class for a stall timeout of 10s: the relay sees a client's reads
 * only as its TCP window reopens, and a receiver whose buffer filled up may announce that
 * only to the sender's persist probes. Observed on macOS loopback: the relay's writes
 * resume 4 to 5s apart at 64KiB per second, and the gaps grow past 15s at 16KiB per
 * second.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class StallTimeoutSlowReaderE2ETest {

	private static final Duration STALL_TIMEOUT = Duration.ofSeconds(10);

	private static final String HOST = "stall-slow.test";

	private static final int BIG = 64 * 1024 * 1024;

	private static int grpcPort;

	private static int dataPort;

	private final List<AutoCloseable> closeables = new CopyOnWriteArrayList<>();

	private @Nullable TunnelClient client;

	@Nullable
	@Autowired
	private MeterRegistry meterRegistry;

	@Nullable
	@Autowired
	private Router router;

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) {
		grpcPort = TestPorts.freePort();
		dataPort = TestPorts.freePort();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(dataPort));
		registry.add("sluice.limits.stall-timeout", () -> STALL_TIMEOUT.toSeconds() + "s");
		registry.add("sluice.token", () -> "it-token");
		registry.add("server.port", () -> String.valueOf(TestPorts.freePort()));
	}

	@BeforeAll
	void startClient() throws IOException {
		ServerSocket upstream = new ServerSocket();
		upstream.bind(new InetSocketAddress("127.0.0.1", 0), 128);
		this.closeables.add(upstream);
		Thread.ofVirtual().name("e2e-stall-slow-accept").start(() -> {
			while (!upstream.isClosed()) {
				try {
					Socket socket = upstream.accept();
					this.closeables.add(socket);
					Thread.ofVirtual().name("e2e-stall-slow-upstream").start(() -> serveBig(socket));
				}
				catch (IOException e) {
					return;
				}
			}
		});
		TunnelClient started = TunnelClient.builder()
			.properties(SluiceClientProperties.builder()
				.serverUrl("grpc://127.0.0.1:" + grpcPort)
				.upstream(Upstream.builder().host(HOST).target("tcp://127.0.0.1:" + upstream.getLocalPort()).build())
				.token("it-token")
				.build())
			.taskExecutor(task -> Thread.ofVirtual().name("e2e-stall-slow-worker").start(task))
			.meterRegistry(new SimpleMeterRegistry())
			.build();
		started.start();
		this.client = started;
		Awaitility.await()
			.atMost(Duration.ofSeconds(10))
			.until(() -> Objects.requireNonNull(this.router).lookup(HOST).isPresent());
	}

	@AfterAll
	void tearDown() {
		if (this.client != null) {
			this.client.stop();
		}
		this.closeables.forEach(closeable -> {
			try {
				closeable.close();
			}
			catch (Exception e) {
				// ignore
			}
		});
	}

	/**
	 * The receive buffer is never found empty, so the client is what limits the relay's
	 * writes, and what it reads over two and a half stall timeouts is a sliver of what
	 * the upstream offered at once.
	 */
	@Test
	void aClientReadingSlowlyButSteadilyIsNotCut() throws Exception {
		double stalls = stallClosed();
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.setSoTimeout(5000);
			socket.getOutputStream()
				.write(("GET /big HTTP/1.1\r\nHost: " + HOST + "\r\n\r\n").getBytes(StandardCharsets.US_ASCII));
			InputStream in = socket.getInputStream();
			byte[] buffer = new byte[64 * 1024];
			long received = 0;
			long end = System.nanoTime() + STALL_TIMEOUT.multipliedBy(5).dividedBy(2).toNanos();
			Thread.sleep(1000);
			while (System.nanoTime() < end) {
				assertThat(in.available()).as("the relay kept the receive buffer filled").isPositive();
				int n = in.readNBytes(buffer, 0, buffer.length);
				assertThat(n).as("no EOF").isEqualTo(buffer.length);
				received += n;
				Thread.sleep(1000);
			}
			assertThat(received).as("a sliver of the response").isLessThan(BIG / 16);
			assertThat(stallClosed()).isEqualTo(stalls);
		}
	}

	/** Answers every request head with {@link #BIG} bytes. */
	private static void serveBig(Socket socket) {
		try (socket; InputStream in = socket.getInputStream(); OutputStream out = socket.getOutputStream()) {
			while (readHead(in)) {
				out.write(
						("HTTP/1.1 200 OK\r\nContent-Length: " + BIG + "\r\n\r\n").getBytes(StandardCharsets.US_ASCII));
				byte[] chunk = new byte[64 * 1024];
				for (int sent = 0; sent < BIG; sent += chunk.length) {
					out.write(chunk);
				}
				out.flush();
			}
		}
		catch (IOException e) {
			// the peer went away
		}
	}

	/** Reads up to and including the blank line; false at EOF. */
	private static boolean readHead(InputStream in) throws IOException {
		ByteArrayOutputStream head = new ByteArrayOutputStream();
		int b;
		while ((b = in.read()) >= 0) {
			head.write(b);
			if (head.toString(StandardCharsets.US_ASCII).endsWith("\r\n\r\n")) {
				return true;
			}
		}
		return false;
	}

	private double stallClosed() {
		return Objects.requireNonNull(this.meterRegistry).get("sluice.data.connections.stall.closed").counter().count();
	}

}
