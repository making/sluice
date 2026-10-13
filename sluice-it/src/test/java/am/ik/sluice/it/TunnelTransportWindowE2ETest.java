package am.ik.sluice.it;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.DoubleSupplier;

import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.Timeout;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.util.unit.DataSize;

import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.client.config.Upstream;
import am.ik.sluice.client.tunnel.TunnelClient;
import am.ik.sluice.server.SluiceServerApplication;
import am.ik.sluice.server.route.Router;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The gRPC connection carrying a tunnel lets the tunnel's connections use their windows
 * at once, both ways: many transfers through one tunnel move what their windows hold per
 * round trip, far more than the 8MiB gRPC's own window tuning reaches at most. The round
 * trips are counted ({@link RoundTripProxy}), not timed.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
@Timeout(120)
class TunnelTransportWindowE2ETest {

	private static final long MIB = 1024 * 1024;

	/** The budget of the server and of the client: room for every connection's window. */
	private static final long BUDGET = 48 * MIB;

	/** Transfers at once, each growing to the full 1MiB window. */
	private static final int TRANSFERS = 48;

	private static final long BODY = 4 * MIB;

	/** The window gRPC's netty transport tunes itself up to at most. */
	private static final long GRPC_TUNED_MAX = 8 * MIB;

	/** How long the sending direction must be quiet to close a round. */
	private static final Duration QUIET = Duration.ofMillis(250);

	private static final Duration TIMEOUT = Duration.ofSeconds(60);

	private static int grpcPort;

	private static int dataPort;

	@Autowired
	private @Nullable MeterRegistry serverMeters;

	@Autowired
	private @Nullable Router router;

	private final List<AutoCloseable> closeables = new CopyOnWriteArrayList<>();

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) {
		grpcPort = TestPorts.freePort();
		dataPort = TestPorts.freePort();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(dataPort));
		registry.add("sluice.limits.tunnel-budget", () -> BUDGET + "B");
		registry.add("sluice.token", () -> "it-token");
		registry.add("server.port", () -> String.valueOf(TestPorts.freePort()));
	}

	@AfterEach
	void tearDown() {
		for (int i = this.closeables.size() - 1; i >= 0; i--) {
			try {
				this.closeables.get(i).close();
			}
			catch (Exception e) {
				// ignore
			}
		}
		this.closeables.clear();
	}

	/**
	 * Downloads: the client sends over the tunnel, against the server's transport window.
	 */
	@Test
	void downloadsMoveTheirWindowsPerRoundTrip() throws Exception {
		String host = "download.transport.window.test";
		RoundTripProxy tunnel = remember(RoundTripProxy.clientSends(grpcPort, QUIET));
		startClient(Upstream.builder().host(host).target("tcp://127.0.0.1:" + download()), tunnel.port());
		PeakSampler budgetUsed = remember(new PeakSampler(() -> server("sluice.tunnel.budget.used.bytes")));
		tunnel.arm();
		List<Thread> readers = new ArrayList<>();
		AtomicLong downloaded = new AtomicLong();
		for (int i = 0; i < TRANSFERS; i++) {
			Socket socket = remember(new Socket("127.0.0.1", dataPort));
			socket.getOutputStream()
				.write(("GET / HTTP/1.1\r\nHost: " + host + "\r\n\r\n").getBytes(StandardCharsets.US_ASCII));
			readers.add(Thread.ofVirtual().name("e2e-transport-window-reader").start(() -> {
				try {
					downloaded.addAndGet(drain(socket.getInputStream()));
				}
				catch (IOException e) {
					// counted short
				}
			}));
		}
		for (Thread reader : readers) {
			reader.join(TIMEOUT);
		}
		assertRoundTrips("downloads", tunnel, downloaded.get(), budgetUsed.peak());
	}

	/**
	 * Uploads: the server sends over the tunnel, against the client's transport window.
	 */
	@Test
	void uploadsMoveTheirWindowsPerRoundTrip() throws Exception {
		String host = "upload.transport.window.test";
		RoundTripProxy tunnel = remember(RoundTripProxy.targetSends(grpcPort, QUIET));
		AtomicLong uploaded = new AtomicLong();
		SimpleMeterRegistry clientMeters = startClient(
				Upstream.builder().host(host).target("tcp://127.0.0.1:" + sink(uploaded)), tunnel.port());
		PeakSampler budgetUsed = remember(
				new PeakSampler(() -> clientMeters.get("sluice.tunnel.budget.used.bytes").gauge().value()));
		tunnel.arm();
		for (int i = 0; i < TRANSFERS; i++) {
			Socket socket = remember(new Socket("127.0.0.1", dataPort));
			Thread.ofVirtual().name("e2e-transport-window-writer").start(() -> {
				try {
					OutputStream out = socket.getOutputStream();
					out.write(("POST / HTTP/1.1\r\nHost: " + host + "\r\nContent-Length: " + BODY + "\r\n\r\n")
						.getBytes(StandardCharsets.US_ASCII));
					byte[] chunk = new byte[64 * 1024];
					for (long sent = 0; sent < BODY; sent += chunk.length) {
						out.write(chunk);
					}
				}
				catch (IOException e) {
					// counted short
				}
			});
		}
		// a transfer still running at the deadline fails below, after the rounds are
		// reported
		long deadline = System.nanoTime() + TIMEOUT.toNanos();
		while (uploaded.get() < TRANSFERS * BODY && System.nanoTime() - deadline < 0) {
			Thread.sleep(50);
		}
		assertRoundTrips("uploads", tunnel, uploaded.get(), budgetUsed.peak());
	}

	private static void assertRoundTrips(String what, RoundTripProxy tunnel, long moved, long budgetPeak) {
		List<Long> rounds = tunnel.rounds();
		long peak = rounds.stream().mapToLong(Long::longValue).max().orElse(0);
		long sent = tunnel.sent();
		System.out.printf("%s: %d x %dMiB in %d round trips, at most %dKiB in one, budget used at most %dKiB%n", what,
				TRANSFERS, BODY / MIB, rounds.size(), peak >> 10, budgetPeak >> 10);
		System.out.printf("%s: KiB per round trip %s%n", what, rounds.stream().map(bytes -> bytes >> 10).toList());
		assertThat(budgetPeak).as("precondition: the connections' windows held more than gRPC's tuning reaches")
			.isGreaterThan(2 * GRPC_TUNED_MAX);
		assertThat(peak).as("one round trip carries what the windows hold").isGreaterThan(2 * GRPC_TUNED_MAX);
		assertThat(moved).as("every transfer completed").isEqualTo(TRANSFERS * BODY);
		assertThat(sent).as("precondition: the transfers passed the proxy").isGreaterThanOrEqualTo(moved);
	}

	private double server(String gauge) {
		return Objects.requireNonNull(this.serverMeters).get(gauge).gauge().value();
	}

	/** Reads the response head and {@link #BODY} bytes after it; the body bytes read. */
	private static long drain(InputStream in) throws IOException {
		int matched = 0;
		while (matched < 4) {
			int b = in.read();
			if (b < 0) {
				return 0;
			}
			matched = (b == "\r\n\r\n".charAt(matched)) ? matched + 1 : (b == '\r' ? 1 : 0);
		}
		byte[] buffer = new byte[64 * 1024];
		long read = 0;
		while (read < BODY) {
			int n = in.read(buffer, 0, (int) Math.min(buffer.length, BODY - read));
			if (n < 0) {
				break;
			}
			read += n;
		}
		return read;
	}

	/** An upstream answering every request head with {@link #BODY} bytes. */
	private int download() throws IOException {
		return serve(socket -> {
			InputStream in = socket.getInputStream();
			int matched = 0;
			while (matched < 4) {
				int b = in.read();
				if (b < 0) {
					return;
				}
				matched = (b == "\r\n\r\n".charAt(matched)) ? matched + 1 : (b == '\r' ? 1 : 0);
			}
			OutputStream out = socket.getOutputStream();
			out.write(("HTTP/1.1 200 OK\r\nContent-Length: " + BODY + "\r\n\r\n").getBytes(StandardCharsets.US_ASCII));
			byte[] chunk = new byte[64 * 1024];
			for (long sent = 0; sent < BODY; sent += chunk.length) {
				out.write(chunk);
			}
			out.flush();
			in.read();
		});
	}

	/**
	 * An upstream counting what it reads, request heads included, into {@code received}.
	 */
	private int sink(AtomicLong received) throws IOException {
		return serve(socket -> {
			InputStream in = socket.getInputStream();
			int matched = 0;
			while (matched < 4) {
				int b = in.read();
				if (b < 0) {
					return;
				}
				matched = (b == "\r\n\r\n".charAt(matched)) ? matched + 1 : (b == '\r' ? 1 : 0);
			}
			byte[] buffer = new byte[64 * 1024];
			int n;
			while ((n = in.read(buffer)) > 0) {
				received.addAndGet(n);
			}
		});
	}

	private int serve(Handler handler) throws IOException {
		ServerSocket server = remember(new ServerSocket());
		server.bind(new InetSocketAddress("127.0.0.1", 0), TRANSFERS);
		Thread.ofVirtual().name("e2e-transport-window-accept").start(() -> {
			while (!server.isClosed()) {
				try {
					Socket socket = remember(server.accept());
					Thread.ofVirtual().name("e2e-transport-window-upstream").start(() -> {
						try (socket) {
							handler.handle(socket);
						}
						catch (IOException e) {
							// torn down
						}
					});
				}
				catch (IOException e) {
					return;
				}
			}
		});
		return server.getLocalPort();
	}

	private SimpleMeterRegistry startClient(Upstream.Builder upstream, int serverPort) {
		Upstream built = upstream.build();
		SimpleMeterRegistry meters = new SimpleMeterRegistry();
		TunnelClient client = TunnelClient.builder()
			.properties(SluiceClientProperties.builder()
				.serverUrl("grpc://127.0.0.1:" + serverPort)
				.upstream(built)
				.token("it-token")
				.tunnelBudget(DataSize.ofBytes(BUDGET))
				.build())
			.taskExecutor(task -> Thread.ofVirtual().name("e2e-transport-window-worker").start(task))
			.meterRegistry(meters)
			.build();
		client.start();
		remember(client::stop);
		Awaitility.await()
			.atMost(TIMEOUT)
			.until(() -> Objects.requireNonNull(this.router).lookup(built.host()).isPresent());
		return meters;
	}

	private <T extends AutoCloseable> T remember(T closeable) {
		this.closeables.add(closeable);
		return closeable;
	}

	@FunctionalInterface
	private interface Handler {

		void handle(Socket socket) throws IOException;

	}

	/** Samples a gauge every 5ms until closed, keeping the largest value. */
	private static final class PeakSampler implements AutoCloseable {

		private final AtomicLong peak = new AtomicLong();

		private final Thread thread;

		PeakSampler(DoubleSupplier gauge) {
			this.thread = Thread.ofVirtual().name("e2e-transport-window-sampler").start(() -> {
				while (!Thread.currentThread().isInterrupted()) {
					long now = (long) gauge.getAsDouble();
					this.peak.accumulateAndGet(now, Math::max);
					try {
						Thread.sleep(5);
					}
					catch (InterruptedException e) {
						return;
					}
				}
			});
		}

		long peak() {
			return this.peak.get();
		}

		@Override
		public void close() {
			this.thread.interrupt();
		}

	}

}
