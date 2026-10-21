package am.ik.sluice.it;

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
import java.util.concurrent.atomic.AtomicLong;

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
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The gRPC connection carrying a tunnel announces windows the tunnel's connections can
 * use at once, both ways: each end's stream and connection windows are twice its tunnel
 * budget, far more than the 8MiB gRPC's own window tuning reaches at most, as the frames
 * on the wire show ({@link H2WindowTap}); a download and an upload go through.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
@Timeout(120)
class TunnelTransportWindowE2ETest {

	private static final long MIB = 1024 * 1024;

	/**
	 * The budget of the server; the client's is half of it, so the two windows differ.
	 */
	private static final long BUDGET = 48 * MIB;

	private static final long CLIENT_BUDGET = BUDGET / 2;

	private static final long BODY = 4 * MIB;

	/** The window gRPC's netty transport tunes itself up to at most. */
	private static final long GRPC_TUNED_MAX = 8 * MIB;

	private static final Duration TIMEOUT = Duration.ofSeconds(60);

	private static int grpcPort;

	private static int dataPort;

	/**
	 * The tap's port: the node's public url, so the tunnel stream is its only connection.
	 */
	private static int tapPort;

	@Autowired
	private @Nullable Router router;

	private final List<AutoCloseable> closeables = new CopyOnWriteArrayList<>();

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) {
		grpcPort = TestPorts.freePort();
		dataPort = TestPorts.freePort();
		tapPort = TestPorts.freePort();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.node.public-url", () -> "grpc://127.0.0.1:" + tapPort);
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

	@Test
	void eachEndAnnouncesTwiceItsBudgetAsItsTransportWindows() throws Exception {
		String downloadHost = "download.transport.window.test";
		String uploadHost = "upload.transport.window.test";
		AtomicLong uploaded = new AtomicLong();
		H2WindowTap tap = remember(H2WindowTap.start(grpcPort, tapPort));
		startClient(grpcPort, Upstream.builder().host(downloadHost).target("tcp://127.0.0.1:" + download()).build(),
				Upstream.builder().host(uploadHost).target("tcp://127.0.0.1:" + sink(uploaded)).build());
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			socket.getOutputStream()
				.write(("GET / HTTP/1.1\r\nHost: " + downloadHost + "\r\n\r\n").getBytes(StandardCharsets.US_ASCII));
			assertThat(drain(socket.getInputStream())).as("the download went through").isEqualTo(BODY);
		}
		try (Socket socket = new Socket("127.0.0.1", dataPort)) {
			OutputStream out = socket.getOutputStream();
			out.write(("POST / HTTP/1.1\r\nHost: " + uploadHost + "\r\nContent-Length: " + BODY + "\r\n\r\n")
				.getBytes(StandardCharsets.US_ASCII));
			byte[] chunk = new byte[64 * 1024];
			for (long sent = 0; sent < BODY; sent += chunk.length) {
				out.write(chunk);
			}
			Awaitility.await().atMost(TIMEOUT).until(() -> uploaded.get() >= BODY);
		}
		assertThat(uploaded.get()).as("the upload went through").isEqualTo(BODY);
		// the server receives the downloads: its windows bound the client's sending
		assertThat(tap.target().initialWindow()).as("the server's stream window").isEqualTo(2 * BUDGET);
		// the single top-up grant pins the configured window; the sampled peak misses it
		// when the peer's first DATA precedes the grant (the grant does not re-credit it)
		assertThat(tap.target().largestGrant()).as("the server's connection window top-up")
			.isEqualTo(2 * BUDGET - H2WindowTap.INITIAL_CONNECTION_WINDOW);
		assertThat(tap.target().maxConnectionWindow()).as("the server's connection window")
			.isGreaterThan(2 * GRPC_TUNED_MAX);
		// the client receives the uploads
		assertThat(tap.connecting().initialWindow()).as("the client's stream window").isEqualTo(2 * CLIENT_BUDGET);
		assertThat(tap.connecting().largestGrant()).as("the client's connection window top-up")
			.isEqualTo(2 * CLIENT_BUDGET - H2WindowTap.INITIAL_CONNECTION_WINDOW);
		assertThat(tap.connecting().maxConnectionWindow()).as("the client's connection window")
			.isGreaterThan(2 * GRPC_TUNED_MAX);
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
		server.bind(new InetSocketAddress("127.0.0.1", 0), 16);
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

	private void startClient(int serverPort, Upstream... upstreams) {
		SluiceClientProperties.Builder properties = SluiceClientProperties.builder()
			.serverUrl("grpc://127.0.0.1:" + serverPort)
			.token("it-token")
			.tunnelBudget(DataSize.ofBytes(CLIENT_BUDGET));
		for (Upstream upstream : upstreams) {
			properties.upstream(upstream);
		}
		TunnelClient client = TunnelClient.builder()
			.properties(properties.build())
			.taskExecutor(task -> Thread.ofVirtual().name("e2e-transport-window-worker").start(task))
			.meterRegistry(new SimpleMeterRegistry())
			.build();
		client.start();
		remember(client::stop);
		for (Upstream upstream : upstreams) {
			Awaitility.await()
				.atMost(TIMEOUT)
				.until(() -> Objects.requireNonNull(this.router).lookup(upstream.host()).isPresent());
		}
	}

	private <T extends AutoCloseable> T remember(T closeable) {
		this.closeables.add(closeable);
		return closeable;
	}

	@FunctionalInterface
	private interface Handler {

		void handle(Socket socket) throws IOException;

	}

}
