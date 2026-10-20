package am.ik.sluice.it;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import am.ik.sluice.server.SluiceServerApplication;
import am.ik.sluice.server.tunnel.SessionRegistry;
import am.ik.sluice.server.tunnel.TunnelSession;
import am.ik.sluice.tunnel.SessionSender;
import am.ik.sluice.tunnel.VirtualConnection;
import am.ik.sluice.v1.proto.Advertise;
import am.ik.sluice.v1.proto.Close;
import am.ik.sluice.v1.proto.Data;
import am.ik.sluice.v1.proto.Frame;
import am.ik.sluice.v1.proto.TunnelGrpc;
import am.ik.sluice.v1.proto.Upstream;
import am.ik.sluice.v1.proto.WindowUpdate;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.MeterRegistry;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The server's tunnel stall watchdog ({@code sluice.limits.tunnel-stall-timeout}): a
 * tunnel client that stops reading its stream, while keeping the connection alive, has
 * its session failed once frames waited for the timeout with none taken -- its stream
 * reset, its routes removed, and the slots of the connections whose frames were queued
 * freed. A client that reads slowly but keeps taking frames is not cut.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class TunnelStallWatchdogE2ETest {

	private static final Duration TIMEOUT = Duration.ofSeconds(2);

	private static final String STALLING_ID = "stalling-client";

	private static final String HEALTHY_ID = "healthy-client";

	/** The stalling client answers a connection to this target with its CLOSE at once. */
	private static final String CLOSING_TARGET = "127.0.0.1:1003";

	/** Connections to this target are kept open and never answered. */
	private static final String HOLDING_TARGET = "127.0.0.1:1004";

	/** The HTTP hosts' targets, answered with {@link #RESPONSE}. */
	private static final Set<String> HTTP_TARGETS = Set.of("127.0.0.1:1001", "127.0.0.1:1002");

	private static final byte[] RESPONSE = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok"
		.getBytes(StandardCharsets.US_ASCII);

	private static int grpcPort;

	private static int dataPort;

	private static int stallingPort;

	private static int healthyPort;

	@Autowired
	private @Nullable MeterRegistry meterRegistry;

	@Autowired
	private @Nullable SessionRegistry registry;

	private final List<Socket> sockets = new CopyOnWriteArrayList<>();

	private final List<RawClient> clients = new CopyOnWriteArrayList<>();

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) {
		grpcPort = TestPorts.freePort();
		dataPort = TestPorts.freePort();
		stallingPort = TestPorts.freePort();
		healthyPort = TestPorts.freePort();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(dataPort));
		registry.add("sluice.tcp-port-range", () -> stallingPort + "," + healthyPort);
		registry.add("sluice.limits.max-connections", () -> "64");
		registry.add("sluice.limits.tunnel-stall-timeout", () -> TIMEOUT.toMillis() + "ms");
		registry.add("sluice.token", () -> "it-token");
		registry.add("server.port", () -> 0);
	}

	@BeforeEach
	void awaitIdle() {
		Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> registry().all().isEmpty() && admitted() == 0);
	}

	@AfterEach
	void disconnect() throws IOException {
		for (RawClient client : this.clients) {
			client.resume.countDown();
			client.channel.shutdownNow();
		}
		for (Socket socket : this.sockets) {
			socket.close();
		}
		Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> registry().all().isEmpty());
	}

	/**
	 * Ended connections whose frames wait on the stalled stream hold their slots until
	 * the watchdog fails the session; then only the healthy client's live connection
	 * holds one. The client's stream is reset, not closed behind the frames it does not
	 * take, and a new stream of the same client is served again.
	 */
	@Test
	void aStalledSessionIsFailedPastTheTimeoutAndItsSlotsFreed() throws Exception {
		RawClient healthy = connect(HEALTHY_ID, healthyPort);
		RawClient stalling = connect(STALLING_ID, stallingPort);
		TunnelSession stalledSession = session(STALLING_ID);
		assertThat(((SessionSender) stalledSession.sender()).stallTimeout())
			.as("precondition: the configured timeout took effect")
			.isEqualTo(TIMEOUT);
		double failuresBefore = stallFailures();
		open(healthyPort);
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> healthy.connects.size() == 1);
		assertThat(admitted()).as("precondition: the healthy client's live connection").isEqualTo(1);

		int connections = 6;
		List<Socket> peers = new ArrayList<>();
		for (int i = 0; i < connections; i++) {
			peers.add(open(stallingPort));
		}
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> stalling.closed.size() == connections);
		long stallStart = System.nanoTime();
		stalling.stall.set(true);
		for (Socket peer : peers) {
			// on its own thread: what the relay does not read fills the socket buffers
			// and may block the write until the relay ends
			Thread.ofVirtual().start(() -> {
				try {
					peer.getOutputStream().write(new byte[512 * 1024]);
				}
				catch (IOException e) {
					// the relay ended and closed its socket
				}
			});
		}
		SessionSender stalledSender = (SessionSender) stalledSession.sender();
		Awaitility.await()
			.atMost(Duration.ofSeconds(5))
			.alias("the stream stopped taking frames")
			.until(() -> stalling.blocked.get() && stalledSender.stalled().compareTo(Duration.ofMillis(300)) >= 0);
		// The client fails each connection with ERROR, which ends its relay wherever its
		// writer waits. No peer half-closes: a relay reaches its peer's FIN only after
		// queueing every byte before it, and the sender caps a connection's queue in
		// DATA frames, one per read, so how the reads split the bytes would decide
		// whether the relay ends.
		for (long id : stalling.closed) {
			stalling.send(Frame.newBuilder()
				.setError(am.ik.sluice.v1.proto.Error.newBuilder().setConnId(id).setMessage("upstream failed"))
				.build());
		}
		Awaitility.await()
			.atMost(Duration.ofSeconds(5))
			.alias("every relay of the stalled client ended")
			.until(() -> stalledSession.connectionCount() == 0);
		int queued = ((SessionSender) stalledSession.sender()).connectionQueues();
		assertThat(queued).as("precondition: frames of ended connections are queued").isPositive();
		assertThat(stalling.blocked.get()).as("precondition: the client stopped reading").isTrue();
		assertThat(admitted()).as("precondition: their slots are held").isEqualTo(1 + queued);
		Awaitility.await()
			.atMost(TIMEOUT)
			.alias("the stall is measured")
			.until(() -> stalledGauge() > 0 && stalledSession.stalled().toMillis() > 0);

		Awaitility.await()
			.atMost(TIMEOUT.plusSeconds(5))
			.alias("the watchdog failed the session")
			.until(() -> registry().find(STALLING_ID).isEmpty());
		assertThat(Duration.ofNanos(System.nanoTime() - stallStart)).as("not before the timeout")
			.isGreaterThanOrEqualTo(TIMEOUT);
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> admitted() == 1);
		assertThat(stallFailures() - failuresBefore).as("counted").isEqualTo(1);
		assertThat(stalledGauge()).isZero();
		assertThat(session(HEALTHY_ID).connectionCount()).as("the other client's connection lives on").isEqualTo(1);
		Awaitility.await()
			.atMost(Duration.ofSeconds(5))
			.alias("the failed session's tcp route is unbound")
			.until(() -> refused(stallingPort));

		stalling.resume.countDown();
		assertThat(stalling.ended.await(5, TimeUnit.SECONDS)).as("the client's stream ended").isTrue();
		assertThat(stalling.status.get()).as("reset, not closed behind the frames")
			.extracting(Status::getCode)
			.isEqualTo(Status.Code.CANCELLED);

		RawClient again = connect(STALLING_ID, stallingPort);
		open(stallingPort);
		Awaitility.await()
			.atMost(Duration.ofSeconds(5))
			.alias("the new stream serves the route")
			.until(() -> again.closed.size() == 1);
	}

	/**
	 * A client that reads its stream slowly -- the stream not ready for stretches, each
	 * shorter than the timeout -- keeps its session over many timeouts and gets every
	 * byte.
	 */
	@Test
	void aClientThatKeepsTakingFramesSlowlyIsNotCut() throws Exception {
		RawClient slow = connect(STALLING_ID, stallingPort);
		slow.millisPer64KiB.set(100);
		TunnelSession session = session(STALLING_ID);
		double failuresBefore = stallFailures();
		Socket peer = open(stallingPort);
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> slow.connects.size() == 1);
		int total = 6 * 1024 * 1024;
		long start = System.nanoTime();
		Thread writer = Thread.ofVirtual().start(() -> {
			try {
				peer.getOutputStream().write(new byte[total]);
			}
			catch (IOException e) {
				// asserted below through the bytes received
			}
		});
		long longestStall = 0;
		while (slow.received.get() < total && registry().find(STALLING_ID).filter(session::equals).isPresent()
				&& System.nanoTime() - start < TimeUnit.SECONDS.toNanos(60)) {
			longestStall = Math.max(longestStall, session.stalled().toNanos());
			TimeUnit.MILLISECONDS.sleep(10);
		}
		writer.join(TimeUnit.SECONDS.toMillis(5));

		assertThat(slow.received.get()).as("every byte arrived").isEqualTo(total);
		assertThat(Duration.ofNanos(System.nanoTime() - start)).as("precondition: the transfer outlasts the timeout")
			.isGreaterThan(TIMEOUT.multipliedBy(2));
		assertThat(Duration.ofNanos(longestStall)).as("precondition: the stream stalled between reads")
			.isGreaterThan(Duration.ofMillis(200));
		assertThat(stallFailures() - failuresBefore).as("no session failed").isZero();
		assertThat(registry().find(STALLING_ID)).as("the same session").contains(session);
	}

	/**
	 * An upstream that does not read holds back only its own connection: the client
	 * grants no credit, so the server queues nothing beyond the window, the stream stays
	 * ready, and the session is not cut however long the upstream waits.
	 */
	@Test
	void anUpstreamThatDoesNotReadIsNoStall() throws Exception {
		RawClient healthy = connect(HEALTHY_ID, healthyPort);
		TunnelSession session = session(HEALTHY_ID);
		SessionSender sender = (SessionSender) session.sender();
		double failuresBefore = stallFailures();
		Socket peer = open(healthyPort);
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> healthy.connects.size() == 1);
		// Writes until the socket is closed after the test: a fixed amount could be
		// absorbed whole by the kernel buffers (autotuned, a few MiB), ending the writer
		// without the window having held anything back.
		AtomicLong written = new AtomicLong();
		AtomicReference<@Nullable IOException> writeFailure = new AtomicReference<>();
		Thread writer = Thread.ofVirtual().start(() -> {
			byte[] chunk = new byte[64 * 1024];
			try {
				while (true) {
					peer.getOutputStream().write(chunk);
					written.addAndGet(chunk.length);
				}
			}
			catch (IOException e) {
				writeFailure.set(e);
			}
		});
		Awaitility.await()
			.atMost(Duration.ofSeconds(5))
			.alias("precondition: the client got the window, no credit was granted back")
			.until(() -> healthy.received.get() == VirtualConnection.DEFAULT_WINDOW);

		long until = System.nanoTime() + TIMEOUT.multipliedBy(3).toNanos();
		while (System.nanoTime() < until) {
			assertThat(sender.stalled()).as("nothing queued: no stall").isEqualTo(Duration.ZERO);
			TimeUnit.MILLISECONDS.sleep(50);
		}
		assertThat(writer.isAlive())
			.as("precondition: the public peer is still held back (written %d bytes, failure %s)", written.get(),
					writeFailure.get())
			.isTrue();
		assertThat(healthy.received.get()).as("nothing past the window").isEqualTo(VirtualConnection.DEFAULT_WINDOW);
		assertThat(stallFailures() - failuresBefore).as("no session failed").isZero();
		assertThat(registry().find(HEALTHY_ID)).as("the same session").contains(session);
		assertThat(session.connectionCount()).isEqualTo(1);
	}

	/**
	 * An HTTP/1.1 connection switching routes waits for the frames queued for the
	 * previous route's leg; when the watchdog fails the stalled session, the dropped
	 * frames end the wait, and the client is answered instead of waiting for good.
	 */
	@Test
	void anH1RouteSwitchWaitingOnAStalledLegEndsWithTheSession() throws Exception {
		RawClient stalling = connect(STALLING_ID, stallingPort);
		Socket h1 = open(dataPort);
		OutputStream out = h1.getOutputStream();
		InputStream in = h1.getInputStream();
		int body = 256 * 1024;
		out.write(("POST /a HTTP/1.1\r\nHost: a.local\r\nContent-Length: " + body + "\r\n\r\n")
			.getBytes(StandardCharsets.US_ASCII));
		out.flush();
		assertThat(in.readNBytes(RESPONSE.length)).isEqualTo(RESPONSE);
		SessionSender sender = (SessionSender) session(STALLING_ID).sender();
		double failuresBefore = stallFailures();
		long stallStart = System.nanoTime();
		stalling.stall.set(true);
		// fillers queue more than the stream window and its buffers take
		for (int i = 0; i < 3; i++) {
			open(stallingPort).getOutputStream().write(new byte[VirtualConnection.DEFAULT_WINDOW]);
		}
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> stalling.blocked.get());
		int fillers = settledQueues(sender);
		out.write(new byte[body]);
		out.flush();
		Awaitility.await()
			.atMost(Duration.ofSeconds(5))
			.alias("precondition: the body waits in the queue of route a's leg")
			.until(() -> sender.connectionQueues() == fillers + 1);
		out.write("GET /b HTTP/1.1\r\nHost: b.local\r\n\r\n".getBytes(StandardCharsets.US_ASCII));
		out.flush();
		long switchedAt = System.nanoTime();
		h1.setSoTimeout((int) TIMEOUT.plusSeconds(5).toMillis());
		byte[] answer = in.readAllBytes();
		Duration waited = Duration.ofNanos(System.nanoTime() - switchedAt);

		assertThat(Duration.ofNanos(System.nanoTime() - stallStart)).as("held until the watchdog fired")
			.isGreaterThanOrEqualTo(TIMEOUT);
		assertThat(waited).as("answered within the timeout and its check").isLessThan(TIMEOUT.plusSeconds(4));
		assertThat(new String(answer, StandardCharsets.US_ASCII))
			.as("route b's leg failed with the session before answering: a 502, then the connection's end")
			.startsWith("HTTP/1.1 502 Bad Gateway\r\n");
		Awaitility.await()
			.atMost(Duration.ofSeconds(5))
			.alias("the watchdog failed the session")
			.until(() -> registry().find(STALLING_ID).isEmpty());
		assertThat(stallFailures() - failuresBefore).isEqualTo(1);
	}

	/** The sender's live queues once they stopped changing for 300ms. */
	private static int settledQueues(SessionSender sender) throws InterruptedException {
		long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
		int last = -1;
		long since = System.nanoTime();
		while (System.nanoTime() < deadline) {
			int current = sender.connectionQueues();
			if (current != last) {
				last = current;
				since = System.nanoTime();
			}
			else if (System.nanoTime() - since > Duration.ofMillis(300).toNanos()) {
				return current;
			}
			TimeUnit.MILLISECONDS.sleep(20);
		}
		return last;
	}

	private RawClient connect(String id, int listenPort) throws InterruptedException {
		ManagedChannel channel = ManagedChannelBuilder.forAddress("127.0.0.1", grpcPort).usePlaintext().build();
		RawClient client = new RawClient(channel, listenPort);
		this.clients.add(client);
		Metadata metadata = new Metadata();
		metadata.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER), "Bearer it-token");
		metadata.put(Metadata.Key.of("x-sluice-id", Metadata.ASCII_STRING_MARSHALLER), id);
		StreamObserver<Frame> requests = TunnelGrpc.newStub(channel)
			.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata))
			.connect(client);
		client.start(requests);
		assertThat(client.acked.await(10, TimeUnit.SECONDS)).as("advertise acknowledged").isTrue();
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> registry().find(id).isPresent());
		return client;
	}

	private static boolean refused(int port) {
		try (Socket probe = new Socket("127.0.0.1", port)) {
			return false;
		}
		catch (IOException e) {
			return true;
		}
	}

	private Socket open(int port) throws IOException {
		Socket socket = new Socket("127.0.0.1", port);
		socket.setSoTimeout(10_000);
		this.sockets.add(socket);
		return socket;
	}

	private int admitted() {
		return (int) meterRegistry().get("sluice.data.connections.admitted").gauge().value();
	}

	private double stallFailures() {
		return meterRegistry().get("sluice.tunnel.stall.failures").counter().count();
	}

	private double stalledGauge() {
		return meterRegistry().get("sluice.tunnel.stall").gauge().value();
	}

	private TunnelSession session(String id) {
		return registry().find(id).orElseThrow();
	}

	private SessionRegistry registry() {
		return Objects.requireNonNull(this.registry, "registry");
	}

	private MeterRegistry meterRegistry() {
		return Objects.requireNonNull(this.meterRegistry, "meterRegistry");
	}

	/**
	 * A tunnel client advertising a tcp route on {@code listenPort} -- whose connections
	 * are answered with a CLOSE on the stalling client's port and held open otherwise --
	 * and two HTTP hosts answered with {@link #RESPONSE}. Once {@link #stall} is set, its
	 * receive callback blocks until {@link #resume}; with {@link #millisPer64KiB} it
	 * sleeps that long per 64KiB of DATA received and grants the payload back.
	 */
	private static final class RawClient implements StreamObserver<Frame> {

		final ManagedChannel channel;

		final int listenPort;

		final CountDownLatch acked = new CountDownLatch(1);

		final AtomicBoolean stall = new AtomicBoolean();

		final AtomicBoolean blocked = new AtomicBoolean();

		final CountDownLatch resume = new CountDownLatch(1);

		final AtomicLong millisPer64KiB = new AtomicLong();

		final AtomicLong received = new AtomicLong();

		final Set<Long> connects = ConcurrentHashMap.newKeySet();

		final Set<Long> closed = ConcurrentHashMap.newKeySet();

		final CountDownLatch ended = new CountDownLatch(1);

		final AtomicReference<@Nullable Status> status = new AtomicReference<>();

		private final Map<Long, String> addresses = new ConcurrentHashMap<>();

		private final Set<Long> answered = ConcurrentHashMap.newKeySet();

		private @Nullable StreamObserver<Frame> requests;

		RawClient(ManagedChannel channel, int listenPort) {
			this.channel = channel;
			this.listenPort = listenPort;
		}

		void start(StreamObserver<Frame> requests) {
			this.requests = requests;
			String tcpTarget = this.listenPort == stallingPort ? CLOSING_TARGET : HOLDING_TARGET;
			send(Frame.newBuilder()
				.setAdvertise(Advertise.newBuilder()
					.addUpstreams(upstream("a.local", "http://127.0.0.1:1001", 0))
					.addUpstreams(upstream("b.local", "http://127.0.0.1:1002", 0))
					.addUpstreams(upstream("route.tcp", "tcp://" + tcpTarget, this.listenPort))
					.setConnectionWindow(VirtualConnection.DEFAULT_WINDOW))
				.build());
		}

		private static Upstream upstream(String host, String target, int listenPort) {
			return Upstream.newBuilder().setHost(host).setTargetUrl(target).setListenPort(listenPort).build();
		}

		private synchronized void send(Frame frame) {
			StreamObserver<Frame> requests = this.requests;
			if (requests != null) {
				requests.onNext(frame);
			}
		}

		@Override
		public void onNext(Frame frame) {
			if (this.stall.get()) {
				this.blocked.set(true);
				try {
					this.resume.await();
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					return;
				}
			}
			switch (frame.getBodyCase()) {
				case ADVERTISE_ACK -> this.acked.countDown();
				case CONNECT -> {
					long id = frame.getConnect().getConnId();
					this.addresses.put(id, frame.getConnect().getAddress());
					this.connects.add(id);
					if (CLOSING_TARGET.equals(frame.getConnect().getAddress()) && this.millisPer64KiB.get() == 0) {
						send(Frame.newBuilder().setClose(Close.newBuilder().setConnId(id)).build());
						this.closed.add(id);
					}
				}
				case DATA -> onData(frame.getData());
				default -> {
					// window updates, membership and the like
				}
			}
		}

		private void onData(Data data) {
			long id = data.getConnId();
			String address = this.addresses.get(id);
			if (address != null && HTTP_TARGETS.contains(address) && this.answered.add(id)) {
				send(Frame.newBuilder()
					.setData(Data.newBuilder().setConnId(id).setPayload(ByteString.copyFrom(RESPONSE)))
					.build());
			}
			int size = data.getPayload().size();
			long millis = this.millisPer64KiB.get();
			if (millis == 0) {
				this.received.addAndGet(size);
			}
			else {
				try {
					TimeUnit.MICROSECONDS.sleep(millis * 1000 * size / 65536);
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					return;
				}
				this.received.addAndGet(size);
				send(Frame.newBuilder()
					.setWindowUpdate(WindowUpdate.newBuilder().setConnId(id).setIncrement(size))
					.build());
			}
		}

		@Override
		public void onError(Throwable t) {
			this.status.set(Status.fromThrowable(t));
			this.ended.countDown();
		}

		@Override
		public void onCompleted() {
			this.status.set(Status.OK);
			this.ended.countDown();
		}

	}

}
