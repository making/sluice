package am.ik.sluice.it;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
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

import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;

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
import am.ik.sluice.server.proxy.DataProxyServer;
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
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.MeterRegistry;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A data plane connection keeps its slot of the connection limit while frames it queued
 * for the tunnel stream still wait in the server's sender: the connection limit bounds
 * what ended connections still queue. The stream is held not ready by a tunnel client
 * that stops reading it (its receive callback blocks), which leaves the server's frames
 * queued.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class QueuedFramesHoldSlotsE2ETest {

	private static final String CLIENT_ID = "stalling-client";

	private static final String TCP_TARGET = "127.0.0.1:1003";

	private static final byte[] RESPONSE = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok"
		.getBytes(StandardCharsets.US_ASCII);

	/** Fillers queue more than the stream window and its buffers take. */
	private static final int FILLERS = 3;

	/**
	 * The payload a relay sends before its peer's FIN while the stream holds its frames:
	 * fewer bytes than the sender queues DATA frames per connection. Each read is one
	 * frame of at least one byte, so the relay queues all of it and reaches the FIN
	 * however its reads split it.
	 */
	private static final int SMALL_PAYLOAD = 8;

	private static int grpcPort;

	private static int dataPort;

	private static int tcpRoutePort;

	@Autowired
	private @Nullable MeterRegistry meterRegistry;

	@Autowired
	private @Nullable SessionRegistry registry;

	@Autowired
	private @Nullable DataProxyServer dataProxyServer;

	private final List<Socket> sockets = new CopyOnWriteArrayList<>();

	private @Nullable StallingClient client;

	private @Nullable ManagedChannel channel;

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) {
		grpcPort = TestPorts.freePort();
		dataPort = TestPorts.freePort();
		tcpRoutePort = TestPorts.freePort();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(dataPort));
		registry.add("sluice.tcp-port-range", () -> String.valueOf(tcpRoutePort));
		registry.add("sluice.limits.max-connections", () -> "64");
		registry.add("sluice.token", () -> "it-token");
		registry.add("server.port", () -> 0);
	}

	@BeforeEach
	void connect() throws Exception {
		StallingClient started = new StallingClient();
		ManagedChannel channel = ManagedChannelBuilder.forAddress("127.0.0.1", grpcPort).usePlaintext().build();
		this.channel = channel;
		this.client = started;
		Metadata metadata = new Metadata();
		metadata.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER), "Bearer it-token");
		metadata.put(Metadata.Key.of("x-sluice-id", Metadata.ASCII_STRING_MARSHALLER), CLIENT_ID);
		StreamObserver<Frame> requests = TunnelGrpc.newStub(channel)
			.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata))
			.connect(started);
		started.start(requests);
		assertThat(started.acked.await(10, TimeUnit.SECONDS)).as("advertise acknowledged").isTrue();
		Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
			try (Socket probe = new Socket("127.0.0.1", tcpRoutePort)) {
				// the tcp route listener is bound
			}
		});
		// the probes' relays ended, so their CONNECTs were answered: none counts below
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> session().connectionCount() == 0);
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> admitted() == 0);
		started.closedTcp.clear();
	}

	@AfterEach
	void disconnect() throws IOException {
		StallingClient client = this.client;
		if (client != null) {
			client.resume.countDown();
		}
		for (Socket socket : this.sockets) {
			socket.close();
		}
		ManagedChannel channel = this.channel;
		if (channel != null) {
			channel.shutdownNow();
		}
		Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> registry().all().isEmpty());
	}

	/**
	 * Ended tcp route connections whose frames wait in the sender keep their slots until
	 * the frames left.
	 */
	@Test
	void endedConnectionsHoldTheirSlotsWhileTheirFramesAreQueued() throws Exception {
		StallingClient client = client();
		endRelaysWithQueuedFrames(6, 512 * 1024);
		int queued = settledQueues();
		assertThat(queued).as("precondition: frames of ended connections are queued").isPositive();
		assertThat(client.blocked.get()).as("precondition: the client stopped reading").isTrue();
		assertHeldFor(Duration.ofMillis(300), () -> assertThat(admitted()).isEqualTo(sender().connectionQueues()));
		assertThat(admitted()).isEqualTo(queued);

		client.resume.countDown();
		Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> sender().connectionQueues() == 0);
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> admitted() == 0);
	}

	/** The end of the tunnel session drops the queued frames and frees their slots. */
	@Test
	void theSessionEndFreesTheSlotsOfQueuedFrames() throws Exception {
		endRelaysWithQueuedFrames(FILLERS, VirtualConnection.DEFAULT_WINDOW);
		int queued = settledQueues();
		assertThat(queued).as("precondition: frames of ended connections are queued").isPositive();
		assertThat(admitted()).as("precondition: their slots are held").isEqualTo(queued);

		Objects.requireNonNull(this.channel, "channel").shutdownNow();
		Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> registry().all().isEmpty());
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> admitted() == 0);
	}

	/**
	 * An HTTP/1.1 connection switching routes waits until the frames still queued for the
	 * previous route's leg left: its slot never covers the queues of two legs.
	 */
	@Test
	void anH1RouteSwitchWaitsForThePreviousLegsQueuedFrames() throws Exception {
		StallingClient client = client();
		Socket h1 = open(dataPort);
		OutputStream out = h1.getOutputStream();
		InputStream in = h1.getInputStream();
		int body = 256 * 1024;
		out.write(("POST /a HTTP/1.1\r\nHost: a.local\r\nContent-Length: " + body + "\r\n\r\n")
			.getBytes(StandardCharsets.US_ASCII));
		out.flush();
		// the client answers at the head: the response is complete before the body
		assertThat(in.readNBytes(RESPONSE.length)).isEqualTo(RESPONSE);

		int fillers = stallWithFillers();
		out.write(new byte[body]);
		out.flush();
		Awaitility.await()
			.atMost(Duration.ofSeconds(5))
			.alias("the body waits in the queue of route a's leg")
			.until(() -> sender().connectionQueues() == fillers + 1);
		out.write("GET /b HTTP/1.1\r\nHost: b.local\r\n\r\n".getBytes(StandardCharsets.US_ASCII));
		out.flush();
		assertHeldFor(Duration.ofMillis(500),
				() -> assertThat(sender().connectionQueues()).as("no frame of route b's leg queued yet")
					.isEqualTo(fillers + 1));

		client.resume.countDown();
		assertThat(in.readNBytes(RESPONSE.length)).as("route b answers once a's frames left").isEqualTo(RESPONSE);
	}

	/** An HTTP/1.1 relay that ended keeps its slot while its leg's frames are queued. */
	@Test
	void anEndedH1RelayHoldsItsSlotWhileItsFramesAreQueued() throws Exception {
		StallingClient client = client();
		Socket h1 = open(dataPort);
		OutputStream out = h1.getOutputStream();
		int body = 256 * 1024;
		out.write(("POST /a HTTP/1.1\r\nHost: a.local\r\nContent-Length: " + body + "\r\n\r\n")
			.getBytes(StandardCharsets.US_ASCII));
		out.flush();
		assertThat(h1.getInputStream().readNBytes(RESPONSE.length)).isEqualTo(RESPONSE);
		long legA = session().connectionsOpened();

		int fillers = stallWithFillers();
		out.write(new byte[body]);
		out.flush();
		Awaitility.await()
			.atMost(Duration.ofSeconds(5))
			.alias("the body waits in the queue of route a's leg")
			.until(() -> sender().connectionQueues() == fillers + 1);
		h1.shutdownOutput();
		// the upstream ends its side too: the relay ends
		client.sendFromTest(Frame.newBuilder().setClose(Close.newBuilder().setConnId(legA)).build());
		Awaitility.await()
			.atMost(Duration.ofSeconds(5))
			.alias("the relay ended")
			.until(() -> dataProxyServer().activeConnections() == 0);
		assertThat(sender().connectionQueues()).as("precondition: the leg's frames are still queued")
			.isEqualTo(fillers + 1);
		assertHeldFor(Duration.ofMillis(300), () -> assertThat(admitted()).isEqualTo(FILLERS + 1));

		client.resume.countDown();
		Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> admitted() == FILLERS);
	}

	/**
	 * An h2 connection whose relay ended keeps its own slot and the slot of its further
	 * leg while the legs' frames are queued.
	 */
	@Test
	void anEndedH2RelayHoldsItsLegSlotsWhileTheirFramesAreQueued() throws Exception {
		StallingClient client = client();
		int fillers = stallWithFillers();
		Socket h2 = open(dataPort);
		OutputStream out = h2.getOutputStream();
		out.write(TestH2.PREFACE);
		TestH2.writeFrame(out, TestH2.SETTINGS, 0, 0, new byte[0]);
		// no END_STREAM: the requests are unfinished, so the client's end ends the relay
		TestH2.writeFrame(out, TestH2.HEADERS, TestH2.FLAG_END_HEADERS, 1,
				TestH2.headerBlock(":method=POST", ":scheme=http", ":authority=a.local", ":path=/"));
		TestH2.writeFrame(out, TestH2.HEADERS, TestH2.FLAG_END_HEADERS, 3,
				TestH2.headerBlock(":method=POST", ":scheme=http", ":authority=b.local", ":path=/"));
		Awaitility.await()
			.atMost(Duration.ofSeconds(5))
			.alias("both legs' frames are queued")
			.until(() -> sender().connectionQueues() == fillers + 2);
		assertThat(admitted()).as("precondition: the fillers, the connection and its further leg")
			.isEqualTo(FILLERS + 2);

		h2.close();
		Awaitility.await()
			.atMost(Duration.ofSeconds(5))
			.alias("the relay ended")
			.until(() -> dataProxyServer().activeConnections() == 0);
		assertHeldFor(Duration.ofMillis(300), () -> assertThat(admitted()).isEqualTo(FILLERS + 2));

		client.resume.countDown();
		Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> admitted() == FILLERS);
	}

	/**
	 * A leg retired while its frames are queued keeps the connection's own slot: the leg
	 * opened next for another route takes a slot of its own.
	 */
	@Test
	void aRetiredLegKeepsTheConnectionsSlotWhileItsFramesAreQueued() throws Exception {
		StallingClient client = client();
		int fillers = stallWithFillers();
		Socket h2 = open(dataPort);
		OutputStream out = h2.getOutputStream();
		InputStream in = h2.getInputStream();
		out.write(TestH2.PREFACE);
		TestH2.writeFrame(out, TestH2.SETTINGS, 0, 0, new byte[0]);
		// no END_STREAM: the client still sends on the stream when its leg retires
		TestH2.writeFrame(out, TestH2.HEADERS, TestH2.FLAG_END_HEADERS, 1,
				TestH2.headerBlock(":method=POST", ":scheme=http", ":authority=a.local", ":path=/"));
		Awaitility.await()
			.atMost(Duration.ofSeconds(5))
			.alias("route a's leg queued its frames")
			.until(() -> sender().connectionQueues() == fillers + 1);
		long legA = session().connectionsOpened();
		// the upstream answers and ends its side without reading the stalled stream
		client.sendFromTest(Frame.newBuilder()
			.setData(Data.newBuilder().setConnId(legA).setPayload(ByteString.copyFrom(TestH2.cannedResponse(1, "ok"))))
			.build());
		client.sendFromTest(Frame.newBuilder().setClose(Close.newBuilder().setConnId(legA)).build());
		awaitReset(in, 1);
		assertThat(sender().connectionQueues()).as("precondition: the retired leg's frames are still queued")
			.isEqualTo(fillers + 1);

		TestH2.writeFrame(out, TestH2.HEADERS, TestH2.FLAG_END_HEADERS, 3,
				TestH2.headerBlock(":method=POST", ":scheme=http", ":authority=b.local", ":path=/"));
		Awaitility.await()
			.atMost(Duration.ofSeconds(5))
			.alias("route b's leg queued its frames")
			.until(() -> sender().connectionQueues() == fillers + 2);
		assertThat(admitted()).as("the fillers, the connection held by the retired leg, and route b's leg")
			.isEqualTo(FILLERS + 2);
	}

	/**
	 * A TLS passthrough connection, relayed verbatim, keeps its slot after its relay
	 * ended while its frames are queued.
	 */
	@Test
	void anEndedPassthroughRelayHoldsItsSlotWhileItsFramesAreQueued() throws Exception {
		StallingClient client = client();
		Socket tls = open(dataPort);
		tls.getOutputStream().write(clientHello("pass.local"));
		tls.getOutputStream().flush();
		// the client answered the CONNECT with its CLOSE before it stalls
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> client.closedTcp.size() == 1);
		int fillers = stallWithFillers();
		tls.getOutputStream().write(new byte[SMALL_PAYLOAD]);
		tls.shutdownOutput();
		Awaitility.await()
			.atMost(Duration.ofSeconds(5))
			.alias("the relay ended with its frames queued")
			.until(() -> dataProxyServer().activeConnections() == 0 && sender().connectionQueues() == fillers + 1);
		assertHeldFor(Duration.ofMillis(300), () -> assertThat(admitted()).isEqualTo(FILLERS + 1));

		client.resume.countDown();
		Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> admitted() == FILLERS);
	}

	/** Reads frames until the relay resets {@code streamId}. */
	private static void awaitReset(InputStream in, int streamId) throws IOException {
		TestH2.Frame frame;
		while ((frame = TestH2.readFrame(in)) != null) {
			if (frame.type() == TestH2.RST_STREAM && frame.streamId() == streamId) {
				return;
			}
		}
		throw new AssertionError("the connection ended before stream %d was reset".formatted(streamId));
	}

	/** A TLS ClientHello naming {@code host}. */
	private static byte[] clientHello(String host) throws Exception {
		SSLEngine engine = SSLContext.getDefault().createSSLEngine(host, 443);
		engine.setUseClientMode(true);
		SSLParameters parameters = engine.getSSLParameters();
		parameters.setServerNames(List.of(new SNIHostName(host)));
		engine.setSSLParameters(parameters);
		ByteBuffer hello = ByteBuffer.allocate(engine.getSession().getPacketBufferSize());
		engine.beginHandshake();
		engine.wrap(ByteBuffer.allocate(0), hello);
		hello.flip();
		byte[] bytes = new byte[hello.remaining()];
		hello.get(bytes);
		return bytes;
	}

	/**
	 * Opens {@code connections} tcp route connections, which the client answers with its
	 * CLOSE, stalls the client, writes {@code bytes} on each and ends every relay while
	 * its frames are queued: once the stream stopped taking frames, the client fails each
	 * connection with ERROR, which ends its relay wherever its writer waits. No peer
	 * half-closes: a relay reaches its peer's FIN only after queueing every byte before
	 * it, and the sender caps a connection's queue in DATA frames, one per read, so how
	 * the reads split the bytes would decide whether the relay ends.
	 */
	private void endRelaysWithQueuedFrames(int connections, int bytes) throws IOException {
		StallingClient client = client();
		List<Socket> peers = new ArrayList<>();
		for (int i = 0; i < connections; i++) {
			peers.add(open(tcpRoutePort));
		}
		// the client answered every CONNECT with its CLOSE before it stalls
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> client.closedTcp.size() == connections);
		client.stall.set(true);
		for (Socket peer : peers) {
			// on its own thread: what the relay does not read fills the socket buffers
			// and may block the write until the relay ends
			Thread.ofVirtual().start(() -> {
				try {
					peer.getOutputStream().write(new byte[bytes]);
				}
				catch (IOException e) {
					// the relay ended and closed its socket
				}
			});
		}
		Awaitility.await()
			.atMost(Duration.ofSeconds(10))
			.alias("the stream stopped taking frames")
			.until(() -> client.blocked.get() && sender().stalled().compareTo(Duration.ofMillis(300)) >= 0);
		for (long id : client.closedTcp) {
			client.sendFromTest(Frame.newBuilder()
				.setError(am.ik.sluice.v1.proto.Error.newBuilder().setConnId(id).setMessage("upstream failed"))
				.build());
		}
		Awaitility.await()
			.atMost(Duration.ofSeconds(10))
			.alias("every relay ended")
			.until(() -> session().connectionCount() == 0);
	}

	/**
	 * Stalls the client and queues more than the stream takes on {@link #FILLERS} open
	 * tcp route connections; returns the queues left once they settled.
	 */
	private int stallWithFillers() throws Exception {
		client().stall.set(true);
		for (int i = 0; i < FILLERS; i++) {
			Socket peer = open(tcpRoutePort);
			peer.getOutputStream().write(new byte[VirtualConnection.DEFAULT_WINDOW]);
		}
		int queued = settledQueues();
		assertThat(queued).as("precondition: the fillers' frames are queued").isPositive();
		assertThat(client().blocked.get()).as("precondition: the client stopped reading").isTrue();
		return queued;
	}

	/** The sender's live queues once they stopped changing for 300ms. */
	private int settledQueues() throws InterruptedException {
		long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
		int last = -1;
		long since = System.nanoTime();
		while (System.nanoTime() < deadline) {
			int current = sender().connectionQueues();
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

	/** Asserts the condition over the whole duration. */
	private static void assertHeldFor(Duration duration, Runnable assertion) throws InterruptedException {
		long deadline = System.nanoTime() + duration.toNanos();
		do {
			assertion.run();
			TimeUnit.MILLISECONDS.sleep(20);
		}
		while (System.nanoTime() < deadline);
	}

	private Socket open(int port) throws IOException {
		Socket socket = new Socket("127.0.0.1", port);
		socket.setSoTimeout(10_000);
		this.sockets.add(socket);
		return socket;
	}

	private int admitted() {
		return (int) Objects.requireNonNull(this.meterRegistry, "meterRegistry")
			.get("sluice.data.connections.admitted")
			.gauge()
			.value();
	}

	private TunnelSession session() {
		return registry().find(CLIENT_ID).orElseThrow();
	}

	private SessionSender sender() {
		return (SessionSender) session().sender();
	}

	private SessionRegistry registry() {
		return Objects.requireNonNull(this.registry, "registry");
	}

	private DataProxyServer dataProxyServer() {
		return Objects.requireNonNull(this.dataProxyServer, "dataProxyServer");
	}

	private StallingClient client() {
		return Objects.requireNonNull(this.client, "client");
	}

	/**
	 * A tunnel client that advertises a tcp route, two HTTP hosts and a TLS passthrough
	 * host, answers each connection of the tcp route and the passthrough host with its
	 * CLOSE and each HTTP request with {@link #RESPONSE}; once {@link #stall} is set, its
	 * receive callback blocks until {@link #resume}, so it stops reading the stream.
	 */
	private static final class StallingClient implements StreamObserver<Frame> {

		final CountDownLatch acked = new CountDownLatch(1);

		final AtomicBoolean stall = new AtomicBoolean();

		final AtomicBoolean blocked = new AtomicBoolean();

		final CountDownLatch resume = new CountDownLatch(1);

		final Set<Long> closedTcp = ConcurrentHashMap.newKeySet();

		private final Map<Long, String> addresses = new ConcurrentHashMap<>();

		private final Set<Long> answered = ConcurrentHashMap.newKeySet();

		private @Nullable StreamObserver<Frame> requests;

		void start(StreamObserver<Frame> requests) {
			this.requests = requests;
			send(Frame.newBuilder()
				.setAdvertise(Advertise.newBuilder()
					.addUpstreams(upstream("a.local", "http://127.0.0.1:1001", 0))
					.addUpstreams(upstream("b.local", "http://127.0.0.1:1002", 0))
					.addUpstreams(upstream("held.tcp", "tcp://" + TCP_TARGET, tcpRoutePort))
					.addUpstreams(upstream("pass.local", "tcp://" + TCP_TARGET, 0).toBuilder().setTlsPassthrough(true))
					.setConnectionWindow(VirtualConnection.DEFAULT_WINDOW))
				.build());
		}

		private static Upstream upstream(String host, String target, int listenPort) {
			return Upstream.newBuilder().setHost(host).setTargetUrl(target).setListenPort(listenPort).build();
		}

		/** Sends from the test's thread, while the receive callback may be blocked. */
		void sendFromTest(Frame frame) {
			send(frame);
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
					if (TCP_TARGET.equals(frame.getConnect().getAddress())) {
						send(Frame.newBuilder().setClose(Close.newBuilder().setConnId(id)).build());
						this.closedTcp.add(id);
					}
				}
				case DATA -> {
					long id = frame.getData().getConnId();
					String address = this.addresses.get(id);
					if (address != null && !TCP_TARGET.equals(address) && this.answered.add(id)) {
						send(Frame.newBuilder()
							.setData(Data.newBuilder().setConnId(id).setPayload(ByteString.copyFrom(RESPONSE)))
							.build());
					}
				}
				default -> {
					// window updates, membership and the like
				}
			}
		}

		@Override
		public void onError(Throwable t) {
		}

		@Override
		public void onCompleted() {
		}

	}

}
