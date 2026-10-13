package am.ik.sluice.it;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;

import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.client.config.Upstream;
import am.ik.sluice.client.tunnel.TunnelClient;
import am.ik.sluice.server.SluiceServerApplication;
import am.ik.sluice.server.config.SluiceServerProperties.H2DemuxEngine;
import am.ik.sluice.server.proxy.H2EngineTest;
import am.ik.sluice.server.proxy.LegacyOnly;
import am.ik.sluice.server.route.Router;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.netty.handler.codec.http2.DefaultHttp2HeadersDecoder;
import io.netty.handler.codec.http2.Http2Headers;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.params.Parameter;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A header block split into HEADERS + CONTINUATION reaches its peer contiguous (RFC 9113
 * 6.10) in both directions, while the relay concurrently emits connection-level frames
 * and other legs' frames on the same connection.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
@H2EngineTest
@LegacyOnly("settings replay and recording stubs without SETTINGS first: per-hop rewrite; not yet on the netty engine")
class H2DemuxFramingE2ETest {

	/** The engine of this invocation, selected by {@link H2EngineTest}. */
	@Nullable
	@Parameter
	private H2DemuxEngine engine;

	private static final int REQUESTS = 1500;

	/** Requests kept in flight, below the relay's stream limit. */
	private static final int IN_FLIGHT = 90;

	private static final int FRAME_SIZE = 16 * 1024;

	private static final ExecutorService UPSTREAM_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

	private final List<AutoCloseable> closeables = new java.util.concurrent.CopyOnWriteArrayList<>();

	private final List<TunnelClient> clients = new ArrayList<>();

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
	void tearDown() {
		this.clients.forEach(TunnelClient::stop);
		this.closeables.forEach(H2DemuxFramingE2ETest::close);
		UPSTREAM_EXECUTOR.shutdown();
	}

	/**
	 * Large request heads (HEADERS + CONTINUATION after re-encoding) towards an upstream
	 * streaming an endless response on the same leg, whose every relayed chunk makes the
	 * relay emit a connection-level WINDOW_UPDATE towards it.
	 */
	@Test
	void requestHeaderBlocksReachTheUpstreamContiguous() throws Exception {
		String route = "frm.up.test";
		AtomicReference<String> violation = new AtomicReference<>("");
		AtomicInteger heads = new AtomicInteger();
		AtomicInteger continuations = new AtomicInteger();
		startClient(route, stub(socket -> checkRequestBlocks(socket, violation, heads, continuations)));
		byte[] bigBlock = bigRequestHead(route);
		try (H2TestClient client = H2TestClient.connect(dataPort)) {
			unboundedStreamWindows(client);
			client.headers(1, true, TestH2.headerBlock(":authority=" + route, ":method=GET", ":path=/stream"));
			sendRequests(client, bigBlock, violation);
			Awaitility.await()
				.atMost(Duration.ofSeconds(30))
				.until(() -> !violation.get().isEmpty() || heads.get() >= REQUESTS + 1);
			assertThat(violation.get()).as("frames interleaved into a header block").isEmpty();
			assertThat(heads.get()).isEqualTo(REQUESTS + 1);
			assertThat(continuations.get()).as("the heads spanned CONTINUATION frames")
				.isGreaterThanOrEqualTo(REQUESTS);
			assertThat(client.closed()).isFalse();
		}
	}

	/**
	 * Large response heads towards the client from one leg while another leg streams an
	 * endless response on the same connection and the client's requests make the relay
	 * grant connection window.
	 */
	@Test
	void responseHeaderBlocksReachTheClientContiguous() throws Exception {
		String streaming = "frm.stream.test";
		String big = "frm.big.test";
		startClient(streaming, stub(H2DemuxFramingE2ETest::streamEndlessly));
		startClient(big, stub(H2DemuxFramingE2ETest::answerWithBigHeads));
		AtomicReference<String> violation = new AtomicReference<>("");
		try (H2TestClient client = H2TestClient.connect(dataPort, 3)) {
			unboundedStreamWindows(client);
			client.headers(1, true, TestH2.headerBlock(":authority=" + streaming, ":method=GET", ":path=/stream"));
			Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> client.dataReceived() > 0);
			byte[] block = TestH2.headerBlock(":authority=" + big, ":method=GET", ":path=/big");
			sendRequests(client, block, violation);
			Awaitility.await()
				.atMost(Duration.ofSeconds(30))
				.until(() -> !client.interleaving().isEmpty() || client.headerBlocks() >= REQUESTS + 1);
			assertThat(client.interleaving()).as("frames interleaved into a header block").isEmpty();
			assertThat(client.headerBlocks()).isGreaterThanOrEqualTo(REQUESTS + 1);
			assertThat(client.frames(3)).as("the response head spans CONTINUATION frames")
				.anyMatch(frame -> frame.type() == TestH2.CONTINUATION);
			assertThat(client.closed()).isFalse();
		}
	}

	/**
	 * A response head larger than the HPACK decoder's 8KiB default reaches the client
	 * whole, re-encoded.
	 */
	@Test
	void largeResponseHeadReachesTheClientIntact() throws Exception {
		String route = "frm.head.test";
		startClient(route, stub(H2DemuxFramingE2ETest::answerWithBigHeads));
		try (H2TestClient client = H2TestClient.connect(dataPort, 1)) {
			client.headers(1, true, TestH2.headerBlock(":authority=" + route, ":method=GET", ":path=/big"));
			assertThat(client.awaitEnd(1, Duration.ofSeconds(10))).isTrue();
			DefaultHttp2HeadersDecoder decoder = new DefaultHttp2HeadersDecoder(true, 64 * 1024);
			Http2Headers headers = decoder.decodeHeaders(1,
					io.netty.buffer.Unpooled.wrappedBuffer(client.firstHeaderBlock(1)));
			assertThat(Objects.requireNonNull(headers.status()).toString()).isEqualTo("200");
			assertThat(headers.size()).isEqualTo(BIG_HEADERS + 1);
			assertThat(Objects.requireNonNull(headers.get("x-r299")).toString()).isEqualTo("v".repeat(100));
		}
	}

	/**
	 * The client's settings reach the upstream, except the header table size -- the
	 * upstream encodes for the relay's decoder, which keeps the default table --, the max
	 * frame size -- the upstream frames for the relay's parser, which takes the default
	 * -- and settings unknown to the relay; later ones too, once the leg is open.
	 */
	@Test
	void clientSettingsReachTheUpstreamWithoutTheHeaderTableSize() throws Exception {
		String route = "frm.settings.test";
		Queue<TestH2.Frame> received = new ConcurrentLinkedQueue<>();
		startClient(route, stub(socket -> recordAndAnswer(socket, received)));
		try (H2TestClient client = H2TestClient.connect(dataPort)) {
			client.frame(TestH2.SETTINGS, 0, 0, settings(0x1, 65536, 0x2, 0, 0x99, 1));
			client.headers(1, true, TestH2.headerBlock(":authority=" + route, ":method=GET", ":path=/"));
			assertThat(client.awaitEnd(1, Duration.ofSeconds(5))).isTrue();
			client.frame(TestH2.SETTINGS, 0, 0, settings(0x1, 4096, 0x4, 1048576, 0x5, 1048576, 0x6, 1048576));
			client.headers(3, true, TestH2.headerBlock(":authority=" + route, ":method=GET", ":path=/"));
			assertThat(client.awaitEnd(3, Duration.ofSeconds(5))).isTrue();
		}
		List<String> forwarded = new ArrayList<>();
		for (TestH2.Frame frame : received) {
			if (frame.type() == TestH2.SETTINGS && (frame.flags() & TestH2.FLAG_ACK) == 0) {
				byte[] p = frame.payload();
				for (int i = 0; i + 6 <= p.length; i += 6) {
					int id = (p[i] & 0xff) << 8 | (p[i + 1] & 0xff);
					long value = (p[i + 2] & 0xffL) << 24 | (p[i + 3] & 0xff) << 16 | (p[i + 4] & 0xff) << 8
							| (p[i + 5] & 0xff);
					forwarded.add(id + "=" + value);
				}
			}
		}
		// the header list size is capped at the relay's decoder limit
		assertThat(forwarded).containsExactly("2=0", "4=1048576", "6=65536");
	}

	/**
	 * PRIORITY (advisory) and frames of unknown types (extensions of this hop) are not
	 * relayed to the upstream.
	 */
	@Test
	void priorityAndUnknownFramesAreNotRelayed() throws Exception {
		String route = "frm.drop.test";
		Queue<TestH2.Frame> received = new ConcurrentLinkedQueue<>();
		startClient(route, stub(socket -> recordAndAnswer(socket, received)));
		try (H2TestClient client = H2TestClient.connect(dataPort)) {
			client.headers(1, false, TestH2.headerBlock(":authority=" + route, ":method=POST", ":path=/"));
			client.frame(0x2, 0, 1, new byte[] { 0, 0, 0, 0, 16 });
			client.frame(0x20, 0, 1, new byte[] { 1, 2, 3 });
			client.sendData(1, new byte[] { 'x' }, TestH2.FLAG_END_STREAM);
			assertThat(client.awaitEnd(1, Duration.ofSeconds(5))).isTrue();
		}
		assertThat(received).extracting(TestH2.Frame::type).doesNotContain(0x2, 0x20).contains(TestH2.DATA);
	}

	/**
	 * Once the client ended a stream, its further frames are not relayed; a later block
	 * on a stream the relay answered itself is not taken for a new request.
	 */
	@Test
	void framesAfterTheClientEndedTheStreamAreDropped() throws Exception {
		String route = "frm.ended.test";
		Queue<TestH2.Frame> received = new ConcurrentLinkedQueue<>();
		startClient(route, stub(socket -> recordAndAnswer(socket, received)));
		try (H2TestClient client = H2TestClient.connect(dataPort, 5)) {
			client.headers(1, true, TestH2.headerBlock(":authority=" + route, ":method=GET", ":path=/"));
			client.sendData(1, new byte[] { 'x' }, 0);
			client.headers(1, true, TestH2.headerBlock("x-trailer=1"));
			client.headers(3, false, TestH2.headerBlock(":authority=" + route, ":method=POST", ":path=/"));
			client.headers(3, true, TestH2.headerBlock("x-trailer=1"));
			client.headers(3, true, TestH2.headerBlock("x-trailer=2"));
			client.headers(5, true, TestH2.headerBlock(":authority=nowhere.test", ":method=GET", ":path=/"));
			client.headers(5, true, TestH2.headerBlock("x-trailer=1"));
			// a sentinel: the upstream sees it after everything relayed before it
			client.headers(7, true, TestH2.headerBlock(":authority=" + route, ":method=GET", ":path=/"));
			assertThat(client.awaitEnd(7, Duration.ofSeconds(5))).isTrue();
			assertThat(client.awaitEnd(5, Duration.ofSeconds(5))).isTrue();
			Thread.sleep(200); // a second answer on stream 5 would follow the first
			assertThat(client.frames(5)).filteredOn(frame -> frame.type() == TestH2.HEADERS)
				.as("one response on the stream the relay answered")
				.hasSize(1);
		}
		assertThat(received).filteredOn(frame -> frame.streamId() == 1)
			.extracting(TestH2.Frame::type)
			.containsExactly(TestH2.HEADERS);
		assertThat(received).filteredOn(frame -> frame.streamId() == 3)
			.extracting(TestH2.Frame::type)
			.containsExactly(TestH2.HEADERS, TestH2.HEADERS);
	}

	private static byte[] settings(long... pairs) {
		ByteArrayOutputStream payload = new ByteArrayOutputStream();
		for (int i = 0; i + 1 < pairs.length; i += 2) {
			int id = (int) pairs[i];
			long value = pairs[i + 1];
			payload.write(id >>> 8);
			payload.write(id);
			payload.writeBytes(
					new byte[] { (byte) (value >>> 24), (byte) (value >>> 16), (byte) (value >>> 8), (byte) value });
		}
		return payload.toByteArray();
	}

	/** Records every frame received and answers every ended request. */
	private static void recordAndAnswer(Socket socket, Queue<TestH2.Frame> received) {
		try (socket) {
			InputStream in = socket.getInputStream();
			OutputStream out = socket.getOutputStream();
			in.readNBytes(TestH2.PREFACE.length);
			TestH2.@Nullable Frame frame;
			while ((frame = TestH2.readFrame(in)) != null) {
				received.add(frame);
				boolean ended = frame.streamId() != 0 && (frame.type() == TestH2.HEADERS || frame.type() == TestH2.DATA)
						&& (frame.flags() & TestH2.FLAG_END_STREAM) != 0;
				if (ended) {
					out.write(endedHead(frame.streamId()));
					out.flush();
				}
			}
		}
		catch (IOException e) {
			// ended
		}
	}

	private static final int BIG_HEADERS = 300;

	/**
	 * ~40KiB of request headers: re-encoded by the relay into HEADERS + 2 CONTINUATION.
	 */
	private static byte[] bigRequestHead(String route) {
		List<String> headers = new ArrayList<>(List.of(":authority=" + route, ":method=GET", ":path=/big"));
		for (int i = 0; i < 320; i++) {
			headers.add("x-h" + i + "=" + "v".repeat(120));
		}
		return TestH2.headerBlock(headers.toArray(String[]::new));
	}

	/** ~35KiB response head: {@code :status 200} plus {@code #BIG_HEADERS} headers. */
	private static byte[] bigResponseHead() {
		ByteArrayOutputStream block = new ByteArrayOutputStream();
		block.write(0x88);
		String[] headers = new String[BIG_HEADERS];
		for (int i = 0; i < BIG_HEADERS; i++) {
			headers[i] = "x-r" + i + "=" + "v".repeat(100);
		}
		block.writeBytes(TestH2.headerBlock(headers));
		return block.toByteArray();
	}

	/**
	 * Sends {@code #REQUESTS} requests with the block on streams 3, 5, ..., keeping at
	 * most {@code #IN_FLIGHT} unanswered; stops early on a violation or a closed
	 * connection.
	 */
	private static void sendRequests(H2TestClient client, byte[] block, AtomicReference<String> violation)
			throws Exception {
		Deque<Integer> outstanding = new ArrayDeque<>();
		for (int i = 0; i < REQUESTS && violation.get().isEmpty() && client.interleaving().isEmpty(); i++) {
			boolean room = client.awaitTrue(() -> {
				outstanding.removeIf(client::ended);
				return outstanding.size() < IN_FLIGHT || client.closed();
			}, Duration.ofSeconds(10));
			assertThat(room && !client.closed())
				.as("request %d: answers keep coming (outstanding %s, resets %s)", i, outstanding, client.resets())
				.isTrue();
			int streamId = 3 + 2 * i;
			client.headers(streamId, true, block);
			outstanding.add(streamId);
		}
	}

	/**
	 * Checks the contiguity of every header block received; streams an endless response
	 * on stream 1 and answers the other requests. The reader never writes: a reader
	 * blocked on the writer would stop draining the socket, which with the tunnel's
	 * stream-wide backpressure stalls the writer in turn.
	 */
	private static void checkRequestBlocks(Socket socket, AtomicReference<String> violation, AtomicInteger heads,
			AtomicInteger continuations) {
		try (socket) {
			InputStream in = socket.getInputStream();
			OutputStream out = socket.getOutputStream();
			Queue<byte[]> responses = new ConcurrentLinkedQueue<>();
			SendWindow window = new SendWindow();
			in.readNBytes(TestH2.PREFACE.length);
			int blockOf = -1;
			boolean streaming = false;
			TestH2.@Nullable Frame frame;
			while ((frame = TestH2.readFrame(in)) != null) {
				window.credit(frame);
				if (blockOf != -1 && (frame.type() != TestH2.CONTINUATION || frame.streamId() != blockOf)) {
					violation.compareAndSet("", "type=%d stream=%d inside the header block of stream %d"
						.formatted(frame.type(), frame.streamId(), blockOf));
				}
				if (frame.type() == TestH2.CONTINUATION) {
					continuations.incrementAndGet();
				}
				if (frame.type() == TestH2.HEADERS || frame.type() == TestH2.CONTINUATION) {
					boolean endHeaders = (frame.flags() & TestH2.FLAG_END_HEADERS) != 0;
					blockOf = endHeaders ? -1 : frame.streamId();
					if (endHeaders) {
						heads.incrementAndGet();
						if (frame.streamId() != 1) {
							responses.add(endedHead(frame.streamId()));
						}
					}
				}
				if (!streaming && frame.type() == TestH2.HEADERS && frame.streamId() == 1) {
					streaming = true;
					Thread.ofVirtual().start(() -> writeEndlessly(out, responses, window));
				}
			}
		}
		catch (IOException e) {
			// ended
		}
	}

	/** A HEADERS frame {@code :status 200} ending the stream. */
	private static byte[] endedHead(int streamId) {
		ByteArrayOutputStream frame = new ByteArrayOutputStream();
		try {
			TestH2.writeFrame(frame, TestH2.HEADERS, TestH2.FLAG_END_HEADERS | TestH2.FLAG_END_STREAM, streamId,
					new byte[] { (byte) 0x88 });
		}
		catch (IOException e) {
			throw new IllegalStateException(e);
		}
		return frame.toByteArray();
	}

	/** Streams an endless response on stream 1 once its request arrives. */
	private static void streamEndlessly(Socket socket) {
		try (socket) {
			InputStream in = socket.getInputStream();
			OutputStream out = socket.getOutputStream();
			in.readNBytes(TestH2.PREFACE.length);
			SendWindow window = new SendWindow();
			TestH2.@Nullable Frame frame;
			boolean streaming = false;
			while ((frame = TestH2.readFrame(in)) != null) {
				window.credit(frame);
				if (!streaming && frame.type() == TestH2.HEADERS) {
					streaming = true;
					Thread.ofVirtual().start(() -> writeEndlessly(out, new ConcurrentLinkedQueue<>(), window));
				}
			}
		}
		catch (IOException e) {
			// ended
		}
	}

	/**
	 * Announces stream windows of 2^31-1, relayed to the upstreams: a stub streaming
	 * endlessly is bound by the connection window the relay grants it alone.
	 */
	private static void unboundedStreamWindows(H2TestClient client) throws IOException {
		client.frame(TestH2.SETTINGS, 0, 0,
				WindowedH2Upstream.setting(WindowedH2Upstream.SETTINGS_INITIAL_WINDOW_SIZE, Integer.MAX_VALUE));
	}

	/**
	 * The connection window the relay granted a stub upstream: its reader credits the
	 * relay's grants, its writer takes from it before every DATA frame.
	 */
	private static final class SendWindow {

		private long window = WindowedH2Upstream.DEFAULT_WINDOW;

		synchronized void credit(TestH2.Frame frame) {
			if (frame.type() == TestH2.WINDOW_UPDATE && frame.streamId() == 0) {
				this.window += TestH2.increment(frame);
				notifyAll();
			}
		}

		synchronized void take(int length) throws InterruptedException {
			while (this.window < length) {
				wait();
			}
			this.window -= length;
		}

	}

	/**
	 * The stub's only writer: an endless response on stream 1 at about 16MiB/s within the
	 * connection window the relay granted, the queued responses written between its
	 * frames.
	 */
	private static void writeEndlessly(OutputStream out, Queue<byte[]> responses, SendWindow window) {
		try {
			TestH2.writeFrame(out, TestH2.SETTINGS, 0, 0, new byte[0]);
			TestH2.writeFrame(out, TestH2.HEADERS, TestH2.FLAG_END_HEADERS, 1, new byte[] { (byte) 0x88 });
			byte[] chunk = new byte[1024];
			for (long i = 0;; i++) {
				for (byte[] response = responses.poll(); response != null; response = responses.poll()) {
					out.write(response);
				}
				window.take(chunk.length);
				TestH2.writeFrame(out, TestH2.DATA, 0, 1, chunk);
				if (i % 16 == 15) {
					Thread.sleep(1);
				}
			}
		}
		catch (IOException e) {
			// ended
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	/**
	 * Answers every ended request with a big head (HEADERS + CONTINUATION, END_STREAM).
	 */
	private static void answerWithBigHeads(Socket socket) {
		try (socket) {
			InputStream in = socket.getInputStream();
			OutputStream out = socket.getOutputStream();
			in.readNBytes(TestH2.PREFACE.length);
			byte[] head = bigResponseHead();
			TestH2.writeFrame(out, TestH2.SETTINGS, 0, 0, new byte[0]);
			TestH2.@Nullable Frame frame;
			while ((frame = TestH2.readFrame(in)) != null) {
				if (frame.type() == TestH2.HEADERS && (frame.flags() & TestH2.FLAG_END_STREAM) != 0) {
					writeBlock(out, frame.streamId(), head);
				}
			}
		}
		catch (IOException e) {
			// ended
		}
	}

	/** Writes the block as HEADERS (END_STREAM) plus CONTINUATION frames. */
	private static void writeBlock(OutputStream out, int streamId, byte[] block) throws IOException {
		int offset = 0;
		boolean first = true;
		do {
			int length = Math.min(FRAME_SIZE, block.length - offset);
			boolean last = offset + length >= block.length;
			int flags = (first ? TestH2.FLAG_END_STREAM : 0) | (last ? TestH2.FLAG_END_HEADERS : 0);
			byte[] fragment = new byte[length];
			System.arraycopy(block, offset, fragment, 0, length);
			TestH2.writeFrame(out, first ? TestH2.HEADERS : TestH2.CONTINUATION, flags, streamId, fragment);
			first = false;
			offset += length;
		}
		while (offset < block.length);
	}

	private void startClient(String host, ServerSocket upstream) {
		TunnelClient client = TunnelClient.builder()
			.properties(SluiceClientProperties.builder()
				.serverUrl("grpc://127.0.0.1:" + grpcPort)
				.upstream(Upstream.builder().host(host).target("tcp://127.0.0.1:" + upstream.getLocalPort()).build())
				.token("it-token")
				.build())
			.taskExecutor(task -> Thread.ofVirtual().name("e2e-frm").start(task))
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
			Thread.ofVirtual().name("e2e-frm-accept").start(() -> {
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
