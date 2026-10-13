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
import am.ik.sluice.server.route.Router;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.netty.handler.codec.http2.DefaultHttp2HeadersDecoder;
import io.netty.handler.codec.http2.Http2Headers;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.TestInstance;

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
class H2DemuxFramingE2ETest {

	private static final int REQUESTS = 1500;

	/** Requests kept in flight, below the relay's stream limit. */
	private static final int IN_FLIGHT = 90;

	private static final int FRAME_SIZE = 16 * 1024;

	private static final int PROTOCOL_ERROR = 0x1;

	private static final int STREAM_CLOSED = 0x5;

	private static final int CANCEL = 0x8;

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
	 * streaming an endless response on the same leg, whose relayed chunks make the relay
	 * grant it window (connection and stream WINDOW_UPDATEs) meanwhile.
	 */
	@Test
	void requestHeaderBlocksReachTheUpstreamContiguous() throws Exception {
		String route = "frm.up.test";
		AtomicReference<String> violation = new AtomicReference<>("");
		AtomicInteger heads = new AtomicInteger();
		AtomicInteger continuations = new AtomicInteger();
		AtomicInteger grants = new AtomicInteger();
		startClient(route, stub(socket -> checkRequestBlocks(socket, violation, heads, continuations, grants)));
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
			assertThat(grants.get()).as("window granted while the heads went by").isGreaterThan(10);
			assertThat(client.resets()).as("the endless response goes on").isEmpty();
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
			assertThat(client.resets()).as("the endless response goes on").doesNotContainKey(1);
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
	 * The settings are per hop: the upstream gets the relay's own SETTINGS once, as its
	 * leg opens -- push off, the 64KiB header list limit, the RFC defaults else --
	 * whatever the client announces, before the leg opened or after; and the client gets
	 * the relay's: 100 concurrent streams and the 64KiB header list limit.
	 */
	@Test
	void eachHopGetsTheRelaysOwnSettings() throws Exception {
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
			assertThat(client.setting(0x3)).as("SETTINGS_MAX_CONCURRENT_STREAMS").isEqualTo(100L);
			assertThat(client.setting(0x6)).as("SETTINGS_MAX_HEADER_LIST_SIZE").isEqualTo(65536L);
			assertThat(client.setting(0x4)).as("SETTINGS_INITIAL_WINDOW_SIZE").isNull();
			assertThat(client.setting(0x5)).as("SETTINGS_MAX_FRAME_SIZE").isNull();
		}
		List<String> announced = new ArrayList<>();
		for (TestH2.Frame frame : received) {
			if (frame.type() == TestH2.SETTINGS && (frame.flags() & TestH2.FLAG_ACK) == 0) {
				byte[] p = frame.payload();
				List<String> entries = new ArrayList<>();
				for (int i = 0; i + 6 <= p.length; i += 6) {
					int id = (p[i] & 0xff) << 8 | (p[i + 1] & 0xff);
					long value = (p[i + 2] & 0xffL) << 24 | (p[i + 3] & 0xff) << 16 | (p[i + 4] & 0xff) << 8
							| (p[i + 5] & 0xff);
					entries.add(id + "=" + value);
				}
				announced.add(String.join(",", entries));
			}
		}
		assertThat(announced).as("the SETTINGS frames the upstream got").containsExactly("2=0,6=65536");
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
	 * Once the client ended a stream, its further frames are not relayed: netty's codec
	 * resets the stream -- {@code STREAM_CLOSED} for DATA (RFC 9113 5.1), and for any
	 * frame on a stream it forgot; {@code PROTOCOL_ERROR} for a header block on a stream
	 * it still holds -- and the relay cancels the upstream stream if its response is
	 * unfinished; a later block on a stream the relay answered itself is not taken for a
	 * new request. The connection goes on.
	 */
	@Test
	void framesAfterTheClientEndedTheStreamAreResetNotRelayed() throws Exception {
		String route = "frm.ended.test";
		Queue<TestH2.Frame> received = new ConcurrentLinkedQueue<>();
		startClient(route, stub(socket -> recordAndAnswer(socket, received)));
		try (H2TestClient client = H2TestClient.connect(dataPort, 7)) {
			// the leg's first stream: its SETTINGS lift the one-stream limit, so the
			// streams below reach the upstream at once rather than wait in the relay
			client.headers(1, true, TestH2.headerBlock(":authority=" + route, ":method=GET", ":path=/"));
			assertThat(client.awaitEnd(1, Duration.ofSeconds(5))).isTrue();
			client.headers(3, true, TestH2.headerBlock(":authority=" + route, ":method=GET", ":path=/"));
			client.sendData(3, new byte[] { 'x' }, 0);
			client.headers(3, true, TestH2.headerBlock("x-trailer=1"));
			client.headers(5, false, TestH2.headerBlock(":authority=" + route, ":method=POST", ":path=/"));
			client.headers(5, true, TestH2.headerBlock("x-trailer=1"));
			client.headers(5, true, TestH2.headerBlock("x-trailer=2"));
			client.headers(7, true, TestH2.headerBlock(":authority=nowhere.test", ":method=GET", ":path=/"));
			client.headers(7, true, TestH2.headerBlock("x-trailer=1"));
			// a sentinel: the upstream sees it after everything relayed before it
			client.headers(9, true, TestH2.headerBlock(":authority=" + route, ":method=GET", ":path=/"));
			assertThat(client.awaitEnd(9, Duration.ofSeconds(5))).isTrue();
			// the page's body, if still held for the stream when the late block came, is
			// dropped with the reset
			assertThat(client.awaitTrue(() -> client.reset(7) != null, Duration.ofSeconds(5))).isTrue();
			Thread.sleep(200); // a second answer on stream 7 would follow the first
			assertThat(client.frames(7)).filteredOn(frame -> frame.type() == TestH2.HEADERS)
				.as("one response on the stream the relay answered")
				.hasSize(1);
			// DATA after the stream's end is STREAM_CLOSED (RFC 9113 5.1), and so is any
			// frame on a stream netty forgot; a header block on a stream netty still
			// holds
			// is PROTOCOL_ERROR -- stream 7 is forgotten once its page is out
			assertThat(client.resets()).as("the streams the late frames came on").containsOnlyKeys(3, 5, 7);
			assertThat(client.reset(3)).isEqualTo(STREAM_CLOSED);
			assertThat(client.reset(5)).isEqualTo(PROTOCOL_ERROR);
			assertThat(client.reset(7)).isIn(PROTOCOL_ERROR, STREAM_CLOSED);
			assertThat(client.closed()).isFalse();
			assertThat(client.goAwayCode()).isEqualTo(-1);
		}
		// the relay's CANCEL may follow, the response being unfinished when the late
		// frame came
		assertThat(received).filteredOn(frame -> frame.streamId() == 3 && frame.type() != TestH2.RST_STREAM)
			.extracting(TestH2.Frame::type)
			.containsExactly(TestH2.HEADERS);
		assertThat(received).filteredOn(frame -> frame.streamId() == 5 && frame.type() != TestH2.RST_STREAM)
			.extracting(TestH2.Frame::type)
			.containsExactly(TestH2.HEADERS, TestH2.HEADERS);
		// netty answers the upstream's response on a stream it cancelled with
		// STREAM_CLOSED
		assertThat(received).filteredOn(frame -> frame.type() == TestH2.RST_STREAM)
			.extracting(TestH2::errorCode)
			.containsOnly(CANCEL, STREAM_CLOSED);
		assertThat(received).filteredOn(frame -> frame.streamId() == 7).isEmpty();
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

	/**
	 * Sends its SETTINGS first (the server preface netty's leg requires), records every
	 * frame received and answers every ended request.
	 */
	private static void recordAndAnswer(Socket socket, Queue<TestH2.Frame> received) {
		try (socket) {
			InputStream in = socket.getInputStream();
			OutputStream out = socket.getOutputStream();
			in.readNBytes(TestH2.PREFACE.length);
			TestH2.writeFrame(out, TestH2.SETTINGS, 0, 0, new byte[0]);
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
			AtomicInteger continuations, AtomicInteger grants) {
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
				if (frame.type() == TestH2.WINDOW_UPDATE && heads.get() > 1) {
					grants.incrementAndGet();
				}
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
	 * Announces stream windows of 2^31-1 to the relay: it writes a stub's endless
	 * response to the client within the connection window alone, which the client grants
	 * back on receipt, so the relay keeps granting the stub window.
	 */
	private static void unboundedStreamWindows(H2TestClient client) throws IOException {
		client.frame(TestH2.SETTINGS, 0, 0,
				WindowedH2Upstream.setting(WindowedH2Upstream.SETTINGS_INITIAL_WINDOW_SIZE, Integer.MAX_VALUE));
	}

	/**
	 * The windows the relay granted a stub upstream for its response on stream 1, the
	 * connection's and the stream's: its reader credits the relay's grants, its writer
	 * takes from both before every DATA frame.
	 */
	private static final class SendWindow {

		private long connection = WindowedH2Upstream.DEFAULT_WINDOW;

		private long stream = WindowedH2Upstream.DEFAULT_WINDOW;

		synchronized void credit(TestH2.Frame frame) {
			if (frame.type() != TestH2.WINDOW_UPDATE) {
				return;
			}
			if (frame.streamId() == 0) {
				this.connection += TestH2.increment(frame);
			}
			else if (frame.streamId() == 1) {
				this.stream += TestH2.increment(frame);
			}
			notifyAll();
		}

		synchronized void take(int length) throws InterruptedException {
			while (Math.min(this.connection, this.stream) < length) {
				wait();
			}
			this.connection -= length;
			this.stream -= length;
		}

	}

	/**
	 * The stub's only writer: an endless response on stream 1 at about 16MiB/s within the
	 * windows the relay granted, the queued responses written between its frames.
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
