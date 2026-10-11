package am.ik.sluice.server.proxy;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.jspecify.annotations.Nullable;

import am.ik.sluice.server.route.Router;
import am.ik.sluice.server.tunnel.SessionRegistry;
import am.ik.sluice.server.tunnel.TunnelSession;
import am.ik.sluice.tunnel.DuplexPipe;
import am.ik.sluice.tunnel.StreamRelay;
import am.ik.sluice.tunnel.VirtualConnection;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Relays one plaintext HTTP/2 client connection over per-route legs: the
 * {@code :authority} of every request HEADERS block is resolved through the router and
 * the stream is relayed, with its stream id unchanged, to a virtual connection of the
 * matching route -- concurrent streams of one connection (h2 multiplexing, connection
 * coalescing) each reach their own upstream. Streams sharing a route share the route's
 * leg. Every leg's writes are performed by the leg's own writer thread off the leg's
 * queue -- the outbound parse only enqueues, so one stalled leg never stops the frames of
 * the others.
 * <p>
 * The relay is the HTTP/2 endpoint of the client connection for everything
 * connection-level: settings are answered and re-announced to the legs, pings are
 * answered, and the connection windows are regenerated from the DATA payload relayed over
 * each hop. It also owns the client's stream windows, which bound what it queues: it
 * advertises {@link #MAX_CONCURRENT_STREAMS}, refuses a stream beyond it, and grants a
 * stream's window as far as the stream's queued payload and its window stay within
 * {@link #STREAM_BUDGET} -- the leg writer's drain makes room, padding never takes any. A
 * client sending past a stream window gets a GOAWAY {@code FLOW_CONTROL_ERROR}. A header
 * block is written whole in either direction, and the relay's own connection-level frames
 * towards an upstream are written ahead of the request frames queued for it.
 * <p>
 * Towards each upstream the relay is a sender bound by the upstream's windows (RFC 9113
 * 6.9): the connection window and every stream's window, initially as the upstream's
 * SETTINGS announce (the spec default until they arrive), adjusted when they change, and
 * replenished by its WINDOW_UPDATEs. A stream's DATA is written only as far as both
 * windows allow; a stream without window waits without holding back the leg's other
 * streams, and the stream's trailers wait behind its data while the client's window
 * updates do not. An upstream pushing a window past 2^31-1 ends the relay.
 * <p>
 * The client ending its side of the connection half-closes each leg once the frames
 * queued for it are written and its responses ended -- until then the relay keeps
 * granting the upstream's connection window, and an upstream that closes on reading EOF
 * (as Netty does by default) still delivers them -- unless a routed request is
 * unfinished: that request can never complete, so the relay ends instead, closing the
 * legs. While a leg waits so, the relay writes a PING to the client whenever it wrote
 * nothing for {@link #PROBE_INTERVAL_NANOS}: a client gone entirely rather than
 * half-closed fails the write and ends the relay. An upstream ending its side with no
 * response owed retires its leg: the data still queued for it is dropped, and a stream
 * the client still sends on is reset with {@code NO_ERROR}.
 * <p>
 * HPACK blocks are re-encoded table-less, so each side's decoder sees self-contained
 * blocks whichever leg produced them, or when.
 */
final class Http2DemuxRelay {

	private static final int BUFFER_SIZE = 64 * 1024;

	private static final String METRIC_NAME = "sluice.tunnel.bytes";

	private static final int FRAME_DATA = 0x0;

	private static final int FRAME_HEADERS = 0x1;

	private static final int FRAME_RST_STREAM = 0x3;

	private static final int FRAME_SETTINGS = 0x4;

	private static final int FRAME_PING = 0x6;

	private static final int FRAME_GOAWAY = 0x7;

	private static final int FRAME_WINDOW_UPDATE = 0x8;

	private static final int FRAME_CONTINUATION = 0x9;

	private static final int FLAG_END_STREAM = 0x1;

	private static final int FLAG_ACK = 0x1;

	private static final int FLAG_END_HEADERS = 0x4;

	private static final int FLAG_PADDED = 0x8;

	private static final int FLAG_PRIORITY = 0x20;

	private static final int FRAME_HEADER_LENGTH = 9;

	private static final int ERROR_NO_ERROR = 0x0;

	private static final int ERROR_FLOW_CONTROL = 0x3;

	private static final int ERROR_REFUSED_STREAM = 0x7;

	private static final int SETTINGS_HEADER_TABLE_SIZE = 0x1;

	private static final int SETTINGS_MAX_CONCURRENT_STREAMS = 0x3;

	private static final int SETTINGS_INITIAL_WINDOW_SIZE = 0x4;

	private static final int SETTINGS_MAX_HEADER_LIST_SIZE = 0x6;

	/** The settings defined by RFC 9113, RFC 8441 and RFC 9218, the ones re-announced. */
	private static final Set<Integer> KNOWN_SETTINGS = Set.of(0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x8, 0x9);

	/**
	 * SETTINGS_MAX_FRAME_SIZE every endpoint accepts without announcing more (RFC 9113).
	 */
	private static final int MAX_FRAME_SIZE = 16 * 1024;

	/** The header list size limit of every decoder, mirroring the connection head. */
	private static final int MAX_HEADER_LIST_SIZE = 64 * 1024;

	/** The concurrent streams the relay advertises to the client. */
	static final int MAX_CONCURRENT_STREAMS = 100;

	/**
	 * The initial window of every stream and connection (RFC 9113 6.9.2); the relay never
	 * re-announces it to the client.
	 */
	private static final int STREAM_WINDOW = 65535;

	/** The largest flow-control window (RFC 9113 6.9.1). */
	private static final long MAX_WINDOW = Integer.MAX_VALUE;

	/**
	 * Per stream, the most the queued payload plus the client's window may reach: the
	 * relay grants the client up to it, and the leg writer's drain makes room. 64KiB
	 * above the initial window.
	 */
	static final int STREAM_BUDGET = STREAM_WINDOW + 64 * 1024;

	/** A re-encoded header block with its frame headers, at most. */
	private static final int MAX_BLOCK_BYTES = MAX_HEADER_LIST_SIZE + 5 * FRAME_HEADER_LENGTH;

	/**
	 * The bound the rules above derive for the bytes queued for one connection's legs: a
	 * stream holding queued bytes counts against the stream limit and queues at most its
	 * budget of payload, its head and trailers, a reset, and one coalesced window update.
	 * A backstop: passing it ends the relay with a GOAWAY {@code FLOW_CONTROL_ERROR}.
	 */
	static final long MAX_QUEUED_BYTES = (long) MAX_CONCURRENT_STREAMS
			* (STREAM_BUDGET + FRAME_HEADER_LENGTH + 2L * MAX_BLOCK_BYTES + 2L * (FRAME_HEADER_LENGTH + 4));

	/** The PING acks one leg may owe its upstream; more end the relay. */
	private static final int MAX_PENDING_PING_ACKS = 1024;

	/**
	 * After the client's EOF, while a leg waits for its responses: the most the relay
	 * goes without writing to the client before it probes with a PING.
	 */
	static final long PROBE_INTERVAL_NANOS = java.util.concurrent.TimeUnit.SECONDS.toNanos(1);

	private static final byte[] PROBE_PAYLOAD = "sluice-p".getBytes(StandardCharsets.US_ASCII);

	private static final byte[] EMPTY = new byte[0];

	private final DuplexPipe pipe;

	private final byte[] seedHead;

	private final Router router;

	private final SessionRegistry sessions;

	private final AccessControl accessControl;

	private final ErrorResponse errorResponse;

	private final AccessLogger.Connection access;

	private final MeterRegistry meterRegistry;

	private final H2DemuxMeters meters;

	private final Router.Route initialRoute;

	private final TunnelSession initialSession;

	private final @Nullable InetAddress peer;

	private final Runnable onFinish;

	/** Serializes the writes of the shared client stream. */
	private final Object sinkLock = new Object();

	/**
	 * Guards the stream flow state, the leg queues and the counters below; never held
	 * across a socket write.
	 */
	private final ReentrantLock lock = new ReentrantLock();

	/**
	 * Stream id to its state, until the stream is closed for the client and drained;
	 * frames arriving later find no stream and are dropped.
	 */
	private final Map<Integer, Stream> streams = new ConcurrentHashMap<>();

	private final List<Leg> legs = new CopyOnWriteArrayList<>();

	/** Signals that a leg half-closed or closed; with {@link #lock}. */
	private final Condition legSettled = this.lock.newCondition();

	/** When the relay last wrote to the client, {@link System#nanoTime()}. */
	private volatile long lastClientWrite = System.nanoTime();

	/** The outbound side plus two sides per leg; the relay ends when all drain. */
	private final AtomicInteger pending = new AtomicInteger(1);

	private final AtomicBoolean finished = new AtomicBoolean();

	/** Streams counted against the advertised limit; guarded by {@link #lock}. */
	private int activeStreams;

	/** The bytes queued for every leg of the connection; guarded by {@link #lock}. */
	private long queuedBytes;

	/** The highest stream id routed, for GOAWAY; outbound parse only. */
	private int lastStreamId;

	/** The highest stream id the client opened, routed or not; outbound parse only. */
	private int highestStreamId;

	/** Client bytes read but not yet parsed into complete frames. */
	private final ByteArrayOutputStream buffer = new ByteArrayOutputStream(BUFFER_SIZE);

	/** Assembles a split request head, with the flags of its head frame. */
	private final ByteArrayOutputStream assembling = new ByteArrayOutputStream(256);

	private int assemblingStream = -1;

	private boolean assemblingEndStream;

	/** The settings announced by the client, replayed to every new leg. */
	private final Map<Integer, Long> clientSettings = new LinkedHashMap<>();

	private final io.netty.handler.codec.http2.DefaultHttp2HeadersDecoder headersDecoder = new io.netty.handler.codec.http2.DefaultHttp2HeadersDecoder(
			true);

	private final io.netty.handler.codec.http2.DefaultHttp2HeadersEncoder headersEncoder = newHeadersEncoder();

	private boolean firstHead = true;

	/**
	 * The request-head encoder shared by the re-encode path: it keeps no dynamic table,
	 * so every emitted block is self-contained and decodable by a leg that joins the
	 * connection mid-flight.
	 */
	static io.netty.handler.codec.http2.DefaultHttp2HeadersEncoder newHeadersEncoder() {
		io.netty.handler.codec.http2.DefaultHttp2HeadersEncoder encoder = new io.netty.handler.codec.http2.DefaultHttp2HeadersEncoder();
		try {
			encoder.maxHeaderListSize(MAX_HEADER_LIST_SIZE);
			encoder.maxHeaderTableSize(0);
		}
		catch (io.netty.handler.codec.http2.Http2Exception e) {
			throw new IllegalStateException(e);
		}
		return encoder;
	}

	Http2DemuxRelay(DuplexPipe pipe, ConnectionHeadParser.Head head, Router router, SessionRegistry sessions,
			AccessControl accessControl, ErrorResponse errorResponse, AccessLogger.Connection access,
			H2DemuxMeters meters, Router.Route initialRoute, TunnelSession initialSession, @Nullable InetAddress peer,
			Runnable onFinish) {
		this.pipe = pipe;
		this.seedHead = head.bytes();
		this.router = router;
		this.sessions = sessions;
		this.accessControl = accessControl;
		this.errorResponse = errorResponse;
		this.access = access;
		this.meterRegistry = meters.registry();
		this.meters = meters;
		this.initialRoute = initialRoute;
		this.initialSession = initialSession;
		this.peer = peer;
		this.onFinish = onFinish;
		try {
			this.headersDecoder.configuration().maxHeaderListSize(MAX_HEADER_LIST_SIZE, MAX_HEADER_LIST_SIZE);
		}
		catch (io.netty.handler.codec.http2.Http2Exception e) {
			throw new IllegalStateException(e);
		}
	}

	/**
	 * Starts the outbound side; the leg of the head consumed with the connection is
	 * opened by its event, further legs on demand.
	 */
	void start() {
		Thread.ofVirtual().name("sluice-h2demux-out").start(this::pumpOutbound);
	}

	/**
	 * The outbound side: parses the frames the client sends, resolves the route of every
	 * request head, and relays each stream to the leg of its route.
	 */
	private void pumpOutbound() {
		try {
			// the server face opens with its own settings (RFC 9113 6.5)
			writeToClient(frame(FRAME_SETTINGS, 0, 0, setting(SETTINGS_MAX_CONCURRENT_STREAMS, MAX_CONCURRENT_STREAMS),
					0, 6));
			// the connection head ends with the magic; the frames follow it
			int magic = ConnectionHeadParser.H2_MAGIC.length();
			feed(this.seedHead, magic, this.seedHead.length - magic);
			InputStream in = this.pipe.source();
			byte[] buffer = new byte[BUFFER_SIZE];
			int n;
			while ((n = in.read(buffer)) > 0) {
				feed(buffer, 0, n);
			}
			if (requestUnfinished()) {
				// HTTP/2 has no half-closed connection: a request without its end can
				// never complete, so the client is gone -- and so is the reason to wait
				// on a leg writer parked behind it
				abort();
			}
			else {
				// the client is done sending: no further request on any leg
				for (Leg leg : this.legs) {
					leg.halfClose();
				}
				probeUntilLegsSettled();
			}
		}
		catch (Exception e) {
			abort();
		}
		finally {
			settle();
		}
	}

	/**
	 * Waits until every leg half-closed or closed; meanwhile probes the client with a
	 * PING whenever nothing was written to it for {@link #PROBE_INTERVAL_NANOS}. A client
	 * that half-closed reads on; one gone entirely fails a write (the second at the
	 * latest, once its stack answered the first with a reset), which ends the relay.
	 */
	private void probeUntilLegsSettled() throws InterruptedException {
		while (true) {
			long idle;
			this.lock.lock();
			try {
				if (this.legs.stream().allMatch(Leg::settledLocked)) {
					return;
				}
				idle = System.nanoTime() - this.lastClientWrite;
				if (idle < PROBE_INTERVAL_NANOS) {
					this.legSettled.awaitNanos(PROBE_INTERVAL_NANOS - idle);
				}
			}
			finally {
				this.lock.unlock();
			}
			if (idle >= PROBE_INTERVAL_NANOS) {
				writeToClient(frame(FRAME_PING, 0, 0, PROBE_PAYLOAD, 0, PROBE_PAYLOAD.length));
			}
		}
	}

	/** Whether a routed stream still waits for the rest of its request. */
	private boolean requestUnfinished() {
		this.lock.lock();
		try {
			return this.streams.values().stream().anyMatch(stream -> !stream.clientEnded);
		}
		finally {
			this.lock.unlock();
		}
	}

	private void feed(byte[] bytes, int offset, int length) {
		this.buffer.write(bytes, offset, length);
		byte[] all = this.buffer.toByteArray();
		int position = 0;
		while (all.length - position >= FRAME_HEADER_LENGTH) {
			ByteBuffer header = ByteBuffer.wrap(all, position, FRAME_HEADER_LENGTH);
			int frameLength = unsigned24(header);
			if (all.length - position < FRAME_HEADER_LENGTH + frameLength) {
				break;
			}
			int type = header.get() & 0xff;
			int flags = header.get() & 0xff;
			int streamId = header.getInt() & 0x7fffffff;
			onFrame(new Frame(type, flags, streamId, all, position + FRAME_HEADER_LENGTH, frameLength));
			position += FRAME_HEADER_LENGTH + frameLength;
		}
		if (position > 0) {
			byte[] remainder = new byte[all.length - position];
			System.arraycopy(all, position, remainder, 0, remainder.length);
			this.buffer.reset();
			this.buffer.write(remainder, 0, remainder.length);
		}
	}

	/**
	 * One parsed frame: its header fields and where its payload lies in {@code bytes}.
	 */
	private record Frame(int type, int flags, int streamId, byte[] bytes, int payload, int length) {

		boolean has(int flag) {
			return (this.flags & flag) != 0;
		}

	}

	private void onFrame(Frame frame) {
		switch (frame.type()) {
			case FRAME_SETTINGS -> onSettings(frame);
			case FRAME_HEADERS -> onHeaders(frame, true);
			case FRAME_CONTINUATION -> onHeaders(frame, false);
			case FRAME_DATA -> onData(frame);
			case FRAME_RST_STREAM -> onClientReset(frame);
			case FRAME_WINDOW_UPDATE -> onClientWindowUpdate(frame);
			case FRAME_PING -> {
				if (!frame.has(FLAG_ACK)) {
					writeToClient(frame(FRAME_PING, FLAG_ACK, 0, frame.bytes(), frame.payload(), frame.length()));
				}
			}
			default -> {
				// priority (advisory, deprecated), push promise (never sent by a client),
				// goaway, and unknown types (hop-by-hop extensions) end here
			}
		}
	}

	/**
	 * A client DATA frame: the whole payload, padding included, leaves the connection
	 * window at once and is released; the stream's window is charged with it, and the
	 * bare payload joins the stream's queued data -- the padding, never queued, is
	 * granted back with the budget's room. Frames of streams unknown or already ended are
	 * dropped.
	 */
	private void onData(Frame frame) {
		if (frame.streamId() == 0) {
			return; // DATA is stream-scoped; an unexpected connection-level frame
		}
		if (frame.length() > 0) {
			grantClient(frame.length());
		}
		Stream stream = this.streams.get(Integer.valueOf(frame.streamId()));
		if (stream == null) {
			return; // frames of streams never routed
		}
		int dataLength = dataPayloadLength(frame);
		int dataOffset = frame.payload() + (frame.has(FLAG_PADDED) ? 1 : 0);
		boolean violation = false;
		int release = 0;
		this.lock.lock();
		try {
			if (stream.clientEnded || stream.trailers) {
				return; // the client ended the stream: its late frames are void
			}
			if (frame.length() > stream.clientWindow) {
				violation = true;
			}
			else {
				stream.clientWindow -= frame.length();
				stream.leg.queueData(stream, frame.bytes(), dataOffset, dataLength, frame.has(FLAG_END_STREAM));
				release = releaseLocked(stream);
			}
		}
		finally {
			this.lock.unlock();
		}
		if (violation) {
			goAway(ERROR_FLOW_CONTROL);
			abort();
			return;
		}
		grantStream(stream.id, release);
		checkQueueBound();
	}

	/**
	 * Grants the client stream window as far as the stream's queued payload plus its
	 * window stay within the budget. A stream the client ended gets nothing more.
	 */
	private int releaseLocked(Stream stream) {
		if (stream.clientEnded) {
			return 0;
		}
		long room = STREAM_BUDGET - stream.queuedPayload - stream.clientWindow;
		if (room <= 0) {
			return 0;
		}
		stream.clientWindow += room;
		return (int) room;
	}

	private void grantStream(int streamId, int increment) {
		if (increment > 0) {
			writeToClient(windowUpdate(streamId, increment));
		}
	}

	/** A client RST_STREAM: relayed once; the stream's pending data is dropped. */
	private void onClientReset(Frame frame) {
		Stream stream = this.streams.get(Integer.valueOf(frame.streamId()));
		if (stream == null) {
			return;
		}
		this.lock.lock();
		try {
			if (stream.reset) {
				return;
			}
			stream.reset = true;
			stream.clientEnded = true;
			stream.leg.purgeData(stream);
			stream.leg.queueBlock(stream, frame(FRAME_RST_STREAM, frame.flags(), frame.streamId(), frame.bytes(),
					frame.payload(), frame.length()), false);
			updateActiveLocked(stream);
		}
		finally {
			this.lock.unlock();
		}
		stream.leg.streamClosed(frame.streamId());
		checkQueueBound();
	}

	/**
	 * A client stream-level WINDOW_UPDATE: relayed to the stream's leg, coalesced into
	 * the one still queued for the stream if any. Connection-level grants are regenerated
	 * from the bytes relayed.
	 */
	private void onClientWindowUpdate(Frame frame) {
		if (frame.streamId() == 0 || frame.length() != 4) {
			return;
		}
		Stream stream = this.streams.get(Integer.valueOf(frame.streamId()));
		int increment = int31(frame.bytes(), frame.payload());
		if (stream == null || increment == 0) {
			return;
		}
		this.lock.lock();
		try {
			if (!stream.reset) {
				stream.leg.queueWindowUpdate(stream, increment);
			}
		}
		finally {
			this.lock.unlock();
		}
		checkQueueBound();
	}

	private void onSettings(Frame frame) {
		if (frame.has(FLAG_ACK)) {
			return; // the ack of the settings announced towards the client
		}
		Map<Integer, Long> changes = new LinkedHashMap<>();
		for (int i = 0; i + 6 <= frame.length(); i += 6) {
			int at = frame.payload() + i;
			int id = (frame.bytes()[at] & 0xff) << 8 | (frame.bytes()[at + 1] & 0xff);
			long value = (frame.bytes()[at + 2] & 0xffL) << 24 | (frame.bytes()[at + 3] & 0xff) << 16
					| (frame.bytes()[at + 4] & 0xff) << 8 | (frame.bytes()[at + 5] & 0xff);
			if (id == SETTINGS_HEADER_TABLE_SIZE || !KNOWN_SETTINGS.contains(id)) {
				// the upstreams encode for the relay's decoders, which keep the default
				// table; unknown settings belong to this hop
				continue;
			}
			if (id == SETTINGS_MAX_HEADER_LIST_SIZE) {
				value = Math.min(value, MAX_HEADER_LIST_SIZE);
			}
			changes.put(id, value);
		}
		if (!changes.isEmpty()) {
			this.clientSettings.putAll(changes);
			// re-announce the peer's settings to the legs already serving the connection
			for (Leg leg : this.legs) {
				leg.queueSettings(changes);
			}
		}
		writeToClient(frame(FRAME_SETTINGS, FLAG_ACK, 0, null, 0, 0));
	}

	/**
	 * The head frame of a possibly split HEADERS; streams already routed receive their
	 * block re-encoded (trailers), new streams are routed by the decoded
	 * {@code :authority}.
	 */
	private void onHeaders(Frame frame, boolean head) {
		int streamId = frame.streamId();
		if (streamId == 0) {
			return;
		}
		if (this.assemblingStream != -1 && streamId != this.assemblingStream) {
			return; // a head is already being assembled; heads never interleave
		}
		if (this.assemblingStream == -1) {
			this.assemblingStream = streamId;
			this.assemblingEndStream = frame.has(FLAG_END_STREAM);
			this.assembling.reset();
		}
		appendFragmentTo(this.assembling, frame, head);
		if (frame.has(FLAG_END_HEADERS)) {
			onHeadBlock();
		}
	}

	private static void appendFragmentTo(ByteArrayOutputStream out, Frame frame, boolean headers) {
		int start = frame.payload();
		int fragmentLength = frame.length();
		if (frame.has(FLAG_PADDED) && frame.length() > 0) {
			int padLength = frame.bytes()[start] & 0xff;
			start += 1;
			fragmentLength -= 1 + padLength;
		}
		if (frame.has(FLAG_PRIORITY) && headers && fragmentLength > 0) {
			start += 5;
			fragmentLength -= 5;
		}
		if (fragmentLength > 0) {
			out.write(frame.bytes(), start, fragmentLength);
		}
	}

	private void onHeadBlock() {
		int streamId = this.assemblingStream;
		boolean endStream = this.assemblingEndStream;
		byte[] block = this.assembling.toByteArray();
		this.assemblingStream = -1;
		// decoded even when dropped, to keep the connection decoder in sync
		io.netty.handler.codec.http2.Http2Headers headers = decode(streamId, block);
		Stream stream = this.streams.get(Integer.valueOf(streamId));
		if (stream != null) {
			onTrailers(stream, endStream, headers);
			return;
		}
		if (streamId <= this.highestStreamId) {
			return; // a later block of a stream refused or answered by the relay
		}
		this.highestStreamId = streamId;
		onRequestHead(streamId, endStream, headers);
	}

	/**
	 * Trailers of a started stream, re-encoded so the leg's decoder needs no earlier
	 * state; one block per stream after its head, none once the client ended it.
	 */
	private void onTrailers(Stream stream, boolean endStream, io.netty.handler.codec.http2.Http2Headers headers) {
		byte[] frames = encodedHead(stream.id, endStream, headers, null, this.headersEncoder);
		this.lock.lock();
		try {
			if (stream.clientEnded || stream.trailers) {
				return;
			}
			stream.trailers = true;
			stream.clientEnded = endStream;
			stream.leg.queueBlock(stream, frames, true);
		}
		finally {
			this.lock.unlock();
		}
		checkQueueBound();
	}

	private void onRequestHead(int streamId, boolean endStream, io.netty.handler.codec.http2.Http2Headers headers) {
		if (activeStreams() >= MAX_CONCURRENT_STREAMS) {
			// beyond the advertised limit (RFC 9113 5.1.2); safe for the client to retry
			writeToClient(rstStream(streamId, ERROR_REFUSED_STREAM));
			return;
		}
		String host = hostOf(headers);
		ConnectionHeadParser.Head.@Nullable Request request = requestOf(headers);
		Resolved resolved;
		if (this.firstHead) {
			// resolved with the connection; the lookup must not run twice (it advances
			// the load balancing state)
			this.firstHead = false;
			resolved = new Resolved(this.initialRoute, this.initialSession);
		}
		else {
			resolved = resolveOrRespond(streamId, host, request, headers);
			if (resolved == null) {
				return; // the error response was written on the stream
			}
		}
		Router.Route route = resolved.route();
		Leg leg = legFor(route, resolved.session());
		Stream stream = new Stream(streamId, leg);
		// always re-encoded: the client's blocks reference its dynamic table, whose
		// state a leg joining later (tunnel recovery, a second upstream of a coalesced
		// connection) does not have -- the table-less encoder keeps every block
		// self-contained no matter which leg decodes it or when
		byte[] frames = encodedHead(streamId, endStream, headers, route.rewriteHost() ? route.address() : null,
				this.headersEncoder);
		boolean admitted;
		this.lock.lock();
		try {
			// a leg its upstream retired meanwhile takes no new stream
			admitted = leg.acceptingLocked();
			if (admitted) {
				this.streams.put(Integer.valueOf(streamId), stream);
				leg.routeMeters.trackedStreams().increment();
				this.activeStreams++;
				stream.active = true;
				stream.clientEnded = endStream;
				stream.upstreamWindow = leg.initialWindow;
				leg.streamOpened(streamId);
				leg.queueBlock(stream, frames, false);
			}
		}
		finally {
			this.lock.unlock();
		}
		if (!admitted) {
			writeToClient(rstStream(streamId, ERROR_REFUSED_STREAM));
			return;
		}
		this.lastStreamId = Math.max(this.lastStreamId, streamId);
		if (request != null) {
			this.access.request(route.routeTag(), request.method(), request.path(), "2");
		}
		checkQueueBound();
	}

	private int activeStreams() {
		this.lock.lock();
		try {
			return this.activeStreams;
		}
		finally {
			this.lock.unlock();
		}
	}

	/**
	 * Uncounts a stream from the advertised limit once it is closed for the client and
	 * nothing of it is queued any more.
	 */
	private void updateActiveLocked(Stream stream) {
		if (!stream.active) {
			return;
		}
		boolean closed = stream.reset || (stream.clientEnded && stream.responseEnded);
		if (closed && stream.queuedBytes == 0) {
			stream.active = false;
			this.activeStreams--;
			if (this.streams.remove(Integer.valueOf(stream.id), stream)) {
				stream.leg.routeMeters.trackedStreams().decrement();
			}
		}
	}

	/** Ends the relay like a flow-control violation once the queue passed its bound. */
	private void checkQueueBound() {
		boolean exceeded;
		this.lock.lock();
		try {
			exceeded = this.queuedBytes > MAX_QUEUED_BYTES;
		}
		finally {
			this.lock.unlock();
		}
		if (exceeded) {
			goAway(ERROR_FLOW_CONTROL);
			abort();
		}
	}

	private io.netty.handler.codec.http2.Http2Headers decode(int streamId, byte[] block) {
		try {
			return this.headersDecoder.decodeHeaders(streamId, io.netty.buffer.Unpooled.wrappedBuffer(block));
		}
		catch (Exception e) {
			// an undecodable block carries no routable authority
			return new io.netty.handler.codec.http2.DefaultHttp2Headers();
		}
	}

	private static @Nullable String hostOf(io.netty.handler.codec.http2.@Nullable Http2Headers headers) {
		if (headers == null) {
			return null;
		}
		CharSequence authority = headers.authority();
		if (authority != null && !authority.isEmpty()) {
			return authority.toString();
		}
		CharSequence host = headers.get(io.netty.util.AsciiString.cached("host"));
		return host == null || host.isEmpty() ? null : host.toString();
	}

	private static ConnectionHeadParser.Head.@Nullable Request requestOf(
			io.netty.handler.codec.http2.@Nullable Http2Headers headers) {
		if (headers == null) {
			return null;
		}
		CharSequence method = headers.method();
		CharSequence path = headers.path();
		if (method == null || path == null) {
			return null;
		}
		return new ConnectionHeadParser.Head.Request(method.toString(), path.toString(), "2");
	}

	private static @Nullable String headerOf(io.netty.handler.codec.http2.Http2Headers headers, String name) {
		CharSequence value = headers.get(io.netty.util.AsciiString.of(name));
		return value == null || value.isEmpty() ? null : value.toString();
	}

	private record Resolved(Router.Route route, TunnelSession session) {
	}

	/**
	 * Resolves the route of a request head; on any refusal the request is logged, an
	 * error response is written on the stream, and {@code null} returned, leaving the
	 * connection alive.
	 */
	private @Nullable Resolved resolveOrRespond(int streamId, @Nullable String host,
			ConnectionHeadParser.Head.@Nullable Request request, io.netty.handler.codec.http2.Http2Headers headers) {
		Optional<Router.Route> route = this.router.lookup(host);
		if (route.isEmpty()) {
			return this.refuse(request, this.errorResponse.noRouteStream(streamId, host, request));
		}
		TunnelSession session = this.sessions.find(route.get().clientId()).orElse(null);
		if (session == null) {
			return this.refuse(request, this.errorResponse.noRouteStream(streamId, host, request));
		}
		String forwardedFor = headerOf(headers, ConnectionHeadParser.Head.FORWARDED_FOR);
		String forwarded = headerOf(headers, ConnectionHeadParser.Head.FORWARDED);
		if (!this.accessControl.allowed(route.get(), this.peer, forwardedFor, forwarded)) {
			return this.refuse(request, this.errorResponse.forbiddenStream(streamId, host, request));
		}
		return new Resolved(route.get(), session);
	}

	/** Logs a refused stream, mirroring the connection-level refusals. */
	private @Nullable Resolved refuse(ConnectionHeadParser.Head.@Nullable Request request, byte[] response) {
		this.access.request("-", request == null ? "-" : request.method(), request == null ? "-" : request.path(), "2");
		writeToClient(response);
		return null;
	}

	/** The leg relaying the route: the alive leg serving it, or a new one. */
	private Leg legFor(Router.Route route, TunnelSession session) {
		for (Leg leg : this.legs) {
			if (!leg.ended && leg.route.equals(route)) {
				return leg;
			}
		}
		return new Leg(route, session);
	}

	/**
	 * The head re-encoded with the given table-less encoder (see
	 * {@link #newHeadersEncoder()}), the authority replaced when {@code address} is
	 * given, as HEADERS plus CONTINUATION frames of the default maximum frame size.
	 */
	private static byte[] encodedHead(int streamId, boolean endStream,
			io.netty.handler.codec.http2.Http2Headers headers, java.lang.@Nullable String address,
			io.netty.handler.codec.http2.DefaultHttp2HeadersEncoder encoder) {
		if (address != null) {
			headers.authority(io.netty.util.AsciiString.of(address));
		}
		io.netty.buffer.ByteBuf encoded = io.netty.buffer.Unpooled.buffer(256);
		byte[] block;
		try {
			encoder.encodeHeaders(streamId, headers, encoded);
			block = new byte[encoded.readableBytes()];
			encoded.readBytes(block);
		}
		catch (io.netty.handler.codec.http2.Http2Exception e) {
			// unreachable for well-formed heads; the empty block keeps the stream alive
			block = new byte[0];
		}
		finally {
			encoded.release();
		}
		return blockFrames(streamId, endStream, block);
	}

	/** Splits an encoded header block to HEADERS plus CONTINUATION frames. */
	private static byte[] blockFrames(int streamId, boolean endStream, byte[] block) {
		ByteArrayOutputStream frames = new ByteArrayOutputStream(block.length + 2 * FRAME_HEADER_LENGTH);
		int offset = 0;
		boolean first = true;
		do {
			int length = Math.min(MAX_FRAME_SIZE, block.length - offset);
			boolean last = offset + length >= block.length;
			int type = first ? FRAME_HEADERS : FRAME_CONTINUATION;
			int flags = first && endStream ? FLAG_END_STREAM : 0;
			if (last) {
				flags |= FLAG_END_HEADERS;
			}
			frames.writeBytes(frame(type, flags, streamId, block, offset, length));
			first = false;
			offset += length;
		}
		while (offset < block.length);
		return frames.toByteArray();
	}

	private static byte[] frame(int type, int flags, int streamId, byte @Nullable [] bytes, int payload, int length) {
		byte[] frame = new byte[FRAME_HEADER_LENGTH + Math.max(0, length)];
		frame[0] = (byte) (length >>> 16 & 0xff);
		frame[1] = (byte) (length >>> 8 & 0xff);
		frame[2] = (byte) (length & 0xff);
		frame[3] = (byte) type;
		frame[4] = (byte) flags;
		frame[5] = (byte) (streamId >>> 24 & 0x7f);
		frame[6] = (byte) (streamId >>> 16 & 0xff);
		frame[7] = (byte) (streamId >>> 8 & 0xff);
		frame[8] = (byte) (streamId & 0xff);
		if (length > 0 && bytes != null) {
			System.arraycopy(bytes, payload, frame, FRAME_HEADER_LENGTH, length);
		}
		return frame;
	}

	private static byte[] setting(int id, long value) {
		return new byte[] { (byte) (id >>> 8), (byte) id, (byte) (value >>> 24), (byte) (value >>> 16),
				(byte) (value >>> 8), (byte) value };
	}

	private static int dataPayloadLength(Frame frame) {
		int dataLength = frame.length();
		if (frame.has(FLAG_PADDED) && frame.length() > 0) {
			int padLength = frame.bytes()[frame.payload()] & 0xff;
			dataLength -= 1 + padLength;
		}
		return Math.max(0, dataLength);
	}

	/**
	 * Grants the client connection window; called as DATA leaves towards the legs.
	 */
	private void grantClient(int units) {
		writeToClient(windowUpdate(0, units));
	}

	private static byte[] windowUpdate(int streamId, int units) {
		return frame(FRAME_WINDOW_UPDATE, 0, streamId, int32(units & 0x7fffffff), 0, 4);
	}

	private static byte[] rstStream(int streamId, int errorCode) {
		return frame(FRAME_RST_STREAM, 0, streamId, int32(errorCode), 0, 4);
	}

	private static byte[] int32(int value) {
		return new byte[] { (byte) (value >>> 24), (byte) (value >>> 16), (byte) (value >>> 8), (byte) value };
	}

	private static int int31(byte[] bytes, int offset) {
		return (bytes[offset] & 0x7f) << 24 | (bytes[offset + 1] & 0xff) << 16 | (bytes[offset + 2] & 0xff) << 8
				| (bytes[offset + 3] & 0xff);
	}

	/** Tells the client the connection ends with the error; best effort. */
	private void goAway(int errorCode) {
		byte[] payload = new byte[8];
		System.arraycopy(int32(this.lastStreamId), 0, payload, 0, 4);
		System.arraycopy(int32(errorCode), 0, payload, 4, 4);
		try {
			writeToClient(frame(FRAME_GOAWAY, 0, 0, payload, 0, payload.length));
		}
		catch (RuntimeException e) {
			// the client is gone already
		}
	}

	private void writeToClient(byte[] bytes) {
		OutputStream sink = this.pipe.sink();
		synchronized (this.sinkLock) {
			try {
				sink.write(bytes);
				sink.flush();
			}
			catch (Exception e) {
				throw new IllegalStateException(e);
			}
		}
		this.lastClientWrite = System.nanoTime();
	}

	/**
	 * Relays response bytes to the client in one write and accounts them to the leg and
	 * the access log; the single reader per leg keeps the writes in stream order.
	 */
	private void writeToClient(byte[] bytes, int offset, int length, Leg leg) {
		synchronized (this.sinkLock) {
			try {
				this.pipe.sink().write(bytes, offset, length);
				this.pipe.sink().flush();
			}
			catch (Exception e) {
				throw new IllegalStateException(e);
			}
		}
		this.lastClientWrite = System.nanoTime();
		leg.outbound.increment(length);
		leg.session.recordRelayed(length, StreamRelay.Direction.TO_LOCAL);
		this.access.bytes(length, StreamRelay.Direction.TO_LOCAL);
	}

	/**
	 * The client-facing state of one routed stream; guarded by {@link #lock}.
	 */
	private static final class Stream {

		final int id;

		final Leg leg;

		/** The client's send window, as the relay granted it. */
		long clientWindow = STREAM_WINDOW;

		/**
		 * The relay's send window towards the upstream, as the upstream granted it; may
		 * be negative after the upstream lowered its initial window (RFC 9113 6.9.2).
		 */
		long upstreamWindow;

		/** DATA payload of the stream queued or being written. */
		int queuedPayload;

		/** Every byte queued for the stream, frame headers of its entries included. */
		long queuedBytes;

		/** Payload not yet written by the leg writer. */
		final ByteArrayOutputStream pendingData = new ByteArrayOutputStream(0);

		boolean pendingEnd;

		/** A {@link LegEntry.Data} of the stream waits in its leg's queue. */
		boolean dataQueued;

		/** The window update of the stream still queued, to coalesce into. */
		LegEntry.@Nullable WindowUpdate windowUpdate;

		/** The client sent END_STREAM or a reset, or the upstream reset the stream. */
		boolean clientEnded;

		/** A second header block (trailers) was relayed. */
		boolean trailers;

		/** Reset by either side. */
		boolean reset;

		/** The response ended (END_STREAM written to the client). */
		boolean responseEnded;

		/** Counted against the advertised limit. */
		boolean active;

		Stream(int id, Leg leg) {
			this.id = id;
			this.leg = leg;
		}

	}

	/**
	 * A leg: the virtual connection of one route, carrying every stream routed to it,
	 * alive until the relay ends or its upstream goes away.
	 */
	private final class Leg {

		final Router.Route route;

		final TunnelSession session;

		final VirtualConnection connection;

		private final Counter inbound;

		private final Counter outbound;

		private final H2DemuxMeters.Route routeMeters;

		/**
		 * Mirrors the upstream's HPACK encoder for the blocks relayed towards the client;
		 * the responses are re-encoded table-less so the client's single decoder is
		 * untouched by whichever leg produced the block, or when.
		 */
		private final io.netty.handler.codec.http2.DefaultHttp2HeadersDecoder responseDecoder = new io.netty.handler.codec.http2.DefaultHttp2HeadersDecoder(
				true, MAX_HEADER_LIST_SIZE);

		private final io.netty.handler.codec.http2.DefaultHttp2HeadersEncoder responseEncoder = Http2DemuxRelay
			.newHeadersEncoder();

		/** Response head blocks being assembled, keyed by stream. */
		private final Map<Integer, ByteArrayOutputStream> inboundHeads = new HashMap<>();

		private final Map<Integer, Boolean> inboundHeadEndStream = new HashMap<>();

		/**
		 * The client role's opening towards the upstream: the preface and the client's
		 * settings, so the upstream's encoder and flow control mirror the client's.
		 */
		private final byte[] preface;

		/** Signals the writer; with {@link #lock}. */
		private final Condition wake = Http2DemuxRelay.this.lock.newCondition();

		/** Relayed client frames, in relay order; guarded by {@link #lock}. */
		private final ArrayDeque<LegEntry> entries = new ArrayDeque<>();

		/** The bytes of {@link #entries}; guarded by {@link #lock}. */
		private long queuedBytes;

		/** The priority lane, written ahead of {@link #entries}; guarded by the lock. */
		private int settingsAcks;

		private final ArrayDeque<byte[]> pingAcks = new ArrayDeque<>();

		private long connectionGrant;

		private final Map<Integer, Long> settings = new LinkedHashMap<>();

		/** The relay's connection send window towards the upstream; with the lock. */
		private long sendWindow = STREAM_WINDOW;

		/** The upstream's SETTINGS_INITIAL_WINDOW_SIZE; guarded by the lock. */
		private long initialWindow = STREAM_WINDOW;

		/** The streams relayed whose response has not ended. */
		private final Set<Integer> openStreams = ConcurrentHashMap.newKeySet();

		/** The leg takes no new stream: its upstream went away, or it closed. */
		private volatile boolean ended;

		/** Set by {@link #close()}; guarded by {@link #lock}. */
		private boolean closed;

		/** The client is done sending: half-close once nothing is owed; with the lock. */
		private boolean halfCloseRequested;

		/** The writer half-closed the upstream and exited; guarded by {@link #lock}. */
		private boolean halfClosed;

		Leg(Router.Route route, TunnelSession session) {
			this.route = route;
			this.session = session;
			this.connection = session.open(route.address());
			this.inbound = Http2DemuxRelay.this.meterRegistry.counter(METRIC_NAME, "direction", "inbound", "route",
					route.routeTag());
			this.outbound = Http2DemuxRelay.this.meterRegistry.counter(METRIC_NAME, "direction", "outbound", "route",
					route.routeTag());
			this.routeMeters = Http2DemuxRelay.this.meters.route(route.routeTag());
			ByteArrayOutputStream preface = new ByteArrayOutputStream(64);
			preface.writeBytes(ConnectionHeadParser.H2_MAGIC.getBytes(StandardCharsets.US_ASCII));
			preface.writeBytes(frame(FRAME_SETTINGS, 0, 0, null, 0, 0));
			if (!Http2DemuxRelay.this.clientSettings.isEmpty()) {
				preface.writeBytes(settingsFrame(Http2DemuxRelay.this.clientSettings));
			}
			this.preface = preface.toByteArray();
			if (Http2DemuxRelay.this.legs.isEmpty()) {
				Http2DemuxRelay.this.access.connectionId(this.connection.connectionId());
			}
			Http2DemuxRelay.this.legs.add(this);
			Http2DemuxRelay.this.pending.addAndGet(2);
			Thread.ofVirtual()
				.name("sluice-h2demux-leg-out-" + this.connection.connectionId())
				.start(() -> pumpLegOutbound(this));
			Thread.ofVirtual()
				.name("sluice-h2demux-in-" + this.connection.connectionId())
				.start(() -> pumpInbound(this));
		}

		/** Whether the writer still takes entries; with {@link #lock}. */
		private boolean acceptingLocked() {
			return !this.closed && !this.halfClosed;
		}

		/** Whether the leg half-closed or closed; with {@link #lock}. */
		private boolean settledLocked() {
			return this.closed || this.halfClosed;
		}

		private void addQueuedLocked(@Nullable Stream stream, long bytes) {
			this.queuedBytes += bytes;
			Http2DemuxRelay.this.queuedBytes += bytes;
			this.routeMeters.queuedBytes().addAndGet(bytes);
			if (stream != null) {
				stream.queuedBytes += bytes;
			}
		}

		private void signalLocked() {
			this.wake.signal();
		}

		/**
		 * Queues a relayed block -- a whole header block, or a reset; with the lock.
		 * {@code followsData}: written only after the stream's data queued before it
		 * (trailers).
		 */
		void queueBlock(Stream stream, byte[] bytes, boolean followsData) {
			if (!acceptingLocked()) {
				return;
			}
			this.entries.addLast(new LegEntry.Block(stream, bytes, followsData));
			addQueuedLocked(stream, bytes.length);
			signalLocked();
		}

		/**
		 * Appends DATA payload to the stream's pending data, queueing an entry for it
		 * unless one still waits; with the lock.
		 */
		void queueData(Stream stream, byte[] bytes, int offset, int length, boolean endStream) {
			if (!acceptingLocked()) {
				return;
			}
			if (length > 0) {
				stream.pendingData.write(bytes, offset, length);
				stream.queuedPayload += length;
				addQueuedLocked(stream, length);
			}
			if (endStream) {
				stream.pendingEnd = true;
				stream.clientEnded = true;
			}
			if ((length > 0 || endStream) && !stream.dataQueued) {
				stream.dataQueued = true;
				this.entries.addLast(new LegEntry.Data(stream));
				addQueuedLocked(stream, FRAME_HEADER_LENGTH);
				signalLocked();
			}
		}

		/**
		 * Drops the stream's payload not yet taken by the writer; with the lock. Its data
		 * entry, now empty, no longer waits for window: the writer is woken to drop it.
		 */
		void purgeData(Stream stream) {
			int pending = stream.pendingData.size();
			if (pending > 0) {
				stream.pendingData.reset();
				stream.queuedPayload -= pending;
				addQueuedLocked(stream, -pending);
			}
			stream.pendingEnd = false;
			signalLocked();
		}

		/** Queues a stream window update, coalescing into a queued one; with the lock. */
		void queueWindowUpdate(Stream stream, int increment) {
			if (!acceptingLocked()) {
				return;
			}
			LegEntry.WindowUpdate queued = stream.windowUpdate;
			if (queued != null) {
				queued.increment = (int) Math.min(Integer.MAX_VALUE, (long) queued.increment + increment);
				return;
			}
			LegEntry.WindowUpdate update = new LegEntry.WindowUpdate(stream, increment);
			stream.windowUpdate = update;
			this.entries.addLast(update);
			addQueuedLocked(stream, FRAME_HEADER_LENGTH + 4);
			signalLocked();
		}

		/** Re-announces client settings to the upstream, in the priority lane. */
		void queueSettings(Map<Integer, Long> changes) {
			Http2DemuxRelay.this.lock.lock();
			try {
				if (acceptingLocked()) {
					this.settings.putAll(changes);
					signalLocked();
				}
			}
			finally {
				Http2DemuxRelay.this.lock.unlock();
			}
		}

		/** Acks an upstream SETTINGS, in the priority lane. */
		void queueSettingsAck() {
			Http2DemuxRelay.this.lock.lock();
			try {
				if (acceptingLocked()) {
					this.settingsAcks++;
					signalLocked();
				}
			}
			finally {
				Http2DemuxRelay.this.lock.unlock();
			}
		}

		/**
		 * Acks an upstream PING, in the priority lane; {@code false} when the upstream
		 * already waits for too many acks.
		 */
		boolean queuePingAck(byte[] payload) {
			Http2DemuxRelay.this.lock.lock();
			try {
				if (!acceptingLocked()) {
					return true;
				}
				if (this.pingAcks.size() >= MAX_PENDING_PING_ACKS) {
					return false;
				}
				this.pingAcks.addLast(payload);
				signalLocked();
				return true;
			}
			finally {
				Http2DemuxRelay.this.lock.unlock();
			}
		}

		/**
		 * Releases the upstream's connection window for DATA payload relayed to the
		 * client, in the priority lane.
		 */
		void queueConnectionGrant(int units) {
			Http2DemuxRelay.this.lock.lock();
			try {
				if (acceptingLocked()) {
					this.connectionGrant = Math.min(Integer.MAX_VALUE, this.connectionGrant + units);
					signalLocked();
				}
			}
			finally {
				Http2DemuxRelay.this.lock.unlock();
			}
		}

		/**
		 * Half-closes the upstream once the queued frames are written and no response is
		 * owed; until then the priority lane keeps flowing.
		 */
		void halfClose() {
			Http2DemuxRelay.this.lock.lock();
			try {
				if (acceptingLocked()) {
					this.halfCloseRequested = true;
					signalLocked();
				}
			}
			finally {
				Http2DemuxRelay.this.lock.unlock();
			}
		}

		/**
		 * Applies the upstream's SETTINGS (RFC 9113 6.5.3, 6.9.2): a changed initial
		 * window shifts the window of every stream of the leg by the difference.
		 * {@code false} when a window would pass 2^31-1, a connection error.
		 */
		boolean applySettings(byte[] bytes, int payload, int length) {
			Http2DemuxRelay.this.lock.lock();
			try {
				for (int i = 0; i + 6 <= length; i += 6) {
					int at = payload + i;
					int id = (bytes[at] & 0xff) << 8 | (bytes[at + 1] & 0xff);
					if (id != SETTINGS_INITIAL_WINDOW_SIZE) {
						continue;
					}
					long value = (bytes[at + 2] & 0xffL) << 24 | (bytes[at + 3] & 0xff) << 16
							| (bytes[at + 4] & 0xff) << 8 | (bytes[at + 5] & 0xff);
					if (value > MAX_WINDOW) {
						return false;
					}
					long delta = value - this.initialWindow;
					this.initialWindow = value;
					for (Stream stream : Http2DemuxRelay.this.streams.values()) {
						if (stream.leg == this && stream.active) {
							stream.upstreamWindow += delta;
							if (stream.upstreamWindow > MAX_WINDOW) {
								return false;
							}
						}
					}
				}
				signalLocked();
				return true;
			}
			finally {
				Http2DemuxRelay.this.lock.unlock();
			}
		}

		/**
		 * The upstream's connection-level WINDOW_UPDATE; {@code false} when the window
		 * would pass 2^31-1.
		 */
		boolean creditConnection(int increment) {
			Http2DemuxRelay.this.lock.lock();
			try {
				this.sendWindow += increment;
				if (this.sendWindow > MAX_WINDOW) {
					return false;
				}
				signalLocked();
				return true;
			}
			finally {
				Http2DemuxRelay.this.lock.unlock();
			}
		}

		/**
		 * The upstream's WINDOW_UPDATE of a stream of the leg; {@code false} when the
		 * window would pass 2^31-1. Updates of streams the relay forgot are void.
		 */
		boolean creditStream(Stream stream, int increment) {
			Http2DemuxRelay.this.lock.lock();
			try {
				if (!stream.active) {
					return true;
				}
				stream.upstreamWindow += increment;
				if (stream.upstreamWindow > MAX_WINDOW) {
					return false;
				}
				signalLocked();
				return true;
			}
			finally {
				Http2DemuxRelay.this.lock.unlock();
			}
		}

		/**
		 * The writer's next batch: the priority lane, then the first entry ready to go;
		 * once the client is done and nothing is owed, the half-close. Blocks while there
		 * is none, {@code null} once the leg is closed.
		 */
		@Nullable Batch take() throws InterruptedException {
			Http2DemuxRelay.this.lock.lock();
			try {
				while (true) {
					if (this.closed) {
						return null;
					}
					byte[] control = drainControlLocked();
					Batch batch = nextEntryLocked(control);
					if (batch != null) {
						return batch;
					}
					if (this.halfCloseRequested && this.entries.isEmpty() && this.openStreams.isEmpty()) {
						this.halfClosed = true;
						Http2DemuxRelay.this.legSettled.signalAll();
						return new Batch(control, EMPTY, null, 0, 0, true);
					}
					if (control.length > 0) {
						return new Batch(control, EMPTY, null, 0, 0, false);
					}
					this.wake.await();
					this.routeMeters.writerWakeups().increment();
				}
			}
			finally {
				Http2DemuxRelay.this.lock.unlock();
			}
		}

		private byte[] drainControlLocked() {
			if (this.settings.isEmpty() && this.settingsAcks == 0 && this.pingAcks.isEmpty()
					&& this.connectionGrant == 0) {
				return EMPTY;
			}
			ByteArrayOutputStream out = new ByteArrayOutputStream(64);
			if (!this.settings.isEmpty()) {
				out.writeBytes(settingsFrame(this.settings));
				this.settings.clear();
			}
			for (; this.settingsAcks > 0; this.settingsAcks--) {
				out.writeBytes(frame(FRAME_SETTINGS, FLAG_ACK, 0, null, 0, 0));
			}
			for (byte[] ping = this.pingAcks.pollFirst(); ping != null; ping = this.pingAcks.pollFirst()) {
				out.writeBytes(frame(FRAME_PING, FLAG_ACK, 0, ping, 0, ping.length));
			}
			if (this.connectionGrant > 0) {
				out.writeBytes(windowUpdate(0, (int) this.connectionGrant));
				this.connectionGrant = 0;
			}
			return out.toByteArray();
		}

		/**
		 * Takes the first queued entry ready to go, in queue order: a stream's data as
		 * far as the upstream's windows allow -- what is left goes to the back of the
		 * queue, so the streams take turns -- and a stream's trailers only once its data
		 * is out. Header blocks, resets and window updates never wait for window (RFC
		 * 9113 5.2.1): a client's window update overtakes its stream's waiting data,
		 * which the upstream may only accept once it could send.
		 */
		private @Nullable Batch nextEntryLocked(byte[] control) {
			Iterator<LegEntry> iterator = this.entries.iterator();
			while (iterator.hasNext()) {
				LegEntry entry = iterator.next();
				switch (entry) {
					case LegEntry.Block block -> {
						if (block.followsData() && block.stream().dataQueued) {
							continue;
						}
						iterator.remove();
						return new Batch(control, block.bytes(), block.stream(), 0, block.bytes().length, false);
					}
					case LegEntry.WindowUpdate update -> {
						iterator.remove();
						update.stream().windowUpdate = null;
						return new Batch(control, windowUpdate(update.stream().id, update.increment), update.stream(),
								0, FRAME_HEADER_LENGTH + 4, false);
					}
					case LegEntry.Data data -> {
						Batch batch = dataBatchLocked(data.stream(), control);
						if (batch == null) {
							continue;
						}
						iterator.remove();
						if (data.stream().dataQueued) {
							this.entries.addLast(data);
						}
						return batch;
					}
				}
			}
			return null;
		}

		/**
		 * The stream's pending payload as DATA frames, as far as the stream's and the
		 * connection's upstream windows allow; {@code null} while they allow none. An
		 * empty END_STREAM needs no window (RFC 9113 6.9.1); a payload a reset purged
		 * yields no frame.
		 */
		private @Nullable Batch dataBatchLocked(Stream stream, byte[] control) {
			int pending = stream.pendingData.size();
			int length = pending;
			if (pending > 0) {
				long allowed = Math.min(stream.upstreamWindow, this.sendWindow);
				if (allowed <= 0) {
					return null;
				}
				length = (int) Math.min(pending, allowed);
			}
			byte[] all = stream.pendingData.toByteArray();
			byte[] payload = all;
			if (length < pending) {
				payload = Arrays.copyOf(all, length);
				stream.pendingData.reset();
				stream.pendingData.write(all, length, pending - length);
			}
			else {
				stream.pendingData.reset();
			}
			stream.upstreamWindow -= length;
			this.sendWindow -= length;
			boolean whole = length == pending;
			boolean end = whole && stream.pendingEnd;
			if (whole) {
				stream.dataQueued = false;
				stream.pendingEnd = false;
			}
			byte[] frames = length > 0 || end ? dataFrames(stream.id, payload, end) : EMPTY;
			// the entry's frame header stays accounted while the entry is queued
			return new Batch(control, frames, stream, length, length + (whole ? FRAME_HEADER_LENGTH : 0L), false);
		}

		/**
		 * Accounts a written batch; returns the stream window the drain released to the
		 * client.
		 */
		int written(Batch batch) {
			Http2DemuxRelay.this.lock.lock();
			try {
				if (this.closed) {
					return 0;
				}
				Stream stream = batch.stream();
				addQueuedLocked(stream, -batch.queued());
				if (stream == null) {
					return 0;
				}
				stream.queuedPayload -= batch.payload();
				int release = releaseLocked(stream);
				updateActiveLocked(stream);
				return release;
			}
			finally {
				Http2DemuxRelay.this.lock.unlock();
			}
		}

		void streamOpened(int streamId) {
			this.openStreams.add(Integer.valueOf(streamId));
		}

		/** No response follows on the stream (reset by either side). */
		void streamClosed(int streamId) {
			forgetOpen(streamId);
		}

		/** Uncounts a stream owing a response; the last one may let the writer finish. */
		private void forgetOpen(int streamId) {
			if (this.openStreams.remove(Integer.valueOf(streamId)) && this.openStreams.isEmpty()) {
				Http2DemuxRelay.this.lock.lock();
				try {
					signalLocked();
				}
				finally {
					Http2DemuxRelay.this.lock.unlock();
				}
			}
		}

		/**
		 * The response of a stream ended on the leg's inbound side; uncounted before its
		 * end reaches the client, so a stream the client opens on seeing it is admitted.
		 */
		void responseEnded(int streamId) {
			forgetOpen(streamId);
			Stream stream = Http2DemuxRelay.this.streams.get(Integer.valueOf(streamId));
			if (stream == null || stream.leg != this) {
				return;
			}
			Http2DemuxRelay.this.lock.lock();
			try {
				stream.responseEnded = true;
				updateActiveLocked(stream);
			}
			finally {
				Http2DemuxRelay.this.lock.unlock();
			}
		}

		/**
		 * Assembles one response-head fragment; on the completed block the head is
		 * decoded with this leg's decoder -- mirroring the upstream's encoder -- and
		 * re-encoded table-less towards the client, in one write so no other frame lands
		 * inside the block.
		 */
		void inboundHead(int type, int flags, int streamId, byte[] all, int payload, int length) {
			boolean head = type == FRAME_HEADERS;
			if (head) {
				this.inboundHeads.put(Integer.valueOf(streamId), new ByteArrayOutputStream(256));
				this.inboundHeadEndStream.put(Integer.valueOf(streamId), (flags & FLAG_END_STREAM) != 0);
			}
			ByteArrayOutputStream buffer = this.inboundHeads.get(Integer.valueOf(streamId));
			if (buffer == null) {
				return; // a continuation of a head this leg never saw the start of
			}
			appendFragmentTo(buffer, new Frame(type, flags, streamId, all, payload, length), head);
			if ((flags & FLAG_END_HEADERS) == 0) {
				return;
			}
			byte[] block = buffer.toByteArray();
			Boolean endStreamFlag = this.inboundHeadEndStream.remove(Integer.valueOf(streamId));
			boolean endStream = endStreamFlag != null && endStreamFlag;
			this.inboundHeads.remove(Integer.valueOf(streamId));
			io.netty.handler.codec.http2.Http2Headers headers;
			try {
				headers = this.responseDecoder.decodeHeaders(streamId, io.netty.buffer.Unpooled.wrappedBuffer(block));
			}
			catch (io.netty.handler.codec.http2.Http2Exception e) {
				// undecodable from the upstream's state; an empty head keeps the stream
				// alive
				headers = new io.netty.handler.codec.http2.DefaultHttp2Headers();
			}
			byte[] frames = Http2DemuxRelay.encodedHead(streamId, endStream, headers, null, this.responseEncoder);
			if (endStream) {
				responseEnded(streamId);
			}
			writeToClient(frames, 0, frames.length, this);
		}

		/** Closes the virtual connection, releasing the upstream. */
		void close() {
			Http2DemuxRelay.this.lock.lock();
			try {
				if (!closeLocked()) {
					return;
				}
			}
			finally {
				Http2DemuxRelay.this.lock.unlock();
			}
			this.connection.close();
		}

		/**
		 * The upstream ended its side: with no response owed the leg retires -- closed,
		 * the data still queued for it dropped, and every stream the client still sends
		 * on reset with {@code NO_ERROR}, its response being complete. {@code false} when
		 * a response is owed, which the caller turns into the end of the relay.
		 */
		boolean retire() {
			List<Integer> sending = new ArrayList<>();
			Http2DemuxRelay.this.lock.lock();
			try {
				if (this.closed) {
					return true;
				}
				if (!this.openStreams.isEmpty()) {
					return false;
				}
				for (Stream stream : Http2DemuxRelay.this.streams.values()) {
					if (stream.leg == this && !stream.clientEnded) {
						sending.add(Integer.valueOf(stream.id));
					}
				}
				closeLocked();
			}
			finally {
				Http2DemuxRelay.this.lock.unlock();
			}
			this.connection.close();
			for (Integer streamId : sending) {
				writeToClient(rstStream(streamId, ERROR_NO_ERROR));
			}
			return true;
		}

		/**
		 * Marks the leg closed and forgets what is queued for it, its streams included,
		 * which end for good; {@code false} when it was closed already. With the lock.
		 */
		private boolean closeLocked() {
			if (this.closed) {
				return false;
			}
			this.closed = true;
			this.ended = true;
			for (Stream stream : Http2DemuxRelay.this.streams.values()) {
				if (stream.leg != this) {
					continue;
				}
				stream.pendingData.reset();
				stream.pendingEnd = false;
				stream.dataQueued = false;
				stream.windowUpdate = null;
				stream.queuedPayload = 0;
				stream.queuedBytes = 0;
				stream.reset = true;
				stream.clientEnded = true;
				stream.responseEnded = true;
				updateActiveLocked(stream);
			}
			addQueuedLocked(null, -this.queuedBytes);
			this.entries.clear();
			this.pingAcks.clear();
			this.wake.signalAll();
			Http2DemuxRelay.this.legSettled.signalAll();
			return true;
		}

	}

	/**
	 * The leg's writer: writes the preface, then batch after batch -- the priority lane
	 * ahead of the next entry ready to go -- on the leg's upstream sink, so a stalled leg
	 * never stops the others. It sleeps while nothing is ready, and exits once the leg is
	 * closed or after the upstream's half-close. A write failure tears the relay down.
	 */
	private void pumpLegOutbound(Leg leg) {
		try {
			OutputStream sink = leg.connection.sink();
			sink.write(leg.preface);
			sink.flush();
			Batch batch;
			while ((batch = leg.take()) != null) {
				byte[] bytes = batch.bytes();
				if (batch.control().length + bytes.length > 0) {
					byte[] all = new byte[batch.control().length + bytes.length];
					System.arraycopy(batch.control(), 0, all, 0, batch.control().length);
					System.arraycopy(bytes, 0, all, batch.control().length, bytes.length);
					sink.write(all);
					sink.flush();
				}
				if (bytes.length > 0) {
					leg.inbound.increment(bytes.length);
					leg.session.recordRelayed(bytes.length, StreamRelay.Direction.TO_REMOTE);
					this.access.bytes(bytes.length, StreamRelay.Direction.TO_REMOTE);
				}
				Stream stream = batch.stream();
				int release = leg.written(batch);
				if (stream != null) {
					grantStream(stream.id, release);
				}
				if (batch.halfClose()) {
					sink.close();
					return;
				}
			}
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt(); // fall through to the exit
		}
		catch (Exception e) {
			abort();
		}
		finally {
			settle();
		}
	}

	/** DATA frames of at most the default maximum frame size carrying the payload. */
	private static byte[] dataFrames(int streamId, byte[] payload, boolean endStream) {
		ByteArrayOutputStream frames = new ByteArrayOutputStream(payload.length + 2 * FRAME_HEADER_LENGTH);
		int offset = 0;
		do {
			int length = Math.min(MAX_FRAME_SIZE, payload.length - offset);
			boolean last = offset + length >= payload.length;
			frames.writeBytes(
					frame(FRAME_DATA, last && endStream ? FLAG_END_STREAM : 0, streamId, payload, offset, length));
			offset += length;
		}
		while (offset < payload.length);
		return frames.toByteArray();
	}

	private static byte[] settingsFrame(Map<Integer, Long> settings) {
		ByteArrayOutputStream payload = new ByteArrayOutputStream(settings.size() * 6);
		settings.forEach((id, value) -> payload.writeBytes(setting(id, value)));
		byte[] bytes = payload.toByteArray();
		return frame(FRAME_SETTINGS, 0, 0, bytes, 0, bytes.length);
	}

	/**
	 * The inbound side of a leg: relays the response frames and releases the leg's
	 * connection window for the DATA forwarded (see {@link #relayInbound}).
	 */
	private void pumpInbound(Leg leg) {
		try {
			InputStream in = leg.connection.source();
			ByteArrayOutputStream scan = new ByteArrayOutputStream(2048);
			byte[] buffer = new byte[BUFFER_SIZE];
			int n;
			while ((n = in.read(buffer)) > 0) {
				relayInbound(leg, scan, buffer, n);
			}
			// the upstream half-closed: owed responses end the relay, an idle leg retires
			if (!leg.retire()) {
				abort();
			}
		}
		catch (Exception e) {
			abort();
		}
		finally {
			leg.ended = true;
			settle();
		}
	}

	/**
	 * Relays the leg's stream-scoped frames to the client and handles the
	 * connection-level ones locally: the client face of the relay owns that state, so the
	 * leg's settings are applied and acked, its pings answered, its window updates
	 * credited to the relay's send windows, and its goaway absorbed -- never forwarded.
	 */
	private void relayInbound(Leg leg, ByteArrayOutputStream scan, byte[] chunk, int length) {
		scan.write(chunk, 0, length);
		byte[] all = scan.toByteArray();
		int position = 0;
		while (all.length - position >= FRAME_HEADER_LENGTH) {
			ByteBuffer header = ByteBuffer.wrap(all, position, FRAME_HEADER_LENGTH);
			int frameLength = unsigned24(header);
			if (all.length - position < FRAME_HEADER_LENGTH + frameLength) {
				break;
			}
			int type = header.get() & 0xff;
			int flags = header.get() & 0xff;
			int streamId = header.getInt() & 0x7fffffff;
			int payload = position + FRAME_HEADER_LENGTH;
			int end = payload + frameLength;
			if (streamId != 0) {
				relayStreamFrame(leg, new Frame(type, flags, streamId, all, payload, frameLength), position);
			}
			else if (type == FRAME_SETTINGS && (flags & FLAG_ACK) == 0) {
				if (!leg.applySettings(all, payload, frameLength)) {
					abort(); // a window past 2^31-1: a connection error
				}
				leg.queueSettingsAck();
			}
			else if (type == FRAME_WINDOW_UPDATE && frameLength == 4) {
				int increment = int31(all, payload);
				if (increment > 0 && !leg.creditConnection(increment)) {
					abort(); // a window past 2^31-1: a connection error
				}
			}
			else if (type == FRAME_PING && (flags & FLAG_ACK) == 0 && frameLength == 8) {
				byte[] ping = new byte[8];
				System.arraycopy(all, payload, ping, 0, 8);
				if (!leg.queuePingAck(ping)) {
					abort(); // a ping flood while the leg is stalled
				}
			}
			position = end;
		}
		byte[] remainder = new byte[all.length - position];
		System.arraycopy(all, position, remainder, 0, remainder.length);
		scan.reset();
		scan.write(remainder, 0, remainder.length);
	}

	private void relayStreamFrame(Leg leg, Frame frame, int position) {
		int frameBytes = FRAME_HEADER_LENGTH + frame.length();
		switch (frame.type()) {
			case FRAME_HEADERS, FRAME_CONTINUATION -> leg.inboundHead(frame.type(), frame.flags(), frame.streamId(),
					frame.bytes(), frame.payload(), frame.length());
			case FRAME_WINDOW_UPDATE -> onUpstreamWindowUpdate(leg, frame);
			case FRAME_DATA -> {
				if (frame.has(FLAG_END_STREAM)) {
					leg.responseEnded(frame.streamId());
				}
				writeToClient(frame.bytes(), position, frameBytes, leg);
				if (frame.length() > 0) {
					// the payload left towards the client: released upstream
					leg.queueConnectionGrant(frame.length());
				}
			}
			case FRAME_RST_STREAM -> {
				onUpstreamReset(leg, frame.streamId());
				writeToClient(frame.bytes(), position, frameBytes, leg);
			}
			default -> writeToClient(frame.bytes(), position, frameBytes, leg);
		}
	}

	/**
	 * The upstream's stream window grant, credited to the relay's send window of the
	 * stream; one past 2^31-1 ends the relay (a stream error, which RFC 9113 5.4.1 lets
	 * an endpoint treat as a connection error).
	 */
	private void onUpstreamWindowUpdate(Leg leg, Frame frame) {
		Stream stream = this.streams.get(Integer.valueOf(frame.streamId()));
		if (stream == null || stream.leg != leg || frame.length() != 4) {
			return;
		}
		int increment = int31(frame.bytes(), frame.payload());
		if (increment > 0 && !leg.creditStream(stream, increment)) {
			abort();
		}
	}

	/**
	 * The upstream reset the stream: closed both ways, its pending data dropped;
	 * uncounted before the reset reaches the client.
	 */
	private void onUpstreamReset(Leg leg, int streamId) {
		leg.streamClosed(streamId);
		Stream stream = this.streams.get(Integer.valueOf(streamId));
		if (stream == null || stream.leg != leg) {
			return;
		}
		this.lock.lock();
		try {
			stream.reset = true;
			stream.clientEnded = true;
			stream.responseEnded = true;
			leg.purgeData(stream);
			updateActiveLocked(stream);
		}
		finally {
			this.lock.unlock();
		}
	}

	private void settle() {
		if (this.pending.decrementAndGet() == 0) {
			finishRelay();
		}
	}

	private void finishRelay() {
		if (!this.finished.compareAndSet(false, true)) {
			return;
		}
		for (Leg leg : this.legs) {
			leg.close();
			leg.session.remove(leg.connection.connectionId());
		}
		for (Stream stream : this.streams.values()) {
			if (this.streams.remove(Integer.valueOf(stream.id), stream)) {
				stream.leg.routeMeters.trackedStreams().decrement();
			}
		}
		try {
			this.pipe.close();
		}
		catch (Exception e) {
			// ignore
		}
		this.access.close();
		this.onFinish.run();
	}

	/** Tears the relay down after a failure or the loss of an owed response. */
	private void abort() {
		for (Leg leg : this.legs) {
			leg.close();
		}
		try {
			this.pipe.close();
		}
		catch (Exception e) {
			// ignore
		}
	}

	private static int unsigned24(ByteBuffer header) {
		return (header.get() & 0xff) << 16 | (header.get() & 0xff) << 8 | (header.get() & 0xff);
	}

	/**
	 * One batch of the leg writer: the priority lane's frames, then one entry's bytes,
	 * with the payload and the queued bytes they release for the stream; or the
	 * half-close.
	 */
	private record Batch(byte[] control, byte[] bytes, @Nullable Stream stream, int payload, long queued,
			boolean halfClose) {
	}

	/** One entry of a leg's queue. */
	private sealed interface LegEntry {

		/**
		 * Relayed frames written as one: a whole header block, or a reset; trailers
		 * ({@code followsData}) wait until the stream's queued data is out.
		 */
		record Block(Stream stream, byte[] bytes, boolean followsData) implements LegEntry {

		}

		/**
		 * The stream's pending DATA payload, framed when the writer takes it; queued
		 * again behind the others while the upstream's windows leave some of it pending.
		 */
		record Data(Stream stream) implements LegEntry {

		}

		/** A stream window update; later ones coalesce into it while it waits. */
		final class WindowUpdate implements LegEntry {

			private final Stream stream;

			int increment;

			WindowUpdate(Stream stream, int increment) {
				this.stream = stream;
				this.increment = increment;
			}

			Stream stream() {
				return this.stream;
			}

		}

	}

}
