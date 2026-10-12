package am.ik.sluice.server.proxy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
import am.ik.sluice.tunnel.ConnectionLimit;
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
 * client sending past a stream window gets a GOAWAY {@code FLOW_CONTROL_ERROR}; a client
 * WINDOW_UPDATE RFC 9113 6.9 makes an error ends the connection or resets its stream
 * ({@link #onClientWindowUpdate}), one from an upstream ends the relay. A header block is
 * written whole in either direction, and the relay's own connection-level frames towards
 * an upstream are written ahead of the request frames queued for it.
 * <p>
 * What the relay queues is bounded across connections too: each relay may queue
 * {@link #CONNECTION_ALLOWANCE}, and the bytes beyond it are charged to the
 * {@link H2DemuxBudget} every relay of the node shares. While a relay is past its
 * allowance and the budget is exhausted, it withholds the client's connection window --
 * the client stops sending DATA once the at most 64KiB it holds are spent -- until its
 * legs drain below the allowance or the budget has room again; a new stream whose head
 * fits neither is refused ({@code REFUSED_STREAM}), and trailers that fit neither reset
 * their stream ({@code ENHANCE_YOUR_CALM} to the client, {@code CANCEL} to the upstream).
 * What a relay may not refuse -- DATA within the windows it granted, resets and window
 * updates -- is bounded per connection ({@link #UNBUDGETED_BYTES}). A leg beyond the
 * first takes a slot of the connection limit; a stream whose route needs a leg while none
 * is free is refused. A closed leg keeps its slot -- its own, or the connection's --
 * until the frames queued for it left the tunnel sender, and the connection's own slot is
 * released once the relay ended and no leg holds it any more.
 * <p>
 * Towards each upstream the relay is a sender bound by the upstream's windows (RFC 9113
 * 6.9): the connection window and every stream's window, initially as the upstream's
 * SETTINGS announce (the spec default until they arrive), adjusted when they change, and
 * replenished by its WINDOW_UPDATEs. A stream's DATA is written only as far as both
 * windows allow; a stream without window waits without holding back the leg's other
 * streams, and the stream's trailers wait behind its data while the client's window
 * updates do not. An upstream pushing a window past 2^31-1 ends the relay.
 * <p>
 * Towards the client the relay is a sender bound by the client's connection window (65535
 * plus the client's connection-level WINDOW_UPDATEs; one past 2^31-1 ends the connection
 * with GOAWAY {@code FLOW_CONTROL_ERROR}); the client's stream windows bind each
 * upstream, to which the client's settings and stream window updates are relayed. A leg's
 * inbound side writes response DATA as it reads it while no response waits and the window
 * admits the frame, and queues it otherwise; a response writer drains the queue within
 * the window, one frame per stream in turn, splitting frames to the window and delivering
 * their padding as padding. The upstream's connection window is granted back only for
 * DATA written or dropped, so a leg holds at most that window (65535) of data for the
 * client, and an upstream sending past it ends the relay. The inbound side never waits
 * for the client's window: the upstream's pings and window updates, response heads and
 * resets go on. Trailers follow their stream's waiting data; past
 * {@link #RESPONSE_BLOCK_ALLOWANCE} of them per leg, the inbound side stops reading until
 * the queue drains. A reset by either side drops the stream's waiting data and grants its
 * window back to the upstream. Whatever the upstream still sends on a stream the relay
 * reset itself is dropped -- its DATA granted back, its header blocks decoded all the
 * same to keep the leg's decoder in step --, and the relay keeps no state for the stream
 * once the reset is written to the upstream, however long the upstream goes on sending on
 * it.
 * <p>
 * The client ending its side of the connection half-closes each leg once the frames
 * queued for it are written and its responses ended -- until then the relay keeps
 * granting the upstream's connection window, and an upstream that closes on reading EOF
 * (as Netty does by default) still delivers them -- unless a routed request is
 * unfinished: that request can never complete, so the relay ends instead, closing the
 * legs. While a leg waits so, or a response waits for the client's connection window, the
 * relay writes a PING to the client whenever it wrote nothing for
 * {@link #PROBE_INTERVAL_NANOS}: a client gone entirely rather than half-closed fails the
 * write and ends the relay. An upstream ending its side -- or its connection failing --
 * with no response owed retires its leg: the leg takes no new stream, its responses
 * waiting for the client's window are written first, then the data still queued for it is
 * dropped, and a stream the client still sends on is reset with {@code NO_ERROR}.
 * <p>
 * While no stream is open the connection lives under the idle timeout of its
 * {@link ConnectionLimits.KeepAliveDeadline}, from the connection head on unless its
 * stream was admitted: only a new stream stops it -- a PING does not -- and a stream the
 * relay refuses or answers itself starts it over. Its expiry ends the connection with a
 * GOAWAY {@code NO_ERROR} naming the highest stream the client opened.
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

	private static final int ERROR_PROTOCOL = 0x1;

	private static final int ERROR_FLOW_CONTROL = 0x3;

	private static final int ERROR_FRAME_SIZE = 0x6;

	private static final int ERROR_REFUSED_STREAM = 0x7;

	private static final int ERROR_CANCEL = 0x8;

	private static final int ERROR_ENHANCE_YOUR_CALM = 0xb;

	private static final int SETTINGS_HEADER_TABLE_SIZE = 0x1;

	private static final int SETTINGS_MAX_CONCURRENT_STREAMS = 0x3;

	private static final int SETTINGS_INITIAL_WINDOW_SIZE = 0x4;

	private static final int SETTINGS_MAX_FRAME_SIZE = 0x5;

	private static final int SETTINGS_MAX_HEADER_LIST_SIZE = 0x6;

	/** The settings defined by RFC 9113, RFC 8441 and RFC 9218, the ones re-announced. */
	private static final Set<Integer> KNOWN_SETTINGS = Set.of(0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x8, 0x9);

	/**
	 * SETTINGS_MAX_FRAME_SIZE every endpoint accepts without announcing more (RFC 9113).
	 * The relay announces no other value to either side, so a larger frame from the
	 * client or an upstream is a connection error: this caps the bytes buffered for a
	 * frame not yet complete.
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
	 * budget of payload (in a buffer less than a granule larger), its head and trailers,
	 * a reset, and one coalesced window update. A backstop: passing it ends the relay
	 * with a GOAWAY {@code FLOW_CONTROL_ERROR}.
	 */
	static final long MAX_QUEUED_BYTES = (long) MAX_CONCURRENT_STREAMS * (STREAM_BUDGET + PendingPayload.GRANULE
			+ FRAME_HEADER_LENGTH + 2L * MAX_BLOCK_BYTES + 2L * (FRAME_HEADER_LENGTH + 4));

	/**
	 * The bytes a relay may queue without charging the shared budget: what it always
	 * admits, so a connection whose legs drain keeps flowing however exhausted the budget
	 * is. One stream budget.
	 */
	static final int CONNECTION_ALLOWANCE = 128 * 1024;

	/**
	 * The most one relay queues outside the shared budget: its allowance, plus what it
	 * cannot refuse once past it -- the DATA of the connection window the client still
	 * holds (at most the initial 64KiB: the relay grants the window only as DATA
	 * arrives), each stream's buffer rounding and data entry, a reset and a coalesced
	 * window update.
	 */
	static final long UNBUDGETED_BYTES = CONNECTION_ALLOWANCE + STREAM_WINDOW + (long) MAX_CONCURRENT_STREAMS
			* (PendingPayload.GRANULE + FRAME_HEADER_LENGTH + 2L * (FRAME_HEADER_LENGTH + 4));

	/**
	 * Per leg, the header block bytes (trailers) the response queue holds behind data
	 * waiting for the client's connection window: one block's worth. A block past it
	 * waits in the leg's inbound side, which stops reading the upstream meanwhile.
	 */
	static final int RESPONSE_BLOCK_ALLOWANCE = MAX_BLOCK_BYTES;

	/**
	 * The most one leg's response queue holds: the upstream's connection window the relay
	 * granted -- granted back only as the queued DATA is written or dropped --, each
	 * stream's buffer rounding, and {@link #RESPONSE_BLOCK_ALLOWANCE}.
	 */
	static final long RESPONSE_QUEUE_BYTES = STREAM_WINDOW + (long) MAX_CONCURRENT_STREAMS * PendingPayload.GRANULE
			+ RESPONSE_BLOCK_ALLOWANCE;

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

	private final ConnectionLimits limits;

	private final H2DemuxBudget budget;

	private final Router.Route initialRoute;

	private final TunnelSession initialSession;

	private final @Nullable InetAddress peer;

	private final Runnable onFinish;

	/** Frees the connection's own slot; run once the relay ended and no leg holds it. */
	private final Runnable release;

	private final ConnectionLimits.KeepAliveDeadline deadline;

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

	/**
	 * Streams whose response waits for the client's connection window, in turn order;
	 * guarded by {@link #lock}.
	 */
	private final ArrayDeque<Stream> responseQueue = new ArrayDeque<>();

	/** Signals that queued response data left or was dropped; with {@link #lock}. */
	private final Condition responseMoved = this.lock.newCondition();

	/**
	 * The relay's send window towards the client (RFC 9113 6.9.1): 65535 plus the
	 * client's connection-level WINDOW_UPDATEs, less the response DATA written; guarded
	 * by {@link #lock}.
	 */
	private long clientSendWindow = STREAM_WINDOW;

	/** A response writer thread drains {@link #responseQueue}; guarded by the lock. */
	private boolean responseWriting;

	/** When the relay last wrote to the client, {@link System#nanoTime()}. */
	private volatile long lastClientWrite = System.nanoTime();

	/** The outbound side plus two sides per leg; the relay ends when all drain. */
	private final AtomicInteger pending = new AtomicInteger(1);

	private final AtomicBoolean finished = new AtomicBoolean();

	/** Streams counted against the advertised limit; guarded by {@link #lock}. */
	private int activeStreams;

	/**
	 * A new stream's head is being routed: the connection is not idle even with no stream
	 * open; guarded by {@link #lock}.
	 */
	private boolean routingHead;

	/** The bytes queued for every leg of the connection; guarded by {@link #lock}. */
	private long queuedBytes;

	/**
	 * The part of {@link #queuedBytes} beyond the allowance, charged to the budget;
	 * guarded by {@link #lock}.
	 */
	private long pooled;

	/** Client connection window withheld, to grant once allowed; with {@link #lock}. */
	private long owedGrant;

	/** The relay finished: no grant is owed or awaited; guarded by {@link #lock}. */
	private boolean grantsClosed;

	/** Grants the withheld window once the budget has room; one instance, to dedupe. */
	private final Runnable owedGrantRetry = this::retryOwedGrant;

	/**
	 * A leg holds the slot of the connection itself -- alive, or closed with frames still
	 * queued; guarded by {@link #lock}.
	 */
	private boolean baseLegTaken;

	/**
	 * {@link #release}, once the relay ended while a leg still held the connection's
	 * slot; guarded by {@link #lock}.
	 */
	private @Nullable Runnable baseRelease;

	/** The highest stream id routed, for GOAWAY; written by the outbound parse only. */
	private volatile int lastStreamId;

	/**
	 * The highest stream id the client opened, routed or not; written by the outbound
	 * parse only.
	 */
	private volatile int highestStreamId;

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
			H2DemuxMeters meters, ConnectionLimits limits, Router.Route initialRoute, TunnelSession initialSession,
			@Nullable InetAddress peer, ConnectionLimits.KeepAliveDeadline deadline, Runnable onFinish,
			Runnable release) {
		this.pipe = pipe;
		this.seedHead = head.bytes();
		this.router = router;
		this.sessions = sessions;
		this.accessControl = accessControl;
		this.errorResponse = errorResponse;
		this.access = access;
		this.meterRegistry = meters.registry();
		this.meters = meters;
		this.limits = limits;
		this.budget = limits.h2Budget();
		this.initialRoute = initialRoute;
		this.initialSession = initialSession;
		this.peer = peer;
		this.onFinish = onFinish;
		this.release = release;
		this.deadline = deadline;
		deadline.onExpiry(this::closeIdle);
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
			// a head whose stream was not admitted (or not even taken as a stream)
			// leaves the connection idle
			this.lock.lock();
			try {
				idleIfQuietLocked();
			}
			finally {
				this.lock.unlock();
			}
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
	 * Waits until every leg half-closed or closed and no response waits for the client's
	 * connection window; meanwhile probes the client with a PING whenever nothing was
	 * written to it for {@link #PROBE_INTERVAL_NANOS}. A client that half-closed reads
	 * on; one gone entirely fails a write (the second at the latest, once its stack
	 * answered the first with a reset), which ends the relay.
	 */
	private void probeUntilLegsSettled() throws InterruptedException {
		while (true) {
			long idle;
			this.lock.lock();
			try {
				if (this.legs.stream().allMatch(Leg::settledLocked) && this.responseQueue.isEmpty()) {
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
			if (frameLength > MAX_FRAME_SIZE) {
				// before its payload arrives: nothing of it is buffered
				goAway(ERROR_FRAME_SIZE);
				throw new IllegalStateException("client frame of %d bytes".formatted(frameLength));
			}
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
	 * window, which is granted back at once while the relay is within its allowance or
	 * the budget has room, and withheld otherwise; the stream's window is charged with
	 * it, and the bare payload joins the stream's queued data -- the padding, never
	 * queued, is granted back with the stream budget's room. Frames of streams unknown or
	 * already ended are dropped, their window granted back at once.
	 */
	private void onData(Frame frame) {
		if (frame.streamId() == 0) {
			return; // DATA is stream-scoped; an unexpected connection-level frame
		}
		Stream stream = this.streams.get(Integer.valueOf(frame.streamId()));
		if (stream == null) {
			grantClient(frame.length()); // frames of streams never routed
			return;
		}
		int dataLength = dataPayloadLength(frame);
		int dataOffset = frame.payload() + (frame.has(FLAG_PADDED) ? 1 : 0);
		boolean violation = false;
		boolean dropped = false;
		int release = 0;
		int connectionGrant = 0;
		this.lock.lock();
		try {
			if (stream.clientEnded || stream.trailers) {
				dropped = true; // the client ended the stream: its late frames are void
			}
			else if (frame.length() > stream.clientWindow) {
				violation = true;
			}
			else {
				stream.clientWindow -= frame.length();
				stream.leg.queueData(stream, frame.bytes(), dataOffset, dataLength, frame.has(FLAG_END_STREAM));
				release = releaseLocked(stream);
				boolean owing = this.owedGrant > 0;
				connectionGrant = connectionGrantLocked(frame.length());
				if (connectionGrant == 0 && this.owedGrant > 0) {
					if (!owing) {
						this.meters.windowWithheld();
					}
					awaitBudgetLocked();
				}
			}
		}
		finally {
			this.lock.unlock();
		}
		if (dropped) {
			grantClient(frame.length());
			return;
		}
		if (violation) {
			goAway(ERROR_FLOW_CONTROL);
			abort();
			return;
		}
		grantClient(connectionGrant);
		grantStream(stream.id, release);
		checkQueueBound();
	}

	/**
	 * Adds {@code units} to the client connection window owed and returns what may be
	 * granted now: all of it while the relay is within its allowance or the budget has
	 * room, nothing otherwise.
	 */
	private int connectionGrantLocked(int units) {
		this.owedGrant += units;
		if (this.owedGrant == 0 || this.grantsClosed || !grantAllowedLocked()) {
			return 0;
		}
		int grant = (int) this.owedGrant;
		this.owedGrant = 0;
		return grant;
	}

	private boolean grantAllowedLocked() {
		return this.queuedBytes <= CONNECTION_ALLOWANCE || this.budget.hasRoom();
	}

	/** Has the withheld window granted once the budget has room; with {@link #lock}. */
	private void awaitBudgetLocked() {
		if (!this.grantsClosed) {
			this.budget.awaitRoom(this.owedGrantRetry);
		}
	}

	/** Grants the withheld window if allowed by now; called as queued bytes leave. */
	private void grantOwed() {
		int grant;
		this.lock.lock();
		try {
			grant = this.owedGrant > 0 ? connectionGrantLocked(0) : 0;
		}
		finally {
			this.lock.unlock();
		}
		grantClient(grant);
	}

	/** The budget has room: grants the withheld window, or waits again. */
	private void retryOwedGrant() {
		int grant;
		this.lock.lock();
		try {
			grant = connectionGrantLocked(0);
			if (grant == 0 && this.owedGrant > 0) {
				awaitBudgetLocked();
			}
		}
		finally {
			this.lock.unlock();
		}
		try {
			grantClient(grant);
		}
		catch (RuntimeException e) {
			abort(); // the client is gone
		}
	}

	/** Moves {@link #queuedBytes} and settles the budget's share; with {@link #lock}. */
	private void accountLocked(long bytes) {
		this.queuedBytes += bytes;
		settleBudgetLocked();
	}

	/**
	 * Charges to the budget, or releases, what makes its share the part of
	 * {@link #queuedBytes} beyond the allowance; with {@link #lock}.
	 */
	private void settleBudgetLocked() {
		long share = Math.max(0, this.queuedBytes - CONNECTION_ALLOWANCE);
		long change = share - this.pooled;
		this.pooled = share;
		if (change > 0) {
			this.budget.charge(change);
		}
		else if (change < 0) {
			this.budget.release(-change);
		}
	}

	/**
	 * Whether {@code bytes} more may be queued: within the allowance, or charged to the
	 * budget now if it has room for the part beyond. The caller queues them, or settles
	 * the budget when it does not. With {@link #lock}.
	 */
	private boolean admitLocked(long bytes) {
		long change = Math.max(0, this.queuedBytes + bytes - CONNECTION_ALLOWANCE) - this.pooled;
		if (change <= 0) {
			return true;
		}
		if (!this.budget.tryCharge(change)) {
			return false;
		}
		this.pooled += change;
		return true;
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

	/**
	 * A client RST_STREAM: relayed once; the stream's pending data is dropped, and so is
	 * its response waiting for the client's window, whose window goes back to the
	 * upstream.
	 */
	private void onClientReset(Frame frame) {
		Stream stream = this.streams.get(Integer.valueOf(frame.streamId()));
		if (stream == null) {
			return;
		}
		int dropped;
		this.lock.lock();
		try {
			if (stream.reset) {
				return;
			}
			dropped = resetLocked(stream);
			stream.leg.queueBlock(stream, frame(FRAME_RST_STREAM, frame.flags(), frame.streamId(), frame.bytes(),
					frame.payload(), frame.length()), false);
			updateActiveLocked(stream);
		}
		finally {
			this.lock.unlock();
		}
		afterReset(stream);
		stream.leg.queueConnectionGrant(dropped);
	}

	/**
	 * Resets the stream both ways on the relay's own account (a stream error, RFC 9113
	 * 5.4.2): RST_STREAM with the error code to the client, {@code CANCEL} to the
	 * upstream once the frames queued for the stream ahead of it are written, the
	 * stream's queued data and its response waiting for the client's window dropped --
	 * the latter's window granted back to the upstream. The stream owes no response any
	 * more, and whatever the upstream or the client still sends on it is dropped. It
	 * stays counted against the advertised limit until its reset is written to the
	 * upstream. Called by the outbound parse; a stream already reset by either side, or
	 * closed both ways (END_STREAM received from the client and written to it), is left
	 * alone, and {@code false} returned.
	 */
	private boolean resetStream(Stream stream, int errorCode) {
		int dropped;
		// under the sink lock, so no response frame of the stream follows the reset
		synchronized (this.sinkLock) {
			this.lock.lock();
			try {
				if (stream.reset || (stream.clientEnded && stream.responseEnded)) {
					return false;
				}
				dropped = resetLocked(stream);
				stream.leg.queueBlock(stream, rstStream(stream.id, ERROR_CANCEL), false);
				updateActiveLocked(stream);
			}
			finally {
				this.lock.unlock();
			}
			writeToClient(rstStream(stream.id, errorCode));
		}
		afterReset(stream);
		stream.leg.queueConnectionGrant(dropped);
		return true;
	}

	/**
	 * Marks the stream reset and drops its queued data and its response waiting for the
	 * client's window; returns the flow-controlled bytes of the latter, for the caller to
	 * grant back to the upstream once the lock is released. With {@link #lock}.
	 */
	private int resetLocked(Stream stream) {
		stream.reset = true;
		stream.clientEnded = true;
		stream.leg.purgeData(stream);
		return purgeResponseLocked(stream);
	}

	/**
	 * After a reset towards the upstream: the stream owes no response, and the bytes it
	 * released may let the withheld connection window go.
	 */
	private void afterReset(Stream stream) {
		stream.leg.streamClosed(stream.id);
		grantOwed();
		checkQueueBound();
	}

	/**
	 * A client stream-level WINDOW_UPDATE: relayed to the stream's leg, coalesced into
	 * the one still queued for the stream if any. A connection-level one credits the
	 * relay's send window towards the client. RFC 9113 6.9: a length other than 4 ends
	 * the connection with GOAWAY {@code FRAME_SIZE_ERROR} whatever the stream, an
	 * increment of 0 with GOAWAY {@code PROTOCOL_ERROR} on stream 0, and resets the
	 * stream with {@code PROTOCOL_ERROR} otherwise -- unless the stream is no longer
	 * tracked, or closed both ways (RFC 9113 5.1 has such frames ignored).
	 */
	private void onClientWindowUpdate(Frame frame) {
		if (frame.length() != 4) {
			goAway(ERROR_FRAME_SIZE);
			abort();
			return;
		}
		int increment = int31(frame.bytes(), frame.payload());
		if (frame.streamId() == 0) {
			if (increment == 0) {
				goAway(ERROR_PROTOCOL);
				abort();
				return;
			}
			creditClientWindow(increment);
			return;
		}
		Stream stream = this.streams.get(Integer.valueOf(frame.streamId()));
		if (stream == null) {
			return;
		}
		if (increment == 0) {
			if (resetStream(stream, ERROR_PROTOCOL)) {
				this.meters.resetForProtocol();
			}
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

	/**
	 * Credits the client's connection-level grant to the relay's send window and has the
	 * responses waiting for it written; a window past 2^31-1 ends the connection with
	 * GOAWAY {@code FLOW_CONTROL_ERROR} (RFC 9113 6.9.1).
	 */
	private void creditClientWindow(int increment) {
		boolean overflow;
		boolean write = false;
		this.lock.lock();
		try {
			this.clientSendWindow += increment;
			overflow = this.clientSendWindow > MAX_WINDOW;
			if (!overflow) {
				write = claimResponseWriterLocked();
			}
		}
		finally {
			this.lock.unlock();
		}
		if (overflow) {
			goAway(ERROR_FLOW_CONTROL);
			abort();
			return;
		}
		if (write) {
			startResponseWriter();
		}
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
			if (id == SETTINGS_HEADER_TABLE_SIZE || id == SETTINGS_MAX_FRAME_SIZE || !KNOWN_SETTINGS.contains(id)) {
				// the upstreams encode for the relay's decoders, which keep the default
				// table, and frame for its parser, which takes the default frame size;
				// unknown settings belong to this hop
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
		if (this.assembling.size() > MAX_HEADER_LIST_SIZE) {
			// an encoded block never outgrows its decoded list, so this block is beyond
			// the decoder's limit; dropping it would desynchronize the decoder
			goAway(ERROR_ENHANCE_YOUR_CALM);
			throw new IllegalStateException("client header block over %d bytes".formatted(MAX_HEADER_LIST_SIZE));
		}
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
		boolean exhausted = false;
		this.lock.lock();
		try {
			if (stream.clientEnded || stream.trailers) {
				return;
			}
			if (!admitLocked(frames.length)) {
				// trailers cannot be refused without leaving the stream unfinished: the
				// stream, its connection past its allowance with the budget exhausted, is
				// reset instead
				exhausted = true;
			}
			else {
				stream.trailers = true;
				stream.clientEnded = endStream;
				stream.leg.queueBlock(stream, frames, true);
				settleBudgetLocked();
			}
		}
		finally {
			this.lock.unlock();
		}
		if (exhausted) {
			if (resetStream(stream, ERROR_ENHANCE_YOUR_CALM)) {
				this.meters.resetForBudget();
			}
			return;
		}
		checkQueueBound();
	}

	private void onRequestHead(int streamId, boolean endStream, io.netty.handler.codec.http2.Http2Headers headers) {
		if (!beginHead()) {
			return; // the idle timeout passed first: the stream is left unprocessed
		}
		try {
			routeRequestHead(streamId, endStream, headers);
		}
		finally {
			endHead();
		}
	}

	/**
	 * Marks a new stream's head as being routed; {@code false} once the deadline passed.
	 */
	private boolean beginHead() {
		this.lock.lock();
		try {
			this.routingHead = this.deadline.enter(ConnectionLimits.KeepAliveDeadline.Phase.BUSY);
			return this.routingHead;
		}
		finally {
			this.lock.unlock();
		}
	}

	/**
	 * The head was routed, refused or answered: with no stream open, the idle timeout.
	 */
	private void endHead() {
		this.lock.lock();
		try {
			this.routingHead = false;
			idleIfQuietLocked();
		}
		finally {
			this.lock.unlock();
		}
	}

	/** Starts the idle timeout when no stream is open or being routed; with the lock. */
	private void idleIfQuietLocked() {
		if (this.activeStreams == 0 && !this.routingHead) {
			this.deadline.enter(ConnectionLimits.KeepAliveDeadline.Phase.IDLE);
		}
	}

	/**
	 * The idle timeout passed: tells the client no stream above the highest it opened was
	 * processed, then ends the relay.
	 */
	private void closeIdle() {
		goAway(this.highestStreamId, ERROR_NO_ERROR);
		abort();
	}

	private void routeRequestHead(int streamId, boolean endStream, io.netty.handler.codec.http2.Http2Headers headers) {
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
		// always re-encoded: the client's blocks reference its dynamic table, whose
		// state a leg joining later (tunnel recovery, a second upstream of a coalesced
		// connection) does not have -- the table-less encoder keeps every block
		// self-contained no matter which leg decodes it or when
		byte[] frames = encodedHead(streamId, endStream, headers, route.rewriteHost() ? route.address() : null,
				this.headersEncoder);
		boolean budgeted;
		this.lock.lock();
		try {
			budgeted = admitLocked(frames.length);
		}
		finally {
			this.lock.unlock();
		}
		if (!budgeted) {
			this.meters.refusedForBudget();
			writeToClient(rstStream(streamId, ERROR_REFUSED_STREAM));
			return;
		}
		Leg leg = legFor(route, resolved.session());
		if (leg == null) {
			this.lock.lock();
			try {
				settleBudgetLocked();
			}
			finally {
				this.lock.unlock();
			}
			this.meters.refusedForLegLimit();
			writeToClient(rstStream(streamId, ERROR_REFUSED_STREAM));
			return;
		}
		Stream stream = new Stream(streamId, leg);
		boolean admitted;
		this.lock.lock();
		try {
			// a leg its upstream retired meanwhile takes no new stream
			admitted = leg.acceptingLocked();
			if (!admitted) {
				settleBudgetLocked();
			}
			else {
				this.streams.put(Integer.valueOf(streamId), stream);
				leg.routeMeters.trackedStreams().increment();
				this.lastStreamId = Math.max(this.lastStreamId, streamId);
				this.activeStreams++;
				stream.active = true;
				stream.clientEnded = endStream;
				stream.upstreamWindow = leg.initialWindow;
				leg.streamOpened(streamId);
				leg.queueBlock(stream, frames, false);
				settleBudgetLocked();
			}
		}
		finally {
			this.lock.unlock();
		}
		if (!admitted) {
			writeToClient(rstStream(streamId, ERROR_REFUSED_STREAM));
			return;
		}
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
			idleIfQuietLocked();
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

	/**
	 * The leg relaying the route: the alive leg serving it, or a new one -- on the
	 * connection's own slot when no leg holds it, else on a slot of its own; null when
	 * the connection limit has none left.
	 */
	private @Nullable Leg legFor(Router.Route route, TunnelSession session) {
		for (Leg leg : this.legs) {
			if (!leg.ended && leg.route.equals(route)) {
				return leg;
			}
		}
		boolean base;
		this.lock.lock();
		try {
			base = !this.baseLegTaken;
			this.baseLegTaken = true;
		}
		finally {
			this.lock.unlock();
		}
		ConnectionLimit.Permit permit = null;
		if (!base) {
			permit = this.limits.tryAcquireLeg();
			if (permit == null) {
				return null;
			}
		}
		try {
			return new Leg(route, session, permit);
		}
		catch (RuntimeException e) {
			releaseSlot(permit);
			throw e;
		}
	}

	/**
	 * Frees a leg's slot: its own permit, or the connection's -- which the relay releases
	 * when it ended already.
	 */
	private void releaseSlot(ConnectionLimit.@Nullable Permit permit) {
		if (permit != null) {
			permit.release();
			return;
		}
		Runnable release;
		this.lock.lock();
		try {
			this.baseLegTaken = false;
			release = this.baseRelease;
			this.baseRelease = null;
		}
		finally {
			this.lock.unlock();
		}
		if (release != null) {
			release.run();
		}
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
		if (units > 0) {
			writeToClient(windowUpdate(0, units));
		}
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
		goAway(this.lastStreamId, errorCode);
	}

	private void goAway(int lastStreamId, int errorCode) {
		byte[] payload = new byte[8];
		System.arraycopy(int32(lastStreamId), 0, payload, 0, 4);
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

		/**
		 * Every byte queued for the stream: the payload's buffer, frame headers of its
		 * entries included.
		 */
		long queuedBytes;

		/** Payload not yet written by the leg writer. */
		final PendingPayload pending = new PendingPayload();

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

		/** Response DATA payload waiting for the client's connection window. */
		final PendingPayload response = new PendingPayload();

		/**
		 * The padding of the waiting response DATA, flow-controlled octets delivered as
		 * padding of the frames written for it.
		 */
		long responsePadding;

		/** END_STREAM follows the waiting response DATA. */
		boolean responseEnd;

		/** A response header block (trailers) waiting behind the response DATA. */
		byte @Nullable [] responseBlock;

		/** {@link #responseBlock} ends the stream. */
		boolean responseBlockEnds;

		/** In the relay's response queue: something of the response waits. */
		boolean responseWaiting;

		Stream(int id, Leg leg) {
			this.id = id;
			this.leg = leg;
		}

		/** Whether anything of the response waits for the client's window. */
		boolean hasResponse() {
			return this.response.size() > 0 || this.responsePadding > 0 || this.responseEnd
					|| this.responseBlock != null;
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

		/**
		 * The relay's receive window towards the upstream: 65535 less the DATA received
		 * and not granted back yet; guarded by the lock.
		 */
		private long receiveWindow = STREAM_WINDOW;

		/**
		 * The header block bytes of the leg's streams in the response queue; with the
		 * lock.
		 */
		private long responseBlockBytes;

		/** The streams relayed whose response has not ended. */
		private final Set<Integer> openStreams = ConcurrentHashMap.newKeySet();

		/** The leg takes no new stream: its upstream went away, or it closed. */
		private volatile boolean ended;

		/**
		 * The leg's own slot of the connection limit; null when it holds the
		 * connection's.
		 */
		private final ConnectionLimit.@Nullable Permit permit;

		/** Set by {@link #close()}; guarded by {@link #lock}. */
		private boolean closed;

		/** The client is done sending: half-close once nothing is owed; with the lock. */
		private boolean halfCloseRequested;

		/** The writer half-closed the upstream and exited; guarded by {@link #lock}. */
		private boolean halfClosed;

		/**
		 * The upstream ended its side; the leg retires once its queued responses are
		 * written. Guarded by {@link #lock}.
		 */
		private boolean gone;

		Leg(Router.Route route, TunnelSession session, ConnectionLimit.@Nullable Permit permit) {
			this.route = route;
			this.session = session;
			this.permit = permit;
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
			return !this.closed && !this.halfClosed && !this.gone;
		}

		/**
		 * The upstream ended its side: the leg takes no new stream nor frame, and its
		 * writer exits.
		 */
		void upstreamGone() {
			Http2DemuxRelay.this.lock.lock();
			try {
				this.gone = true;
				this.ended = true;
				this.wake.signalAll();
			}
			finally {
				Http2DemuxRelay.this.lock.unlock();
			}
		}

		/** Whether the upstream ended its side. */
		boolean isGone() {
			Http2DemuxRelay.this.lock.lock();
			try {
				return this.gone;
			}
			finally {
				Http2DemuxRelay.this.lock.unlock();
			}
		}

		/** Whether the leg half-closed or closed; with {@link #lock}. */
		private boolean settledLocked() {
			return this.closed || this.halfClosed;
		}

		private void addQueuedLocked(@Nullable Stream stream, long bytes) {
			this.queuedBytes += bytes;
			Http2DemuxRelay.this.accountLocked(bytes);
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
				int capacity = stream.pending.capacity();
				stream.pending.append(bytes, offset, length);
				stream.queuedPayload += length;
				addQueuedLocked(stream, stream.pending.capacity() - capacity);
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
			int pending = stream.pending.size();
			int capacity = stream.pending.capacity();
			if (capacity > 0) {
				stream.pending.clear();
				stream.queuedPayload -= pending;
				addQueuedLocked(stream, -capacity);
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
		 * Charges an upstream DATA frame to the relay's receive window; {@code false}
		 * when the upstream sent past it.
		 */
		boolean receive(int length) {
			Http2DemuxRelay.this.lock.lock();
			try {
				this.receiveWindow -= length;
				return this.receiveWindow >= 0;
			}
			finally {
				Http2DemuxRelay.this.lock.unlock();
			}
		}

		/**
		 * Releases the upstream's connection window for DATA written to the client or
		 * dropped, in the priority lane.
		 */
		void queueConnectionGrant(int units) {
			if (units <= 0) {
				return;
			}
			Http2DemuxRelay.this.lock.lock();
			try {
				if (acceptingLocked()) {
					this.connectionGrant = Math.min(Integer.MAX_VALUE, this.connectionGrant + units);
					this.receiveWindow += units;
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
		 * The upstream's connection-level WINDOW_UPDATE; {@code false} when the increment
		 * is 0 or the window would pass 2^31-1.
		 */
		boolean creditConnection(int increment) {
			if (increment == 0) {
				return false;
			}
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
		 * increment is 0 or the window would pass 2^31-1. Updates of streams the relay
		 * forgot are void.
		 */
		boolean creditStream(Stream stream, int increment) {
			Http2DemuxRelay.this.lock.lock();
			try {
				if (!stream.active) {
					return true;
				}
				if (increment == 0) {
					return false;
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
					if (this.closed || this.gone) {
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
			int pending = stream.pending.size();
			int length = pending;
			if (pending > 0) {
				long allowed = Math.min(stream.upstreamWindow, this.sendWindow);
				if (allowed <= 0) {
					return null;
				}
				length = (int) Math.min(pending, allowed);
			}
			int capacity = stream.pending.capacity();
			byte[] payload = stream.pending.take(length);
			long released = capacity - stream.pending.capacity();
			stream.upstreamWindow -= length;
			this.sendWindow -= length;
			boolean whole = length == pending;
			boolean end = whole && stream.pendingEnd;
			if (whole) {
				stream.dataQueued = false;
				stream.pendingEnd = false;
			}
			byte[] frames = length > 0 || end ? dataFrames(stream.id, payload, end) : EMPTY;
			// the entry's frame header stays accounted while the entry is queued; the
			// buffer released stays accounted until the payload is written
			return new Batch(control, frames, stream, length, released + (whole ? FRAME_HEADER_LENGTH : 0L), false);
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
		 * Assembles one response-head fragment; on the completed block the head is
		 * decoded with this leg's decoder -- mirroring the upstream's encoder -- and
		 * re-encoded table-less towards the client (see {@link #relayHead}).
		 */
		void inboundHead(int type, int flags, int streamId, byte[] all, int payload, int length) {
			boolean head = type == FRAME_HEADERS;
			if (head && !this.inboundHeads.isEmpty()) {
				// header blocks never interleave (RFC 9113 6.10)
				throw new IllegalStateException("upstream header blocks interleaved");
			}
			if (head) {
				this.inboundHeads.put(Integer.valueOf(streamId), new ByteArrayOutputStream(256));
				this.inboundHeadEndStream.put(Integer.valueOf(streamId), (flags & FLAG_END_STREAM) != 0);
			}
			ByteArrayOutputStream buffer = this.inboundHeads.get(Integer.valueOf(streamId));
			if (buffer == null) {
				return; // a continuation of a head this leg never saw the start of
			}
			appendFragmentTo(buffer, new Frame(type, flags, streamId, all, payload, length), head);
			if (buffer.size() > MAX_HEADER_LIST_SIZE) {
				throw new IllegalStateException("upstream header block over %d bytes".formatted(MAX_HEADER_LIST_SIZE));
			}
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
				forgetOpen(streamId);
			}
			relayHead(this, streamId, frames, endStream);
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
			releaseSlotAfterQueued();
		}

		/**
		 * Frees the closed leg's slot once the frames queued for it left the tunnel
		 * sender.
		 */
		private void releaseSlotAfterQueued() {
			ConnectionLimit.@Nullable Permit permit = this.permit;
			this.connection.afterQueued(() -> releaseSlot(permit));
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
			releaseSlotAfterQueued();
			for (Integer streamId : sending) {
				writeToClient(rstStream(streamId, ERROR_NO_ERROR));
			}
			grantOwed();
			return true;
		}

		/**
		 * Marks the leg closed and forgets what is queued for it and what of its
		 * responses waits for the client's window, its streams included, which end for
		 * good; {@code false} when it was closed already. With the lock.
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
				stream.pending.clear();
				stream.pendingEnd = false;
				stream.dataQueued = false;
				stream.windowUpdate = null;
				purgeResponseLocked(stream);
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
			Http2DemuxRelay.this.responseMoved.signalAll();
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
				grantOwed();
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
			if (!leg.isGone()) {
				abort();
			}
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
			while ((n = readUpstream(in, buffer)) > 0) {
				relayInbound(leg, scan, buffer, n);
			}
			// the upstream ended its side or failed: owed responses end the relay; an
			// idle leg retires once its responses waiting for the client's window left
			if (leg.openStreams.isEmpty()) {
				leg.upstreamGone();
				awaitResponsesWritten(leg);
			}
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
	 * Reads from the leg's upstream; a failed connection reads as its end, which the
	 * caller tells apart by whether a response is owed.
	 */
	private static int readUpstream(InputStream in, byte[] buffer) {
		try {
			return in.read(buffer);
		}
		catch (IOException e) {
			return -1;
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
			if (frameLength > MAX_FRAME_SIZE) {
				throw new IllegalStateException("upstream frame of %d bytes".formatted(frameLength));
			}
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
			else if (type == FRAME_WINDOW_UPDATE) {
				requireWindowUpdateLength(frameLength);
				// an increment of 0, or a window past 2^31-1: a connection error
				if (!leg.creditConnection(int31(all, payload))) {
					abort();
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
			case FRAME_DATA -> relayData(leg, frame, position);
			case FRAME_RST_STREAM -> onUpstreamReset(leg, frame, position);
			default -> {
				Stream stream = this.streams.get(Integer.valueOf(frame.streamId()));
				synchronized (this.sinkLock) {
					if (trackedUnreset(leg, stream)) {
						writeToClient(frame.bytes(), position, frameBytes, leg);
					}
				}
			}
		}
	}

	/**
	 * Whether the stream is tracked on the leg and reset by neither side: the frames of
	 * the upstream on it may go to the client. Called with the sink lock, under which the
	 * caller writes, so no frame follows the relay's own reset of the stream.
	 */
	private boolean trackedUnreset(Leg leg, @Nullable Stream stream) {
		if (stream == null || stream.leg != leg) {
			return false;
		}
		this.lock.lock();
		try {
			return !stream.reset;
		}
		finally {
			this.lock.unlock();
		}
	}

	/**
	 * An upstream DATA frame, charged to the leg's receive window -- an upstream sending
	 * past it ends the relay. Written as it is while no response waits and the client's
	 * connection window admits it, else queued for the response writer: its payload, its
	 * padding as octets owed, its END_STREAM. Frames of streams the relay no longer
	 * relays a response on -- ended, reset by either side, or no longer tracked -- are
	 * dropped. The upstream's connection window comes back for what is written or
	 * dropped, so a leg holds at most that window for the client.
	 */
	private void relayData(Leg leg, Frame frame, int position) {
		int length = frame.length();
		if (!leg.receive(length)) {
			throw new IllegalStateException("upstream DATA past the relay's connection window");
		}
		if (frame.has(FLAG_END_STREAM)) {
			leg.forgetOpen(frame.streamId());
		}
		Stream stream = this.streams.get(Integer.valueOf(frame.streamId()));
		boolean queued = false;
		boolean write = false;
		synchronized (this.sinkLock) {
			boolean direct = false;
			this.lock.lock();
			try {
				if (stream != null && relayingResponseLocked(leg, stream)) {
					if (!stream.responseWaiting && this.responseQueue.isEmpty() && this.clientSendWindow >= length) {
						this.clientSendWindow -= length;
						if (frame.has(FLAG_END_STREAM)) {
							endResponseLocked(stream);
						}
						direct = true;
					}
					else {
						queueResponseLocked(stream, frame);
						queued = true;
						write = claimResponseWriterLocked();
					}
				}
			}
			finally {
				this.lock.unlock();
			}
			if (direct) {
				writeToClient(frame.bytes(), position, FRAME_HEADER_LENGTH + length, leg);
			}
		}
		if (!queued) {
			// written or dropped: released upstream
			leg.queueConnectionGrant(length);
		}
		if (write) {
			startResponseWriter();
		}
	}

	/**
	 * Whether the leg still relays a response on the stream: neither side reset it and
	 * the upstream did not end it yet. With {@link #lock}.
	 */
	private static boolean relayingResponseLocked(Leg leg, Stream stream) {
		return stream.leg == leg && !stream.reset && !stream.responseEnded && !stream.responseEnd
				&& stream.responseBlock == null;
	}

	/** Queues a DATA frame's payload, padding and END_STREAM; with {@link #lock}. */
	private void queueResponseLocked(Stream stream, Frame frame) {
		int dataLength = dataPayloadLength(frame);
		if (dataLength > 0) {
			stream.response.append(frame.bytes(), frame.payload() + (frame.has(FLAG_PADDED) ? 1 : 0), dataLength);
		}
		stream.responsePadding += frame.length() - dataLength;
		if (frame.has(FLAG_END_STREAM)) {
			stream.responseEnd = true;
		}
		if (!stream.responseWaiting && stream.hasResponse()) {
			stream.responseWaiting = true;
			this.responseQueue.addLast(stream);
		}
	}

	/**
	 * A re-encoded response header block, written whole in one write so no other frame
	 * lands inside it -- or queued behind the stream's response data waiting for the
	 * client's window (trailers). Before queueing, the leg's inbound side waits while the
	 * blocks queued for the leg would pass {@link #RESPONSE_BLOCK_ALLOWANCE}. A block of
	 * a stream the relay no longer relays a response on -- ended, reset by either side,
	 * or no longer tracked -- is dropped; the caller decoded it all the same, keeping the
	 * leg's decoder in step with the upstream's encoder.
	 */
	private void relayHead(Leg leg, int streamId, byte[] frames, boolean endStream) {
		Stream stream = this.streams.get(Integer.valueOf(streamId));
		if (stream == null || stream.leg != leg) {
			return;
		}
		awaitBlockRoom(stream, frames.length);
		synchronized (this.sinkLock) {
			if (relayHeadLocked(stream, frames, endStream)) {
				writeToClient(frames, 0, frames.length, leg);
			}
		}
	}

	/**
	 * Queues the block behind the stream's waiting response, or drops it once the relay
	 * no longer relays a response on the stream; {@code true} when it is to be written
	 * now.
	 */
	private boolean relayHeadLocked(Stream stream, byte[] frames, boolean endStream) {
		this.lock.lock();
		try {
			if (!relayingResponseLocked(stream.leg, stream)) {
				return false;
			}
			if (stream.responseWaiting) {
				stream.responseBlock = frames;
				stream.responseBlockEnds = endStream;
				stream.leg.responseBlockBytes += frames.length;
				return false;
			}
			if (endStream) {
				endResponseLocked(stream);
			}
			return true;
		}
		finally {
			this.lock.unlock();
		}
	}

	/**
	 * Waits while the block would be queued and the leg's queued blocks would pass
	 * {@link #RESPONSE_BLOCK_ALLOWANCE}; a block always fits an empty allowance.
	 */
	private void awaitBlockRoom(Stream stream, int length) {
		Leg leg = stream.leg;
		this.lock.lock();
		try {
			while (stream.responseWaiting && leg.responseBlockBytes > 0
					&& leg.responseBlockBytes + length > RESPONSE_BLOCK_ALLOWANCE && !leg.closed) {
				this.responseMoved.awaitUninterruptibly();
			}
		}
		finally {
			this.lock.unlock();
		}
	}

	/** Waits until nothing of the leg's responses waits for the client's window. */
	private void awaitResponsesWritten(Leg leg) {
		this.lock.lock();
		try {
			while (!leg.closed && this.responseQueue.stream().anyMatch(stream -> stream.leg == leg)) {
				this.responseMoved.awaitUninterruptibly();
			}
		}
		finally {
			this.lock.unlock();
		}
	}

	/**
	 * The response of the stream ends as its END_STREAM is about to be written: uncounted
	 * before it reaches the client, so a stream the client opens on seeing it is
	 * admitted. With {@link #lock}.
	 */
	private void endResponseLocked(Stream stream) {
		stream.responseEnded = true;
		updateActiveLocked(stream);
	}

	/**
	 * Drops what of the stream's response waits for the client's window; returns the
	 * flow-controlled bytes dropped, for the caller to grant back to the upstream. With
	 * {@link #lock}.
	 */
	private int purgeResponseLocked(Stream stream) {
		if (!stream.responseWaiting) {
			return 0;
		}
		int dropped = stream.response.size() + (int) stream.responsePadding;
		stream.response.clear();
		stream.responsePadding = 0;
		stream.responseEnd = false;
		byte[] block = stream.responseBlock;
		if (block != null) {
			stream.leg.responseBlockBytes -= block.length;
			stream.responseBlock = null;
		}
		stream.responseWaiting = false;
		this.responseQueue.remove(stream);
		responseLeftLocked();
		return dropped;
	}

	private void responseLeftLocked() {
		this.responseMoved.signalAll();
		if (this.responseQueue.isEmpty()) {
			this.legSettled.signalAll();
		}
	}

	/**
	 * Claims the response writer when something queued can be written now and no writer
	 * runs; the caller starts it. With {@link #lock}.
	 */
	private boolean claimResponseWriterLocked() {
		if (this.responseWriting) {
			return false;
		}
		for (Stream stream : this.responseQueue) {
			if (writableLocked(stream)) {
				this.responseWriting = true;
				return true;
			}
		}
		return false;
	}

	/** Whether the stream's next response frame can be written now; with the lock. */
	private boolean writableLocked(Stream stream) {
		if (stream.response.size() > 0 || stream.responsePadding > 0) {
			return this.clientSendWindow > 0;
		}
		return stream.responseEnd || stream.responseBlock != null;
	}

	private void startResponseWriter() {
		Thread.ofVirtual().name("sluice-h2demux-resp").start(this::writeResponses);
	}

	/**
	 * The response writer: writes the queued responses within the client's connection
	 * window, one frame per stream in turn, and exits once nothing can be written; the
	 * next window update or queued frame starts it again. Takes and writes under the sink
	 * lock, so a frame the leg's inbound side writes for the same stream follows it.
	 */
	private void writeResponses() {
		try {
			while (true) {
				ResponseFrame next;
				synchronized (this.sinkLock) {
					this.lock.lock();
					try {
						next = nextResponseFrameLocked();
						if (next == null) {
							this.responseWriting = false;
							return;
						}
					}
					finally {
						this.lock.unlock();
					}
					writeToClient(next.bytes(), 0, next.bytes().length, next.leg());
				}
				next.leg().queueConnectionGrant(next.window());
			}
		}
		catch (RuntimeException e) {
			this.lock.lock();
			try {
				this.responseWriting = false;
			}
			finally {
				this.lock.unlock();
			}
			abort(); // the client is gone
		}
	}

	/**
	 * Takes the next response frame: the first stream in turn whose next frame can be
	 * written, which then goes to the back of the queue while more of it waits. With
	 * {@link #lock}.
	 */
	private @Nullable ResponseFrame nextResponseFrameLocked() {
		Iterator<Stream> iterator = this.responseQueue.iterator();
		while (iterator.hasNext()) {
			Stream stream = iterator.next();
			if (!writableLocked(stream)) {
				continue;
			}
			iterator.remove();
			ResponseFrame next = takeResponseFrameLocked(stream);
			if (stream.hasResponse()) {
				this.responseQueue.addLast(stream);
			}
			else {
				stream.responseWaiting = false;
			}
			responseLeftLocked();
			return next;
		}
		return null;
	}

	/**
	 * The stream's next response frame, within the client's window: its payload up to a
	 * frame, then its padding in padded frames of up to 256 octets, then END_STREAM or
	 * the trailers. The flow-controlled octets written equal those queued, so the
	 * client's windows and the upstream's view of them stay in step. With the lock.
	 */
	private ResponseFrame takeResponseFrameLocked(Stream stream) {
		int data = stream.response.size();
		if (data > 0) {
			int length = (int) Math.min(Math.min(data, MAX_FRAME_SIZE), this.clientSendWindow);
			byte[] payload = stream.response.take(length);
			this.clientSendWindow -= length;
			boolean end = takeEndLocked(stream);
			return new ResponseFrame(stream.leg,
					frame(FRAME_DATA, end ? FLAG_END_STREAM : 0, stream.id, payload, 0, length), length);
		}
		if (stream.responsePadding > 0) {
			// the pad length octet plus up to 255 octets of padding
			int length = (int) Math.min(Math.min(stream.responsePadding, 256), this.clientSendWindow);
			byte[] payload = new byte[length];
			payload[0] = (byte) (length - 1);
			stream.responsePadding -= length;
			this.clientSendWindow -= length;
			boolean end = takeEndLocked(stream);
			return new ResponseFrame(stream.leg,
					frame(FRAME_DATA, FLAG_PADDED | (end ? FLAG_END_STREAM : 0), stream.id, payload, 0, length),
					length);
		}
		if (takeEndLocked(stream)) {
			return new ResponseFrame(stream.leg, frame(FRAME_DATA, FLAG_END_STREAM, stream.id, null, 0, 0), 0);
		}
		byte[] block = Objects.requireNonNull(stream.responseBlock);
		stream.responseBlock = null;
		stream.leg.responseBlockBytes -= block.length;
		if (stream.responseBlockEnds) {
			endResponseLocked(stream);
		}
		return new ResponseFrame(stream.leg, block, 0);
	}

	/**
	 * Whether the frame being taken carries the stream's END_STREAM: no payload or
	 * padding is left and the upstream ended the stream with them. With the lock.
	 */
	private boolean takeEndLocked(Stream stream) {
		if (!stream.responseEnd || stream.response.size() > 0 || stream.responsePadding > 0) {
			return false;
		}
		stream.responseEnd = false;
		endResponseLocked(stream);
		return true;
	}

	/**
	 * One response frame the writer took, with the flow-controlled octets it carries,
	 * granted back to the leg's upstream once written.
	 */
	private record ResponseFrame(Leg leg, byte[] bytes, int window) {
	}

	/**
	 * The upstream's stream window grant, credited to the relay's send window of the
	 * stream; an increment of 0 or a window past 2^31-1 ends the relay (stream errors,
	 * which RFC 9113 5.4.2 lets an endpoint treat as connection errors).
	 */
	private void onUpstreamWindowUpdate(Leg leg, Frame frame) {
		requireWindowUpdateLength(frame.length());
		Stream stream = this.streams.get(Integer.valueOf(frame.streamId()));
		if (stream == null || stream.leg != leg) {
			return;
		}
		if (!leg.creditStream(stream, int31(frame.bytes(), frame.payload()))) {
			abort();
		}
	}

	/**
	 * An upstream WINDOW_UPDATE of a length other than 4 is a connection error
	 * {@code FRAME_SIZE_ERROR} whatever its stream (RFC 9113 6.9): it ends the relay.
	 */
	private static void requireWindowUpdateLength(int length) {
		if (length != 4) {
			throw new IllegalStateException("upstream WINDOW_UPDATE of %d bytes".formatted(length));
		}
	}

	/**
	 * The upstream reset the stream: closed both ways, its pending data and its response
	 * waiting for the client's window dropped, the latter's window granted back to the
	 * upstream; uncounted before the reset, relayed as it is, reaches the client. The
	 * reset of a stream already reset by either side, or no longer tracked, is dropped.
	 */
	private void onUpstreamReset(Leg leg, Frame frame, int position) {
		leg.streamClosed(frame.streamId());
		Stream stream = this.streams.get(Integer.valueOf(frame.streamId()));
		int dropped = 0;
		synchronized (this.sinkLock) {
			boolean relay = false;
			if (stream != null && stream.leg == leg) {
				this.lock.lock();
				try {
					relay = !stream.reset;
					if (relay) {
						dropped = resetLocked(stream);
						stream.responseEnded = true;
						updateActiveLocked(stream);
					}
				}
				finally {
					this.lock.unlock();
				}
			}
			if (relay) {
				writeToClient(frame.bytes(), position, FRAME_HEADER_LENGTH + frame.length(), leg);
			}
		}
		leg.queueConnectionGrant(dropped);
		grantOwed();
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
		this.deadline.disarm();
		for (Leg leg : this.legs) {
			leg.close();
			leg.session.remove(leg.connection.connectionId());
		}
		Runnable release = null;
		this.lock.lock();
		try {
			// the legs released the budget's share; no grant is owed any more
			this.grantsClosed = true;
			this.owedGrant = 0;
			this.budget.cancel(this.owedGrantRetry);
			// a closed leg with frames queued still holds the connection's slot
			if (this.baseLegTaken) {
				this.baseRelease = this.release;
			}
			else {
				release = this.release;
			}
		}
		finally {
			this.lock.unlock();
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
		if (release != null) {
			release.run();
		}
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
