package am.ik.sluice.server.proxy;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.SequenceInputStream;
import java.net.InetAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Predicate;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import am.ik.sluice.server.auth.RouteAuthenticator;
import am.ik.sluice.server.auth.Verdict;
import am.ik.sluice.server.proxy.hop.Hop;
import am.ik.sluice.server.proxy.hop.HopHandler;
import am.ik.sluice.server.proxy.hop.Loop;
import am.ik.sluice.server.route.Router;
import am.ik.sluice.server.tunnel.SessionRegistry;
import am.ik.sluice.server.tunnel.TunnelSession;
import am.ik.sluice.tunnel.DuplexPipe;
import am.ik.sluice.tunnel.StreamRelay;
import am.ik.sluice.tunnel.VirtualConnection;
import io.micrometer.core.instrument.Counter;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpStatusClass;
import io.netty.handler.codec.http2.Http2Connection;
import io.netty.handler.codec.http2.Http2ConnectionAdapter;
import io.netty.handler.codec.http2.Http2Error;
import io.netty.handler.codec.http2.Http2Exception;
import io.netty.handler.codec.http2.Http2FrameAdapter;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2LocalFlowController;
import io.netty.handler.codec.http2.Http2RemoteFlowController;
import io.netty.handler.codec.http2.Http2Settings;
import io.netty.handler.codec.http2.Http2Stream;
import io.netty.util.AsciiString;

/**
 * Relays one plaintext or TLS-terminated HTTP/2 client connection over per-route legs on
 * netty's HTTP/2 codec: the client connection is a server endpoint ({@link Hop}), each
 * route's leg -- a virtual connection of its tunnel session -- a client endpoint, all of
 * them on one {@link Loop}, so every channel operation of the relay runs under its lock.
 * <p>
 * The connection head {@link DataProxyServer} read (the preface up to the first header
 * block) is fed to the client endpoint first, the socket after it, so netty's codec
 * validates the first stream like any other; the first stream takes the route
 * {@code DataProxyServer} resolved for the connection without a second lookup (a lookup
 * advances the load balancing state). Every later request head is routed by its
 * {@code :authority} (else {@code host}) and access-checked on its own: a refused stream
 * gets the no-route ({@code 503}) or forbidden ({@code 403}) page written through the
 * client endpoint's flow controller, and the connection goes on. Every stream, the first
 * one included, is then checked against its route's credentials
 * ({@link RouteAuthenticator}): a refused one gets the unauthorized ({@code 401}) or, its
 * credential not checked for the checks in flight, the busy ({@code 503}) page the same
 * way; the headers of the route's credentials are removed from a head that passes unless
 * the route forwards them. A check against a slow hash runs off the loop, its stream
 * parked meanwhile ({@link #parkLocked}): the connection's other streams go on, and the
 * parked stream resumes once the check ended -- to a stream id on its leg above those the
 * leg opened meanwhile. A stream of a {@code force-http1} route is answered {@code 421}
 * without a body, then reset with {@code NO_ERROR} while its request is still being sent;
 * it opens no leg. When the connection's handshake was the stream's host's own and
 * offered http/1.1 -- h2 was picked as the handshake resolved no force-http1 route, say
 * while the route was gated shut --, the {@code 421} comes with a GOAWAY
 * {@code NO_ERROR}: Chromium retries a 421 on a new connection only past one it coalesced
 * the host onto. A head that is no request (no {@code :method}, no {@code :path} but for
 * a CONNECT, a CONNECT without {@code :authority}) is reset with {@code PROTOCOL_ERROR}.
 * A CONNECT stream is routed by its {@code :authority} and logged with it as its path.
 * <p>
 * Streams of one route share the route's leg and keep their stream ids on it, but for a
 * parked stream a later one overtook ({@link Leg#streamIdLocked}); with
 * {@code rewrite-host} the head's {@code :authority} becomes the route's address. Windows
 * are per hop: DATA read on one hop is written on the other ({@link Flow}: a copy, small
 * frames coalesced) and consumed -- its stream window granted back -- once written, so
 * what the relay holds per stream and direction stays within the stream window it
 * announced ({@link #STREAM_WINDOW} to the client, {@link #LEG_STREAM_WINDOW} to an
 * upstream); the connection windows refill as DATA arrives, not as it is written, so a
 * stream held by its upstream or its client holds back no other stream. Each hop's
 * connection window starts at RFC 9113's 65535 bytes and grows with what its peer sends
 * per round trip up to {@link #CONNECTION_WINDOW} (the client's) or
 * {@link #LEG_CONNECTION_WINDOW} (an upstream's) ({@link HopWindow}): the hop holds back
 * netty's connection-level WINDOW_UPDATEs and the review grants them
 * ({@link #grantLocked}). Resets pass either way with their code; an upstream's reset
 * after the response's end follows that end to the client. A stream the client ends
 * without its request's end resets its upstream stream with {@code CANCEL}; one an early
 * response closed for the client still delivers the request's rest.
 * <p>
 * What the relay holds -- DATA, header blocks and pages not yet written, and every hop's
 * writer queue -- and the connection window its peers hold -- what they may still send --
 * are accounted after each action of its loop: the part of the former past
 * {@link #CONNECTION_ALLOWANCE} plus the part of the latter past RFC 9113's window per
 * hop, its share, is charged to the {@link H2DemuxBudget} every relay shares. A grant
 * raising a peer's window past RFC 9113's is charged before it leaves, within the
 * budget's room and below {@link #CONNECTION_SHARE} ({@link #chargeCreditLocked}): what a
 * peer sends was charged before it could arrive, but for RFC 9113's window. A grant the
 * charge does not take leaves the peer at a smaller window, at worst RFC 9113's. Past the
 * allowance with the budget exhausted, or with its share at {@link #CONNECTION_SHARE},
 * the relay grants no connection window at all (each peer stops once the window it holds
 * is spent) and refuses the most recent waiting streams past the allowance
 * ({@link #refuseWaitingLocked}), until its hops drained below the allowance, or below
 * the share with the budget's room back. A header block that does not fit -- one the
 * budget has no room for, or taking the share past {@link #BLOCK_SHARE} -- refuses its
 * new stream ({@code REFUSED_STREAM}), or resets its stream when a later block -- the
 * client's trailers, the upstream's interim (1xx) heads, final head and trailers, these
 * once the header blocks heading for the client passed {@link #RESPONSE_BLOCK_ALLOWANCE}
 * ({@link #admitResponseBlockLocked}) -- ({@code ENHANCE_YOUR_CALM} to the client,
 * {@code CANCEL} to the upstream). A page the relay answers a stream with is admitted the
 * same way: one that does not fit refuses its stream ({@code REFUSED_STREAM}), or resets
 * it ({@code ENHANCE_YOUR_CALM}) in place of the bad-gateway page. A header block is
 * admitted at its size as HPACK counts it ({@link #headerBytes}), whatever it took on the
 * wire: one referring to the peer's dynamic table, a few bytes read, is admitted at about
 * what the relay writes for it. The interim heads of one response may sum to
 * {@link #MAX_INTERIM_BYTES}: the one past it is not written, the upstream's stream is
 * reset ({@code ENHANCE_YOUR_CALM}) and the stream is lost for the client (the
 * bad-gateway page). The bounds outside the budget: {@link #UNBUDGETED_BYTES} per
 * connection, {@link #RESPONSE_QUEUE_BYTES} per leg.
 * <p>
 * An upstream's failure ends no more than it covers. netty's codec answers the upstream's
 * protocol errors itself: a stream error resets that stream towards the upstream, a
 * connection error sends the upstream a GOAWAY and closes the leg at once. A stream lost
 * to its leg that way, or to the leg closing or failing, is answered by the relay with
 * the bad-gateway page ({@code 502}) while no final response head was relayed on it, and
 * reset with {@code INTERNAL_ERROR} once one was (see {@link #loseLocked}); a stream the
 * leg never took, or above the last stream id of the upstream's GOAWAY, is refused
 * ({@code REFUSED_STREAM}); a response the upstream ended is still delivered. A leg that
 * received a GOAWAY closes gracefully once its other streams ended. A leg whose channel
 * closed or that received or sent a GOAWAY takes no new stream: the route's next stream
 * opens a new leg. The client's connection goes on throughout.
 * <p>
 * A leg opens at most as many streams on its upstream as the upstream's
 * {@code SETTINGS_MAX_CONCURRENT_STREAMS} allows, one until the upstream's first
 * SETTINGS; a stream past it waits, its head and DATA buffered in the leg's encoder
 * (netty's {@code StreamBufferingEncoder}), until a stream of the leg closes. A waiting
 * stream the client resets, or whose client stream closes, leaves the buffer without
 * reaching the upstream. One the upstream's GOAWAY or the leg's end drops is refused
 * ({@code REFUSED_STREAM}), and so is one still waiting at the wait deadline.
 * <p>
 * The first leg opens on the connection's own slot of the connection limit, each further
 * leg open at once on a slot of its own ({@link LegSlots}): a stream whose route needs a
 * new leg while the limit has no slot left is refused ({@code REFUSED_STREAM}). A leg
 * retires once its channel closed and its threads ended: the relay lets go of it, its
 * virtual connection leaves the session, and its slot comes back once the frames it
 * queued left the tunnel sender. A leg whose client withdrew
 * ({@link TunnelSession#withdrawn()}) closes as soon as it has no stream left -- none
 * open on its upstream, none waiting for one, none parked for its route
 * ({@link #retireWithdrawnLocked}) --, so the client's drain does not wait on an idle
 * connection; the route's next stream is resolved anew. The bytes each leg reads and
 * writes count as the route's tunnel bytes ({@code sluice.tunnel.bytes}), the session's
 * traffic and the connection's access log bytes. A stream netty's codec resets on a
 * stream error of the client or of an upstream, and a leg sending its upstream a GOAWAY
 * with an error code, are counted ({@link H2DemuxMeters}); a client or an upstream going
 * away mid-stream is neither ({@link HopHandler}).
 * <p>
 * netty's codec answers the client's protocol errors itself; a header block that does not
 * decode ends the connection with a GOAWAY {@code COMPRESSION_ERROR}, logged as a request
 * line naming neither route, method nor path.
 * <p>
 * While the client has no stream open, the connection lives under the idle timeout of its
 * {@link ConnectionLimits.KeepAliveDeadline}: a new stream stops it -- a PING does not --
 * and a stream the relay answers itself starts it over. While a stream is open, the
 * deadline's stall timeout counts the time the relay waits on the client
 * ({@link #waitsOnClientLocked}); DATA read from the client, and each piece the client
 * took of what the relay wrote ({@link ConnectionLimits.KeepAliveDeadline#WRITE_PIECE}),
 * are progress. Either expiry sends the client a GOAWAY -- {@code NO_ERROR} for the idle
 * timeout, {@code ENHANCE_YOUR_CALM} for the stall timeout -- naming the highest stream
 * it opened, and closes the client endpoint; the transport is closed under a write the
 * client does not read after the teardown grace.
 * <p>
 * The client's EOF with a request unfinished closes the client endpoint; otherwise the
 * relay drains and probes the client ({@link #clientEndedLocked}). The client endpoint's
 * end -- the client gone, an error, the codec closing it, a timeout -- cancels every
 * stream the legs still have ({@code CANCEL}) and closes every leg at once; the drain
 * closes each leg as it empties, and the client endpoint after the last. Once every hop's
 * threads ended, the relay finishes: the connection is logged closed and {@code onFinish}
 * runs; {@code release} follows once the frames the leg on the connection's own slot
 * queued left the tunnel sender too.
 */
final class NettyHttp2DemuxRelay {

	private static final Logger log = LoggerFactory.getLogger(NettyHttp2DemuxRelay.class);

	/** The concurrent streams the relay advertises to the client. */
	static final int MAX_CONCURRENT_STREAMS = 100;

	/** The header list size limit of both faces, mirroring the connection head's. */
	static final int MAX_HEADER_LIST_SIZE = 64 * 1024;

	/**
	 * The most the interim (1xx) heads of one response may sum to, as HPACK counts them
	 * ({@link #headerBytes}): one header list's limit, so a response's interim heads
	 * weigh no more than its final head may.
	 */
	static final int MAX_INTERIM_BYTES = MAX_HEADER_LIST_SIZE;

	/**
	 * The initial stream window the relay announces to the client: what it reads of a
	 * stream from the client and has not written to the upstream yet, its consumed bytes
	 * not granted back, stays within it. A stream moves at most this much per round trip
	 * of the client's hop: the tunnel's own per-connection window
	 * ({@link VirtualConnection#DEFAULT_WINDOW}), what a leg carries towards its
	 * upstream.
	 */
	static final int STREAM_WINDOW = VirtualConnection.DEFAULT_WINDOW;

	/**
	 * The largest connection window the relay grants the client, refilled as DATA
	 * arrives: the most a connection's streams together move per round trip of the hop,
	 * the same bound as one stream's ({@link #STREAM_WINDOW}) for the same reason. A
	 * hop's window grows to it from RFC 9113's as its peer sends ({@link HopWindow}), the
	 * part past RFC 9113's charged to the budget.
	 */
	static final int CONNECTION_WINDOW = VirtualConnection.DEFAULT_WINDOW;

	/**
	 * The initial stream window the relay announces to an upstream: twice the leg's
	 * tunnel window ({@link VirtualConnection#DEFAULT_WINDOW}). netty grants a stream's
	 * window back once half of it was consumed, so an upstream spending it in bursts, one
	 * per tunnel round trip, finds up to half of it held back for a round trip; at twice
	 * the tunnel window it still has the tunnel's full window to send, and the leg's
	 * virtual connection, whose own grants hold back nothing from a blocked writer,
	 * bounds it.
	 */
	static final int LEG_STREAM_WINDOW = 2 * VirtualConnection.DEFAULT_WINDOW;

	/**
	 * The largest connection window the relay grants an upstream: twice the leg's tunnel
	 * window, as {@link #LEG_STREAM_WINDOW} and for the same reason -- the window is
	 * refilled once its peer spent half of it ({@link HopWindow}). It grows to it from
	 * RFC 9113's as the upstream sends, the part past RFC 9113's charged to the budget.
	 */
	static final int LEG_CONNECTION_WINDOW = 2 * VirtualConnection.DEFAULT_WINDOW;

	/**
	 * The bytes a relay may queue without charging the shared budget: what it always
	 * admits, so a connection whose hops drain keeps flowing however exhausted the budget
	 * is.
	 */
	static final int CONNECTION_ALLOWANCE = 128 * 1024;

	/**
	 * The most of the shared budget one relay takes -- what it queues past its allowance
	 * and the window its peers hold past RFC 9113's -- before it grants its hops no more
	 * connection window though the budget has room: one connection window each way, so no
	 * connection holds the budget the others share. A grant takes the share no further
	 * than below it. Once nothing is granted, its peers can still send what windows they
	 * hold, charged but for RFC 9113's ({@link #WITHHELD_ARRIVALS} each).
	 */
	static final long CONNECTION_SHARE = (long) CONNECTION_WINDOW + LEG_CONNECTION_WINDOW;

	/**
	 * The most of the shared budget one relay takes for a header block -- a new stream's
	 * head, trailers, an upstream's head past {@link #RESPONSE_BLOCK_ALLOWANCE}, a page
	 * --: twice its {@link #CONNECTION_SHARE}, which a relay granting nothing passes by
	 * what its peers still send uncharged, so a client reading slower than its upstream
	 * sends still gets its new streams relayed. Bounds what header blocks held for a peer
	 * that does not read take beyond the share.
	 */
	static final long BLOCK_SHARE = 2 * CONNECTION_SHARE;

	/**
	 * The most one hop's writer queue holds of control bytes ({@link Hop#controlBytes}:
	 * frame headers, acks, resets, window updates), which reach it uncharged: the reader
	 * pauses past {@link Hop#DEFAULT_READ_PAUSE_BYTES} of them, so the queue passes it by
	 * what one read makes netty and the relay answer at most (under 32KiB in
	 * {@code HopFailureTest}) and the relay's own frames (window updates, PINGs, a reset
	 * per open stream), 64KiB here. The rest of a queue was charged before it got there:
	 * DATA and header blocks move into it from what the relay holds ({@link Flow}, a
	 * stream's head, a page), each admitted or charged as window first, but for the
	 * response header blocks within {@link #RESPONSE_BLOCK_ALLOWANCE}.
	 */
	static final long HOP_CONTROL_BYTES = Hop.DEFAULT_READ_PAUSE_BYTES + 64 * 1024;

	/**
	 * The upstreams' header blocks the relay writes on the client hop without admitting
	 * them ({@link #admitResponseBlockLocked}): while the blocks the client hop queues
	 * and the responses' blocks not yet in it stay within one header list's limit, so a
	 * response is not held back for bytes queued towards the upstreams.
	 */
	static final long RESPONSE_BLOCK_ALLOWANCE = MAX_HEADER_LIST_SIZE;

	/**
	 * What a peer can still make the relay hold uncharged once the relay grants it
	 * nothing more: the read under way when the budget ran out (the decision follows each
	 * read), then the connection window the peer holds up to RFC 9113's -- what it holds
	 * past that was charged when granted ({@link HopWindow#charged}) --, copied into the
	 * streams' backlogs, which round up by less than one {@link PendingPayload#GRANULE}
	 * each.
	 */
	static final long WITHHELD_ARRIVALS = Hop.MAX_READ_BYTES + HopWindow.FLOOR
			+ (long) MAX_CONCURRENT_STREAMS * PendingPayload.GRANULE;

	/**
	 * The most one relay queues outside the shared budget: its allowance, what the client
	 * can still send uncharged once it is granted nothing more, the control bytes of the
	 * client hop's writer queue and the response header blocks written unadmitted.
	 */
	static final long UNBUDGETED_BYTES = CONNECTION_ALLOWANCE + WITHHELD_ARRIVALS + HOP_CONTROL_BYTES
			+ RESPONSE_BLOCK_ALLOWANCE;

	/**
	 * The most one leg adds outside the shared budget: what its upstream can still send
	 * uncharged once it is granted nothing more, and the control bytes of the leg hop's
	 * writer queue.
	 */
	static final long RESPONSE_QUEUE_BYTES = WITHHELD_ARRIVALS + HOP_CONTROL_BYTES;

	/** How long a hop's threads may take to end once its channel closed. */
	static final Duration HOP_DONE_TIMEOUT = Duration.ofSeconds(30);

	private static final String HTTP_2 = "2";

	private static final String TUNNEL_BYTES = "sluice.tunnel.bytes";

	private static final String CONNECT = "CONNECT";

	private static final AsciiString EXPECT = AsciiString.cached("expect");

	private static final AsciiString CONTINUE = AsciiString.cached("100-continue");

	/** The payload of the PINGs probing a half-closed client: "sluice-p". */
	static final long PROBE_PAYLOAD = 0x736c756963652d70L;

	/** The payload of the PINGs measuring a peer's round trip: "sluice-b". */
	private static final long ROUND_TRIP_PAYLOAD = 0x736c756963652d62L;

	private final DuplexPipe pipe;

	private final byte[] seed;

	/** The stream of the connection head, routed to the initial route; -1 = none. */
	private int seedStreamId;

	private final Router router;

	private final SessionRegistry sessions;

	private final AccessControl accessControl;

	private final RouteAuthenticator routeAuth;

	private final ErrorResponse errorResponse;

	private final AccessLogger.Connection access;

	private final H2DemuxMeters meters;

	private final Router.Route initialRoute;

	private final TunnelSession initialSession;

	private final @Nullable InetAddress peer;

	/** The handshake the data plane terminated; null for plaintext h2. */
	private final @Nullable TlsHandshake handshake;

	private final ConnectionLimits.KeepAliveDeadline deadline;

	/** How long a stream may wait for its upstream's concurrency limit; zero = no end. */
	private final Duration streamWaitTimeout;

	/** The quiet after which a half-closed client is probed with a PING; zero = never. */
	private final Duration probeInterval;

	/** The time the relay waited on its client, read by the deadline's stall watch. */
	private final WaitClock clientWait = new WaitClock(() -> this.waitingOnClient);

	/** When the client last took a piece of what the relay wrote, in nanoTime. */
	private volatile long lastClientWrite = System.nanoTime();

	/** The relay waits on its client right now; with the loop's lock. */
	private boolean waitingOnClient;

	/** The client ended its side (EOF) with every request ended; with the loop's lock. */
	private boolean clientEnded;

	/**
	 * The client's side ended, every stream completed and every leg closed: the client
	 * hop closes; with the loop's lock.
	 */
	private boolean drained;

	private final Runnable onFinish;

	private final Runnable release;

	private final H2DemuxBudget budget;

	private final LegSlots legSlots;

	private final LegSeam legSeam;

	/**
	 * A leg holds the connection's own slot: open, or closed with frames still queued for
	 * the tunnel.
	 */
	private final AtomicBoolean ownSlotTaken = new AtomicBoolean();

	/**
	 * What holds the connection's own slot: the relay until it finished, and the leg
	 * holding it ({@link #ownSlotTaken}); zero = {@link #release} ran.
	 */
	private final AtomicInteger ownSlotHolders = new AtomicInteger(1);

	private final Loop loop;

	/**
	 * The part of the relay's queued bytes beyond its allowance, charged to the budget;
	 * with the loop's lock.
	 */
	private long pooled;

	/** No connection window is granted on any hop; with the loop's lock. */
	private boolean withheld;

	/** The bytes the relay queued at the last review; with the loop's lock. */
	private long queued;

	/**
	 * The bytes of the pages the relay answers streams with, until netty wrote them to
	 * the client hop's writer queue; with the loop's lock.
	 */
	private long pageBytes;

	/**
	 * The streams whose credential check runs ({@link #parkLocked}), by id; with the
	 * loop's lock.
	 */
	private final Map<Integer, NewStream> parked = new HashMap<>();

	/**
	 * What the parked streams hold -- heads, DATA, trailers --; with the loop's lock.
	 */
	private long parkedBytes;

	/** The connection window its peers held at the last review; with the loop's lock. */
	private long credit;

	/**
	 * The part of {@link #credit} past the floors, charged to the budget with the bytes
	 * queued past the allowance ({@link #pooled}); with the loop's lock.
	 */
	private long chargedCredit;

	/** The connection window granted the client; with the loop's lock. */
	private final HopWindow clientWindow = new HopWindow(CONNECTION_WINDOW);

	/** The accounts are closed: the relay finished; with the loop's lock. */
	private boolean accountsClosed;

	/** Reviews the accounts once the budget has room; one instance, to dedupe. */
	private final Runnable budgetRoom = this::reviewSoon;

	/** The client endpoint; set once in {@link #start()}. */
	private @Nullable Hop client;

	/** The leg taking new streams, by route; with the loop's lock. */
	private final Map<Router.Route, Leg> legs = new HashMap<>();

	/** Every leg opened; with the loop's lock. */
	private final List<Leg> opened = new ArrayList<>();

	/**
	 * The streams relayed to a leg, by id, until closed for the client; with the lock.
	 */
	private final Map<Integer, Exchange> exchanges = new HashMap<>();

	/** The highest stream id the client opened; with the loop's lock. */
	private int highestStreamId;

	/**
	 * The streams of every hop the relay reset itself or whose peer reset them, until
	 * they close: a stream closing with a reset sent and not in here was reset by netty's
	 * codec, on a stream error. With the loop's lock.
	 */
	private final Set<Http2Stream> resetsNotByCodec = Collections.newSetFromMap(new IdentityHashMap<>());

	/** The client hop, plus each leg until its threads ended; zero = finished. */
	private final AtomicInteger pending = new AtomicInteger(1);

	private final AtomicBoolean finished = new AtomicBoolean();

	private NettyHttp2DemuxRelay(Builder builder) {
		this.pipe = Objects.requireNonNull(builder.pipe, "pipe is required");
		ConnectionHeadParser.Head head = Objects.requireNonNull(builder.head, "head is required");
		this.seed = head.bytes();
		this.seedStreamId = head.h2() == null ? -1 : head.h2().streamId();
		this.router = Objects.requireNonNull(builder.router, "router is required");
		this.sessions = Objects.requireNonNull(builder.sessions, "sessions is required");
		this.accessControl = Objects.requireNonNull(builder.accessControl, "accessControl is required");
		this.routeAuth = Objects.requireNonNull(builder.routeAuth, "routeAuth is required");
		this.errorResponse = Objects.requireNonNull(builder.errorResponse, "errorResponse is required");
		this.access = Objects.requireNonNull(builder.access, "access is required");
		this.meters = Objects.requireNonNull(builder.meters, "meters is required");
		this.initialRoute = Objects.requireNonNull(builder.initialRoute, "initialRoute is required");
		this.initialSession = Objects.requireNonNull(builder.initialSession, "initialSession is required");
		this.peer = builder.peer;
		this.handshake = builder.handshake;
		this.deadline = Objects.requireNonNull(builder.deadline, "deadline is required");
		this.streamWaitTimeout = builder.streamWaitTimeout;
		if (this.streamWaitTimeout.isNegative()) {
			throw new IllegalArgumentException("streamWaitTimeout must not be negative");
		}
		this.probeInterval = builder.probeInterval;
		if (this.probeInterval.isNegative()) {
			throw new IllegalArgumentException("probeInterval must not be negative");
		}
		this.onFinish = Objects.requireNonNull(builder.onFinish, "onFinish is required");
		this.release = Objects.requireNonNull(builder.release, "release is required");
		this.budget = Objects.requireNonNull(builder.budget, "budget is required");
		this.legSlots = Objects.requireNonNull(builder.legSlots, "legSlots is required");
		this.legSeam = builder.legSeam;
		this.loop = new Loop("sluice-h2n",
				(source, cause) -> log.warn("h2 demux {}: {}", source, cause.toString(), cause));
		this.loop.afterSettle(this::reviewLocked);
		this.deadline.onExpiry(() -> tearDown(Http2Error.NO_ERROR));
		this.deadline.watchStallClock(this.clientWait::nanos, () -> tearDown(Http2Error.ENHANCE_YOUR_CALM));
	}

	static Builder builder() {
		return new Builder();
	}

	/**
	 * Starts the client endpoint over the connection head and the socket; the legs open
	 * as streams need them.
	 */
	void start() {
		Hop started;
		try {
			started = this.loop.call(() -> {
				Hop hop = Hop.builder()
					.name("sluice-h2n-client")
					.loop(this.loop)
					.pipe(seeded(this.pipe, this.seed))
					.server(true)
					.listener(ClientListener::new)
					.codec(codec -> configure(codec, new Http2Settings().maxConcurrentStreams(MAX_CONCURRENT_STREAMS),
							STREAM_WINDOW))
					// each reset the client has not read yet is of a stream it still
					// counts against the concurrency limit
					.rememberedResets(MAX_CONCURRENT_STREAMS)
					// granted as the review charges it
					.withholdConnectionWindow(true)
					.connectionWindow(CONNECTION_WINDOW)
					.onInactive(this::clientClosedLocked)
					.onEof(this::clientEndedLocked)
					.onWritten(NettyHttp2DemuxRelay::reviewed)
					// each piece the client takes is progress against the stall timeout
					.writePiece(ConnectionLimits.KeepAliveDeadline.WRITE_PIECE)
					.progress(this::clientTook)
					.build();
				this.client = hop;
				hop.addConnectionListener(new ClientStreams());
				return hop;
			});
		}
		catch (RuntimeException | Error e) {
			// the deadline watches the relay from its construction on
			this.deadline.disarm();
			this.loop.close();
			throw e;
		}
		watch(started, this::settle);
	}

	/**
	 * Both faces' codec: the settings plus the header list limit (netty's default is
	 * 8KiB) and the initial stream window ({@link #STREAM_WINDOW} to the client,
	 * {@link #LEG_STREAM_WINDOW} to an upstream), and header blocks encoded as
	 * never-indexed literals, so every block the relay writes decodes on its own whatever
	 * the peer's decoder saw before.
	 */
	private static void configure(HopHandler.Builder codec, Http2Settings settings, int streamWindow) {
		codec.initialSettings(settings.maxHeaderListSize(MAX_HEADER_LIST_SIZE).initialWindowSize(streamWindow))
			.headerSensitivityDetector((name, value) -> true);
	}

	/**
	 * Sets the streams the leg opens at once on its upstream, the rest buffered in the
	 * leg's encoder; with the loop's lock.
	 */
	private static void limitStreams(Hop leg, long maxConcurrentStreams) {
		try {
			leg.encoder().remoteSettings(new Http2Settings().maxConcurrentStreams(maxConcurrentStreams));
		}
		catch (Http2Exception e) {
			throw new IllegalStateException(leg.name() + ": the stream limit did not apply", e);
		}
	}

	private Hop client() {
		return Objects.requireNonNull(this.client, "the relay is not started");
	}

	/**
	 * The client pipe reading the connection head first, then the socket; writes,
	 * half-close and close stay the pipe's. The socket's EOF leaves the pipe open: a
	 * {@code SequenceInputStream} closes each stream it read to the end, which would
	 * close the socket under the responses a half-closed client still reads.
	 */
	private static DuplexPipe seeded(DuplexPipe pipe, byte[] seed) {
		InputStream socket = new FilterInputStream(pipe.source()) {

			@Override
			public void close() {
				// the pipe's close closes it
			}

		};
		InputStream source = new SequenceInputStream(new ByteArrayInputStream(seed), socket);
		return new DuplexPipe() {

			@Override
			public InputStream source() {
				return source;
			}

			@Override
			public OutputStream sink() {
				return pipe.sink();
			}

			@Override
			public void shutdownOutput() throws Exception {
				pipe.shutdownOutput();
			}

			@Override
			public void close() throws Exception {
				pipe.close();
			}

		};
	}

	/** Routes a new stream's head; with the loop's lock. */
	private void routeLocked(int streamId, Http2Headers headers, boolean endStream) {
		boolean seedStream = streamId == this.seedStreamId;
		this.seedStreamId = -1;
		ConnectionHeadParser.Head.@Nullable Request request = ConnectionHeadParser.http2RequestOf(headers);
		if (request == null) {
			// malformed: a stream error, never forwarded (RFC 9113 8.1.1)
			CharSequence method = headers.method();
			this.access.request("-", method == null ? "-" : method.toString(), "-", HTTP_2,
					ConnectionHeadParser.http2UserAgentOf(headers));
			resetClientLocked(streamId, Http2Error.PROTOCOL_ERROR);
			return;
		}
		Router.Route route;
		TunnelSession session;
		String host = hostOf(headers);
		if (seedStream) {
			// resolved with the connection; the lookup must not run twice
			route = this.initialRoute;
			session = this.initialSession;
		}
		else {
			Optional<Router.Route> found = this.router.lookup(host);
			TunnelSession serving = found.flatMap(candidate -> this.sessions.find(candidate.clientId())).orElse(null);
			if (found.isEmpty() || serving == null) {
				refuseLocked(streamId, request, this.errorResponse.noRoutePage(host, request));
				return;
			}
			if (!this.accessControl.allowed(found.get(), this.peer,
					headerOf(headers, ConnectionHeadParser.Head.FORWARDED_FOR),
					headerOf(headers, ConnectionHeadParser.Head.FORWARDED))) {
				refuseLocked(streamId, request, this.errorResponse.forbiddenPage(host, request));
				return;
			}
			route = found.get();
			session = serving;
		}
		NewStream stream = new NewStream(streamId, route, session, host, request, headers, endStream);
		CompletableFuture<Verdict> verdict = this.routeAuth.verifyAsync(route,
				ConnectionHeadParser.http2Headers(headers));
		if (verdict.isDone()) {
			verifiedLocked(stream, verdict);
		}
		else {
			parkLocked(stream, verdict);
		}
	}

	/**
	 * Goes on with a new stream once its credential check ended: a refused one is
	 * answered ({@code 401}, {@code 503}), one that passes loses the headers of its
	 * route's credentials and goes to its route's leg -- with what the client sent while
	 * it was parked --, or is answered {@code 421} for a {@code force-http1} route; a
	 * check that failed resets it ({@code INTERNAL_ERROR}). With the loop's lock.
	 */
	private void verifiedLocked(NewStream stream, CompletableFuture<Verdict> checked) {
		int streamId = stream.id;
		Router.Route route = stream.route;
		ConnectionHeadParser.Head.Request request = stream.request;
		Http2Headers headers = stream.headers;
		Verdict verdict;
		try {
			verdict = checked.join();
		}
		catch (CompletionException | CancellationException e) {
			Throwable cause = e.getCause() == null ? e : e.getCause();
			log.warn("h2 demux: the credential check of a stream of route {} failed: {}", route.routeTag(),
					cause.toString(), cause);
			releaseParkedDataLocked(stream);
			resetClientLocked(streamId, Http2Error.INTERNAL_ERROR);
			return;
		}
		if (!(verdict instanceof Verdict.Pass pass)) {
			this.access.refused(route.routeTag(), request, verdict);
			releaseParkedDataLocked(stream);
			Shortfall shortfall = answerLocked(streamId, this.errorResponse.refusalPage(stream.host, request, verdict),
					false);
			if (shortfall != null) {
				refuseForBudgetLocked(streamId, shortfall);
			}
			return;
		}
		for (String name : pass.strip()) {
			headers.remove(AsciiString.of(name));
		}
		if (route.forceHttp1()) {
			releaseParkedDataLocked(stream);
			misdirectLocked(streamId, stream.host, route, request, stream.requestEnded);
			return;
		}
		int headBytes = headerBytes(headers);
		Shortfall shortfall = admitLocked(headBytes);
		if (shortfall != null) {
			releaseParkedDataLocked(stream);
			refuseForBudgetLocked(streamId, shortfall);
			return;
		}
		Leg leg = legLocked(route, stream.session, streamId);
		if (leg == null) {
			releaseParkedDataLocked(stream);
			resetClientLocked(streamId, Http2Error.REFUSED_STREAM);
			return;
		}
		if (route.rewriteHost()) {
			headers.authority(AsciiString.of(route.address()));
		}
		int legId = leg.streamIdLocked(streamId);
		Exchange exchange = new Exchange(streamId, legId, leg, client());
		exchange.host = stream.host;
		exchange.request = request;
		exchange.requestEnded = stream.requestEnded;
		exchange.tunnel = CONNECT.equals(request.method());
		CharSequence expect = headers.get(EXPECT);
		exchange.continueAwaited = !stream.dataRead && expect != null
				&& AsciiString.contentEqualsIgnoreCase(expect, CONTINUE);
		exchange.responseFlow.onEndWritten(() -> responseWrittenLocked(exchange));
		this.exchanges.put(streamId, exchange);
		leg.streams.put(legId, exchange);
		leg.routeMeters.trackedStreams().increment();
		Hop hop = leg.hop();
		// the head is held until written: in the leg's buffer while the stream waits
		exchange.headBytes = headBytes;
		leg.backlog += headBytes;
		ChannelPromise written = hop.newPromise(future -> {
			leg.backlog -= exchange.headBytes;
			exchange.headBytes = 0;
			if (!future.isSuccess()) {
				// the leg never took the stream -- it failed it, or dropped it from its
				// buffer on the upstream's GOAWAY or its close: safe for the client to
				// retry
				stopWaitingLocked(exchange);
				exchange.refused = true;
				loseLocked(exchange);
			}
		});
		hop.encoder().writeHeaders(hop.ctx(), legId, headers, 0, stream.headEnds, written);
		if (!written.isDone() && hop.connection().stream(legId) == null) {
			// past the upstream's concurrency limit: buffered until a stream of the leg
			// closes
			waitLocked(exchange);
		}
		if (exchange.reset) {
			releaseParkedDataLocked(stream);
		}
		else {
			// what the client sent while the stream was parked follows its head
			if (stream.dataRead) {
				exchange.requestFlow.parked(stream.data, stream.dataEnds);
			}
			Http2Headers trailers = stream.trailers;
			if (trailers != null) {
				exchange.requestFlow.block(trailers, stream.trailersEnd);
			}
		}
		this.access.request(route.routeTag(), request);
	}

	/**
	 * Parks a new stream while its credential check runs off the loop: its head is
	 * admitted like a new stream's -- one that does not fit is refused
	 * ({@code REFUSED_STREAM}) --, and what the client sends on it meanwhile is held in
	 * the relay's account (its DATA not consumed, so within the stream window the relay
	 * announced). The check's end resumes it on the loop ({@link #verifiedLocked}); it is
	 * refused ({@code REFUSED_STREAM}) at the wait deadline, or like a stream waiting for
	 * its upstream's concurrency limit while the windows are withheld
	 * ({@link #refuseWaitingLocked}). A parked stream the client resets or that closes
	 * otherwise, or the client endpoint closing, drops it: the check's end then finds
	 * nothing to resume. With the loop's lock.
	 */
	private void parkLocked(NewStream stream, CompletableFuture<Verdict> verdict) {
		Shortfall shortfall = admitLocked(stream.headBytes);
		if (shortfall != null) {
			verdict.cancel(false);
			refuseForBudgetLocked(stream.id, shortfall);
			return;
		}
		stream.verdict = verdict;
		this.parked.put(stream.id, stream);
		this.parkedBytes += stream.headBytes;
		if (!this.streamWaitTimeout.isZero()) {
			// on the client hop's executor, whose settle runs the task
			stream.deadline = client().ctx()
				.executor()
				.schedule(() -> parkTimedOutLocked(stream), this.streamWaitTimeout.toNanos(), TimeUnit.NANOSECONDS);
		}
		// last: a check ending meanwhile resumes the stream at once
		verdict.whenComplete((result, failure) -> {
			try {
				this.loop.run(() -> {
					if (unparkLocked(stream)) {
						verifiedLocked(stream, verdict);
					}
				});
			}
			catch (RuntimeException e) {
				// on the check's thread, which has no one to tell
				log.warn("h2 demux: resuming a parked stream failed: {}", e.toString(), e);
			}
		});
	}

	/**
	 * DATA read on a parked stream: held, not consumed -- its stream window is not
	 * granted back while the stream is parked. With the loop's lock.
	 */
	private void parkDataLocked(NewStream stream, ByteBuf data, boolean endStream) {
		int before = stream.data.capacity();
		stream.data.append(data);
		this.parkedBytes += stream.data.capacity() - before;
		stream.dataRead = true;
		stream.dataEnds |= endStream;
		stream.requestEnded |= endStream;
	}

	/**
	 * The trailers of a parked stream: held once admitted, like an exchange's; ones that
	 * do not fit reset the stream ({@code ENHANCE_YOUR_CALM}). With the loop's lock.
	 */
	private void parkTrailersLocked(NewStream stream, Http2Headers trailers, boolean endStream) {
		int bytes = headerBytes(trailers);
		Shortfall shortfall = admitLocked(bytes);
		if (shortfall != null) {
			dropParkedLocked(stream);
			// counted first: the reset goes out at once
			if (shortfall == Shortfall.SHARE) {
				this.meters.resetForShare();
			}
			else {
				this.meters.resetForBudget();
			}
			resetClientLocked(stream.id, Http2Error.ENHANCE_YOUR_CALM);
			return;
		}
		stream.trailers = trailers;
		stream.trailersEnd = endStream;
		stream.trailerBytes = bytes;
		this.parkedBytes += bytes;
		stream.requestEnded |= endStream;
	}

	/**
	 * The wait deadline of a parked stream whose check has not ended: it is refused
	 * ({@code REFUSED_STREAM}: no upstream saw it), the check left to run to its end.
	 * With the loop's lock.
	 */
	private void parkTimedOutLocked(NewStream stream) {
		stream.deadline = null;
		if (this.parked.get(stream.id) != stream) {
			return;
		}
		this.meters.refusedForWaitTimeout();
		dropParkedLocked(stream);
		resetClientLocked(stream.id, Http2Error.REFUSED_STREAM);
	}

	/**
	 * The stream leaves the parked ones: its deadline is cancelled and what it holds
	 * leaves the account (its DATA stays with it). Whether it was parked; with the loop's
	 * lock.
	 */
	private boolean unparkLocked(NewStream stream) {
		if (!this.parked.remove(stream.id, stream)) {
			return false;
		}
		this.parkedBytes -= stream.heldBytes();
		ScheduledFuture<?> deadline = stream.deadline;
		if (deadline != null) {
			stream.deadline = null;
			deadline.cancel(false);
		}
		return true;
	}

	/**
	 * Drops a parked stream: it leaves the parked ones and its check's verdict is
	 * cancelled (the check itself runs to its end). With the loop's lock.
	 */
	private void dropParkedLocked(NewStream stream) {
		if (!unparkLocked(stream)) {
			return;
		}
		stream.data.clear();
		CompletableFuture<Verdict> verdict = stream.verdict;
		if (verdict != null) {
			verdict.cancel(false);
		}
	}

	/**
	 * Returns the DATA the client sent while the stream was parked to the client's
	 * windows: the stream is not relayed. With the loop's lock.
	 */
	private void releaseParkedDataLocked(NewStream stream) {
		consume(client(), stream.id, stream.data.size());
		stream.data.clear();
	}

	/**
	 * The exchange's stream waits in its leg's buffer; past the wait deadline it is
	 * refused. The deadline runs on the client hop's executor, whose settle runs the
	 * task. With the loop's lock.
	 */
	private void waitLocked(Exchange exchange) {
		exchange.waiting = true;
		if (this.streamWaitTimeout.isZero()) {
			return;
		}
		exchange.waitDeadline = client().ctx()
			.executor()
			.schedule(() -> waitTimedOutLocked(exchange), this.streamWaitTimeout.toNanos(), TimeUnit.NANOSECONDS);
	}

	/**
	 * The exchange's stream no longer waits -- the leg opened it, dropped it, or the
	 * relay reset it: its deadline is cancelled. With the loop's lock.
	 */
	private static void stopWaitingLocked(Exchange exchange) {
		exchange.waiting = false;
		ScheduledFuture<?> deadline = exchange.waitDeadline;
		if (deadline != null) {
			exchange.waitDeadline = null;
			deadline.cancel(false);
		}
	}

	/**
	 * The wait deadline of a stream still waiting: the stream leaves the leg's buffer
	 * unseen by the upstream, its queued DATA released, and the client gets RST_STREAM
	 * {@code REFUSED_STREAM}, safe to retry (RFC 9113 8.7). With the loop's lock.
	 */
	private void waitTimedOutLocked(Exchange exchange) {
		exchange.waitDeadline = null;
		if (!exchange.waiting || exchange.reset) {
			return;
		}
		exchange.reset = true;
		exchange.refused = true;
		this.meters.refusedForWaitTimeout();
		dropWaitingLocked(exchange, Http2Error.CANCEL.code());
		resetClientLocked(exchange.id, Http2Error.REFUSED_STREAM);
	}

	/**
	 * Drops a waiting stream from its leg's buffer: netty's buffering encoder removes it
	 * on a reset, completing its frames' promises and releasing its DATA, and writes
	 * nothing. With the loop's lock.
	 */
	private void dropWaitingLocked(Exchange exchange, long errorCode) {
		stopWaitingLocked(exchange);
		Hop leg = exchange.leg.hop();
		Http2Stream stream = leg.connection().stream(exchange.legId);
		if (stream != null) {
			// opened meanwhile: the buffering encoder resets it for real
			this.resetsNotByCodec.add(stream);
		}
		leg.encoder().writeRstStream(leg.ctx(), exchange.legId, errorCode, leg.ctx().newPromise());
	}

	/**
	 * Resets the exchange's stream for a header block that does not fit
	 * ({@link #admitLocked}) and cannot be refused without leaving the stream unfinished:
	 * {@code ENHANCE_YOUR_CALM} to the client, {@code CANCEL} to the upstream. With the
	 * loop's lock.
	 */
	private void resetForBudgetLocked(Exchange exchange, Shortfall shortfall) {
		exchange.reset = true;
		// counted first: the resets go out at once
		if (shortfall == Shortfall.SHARE) {
			this.meters.resetForShare();
		}
		else {
			this.meters.resetForBudget();
		}
		resetClientLocked(exchange.id, Http2Error.ENHANCE_YOUR_CALM);
		resetLocked(exchange.leg.hop(), exchange.legId, Http2Error.CANCEL.code());
	}

	/** Whether a response header block is an interim (1xx) head. */
	private static boolean interim(Http2Headers headers) {
		CharSequence status = headers.status();
		return status != null && HttpStatusClass.valueOf(status) == HttpStatusClass.INFORMATIONAL;
	}

	/**
	 * Refuses a new stream whose head or page does not fit ({@link #admitLocked}) with
	 * {@code REFUSED_STREAM}: no upstream saw it, safe for the client to retry. With the
	 * loop's lock.
	 */
	private void refuseForBudgetLocked(int streamId, Shortfall shortfall) {
		// counted first, as the reset goes out at once
		if (shortfall == Shortfall.SHARE) {
			this.meters.refusedForShare();
		}
		else {
			this.meters.refusedForBudget();
		}
		resetClientLocked(streamId, Http2Error.REFUSED_STREAM);
	}

	/**
	 * Answers a refused stream with the page, or refuses it with {@code REFUSED_STREAM}
	 * when the page does not fit; with the loop's lock.
	 */
	private void refuseLocked(int streamId, ConnectionHeadParser.Head.Request request, ErrorResponse.StreamPage page) {
		this.access.request("-", request);
		Shortfall shortfall = answerLocked(streamId, page, false);
		if (shortfall != null) {
			refuseForBudgetLocked(streamId, shortfall);
		}
	}

	/**
	 * Answers a stream of a route reached over HTTP/1.1 only {@code 421}; a request still
	 * being sent is reset with {@code NO_ERROR} once the answer is written (RFC 9113
	 * 8.1). A connection whose handshake was the host's own and offered http/1.1 also
	 * gets a GOAWAY {@code NO_ERROR}: a new connection for the host would be given
	 * http/1.1, and Chromium's retry of the 421 passes over coalesced connections only,
	 * so it would come back here. With the loop's lock.
	 */
	private void misdirectLocked(int streamId, @Nullable String host, Router.Route route,
			ConnectionHeadParser.Head.Request request, boolean endStream) {
		this.meters.misdirectedForHttp1();
		this.access.misdirected(route.routeTag(), request);
		TlsHandshake handshake = this.handshake;
		Hop client = client();
		if (handshake != null && handshake.http1Offered() && handshake.isFor(host)
				&& !client.connection().goAwaySent()) {
			client.handler()
				.goAway(client.ctx(), this.highestStreamId, Http2Error.NO_ERROR.code(), Unpooled.EMPTY_BUFFER,
						client.ctx().newPromise());
		}
		Shortfall shortfall = answerLocked(streamId,
				new ErrorResponse.StreamPage(this.errorResponse.misdirectedHeaders(), new byte[0]), !endStream);
		if (shortfall != null) {
			refuseForBudgetLocked(streamId, shortfall);
		}
	}

	/**
	 * Answers the client's stream with the page, its body through the flow controller --
	 * held to the client's windows; with {@code endsRequest}, a request still being sent
	 * is then reset with {@code NO_ERROR} once the page's end is written (RFC 9113 8.1:
	 * no more of it is wanted). The page -- its head as HPACK counts it, and its body --
	 * is admitted first ({@link #admitLocked}) and held until netty wrote it to the
	 * client hop's writer queue ({@link #pageBytes}). What fell short, {@code null} when
	 * answered; with the loop's lock.
	 */
	private @Nullable Shortfall answerLocked(int streamId, ErrorResponse.StreamPage page, boolean endsRequest) {
		byte[] body = page.body();
		int headBytes = headerBytes(page.headers());
		Shortfall shortfall = admitLocked((long) headBytes + body.length);
		if (shortfall != null) {
			return shortfall;
		}
		this.pageBytes += (long) headBytes + body.length;
		Hop hop = client();
		ChannelFutureListener ended = future -> {
			if (endsRequest && future.isSuccess()) {
				resetRequestLocked(streamId, Http2Error.NO_ERROR.code());
			}
		};
		hop.encoder().writeHeaders(hop.ctx(), streamId, page.headers(), 0, body.length == 0, hop.newPromise(future -> {
			this.pageBytes -= headBytes;
			if (body.length == 0) {
				ended.operationComplete(future);
			}
		}));
		if (body.length > 0) {
			hop.encoder()
				.writeData(hop.ctx(), streamId, Unpooled.wrappedBuffer(body), 0, true, hop.newPromise(future -> {
					this.pageBytes -= body.length;
					ended.operationComplete(future);
				}));
		}
		return null;
	}

	/**
	 * The leg taking the route's new streams: the open one, else a new one on a slot of
	 * the connection limit ({@link #acquireSlotLocked}); null when the limit has no slot
	 * left or the leg could not be opened. With the loop's lock.
	 */
	private @Nullable Leg legLocked(Router.Route route, TunnelSession session, int clientStreamId) {
		Leg leg = this.legs.get(route);
		if (leg != null && leg.takesStreamsLocked(clientStreamId)) {
			return leg;
		}
		Runnable releaseSlot = acquireSlotLocked();
		if (releaseSlot == null) {
			this.meters.refusedForLegLimit();
			return null;
		}
		VirtualConnection connection;
		try {
			connection = this.legSeam.open(session, route.address());
		}
		catch (RuntimeException e) {
			releaseSlot.run();
			log.warn("h2 demux: the leg of route {} did not open: {}", route.routeTag(), e.toString(), e);
			return null;
		}
		Leg opening = new Leg(route.routeTag(), session, this.meters.route(route.routeTag()),
				this.meters.writesHeldOf(route.routeTag()));
		try {
			Hop hop = Hop.builder()
				.name("sluice-h2n-leg-" + connection.connectionId())
				.loop(this.loop)
				.pipe(DuplexPipe.of(connection))
				.listener(built -> new LegListener(opening))
				.codec(codec -> {
					configure(codec, new Http2Settings().pushEnabled(false), LEG_STREAM_WINDOW);
					// streams past the upstream's concurrency limit wait in netty's
					// StreamBufferingEncoder
					codec.encoderEnforceMaxConcurrentStreams(true);
				})
				// a leg's streams relay the client's
				.rememberedResets(MAX_CONCURRENT_STREAMS)
				// granted as the review charges it
				.withholdConnectionWindow(true)
				.connectionWindow(LEG_CONNECTION_WINDOW)
				.onInactive(() -> this.legs.remove(route, opening))
				.onWritten(NettyHttp2DemuxRelay::reviewed)
				.traffic(traffic(route, session))
				.build();
			opening.hop = hop;
			hop.watchWrites(watchingWrites());
			hop.addConnectionListener(new LegStreams(opening));
			this.legSeam.built(hop);
			// one stream until the upstream's SETTINGS tell its limit (RFC 9113 5.1.2
			// leaves it open until then); through the encoder, as the buffering encoder
			// keeps a limit of its own (100 until the peer's SETTINGS) and takes the
			// connection's only in remoteSettings
			limitStreams(hop, 1);
		}
		catch (RuntimeException e) {
			Hop built = opening.hop;
			if (built != null) {
				built.abortLocked();
			}
			log.warn("h2 demux: the leg of route {} did not open: {}", route.routeTag(), e.toString(), e);
			connection.close();
			session.remove(connection.connectionId());
			connection.afterQueued(releaseSlot);
			return null;
		}
		if (this.opened.isEmpty()) {
			this.access.connectionId(connection.connectionId());
		}
		this.opened.add(opening);
		opening.routeMeters.legs().increment();
		this.legs.put(route, opening);
		this.pending.incrementAndGet();
		session.onWithdrawn(connection.connectionId(), this::reviewSoon);
		watch(opening.hop(), () -> {
			try {
				connection.close();
				session.remove(connection.connectionId());
				// a writer that ended on a failed write dropped its queue unreviewed: the
				// review sees it gone and lets go of the leg once it holds nothing
				this.loop.run(NettyHttp2DemuxRelay::reviewed);
			}
			finally {
				try {
					// the slot covers what the leg still queues for the tunnel
					connection.afterQueued(releaseSlot);
				}
				finally {
					settle();
				}
			}
		});
		return opening;
	}

	/**
	 * A slot for a new leg: the connection's own while no leg holds it, else one of its
	 * own; what frees it, or null when the connection limit has no slot left. With the
	 * loop's lock.
	 */
	private @Nullable Runnable acquireSlotLocked() {
		if (this.ownSlotTaken.compareAndSet(false, true)) {
			this.ownSlotHolders.incrementAndGet();
			return () -> {
				this.ownSlotTaken.set(false);
				releaseOwnSlot();
			};
		}
		ConnectionReserve.Reservation reservation = this.legSlots.tryAcquire();
		return reservation == null ? null : reservation::release;
	}

	/**
	 * Counts the bytes the route's leg reads (towards the client, {@code outbound}) and
	 * writes (towards the upstream, {@code inbound}) as the route's tunnel bytes, the
	 * session's traffic and the connection's access log bytes.
	 */
	private Hop.Traffic traffic(Router.Route route, TunnelSession session) {
		Counter inbound = this.meters.registry()
			.counter(TUNNEL_BYTES, "direction", "inbound", "route", route.routeTag());
		Counter outbound = this.meters.registry()
			.counter(TUNNEL_BYTES, "direction", "outbound", "route", route.routeTag());
		AccessLogger.Connection access = this.access;
		return new Hop.Traffic() {

			@Override
			public void read(long bytes) {
				outbound.increment(bytes);
				session.recordRelayed(bytes, StreamRelay.Direction.TO_LOCAL);
				access.bytes(bytes, StreamRelay.Direction.TO_LOCAL);
			}

			@Override
			public void written(long bytes) {
				inbound.increment(bytes);
				session.recordRelayed(bytes, StreamRelay.Direction.TO_REMOTE);
				access.bytes(bytes, StreamRelay.Direction.TO_REMOTE);
			}

		};
	}

	/** A leg left {@link #opened}: its route's meters stop counting it. */
	private static void forgottenLocked(Leg leg) {
		leg.gauge(0);
		leg.routeMeters.legs().decrement();
	}

	/**
	 * The client endpoint closed -- before netty closes its streams: nothing more of the
	 * client's streams is relayed, the parked streams are dropped (their checks' ends
	 * resume nothing), each stream a leg still has is cancelled ({@code CANCEL}; one
	 * waiting in the leg's buffer leaves it unseen), then the legs close (closed already
	 * after a drain), and the deadline ends: nothing is left to wait on the client for.
	 * With the loop's lock.
	 */
	private void clientClosedLocked() {
		this.deadline.disarm();
		for (NewStream parked : List.copyOf(this.parked.values())) {
			dropParkedLocked(parked);
		}
		for (Exchange exchange : List.copyOf(this.exchanges.values())) {
			exchange.reset = true;
			if (exchange.waiting) {
				// first: closing a stream below would open a waiting one on its upstream
				dropWaitingLocked(exchange, Http2Error.CANCEL.code());
			}
		}
		for (Leg leg : this.opened) {
			Hop hop = leg.hop();
			for (Http2Stream stream : activeStreams(hop)) {
				resetLocked(hop, stream.id(), Http2Error.CANCEL.code());
			}
			hop.abortLocked();
		}
	}

	/** The hop's active streams, copied; with the loop's lock. */
	private static List<Http2Stream> activeStreams(Hop hop) {
		List<Http2Stream> streams = new ArrayList<>();
		activeStream(hop, stream -> {
			streams.add(stream);
			return false;
		});
		return streams;
	}

	/**
	 * The client ended its side of the connection (EOF). HTTP/2 has no half-closed
	 * connection, yet a client may half-close its socket and still read: with a request
	 * unfinished -- a stream whose client side is still open, which can never complete
	 * now -- the client endpoint closes at once; otherwise the relay drains: the
	 * responses owed are written ({@link #drainLocked}), and the client is probed with a
	 * PING after each probe interval without a write to it, so a client gone fails a
	 * write and ends the relay. With the loop's lock.
	 */
	private void clientEndedLocked() {
		Hop client = client();
		if (activeStream(client, stream -> stream.state().remoteSideOpen()) != null) {
			client.abortLocked();
			return;
		}
		this.clientEnded = true;
		probeLaterLocked(this.probeInterval.toNanos());
	}

	/** The client took a piece of what the relay wrote; on the client hop's writer. */
	private void clientTook(long bytes) {
		this.lastClientWrite = System.nanoTime();
		this.deadline.progress(bytes);
	}

	/**
	 * After the client's EOF: each leg with no stream left -- none open on its upstream
	 * (its request bytes written), none waiting for one -- closes (its upstream sees the
	 * end once the frames queued for it are out), and once the client has no stream open
	 * and every leg closed, the client endpoint closes too. Until then the client stays
	 * probed, and with no stream of its own open it idles under the idle timeout. Whether
	 * it closed anything; with the loop's lock.
	 */
	private boolean drainLocked(Hop client) {
		boolean acted = false;
		boolean legsOpen = false;
		for (Leg leg : this.opened) {
			Hop hop = leg.hop();
			if (hop.open() && hop.connection().numActiveStreams() == 0 && !waitsInLocked(leg)) {
				hop.closeLocked();
				acted = true;
			}
			legsOpen |= hop.open();
		}
		if (!legsOpen && client.connection().numActiveStreams() == 0) {
			this.drained = true;
			client.closeLocked();
			acted = true;
		}
		return acted;
	}

	/**
	 * Closes each leg whose client withdrew and that has no stream left: none open on its
	 * upstream, none waiting in its buffer, none parked for its route (a parked stream
	 * goes to the route's leg once its check ended). Its upstream sees the end once the
	 * frames queued for it are out. Whether it closed anything; with the loop's lock.
	 */
	private boolean retireWithdrawnLocked() {
		boolean acted = false;
		for (Leg leg : this.opened) {
			Hop hop = leg.hop();
			if (leg.session.withdrawn() && hop.open() && hop.connection().numActiveStreams() == 0 && !waitsInLocked(leg)
					&& !parkedForLocked(leg)) {
				log.debug("h2 demux: the client of route {} withdrew; closing its idle leg", leg.routeTag);
				hop.closeLocked();
				acted = true;
			}
		}
		return acted;
	}

	/** Whether a parked stream is bound for the leg; with the loop's lock. */
	private boolean parkedForLocked(Leg leg) {
		for (NewStream stream : this.parked.values()) {
			if (this.legs.get(stream.route) == leg) {
				return true;
			}
		}
		return false;
	}

	/** Whether a stream waits in the leg's buffer; with the loop's lock. */
	private boolean waitsInLocked(Leg leg) {
		for (Exchange exchange : this.exchanges.values()) {
			if (exchange.leg == leg && exchange.waiting) {
				return true;
			}
		}
		return false;
	}

	/** Schedules the next probe of a half-closed client; with the loop's lock. */
	private void probeLaterLocked(long delayNanos) {
		if (this.probeInterval.isZero()) {
			return;
		}
		client().ctx().executor().schedule(this::probeLocked, delayNanos, TimeUnit.NANOSECONDS);
	}

	/**
	 * Probes the half-closed client with a PING once nothing was written to it for the
	 * probe interval and nothing waits to be (a write blocked on the client is the stall
	 * timeout's): a client gone answers with a reset, failing the next write. With the
	 * loop's lock.
	 */
	private void probeLocked() {
		Hop client = client();
		if (!client.open() || this.drained) {
			return;
		}
		long interval = this.probeInterval.toNanos();
		long quiet = System.nanoTime() - this.lastClientWrite;
		if (quiet < interval || client.queuedBytes() > 0) {
			probeLaterLocked(Math.max(interval - quiet, TimeUnit.MILLISECONDS.toNanos(1)));
			return;
		}
		client.encoder().writePing(client.ctx(), false, PROBE_PAYLOAD, client.ctx().newPromise());
		probeLaterLocked(interval);
	}

	/**
	 * The idle or the stall timeout passed (on a thread of the deadline's): the client
	 * gets a GOAWAY of {@code error} naming the highest stream it opened, and the client
	 * endpoint closes, ending the relay. Returns once the client hop's threads ended, or
	 * after the teardown grace -- a write the client does not read holds its writer --,
	 * whereupon the deadline closes the transport under it.
	 */
	private void tearDown(Http2Error error) {
		Hop client = this.client;
		if (client == null) {
			return;
		}
		this.loop.run(() -> {
			if (!client.open()) {
				return;
			}
			client.handler()
				.goAway(client.ctx(), this.highestStreamId, error.code(), Unpooled.EMPTY_BUFFER,
						client.ctx().newPromise());
			// out before the close, which drops what the channel did not flush
			client.ctx().flush();
			client.abortLocked();
		});
		try {
			client.awaitDone(ConnectionLimits.KeepAliveDeadline.TEARDOWN_GRACE);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	/** The first active stream of the hop matching the test, or null; with the lock. */
	private static @Nullable Http2Stream activeStream(Hop hop, Predicate<Http2Stream> test) {
		try {
			return hop.connection().forEachActiveStream(stream -> !test.test(stream));
		}
		catch (Http2Exception e) {
			throw new IllegalStateException(e);
		}
	}

	/**
	 * The exchange's leg stream ended without the exchange's end: the leg closed under
	 * it, the upstream or netty reset it, a GOAWAY dropped it, or the leg never took it.
	 * Unless the exchange was reset already, the client gets: for a response the upstream
	 * ended, its end first, then RST_STREAM {@code NO_ERROR} if the client still sends
	 * its request; for a response whose (final) head was relayed, RST_STREAM
	 * {@code INTERNAL_ERROR}; for a stream the upstream never saw
	 * ({@link Exchange#refused}), RST_STREAM {@code REFUSED_STREAM}, safe to retry (RFC
	 * 9113 8.7); else the bad-gateway page, then RST_STREAM {@code NO_ERROR} if the
	 * client still sends its request. With the loop's lock.
	 */
	private void loseLocked(Exchange exchange) {
		if (exchange.reset) {
			return;
		}
		if (exchange.responseEnded) {
			resetAfterResponseLocked(exchange, Http2Error.NO_ERROR.code());
			return;
		}
		exchange.reset = true;
		Http2Stream stream = client().connection().stream(exchange.id);
		if (stream == null || stream.state() == Http2Stream.State.CLOSED) {
			return;
		}
		if (stream.isHeadersSent()) {
			// a final head: interim (1xx) ones leave the response to come
			resetClientLocked(exchange.id, Http2Error.INTERNAL_ERROR);
		}
		else if (exchange.refused) {
			resetClientLocked(exchange.id, Http2Error.REFUSED_STREAM);
		}
		else {
			Shortfall shortfall = answerLocked(exchange.id,
					this.errorResponse.badGatewayPage(exchange.host, exchange.request), true);
			if (shortfall != null) {
				// counted first: the reset goes out at once
				if (shortfall == Shortfall.SHARE) {
					this.meters.resetForShare();
				}
				else {
					this.meters.resetForBudget();
				}
				resetClientLocked(exchange.id, Http2Error.ENHANCE_YOUR_CALM);
			}
		}
	}

	/**
	 * Resets the client's stream of an exchange whose response the upstream ended with
	 * the code once that end is written -- a reset written before would drop what of the
	 * response waits for the client's window -- unless the client ended its request (its
	 * stream closes with the response's end). The exchange takes no more of either side.
	 * With the loop's lock.
	 */
	private void resetAfterResponseLocked(Exchange exchange, long errorCode) {
		exchange.reset = true;
		if (exchange.requestEnded) {
			return;
		}
		if (exchange.responseWritten) {
			resetRequestLocked(exchange.id, errorCode);
		}
		else {
			exchange.resetAfterResponse = errorCode;
		}
	}

	/**
	 * The client write carrying the response's end completed (or failed): a reset held
	 * for it goes out now. With the loop's lock.
	 */
	private void responseWrittenLocked(Exchange exchange) {
		exchange.responseWritten = true;
		Long errorCode = exchange.resetAfterResponse;
		if (errorCode != null) {
			exchange.resetAfterResponse = null;
			resetRequestLocked(exchange.id, errorCode);
		}
	}

	private void resetClientLocked(int streamId, Http2Error error) {
		resetLocked(client(), streamId, error.code());
	}

	/**
	 * Resets the client's stream while the client still sends its request -- a stream the
	 * client ended closes with the response's end (netty closes it right after the
	 * write's listeners ran). With the loop's lock.
	 */
	private void resetRequestLocked(int streamId, long errorCode) {
		Hop hop = client();
		Http2Stream stream = hop.connection().stream(streamId);
		if (stream != null && stream.state().remoteSideOpen()) {
			this.resetsNotByCodec.add(stream);
			hop.encoder().writeRstStream(hop.ctx(), streamId, errorCode, hop.ctx().newPromise());
		}
	}

	/** Resets the hop's stream unless it is closed or gone; with the loop's lock. */
	private void resetLocked(Hop hop, int streamId, long errorCode) {
		Http2Stream stream = hop.connection().stream(streamId);
		if (stream == null || stream.state() == Http2Stream.State.CLOSED) {
			// netty would write a reset of an unknown stream all the same
			return;
		}
		this.resetsNotByCodec.add(stream);
		hop.encoder().writeRstStream(hop.ctx(), streamId, errorCode, hop.ctx().newPromise());
	}

	/**
	 * The peer reset the hop's stream, which netty closes right after the listener: marks
	 * the reset as sent, so netty does not answer it with one of its own (RFC 9113 5.4.2)
	 * -- as it would with RST_STREAM {@code STREAM_CLOSED} when closing fails a write of
	 * the stream still waiting for its window. With the loop's lock.
	 */
	private void resetByPeerLocked(Hop hop, int streamId) {
		Http2Stream stream = hop.connection().stream(streamId);
		if (stream != null) {
			this.resetsNotByCodec.add(stream);
			stream.resetSent();
		}
	}

	/**
	 * Whether the stream, closing, was reset by netty's codec on a stream error: a reset
	 * sent that neither the relay nor the peer made. Forgets the stream; with the loop's
	 * lock.
	 */
	private boolean resetByCodecLocked(Http2Stream stream) {
		return !this.resetsNotByCodec.remove(stream) && stream.isResetSent();
	}

	/**
	 * Counts DATA read from the hop's peer in its window, and sends the peer the PING
	 * measuring its round trip when the window wants one ({@link HopWindow#wantsPing}).
	 * With the loop's lock.
	 */
	private static void measureLocked(Hop hop, HopWindow window, int bytes) {
		if (window.wantsPing(bytes) && hop.open()) {
			hop.encoder().writePing(hop.ctx(), false, ROUND_TRIP_PAYLOAD, hop.ctx().newPromise());
			window.pinged();
		}
	}

	/** Returns DATA to the reading hop's window once the other hop wrote it. */
	private static void consume(Hop hop, int streamId, int bytes) {
		Http2Stream stream = hop.connection().stream(streamId);
		if (stream == null || bytes == 0) {
			return; // closed: netty returned its unconsumed bytes to the connection
		}
		try {
			hop.localFlow().consumeBytes(stream, bytes);
		}
		catch (Http2Exception e) {
			throw new IllegalStateException(e);
		}
	}

	/**
	 * The size of a header list as HPACK counts it (RFC 7541 4.1): what the relay
	 * accounts for a header block it holds.
	 */
	static int headerBytes(Http2Headers headers) {
		long bytes = 0;
		for (Map.Entry<CharSequence, CharSequence> header : headers) {
			bytes += header.getKey().length() + header.getValue().length() + 32;
		}
		return (int) Math.min(bytes, Integer.MAX_VALUE);
	}

	/**
	 * Whether {@code bytes} more of a header block may be held: within what is charged
	 * already, or charged to the budget now if the share it makes stays within
	 * {@link #BLOCK_SHARE} and the budget has room for it. What fell short, {@code null}
	 * when admitted; with the loop's lock.
	 */
	private @Nullable Shortfall admitLocked(long bytes) {
		long share = share(queuedLocked() + bytes, this.chargedCredit);
		long change = share - this.pooled;
		if (change <= 0) {
			return null;
		}
		if (pastBlockShare(share)) {
			return Shortfall.SHARE;
		}
		if (!this.budget.tryCharge(change)) {
			return Shortfall.BUDGET;
		}
		this.pooled += change;
		return null;
	}

	/**
	 * Whether an upstream's header block of {@code bytes} may be written on the client
	 * hop: while the header blocks heading for the client -- those the hop queues and
	 * those of the responses not yet in it -- stay within
	 * {@link #RESPONSE_BLOCK_ALLOWANCE}, so a response is not held back for bytes queued
	 * the other way; past it, when {@link #admitLocked} admits it. What fell short,
	 * {@code null} when admitted; with the loop's lock.
	 */
	private @Nullable Shortfall admitResponseBlockLocked(long bytes) {
		long towardsClient = client().blockBytes();
		for (Exchange exchange : this.exchanges.values()) {
			towardsClient += exchange.responseFlow.blockBytes;
		}
		return towardsClient + bytes <= RESPONSE_BLOCK_ALLOWANCE ? null : admitLocked(bytes);
	}

	/**
	 * The bytes the relay holds: every leg's backlog (what it read and has not written on
	 * the other hop yet, both ways), the parked streams, the pages not yet written and
	 * every hop's writer queue. Updates the route gauges and lets go of the legs that
	 * closed holding nothing; with the loop's lock.
	 */
	private long queuedLocked() {
		Hop client = this.client;
		long queued = this.pageBytes + this.parkedBytes + (client == null ? 0 : client.queuedBytes());
		for (Iterator<Leg> legs = this.opened.iterator(); legs.hasNext();) {
			Leg leg = legs.next();
			Hop hop = leg.hop();
			long held = leg.backlog + hop.queuedBytes();
			queued += held;
			leg.gauge(held);
			if (held == 0 && !hop.open()) {
				legs.remove();
				forgottenLocked(leg);
			}
		}
		return queued;
	}

	/**
	 * Reviews the accounts once the channels settled (the loop's hook): the share -- the
	 * queued bytes beyond the allowance and the open hops' peers' connection window past
	 * RFC 9113's -- is charged to the budget or released; past the allowance with the
	 * budget exhausted, or with the share at {@link #CONNECTION_SHARE}, no hop's
	 * connection window is granted -- the peers stop sending DATA once the windows they
	 * hold are spent -- and the waiting streams past the allowance are refused
	 * ({@link #refuseWaitingLocked}); otherwise each hop is granted what its window
	 * decides ({@link #grantLocked}). Only a relay held back by the budget waits for its
	 * room; one held back by its share is reviewed again as its own hops drain. Whether
	 * it wrote anything; with the loop's lock.
	 */
	private boolean reviewLocked() {
		if (this.accountsClosed || this.client == null) {
			return false;
		}
		// watched before the queues are read: a writer emptying its queue after the
		// reading below runs the review again
		this.meters.reviewed();
		watchWritesLocked(true);
		long queued = queuedLocked();
		this.meters.held(queued - this.queued);
		this.queued = queued;
		long credit = creditLocked();
		this.meters.credit(credit - this.credit);
		this.credit = credit;
		settleShareLocked(queued, chargedCreditLocked());
		boolean wrote = false;
		boolean exhausted = queued > CONNECTION_ALLOWANCE && !this.budget.hasRoom();
		if (exhausted || atShare(this.pooled)) {
			if (!this.withheld) {
				this.withheld = true;
				if (exhausted) {
					this.meters.windowWithheldForBudget();
				}
				else {
					this.meters.windowWithheldForShare();
				}
			}
			if (exhausted) {
				this.budget.awaitRoom(this.budgetRoom);
			}
			else {
				// its own drains review it again; a waiter would run at once
				this.budget.cancel(this.budgetRoom);
			}
			wrote |= refuseWaitingLocked();
		}
		else {
			if (this.withheld) {
				this.withheld = false;
				this.budget.cancel(this.budgetRoom);
			}
			wrote |= grantLocked(client(), this.clientWindow, queued);
			for (Leg leg : this.opened) {
				wrote |= grantLocked(leg.hop(), leg.window, queued);
			}
		}
		watchWritesLocked(watchingWrites());
		wrote |= retireWithdrawnLocked();
		wrote |= reviewDeadlineLocked();
		return wrote;
	}

	/**
	 * Grants the hop's peer what its window decides ({@link HopWindow#grant}), the part
	 * of its credit past the floor charged first ({@link #chargeCreditLocked}); whether
	 * it wrote. With the loop's lock.
	 */
	private boolean grantLocked(Hop hop, HopWindow window, long queued) {
		if (!hop.open()) {
			return false;
		}
		HopWindow.Grant grant = window.grant(hop.connectionCreditLocked(), hop.owedConnectionWindowLocked(),
				cost -> chargeCreditLocked(queued, cost));
		if (grant.declined()) {
			this.meters.windowDeclined();
		}
		return grant.bytes() > 0 && hop.grantConnectionWindowLocked(grant.bytes()) > 0;
	}

	/**
	 * A connection's share of the budget: the {@code queued} bytes beyond
	 * {@link #CONNECTION_ALLOWANCE} plus the {@code chargedCredit} (the window its peers
	 * hold past the floors).
	 */
	static long share(long queued, long chargedCredit) {
		return Math.max(0, queued - CONNECTION_ALLOWANCE) + chargedCredit;
	}

	/**
	 * Whether a header block making the connection's {@code share} is refused: past
	 * {@link #BLOCK_SHARE}, exactly at it is still admitted.
	 */
	static boolean pastBlockShare(long share) {
		return share > BLOCK_SHARE;
	}

	/**
	 * Whether a connection's {@code share} of the budget (the queued bytes beyond the
	 * allowance plus the charged window) has reached {@link #CONNECTION_SHARE}: exactly
	 * at it counts, one byte under it does not.
	 */
	static boolean atShare(long share) {
		return share >= CONNECTION_SHARE;
	}

	/**
	 * Charges {@code cost} more credit past the floors if the share it makes stays below
	 * {@link #CONNECTION_SHARE} -- credit alone never makes the review withhold the
	 * windows, which would keep a peer at the floor from its refill while the others sit
	 * on theirs -- and the budget has room for it; whether it did. With the loop's lock.
	 */
	private boolean chargeCreditLocked(long queued, long cost) {
		long share = share(queued, this.chargedCredit + cost);
		long change = share - this.pooled;
		if (change > 0) {
			if (atShare(share) || !this.budget.tryCharge(change)) {
				return false;
			}
			this.pooled = share;
		}
		this.chargedCredit += cost;
		return true;
	}

	/**
	 * The part of the credit the peers of the open hops hold past the floors
	 * ({@link HopWindow#charged}); with the loop's lock.
	 */
	private long chargedCreditLocked() {
		Hop client = client();
		long charged = client.open() ? HopWindow.charged(client.connectionCreditLocked()) : 0;
		for (Leg leg : this.opened) {
			Hop hop = leg.hop();
			charged += hop.open() ? HopWindow.charged(hop.connectionCreditLocked()) : 0;
		}
		return charged;
	}

	/**
	 * The connection window the peers of the open hops hold: what they may still send.
	 * With the loop's lock.
	 */
	private long creditLocked() {
		Hop client = client();
		long credit = client.open() ? client.connectionCreditLocked() : 0;
		for (Leg leg : this.opened) {
			Hop hop = leg.hop();
			credit += hop.open() ? hop.connectionCreditLocked() : 0;
		}
		return credit;
	}

	private void watchWritesLocked(boolean watch) {
		client().watchWrites(watch);
		for (Leg leg : this.opened) {
			leg.hop().watchWrites(watch);
		}
	}

	/**
	 * Drives the connection's deadline: the idle timeout while the client has no stream
	 * open, else the busy phase, whose stall timeout reads the time the relay waited on
	 * the client ({@link #waitsOnClientLocked}); after the client's EOF, drains
	 * ({@link #drainLocked}). Whether it closed anything; with the loop's lock.
	 */
	private boolean reviewDeadlineLocked() {
		Hop client = client();
		boolean acted = false;
		if (client.open() && this.clientEnded && !this.drained) {
			acted = drainLocked(client);
		}
		boolean busy = false;
		if (client.open()) {
			busy = client.connection().numActiveStreams() > 0;
			this.deadline.enter(busy ? ConnectionLimits.KeepAliveDeadline.Phase.BUSY
					: ConnectionLimits.KeepAliveDeadline.Phase.IDLE);
		}
		this.waitingOnClient = busy && waitsOnClientLocked(client);
		this.clientWait.update();
		return acted;
	}

	/**
	 * Whether the relay waits on its client: what it wrote waits in the client hop's
	 * writer queue (the client is not reading it, or has not yet), DATA towards the
	 * client waits for a window the client has not granted (a stream's, or the
	 * connection's), or a request is due from the client ({@link #requestDueLocked})
	 * while it holds connection window to send it -- a window the relay withholds and the
	 * client spent is not the client's stall. With the loop's lock.
	 */
	private boolean waitsOnClientLocked(Hop client) {
		if (client.queuedBytes() > 0) {
			return true;
		}
		Http2Connection connection = client.connection();
		Http2RemoteFlowController remote = connection.remote().flowController();
		Http2LocalFlowController local = connection.local().flowController();
		Http2Stream connectionStream = connection.connectionStream();
		boolean sendable = remote.windowSize(connectionStream) > 0;
		// netty counts the connection window the hop holds back as granted
		boolean clientMaySend = local.windowSize(connectionStream) - client.owedConnectionWindowLocked() > 0;
		return activeStream(client, stream -> {
			if (remote.hasFlowControlled(stream) && (!sendable || remote.windowSize(stream) <= 0)) {
				return true;
			}
			Exchange exchange = this.exchanges.get(stream.id());
			return clientMaySend && exchange != null && requestDueLocked(exchange, stream, local);
		}) != null;
	}

	/**
	 * Whether the exchange waits for request bytes its client may send: the request not
	 * ended, the client holding stream window, no {@code 100 Continue} awaited, no
	 * response in progress -- a final response head relayed and not ended makes it a
	 * streaming exchange, whose request may rest -- and no CONNECT tunnel. With the
	 * loop's lock.
	 */
	private static boolean requestDueLocked(Exchange exchange, Http2Stream stream, Http2LocalFlowController local) {
		return !exchange.reset && !exchange.requestEnded && !exchange.tunnel && !exchange.continueAwaited
				&& stream.state().remoteSideOpen() && (!stream.isHeadersSent() || exchange.responseEnded)
				&& local.windowSize(stream) > 0;
	}

	/**
	 * Whether a writer's progress may change what the review decides or what the route
	 * gauges show: while the relay queues anything; with the loop's lock.
	 */
	private boolean watchingWrites() {
		return this.queued > 0;
	}

	/** Runs nothing: the settle after a writer's batch runs the review. */
	private static void reviewed() {
	}

	/** Has the loop review the accounts; on the budget's thread once it has room. */
	private void reviewSoon() {
		this.loop.run(NettyHttp2DemuxRelay::reviewed);
	}

	/**
	 * Charges or releases what makes the share the part of the queued bytes beyond the
	 * allowance plus the credit past the floors.
	 */
	private void settleShareLocked(long queued, long chargedCredit) {
		this.chargedCredit = chargedCredit;
		long share = share(queued, chargedCredit);
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
	 * While the windows are withheld, keeps the bytes held for streams waiting for an
	 * upstream's concurrency limit within the allowance: past it, the most recent waiting
	 * streams leave their leg's buffer and are refused ({@code REFUSED_STREAM}: their
	 * upstream never saw them, RFC 9113 8.7) until the rest fits. Their bytes drain only
	 * once a stream of their leg closed on the upstream, which may need the very request
	 * body the withheld window holds back; the open streams' bytes drain on their own.
	 * The parked streams ({@link #parkLocked}) count as waiting ones: their check's end
	 * alone moves their bytes on, to a leg where they may wait again. Whether a stream
	 * was refused; with the loop's lock.
	 */
	private boolean refuseWaitingLocked() {
		List<Waiter> waiting = new ArrayList<>();
		long bytes = 0;
		for (Exchange exchange : this.exchanges.values()) {
			if (exchange.waiting && !exchange.reset) {
				waiting.add(new Waiter(exchange.id, exchange.waitingBytes(), () -> {
					exchange.reset = true;
					exchange.refused = true;
					dropWaitingLocked(exchange, Http2Error.CANCEL.code());
				}));
				bytes += exchange.waitingBytes();
			}
		}
		for (NewStream parked : this.parked.values()) {
			waiting.add(new Waiter(parked.id, parked.heldBytes(), () -> dropParkedLocked(parked)));
			bytes += parked.heldBytes();
		}
		if (bytes <= CONNECTION_ALLOWANCE) {
			return false;
		}
		waiting.sort(Comparator.comparingInt(Waiter::streamId));
		for (int i = waiting.size() - 1; i >= 0 && bytes > CONNECTION_ALLOWANCE; i--) {
			Waiter waiter = waiting.get(i);
			bytes -= waiter.bytes();
			this.meters.refusedWaiting();
			waiter.leave().run();
			resetClientLocked(waiter.streamId(), Http2Error.REFUSED_STREAM);
		}
		return true;
	}

	/**
	 * A stream {@link #refuseWaitingLocked} may refuse: the bytes it holds, and what lets
	 * go of it.
	 */
	private record Waiter(int streamId, long bytes, Runnable leave) {
	}

	/**
	 * Closes the accounts once the relay finished: its share of the budget goes back, its
	 * route gauges drop what it still counted, and no review runs any more.
	 */
	private void closeAccountsLocked() {
		this.accountsClosed = true;
		this.meters.held(-this.queued);
		this.queued = 0;
		this.meters.credit(-this.credit);
		this.credit = 0;
		this.budget.cancel(this.budgetRoom);
		settleShareLocked(0, 0);
		for (Leg leg : this.opened) {
			if (leg.backlog != 0) {
				log.warn("h2 demux: a leg of route {} finished with {} bytes accounted", leg.routeTag, leg.backlog);
			}
			forgottenLocked(leg);
		}
		this.opened.clear();
	}

	/**
	 * Waits on a virtual thread until the hop's channel closed and its threads ended --
	 * at most {@link #HOP_DONE_TIMEOUT} after the close --, then runs {@code done}, which
	 * settles the hop's share of {@link #pending}.
	 */
	private void watch(Hop hop, Runnable done) {
		Thread.ofVirtual().name(hop.name() + "-done").start(() -> {
			try {
				if (!hop.awaitEnded(HOP_DONE_TIMEOUT)) {
					log.warn("h2 demux: {} did not end within {}", hop.name(), HOP_DONE_TIMEOUT);
				}
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			finally {
				done.run();
			}
		});
	}

	/**
	 * One of {@link #pending} settled; the last finishes the relay: the connection is
	 * logged closed, {@code onFinish} runs, and the relay lets go of the connection's own
	 * slot.
	 */
	private void settle() {
		if (this.pending.decrementAndGet() == 0 && this.finished.compareAndSet(false, true)) {
			this.deadline.disarm();
			this.loop.run(this::closeAccountsLocked);
			this.loop.close();
			this.access.close();
			try {
				this.onFinish.run();
			}
			finally {
				releaseOwnSlot();
			}
		}
	}

	/**
	 * One holder let go of the connection's own slot; the last runs {@link #release}: the
	 * relay finished and no leg's frames hold it any more.
	 */
	private void releaseOwnSlot() {
		if (this.ownSlotHolders.decrementAndGet() == 0) {
			this.release.run();
		}
	}

	private static @Nullable String hostOf(Http2Headers headers) {
		CharSequence authority = headers.authority();
		if (authority != null && !authority.isEmpty()) {
			return authority.toString();
		}
		CharSequence host = headers.get(AsciiString.cached("host"));
		return host == null || host.isEmpty() ? null : host.toString();
	}

	private static @Nullable String headerOf(Http2Headers headers, String name) {
		CharSequence value = headers.get(AsciiString.of(name));
		return value == null || value.isEmpty() ? null : value.toString();
	}

	/** What a header block that does not fit fell short of ({@link #admitLocked}). */
	private enum Shortfall {

		/** The shared budget has no room for it. */
		BUDGET,

		/** It takes the relay's share past {@link #BLOCK_SHARE}. */
		SHARE

	}

	/** One route's leg: the hop over its virtual connection. */
	private static final class Leg {

		final String routeTag;

		/** The session the leg's virtual connection is on. */
		final TunnelSession session;

		final H2DemuxMeters.Route routeMeters;

		/** Set once the hop is built. */
		@Nullable Hop hop;

		/** The connection window granted the upstream; with the loop's lock. */
		final HopWindow window = new HopWindow(LEG_CONNECTION_WINDOW);

		/** The upstream's first SETTINGS has not arrived; with the loop's lock. */
		boolean awaitingSettings = true;

		/** A GOAWAY with an error code went to the upstream; with the loop's lock. */
		boolean failed;

		/**
		 * What the relay holds of the leg's exchanges, both ways: DATA read and not yet
		 * written on the other hop, and header blocks not yet written; with the lock.
		 */
		long backlog;

		/** The DATA writes netty holds for the route's exchanges. */
		final LongAdder writesHeld;

		/** What the route's gauge counts of the leg; with the loop's lock. */
		private long gauged;

		/**
		 * The exchanges on the leg, by their stream id on it, until closed for the
		 * client; with the loop's lock.
		 */
		final Map<Integer, Exchange> streams = new HashMap<>();

		/** The lowest stream id the leg may open next; with the loop's lock. */
		private long nextStreamId = 1;

		Leg(String routeTag, TunnelSession session, H2DemuxMeters.Route routeMeters, LongAdder writesHeld) {
			this.routeTag = routeTag;
			this.session = session;
			this.routeMeters = routeMeters;
			this.writesHeld = writesHeld;
		}

		/** Sets what the route's gauge counts of the leg; with the loop's lock. */
		void gauge(long bytes) {
			if (bytes != this.gauged) {
				this.routeMeters.queuedBytes().addAndGet(bytes - this.gauged);
				this.gauged = bytes;
			}
		}

		Hop hop() {
			return Objects.requireNonNull(this.hop, "the leg's hop is not built");
		}

		/**
		 * Whether the client's new stream may go to the leg: open, no GOAWAY either way,
		 * a stream id left for it ({@link #streamIdLocked}); with the loop's lock.
		 */
		boolean takesStreamsLocked(int clientStreamId) {
			Hop hop = hop();
			return hop.open() && !hop.connection().goAwayReceived() && !hop.connection().goAwaySent()
					&& Math.max(clientStreamId, this.nextStreamId) <= Integer.MAX_VALUE;
		}

		/**
		 * The stream id on the leg of the client's new stream: the client's own, unless a
		 * stream the client opened after it went to the leg first (its credential check
		 * took longer), then the lowest the leg may still open -- a leg opens its streams
		 * in ascending order (RFC 9113 5.1.1). With the loop's lock.
		 */
		int streamIdLocked(int clientStreamId) {
			long id = Math.max(clientStreamId, this.nextStreamId);
			this.nextStreamId = id + 2;
			return (int) id;
		}

	}

	/**
	 * A new client stream, routed, on its way to its leg while its credentials are
	 * checked; while the check runs it is parked ({@link #parkLocked}), and holds what
	 * the client sends on it. With the loop's lock.
	 */
	private static final class NewStream {

		final int id;

		final Router.Route route;

		final TunnelSession session;

		final @Nullable String host;

		final ConnectionHeadParser.Head.Request request;

		final Http2Headers headers;

		/** The head ended the request. */
		final boolean headEnds;

		/** The head's bytes as HPACK counts them, held while parked. */
		final int headBytes;

		/** The client ended the request: with the head, DATA or trailers. */
		boolean requestEnded;

		/** DATA read while parked, not consumed. */
		final PendingPayload data = new PendingPayload();

		/** A DATA frame was read while parked. */
		boolean dataRead;

		/** A DATA frame read while parked ended the request. */
		boolean dataEnds;

		/** The trailers read while parked. */
		@Nullable Http2Headers trailers;

		boolean trailersEnd;

		int trailerBytes;

		/** The verdict of the check while parked. */
		@Nullable CompletableFuture<Verdict> verdict;

		/** The wait deadline while parked, unless the deadline is off. */
		@Nullable ScheduledFuture<?> deadline;

		NewStream(int id, Router.Route route, TunnelSession session, @Nullable String host,
				ConnectionHeadParser.Head.Request request, Http2Headers headers, boolean headEnds) {
			this.id = id;
			this.route = route;
			this.session = session;
			this.host = host;
			this.request = request;
			this.headers = headers;
			this.headEnds = headEnds;
			this.requestEnded = headEnds;
			this.headBytes = headerBytes(headers);
		}

		/** What the stream holds while parked: its head, DATA and trailers. */
		long heldBytes() {
			return this.headBytes + this.data.capacity() + this.trailerBytes;
		}

	}

	/** A client stream relayed to a leg; with the loop's lock. */
	private static final class Exchange {

		/** The stream's id on the client connection. */
		final int id;

		/** The stream's id on the leg ({@link Leg#streamIdLocked}). */
		final int legId;

		final Leg leg;

		/** The client's END_STREAM was forwarded to the leg. */
		boolean requestEnded;

		/** The upstream's END_STREAM was forwarded to the client. */
		boolean responseEnded;

		/**
		 * A reset was sent or relayed on either side, or the stream was answered or is to
		 * be reset for the client: the exchange takes no more frames of either side.
		 */
		boolean reset;

		/** The client write carrying the response's end completed. */
		boolean responseWritten;

		/** The code to reset the client's stream with once the response is written. */
		@Nullable Long resetAfterResponse;

		/** The upstream never saw the stream: the leg did not take it, or a GOAWAY. */
		boolean refused;

		/** A CONNECT stream: a tunnel, whose request may rest. */
		boolean tunnel;

		/**
		 * The request asked for {@code 100 Continue} and neither a response head nor a
		 * byte of its body passed yet: the client may hold its body back.
		 */
		boolean continueAwaited;

		/** The stream waits in the leg's buffer for the upstream's concurrency limit. */
		boolean waiting;

		/** The wait deadline while waiting, unless the deadline is off. */
		@Nullable ScheduledFuture<?> waitDeadline;

		/** The request's host, for the bad-gateway page. */
		@Nullable String host;

		/** The request, for the bad-gateway page. */
		ConnectionHeadParser.Head.@Nullable Request request;

		/** The request head's bytes while the leg has not written it. */
		int headBytes;

		/**
		 * The bytes of the interim (1xx) heads the upstream sent, as HPACK counts them.
		 */
		int interimBytes;

		/** The client's DATA and trailers towards the leg. */
		final Flow requestFlow;

		/** The upstream's DATA and trailers towards the client. */
		final Flow responseFlow;

		Exchange(int id, int legId, Leg leg, Hop client) {
			this.id = id;
			this.legId = legId;
			this.leg = leg;
			this.requestFlow = new Flow(this, client, id, leg.hop(), legId);
			this.responseFlow = new Flow(this, leg.hop(), legId, client, id);
		}

		/** What the relay holds for the stream while it waits in its leg's buffer. */
		long waitingBytes() {
			return this.headBytes + this.requestFlow.held();
		}

	}

	/**
	 * One direction of an exchange: the DATA and trailers read on one hop, written on the
	 * other. One DATA write at a time is in netty's hands (its flow controller, or the
	 * leg's buffer while the stream waits): it is a copy, so it pins none of the reading
	 * hop's inbound buffers, and what arrives meanwhile is appended to one backlog buffer
	 * (a {@link PendingPayload}), written whole once the write before completed -- so
	 * small frames cost no object each, and the bytes held are the bytes accounted. The
	 * reading hop consumes a write's bytes (grants its window back) once the other hop
	 * wrote them, so what a flow holds stays within the window the relay announced
	 * ({@link #STREAM_WINDOW}), plus less than a granule of rounding. Trailers follow the
	 * DATA held before them. Everything the flow holds counts in its leg's backlog. With
	 * the loop's lock.
	 */
	private static final class Flow {

		private final Exchange exchange;

		private final Hop from;

		/** The stream's id on {@link #from}. */
		private final int fromId;

		private final Hop to;

		/** The stream's id on {@link #to}. */
		private final int toId;

		/** The bytes of the DATA write netty holds; -1 = none. */
		private int writing = -1;

		/** The buffer bytes of that write. */
		private int writingBytes;

		private final PendingPayload backlog = new PendingPayload();

		/** The backlog ends the stream. */
		private boolean backlogEnds;

		/** Trailers held behind the backlog. */
		private @Nullable Http2Headers trailers;

		private boolean trailersEnd;

		/** The bytes of the trailers and other blocks held, until netty wrote them. */
		private int blockBytes;

		/** Nothing more of the stream is written: reset, or a hop closed it. */
		private boolean dropped;

		/** Runs once the write carrying the stream's end completed, either way. */
		private @Nullable Runnable endWritten;

		Flow(Exchange exchange, Hop from, int fromId, Hop to, int toId) {
			this.exchange = exchange;
			this.from = from;
			this.fromId = fromId;
			this.to = to;
			this.toId = toId;
		}

		void onEndWritten(Runnable endWritten) {
			this.endWritten = endWritten;
		}

		/** What the flow holds: the write in netty's hands, the backlog, the trailers. */
		long held() {
			return this.writingBytes + this.backlog.capacity() + this.blockBytes;
		}

		/** Whether DATA waits in netty's hands or the backlog. */
		boolean busy() {
			return this.writing >= 0 || this.backlog.size() > 0 || this.backlogEnds;
		}

		/** DATA read on the stream; its padding is the caller's to consume. */
		void data(ByteBuf data, boolean endStream) {
			if (this.dropped) {
				return;
			}
			if (this.writing < 0) {
				int length = data.readableBytes();
				writeData(length == 0 ? Unpooled.EMPTY_BUFFER : Unpooled.copiedBuffer(data), length, length, endStream);
				return;
			}
			int before = this.backlog.capacity();
			this.backlog.append(data);
			this.backlogEnds |= endStream;
			account(this.backlog.capacity() - before);
		}

		/**
		 * The DATA read on the stream while it was parked ({@link NewStream}), taken as
		 * one write: the first of the flow.
		 */
		void parked(PendingPayload held, boolean endStream) {
			int size = held.size();
			int capacity = held.capacity();
			writeData(held.takeAll(), size, capacity, endStream);
		}

		/**
		 * A header block after the head: after the DATA held before it. netty takes no
		 * more than one after DATA -- trailers, which end the stream.
		 */
		void block(Http2Headers headers, boolean endStream) {
			if (this.dropped) {
				return;
			}
			int bytes = headerBytes(headers);
			if (busy()) {
				this.trailers = headers;
				this.trailersEnd = endStream;
				this.blockBytes += bytes;
				account(bytes);
				return;
			}
			writeBlock(headers, bytes, endStream);
		}

		/**
		 * Nothing more of the stream is written: the backlog and trailers go; the write
		 * netty holds completes as netty fails or finishes it.
		 */
		void drop() {
			if (this.dropped) {
				return;
			}
			this.dropped = true;
			int held = this.backlog.capacity();
			this.backlog.clear();
			this.backlogEnds = false;
			if (this.trailers != null) {
				held += headerBytes(this.trailers);
				this.blockBytes -= headerBytes(this.trailers);
				this.trailers = null;
			}
			account(-held);
		}

		private void writeData(ByteBuf data, int length, int bytes, boolean endStream) {
			this.writing = length;
			this.writingBytes = bytes;
			account(bytes);
			this.exchange.leg.writesHeld.increment();
			this.to.encoder()
				.writeData(this.to.ctx(), this.toId, data, 0, endStream,
						this.to.newPromise(future -> dataWritten(future.isSuccess(), endStream)));
		}

		private void dataWritten(boolean success, boolean endStream) {
			int length = this.writing;
			account(-this.writingBytes);
			this.exchange.leg.writesHeld.decrement();
			this.writing = -1;
			this.writingBytes = 0;
			if (success) {
				consume(this.from, this.fromId, length);
			}
			if (endStream) {
				ended();
			}
			if (!success) {
				drop();
				return;
			}
			if (this.backlog.size() > 0 || this.backlogEnds) {
				int size = this.backlog.size();
				int capacity = this.backlog.capacity();
				ByteBuf bytes = this.backlog.takeAll();
				boolean ends = this.backlogEnds;
				this.backlogEnds = false;
				account(-capacity);
				writeData(bytes, size, capacity, ends);
			}
			Http2Headers held = this.trailers;
			if (held != null) {
				int bytes = headerBytes(held);
				this.trailers = null;
				this.blockBytes -= bytes;
				account(-bytes);
				// netty writes it behind the DATA it holds
				writeBlock(held, bytes, this.trailersEnd);
			}
		}

		private void writeBlock(Http2Headers headers, int bytes, boolean endStream) {
			this.blockBytes += bytes;
			account(bytes);
			this.to.encoder()
				.writeHeaders(this.to.ctx(), this.toId, headers, 0, endStream, this.to.newPromise(future -> {
					this.blockBytes -= bytes;
					account(-bytes);
					if (endStream) {
						ended();
					}
				}));
		}

		private void ended() {
			Runnable endWritten = this.endWritten;
			if (endWritten != null) {
				endWritten.run();
			}
		}

		private void account(long bytes) {
			this.exchange.leg.backlog += bytes;
		}

	}

	/** The client's frames: new heads are routed; the rest goes to the stream's leg. */
	private final class ClientListener extends Http2FrameAdapter {

		private final Hop hop;

		ClientListener(Hop hop) {
			this.hop = hop;
		}

		@Override
		public void onHeadersRead(ChannelHandlerContext ctx, int streamId, Http2Headers headers, int streamDependency,
				short weight, boolean exclusive, int padding, boolean endStream) {
			onHeadersRead(ctx, streamId, headers, padding, endStream);
		}

		@Override
		public void onHeadersRead(ChannelHandlerContext ctx, int streamId, Http2Headers headers, int padding,
				boolean endStream) {
			Exchange exchange = NettyHttp2DemuxRelay.this.exchanges.get(streamId);
			if (exchange != null) {
				// trailers
				if (exchange.reset) {
					return;
				}
				Shortfall shortfall = admitLocked(headerBytes(headers));
				if (shortfall != null) {
					resetForBudgetLocked(exchange, shortfall);
					return;
				}
				exchange.requestEnded |= endStream;
				exchange.requestFlow.block(headers, endStream);
				return;
			}
			NewStream parked = NettyHttp2DemuxRelay.this.parked.get(streamId);
			if (parked != null) {
				parkTrailersLocked(parked, headers, endStream);
				return;
			}
			if (streamId <= NettyHttp2DemuxRelay.this.highestStreamId) {
				return; // a later block of a stream the relay answered itself
			}
			// a new stream: the idle timeout, if it runs, starts over once the stream
			// closed
			if (!NettyHttp2DemuxRelay.this.deadline.enter(ConnectionLimits.KeepAliveDeadline.Phase.BUSY)) {
				return; // a timeout passed: the connection is closing, the stream
						// unprocessed
			}
			NettyHttp2DemuxRelay.this.highestStreamId = streamId;
			routeLocked(streamId, headers, endStream);
		}

		@Override
		public int onDataRead(ChannelHandlerContext ctx, int streamId, ByteBuf data, int padding, boolean endStream) {
			NettyHttp2DemuxRelay.this.deadline.progress(data.readableBytes() + padding);
			measureLocked(this.hop, NettyHttp2DemuxRelay.this.clientWindow, data.readableBytes() + padding);
			Exchange exchange = NettyHttp2DemuxRelay.this.exchanges.get(streamId);
			if (exchange == null) {
				NewStream parked = NettyHttp2DemuxRelay.this.parked.get(streamId);
				if (parked != null) {
					parkDataLocked(parked, data, endStream);
					// padding is consumed at once
					return padding;
				}
			}
			if (exchange == null || exchange.reset) {
				return data.readableBytes() + padding;
			}
			exchange.continueAwaited = false;
			exchange.requestEnded |= endStream;
			exchange.requestFlow.data(data, endStream);
			// padding is consumed at once
			return padding;
		}

		@Override
		public void onPingAckRead(ChannelHandlerContext ctx, long data) {
			if (data == ROUND_TRIP_PAYLOAD) {
				NettyHttp2DemuxRelay.this.clientWindow.acked();
			}
		}

		@Override
		public void onRstStreamRead(ChannelHandlerContext ctx, int streamId, long errorCode) {
			resetByPeerLocked(this.hop, streamId);
			Exchange exchange = NettyHttp2DemuxRelay.this.exchanges.get(streamId);
			if (exchange != null && !exchange.reset) {
				exchange.reset = true;
				// a waiting stream leaves the leg's buffer as netty closes the client's
				// stream right after this call (ClientStreams)
				resetLocked(exchange.leg.hop(), exchange.legId, errorCode);
			}
		}

	}

	/**
	 * The client's stream lifecycle: a stream closed before both its request and its
	 * response ended -- reset by netty's codec, say -- cancels its upstream stream, and a
	 * stream still waiting for its leg leaves the leg's buffer; a GOAWAY
	 * {@code COMPRESSION_ERROR} is logged.
	 */
	private final class ClientStreams extends Http2ConnectionAdapter {

		@Override
		public void onStreamClosed(Http2Stream stream) {
			if (resetByCodecLocked(stream)) {
				NettyHttp2DemuxRelay.this.meters.resetForProtocol();
			}
			NewStream parked = NettyHttp2DemuxRelay.this.parked.get(stream.id());
			if (parked != null) {
				dropParkedLocked(parked);
			}
			Exchange exchange = NettyHttp2DemuxRelay.this.exchanges.remove(stream.id());
			if (exchange == null) {
				return;
			}
			exchange.leg.streams.remove(exchange.legId);
			exchange.leg.routeMeters.trackedStreams().decrement();
			exchange.responseFlow.drop();
			if (exchange.reset || !(exchange.requestEnded && exchange.responseEnded)) {
				exchange.requestFlow.drop();
			}
			// else: the request's rest, its end included, still goes to the upstream
			if (exchange.waiting) {
				exchange.reset = true;
				dropWaitingLocked(exchange, Http2Error.CANCEL.code());
			}
			else if (!exchange.reset && !(exchange.requestEnded && exchange.responseEnded)) {
				exchange.reset = true;
				resetLocked(exchange.leg.hop(), exchange.legId, Http2Error.CANCEL.code());
			}
		}

		@Override
		public void onGoAwaySent(int lastStreamId, long errorCode, ByteBuf debugData) {
			if (errorCode == Http2Error.COMPRESSION_ERROR.code()) {
				// a header block that did not decode: nothing of it is known
				NettyHttp2DemuxRelay.this.access.request("-", "-", "-", HTTP_2, null);
			}
		}

	}

	/** A leg's frames: heads, DATA and resets go to the client. */
	private final class LegListener extends Http2FrameAdapter {

		private final Leg leg;

		LegListener(Leg leg) {
			this.leg = leg;
		}

		/** The exchange of the leg's stream, unless reset. */
		private @Nullable Exchange exchange(int streamId) {
			Exchange exchange = this.leg.streams.get(streamId);
			return exchange == null || exchange.reset ? null : exchange;
		}

		/**
		 * The upstream's first SETTINGS without a concurrency limit lifts the relay's one
		 * stream: no limit announced is no limit (RFC 9113 6.5.2). netty applied the
		 * SETTINGS before this call, a limit announced included.
		 */
		@Override
		public void onSettingsRead(ChannelHandlerContext ctx, Http2Settings settings) {
			if (!this.leg.awaitingSettings) {
				return;
			}
			this.leg.awaitingSettings = false;
			if (settings.maxConcurrentStreams() == null) {
				limitStreams(this.leg.hop(), Integer.MAX_VALUE);
			}
		}

		@Override
		public void onHeadersRead(ChannelHandlerContext ctx, int streamId, Http2Headers headers, int streamDependency,
				short weight, boolean exclusive, int padding, boolean endStream) {
			onHeadersRead(ctx, streamId, headers, padding, endStream);
		}

		/**
		 * A header block of the upstream -- an interim (1xx) head, the final head, the
		 * trailers -- goes to the client once admitted
		 * ({@link #admitResponseBlockLocked}): past {@link #RESPONSE_BLOCK_ALLOWANCE},
		 * one that does not fit ({@link #admitLocked}) resets the stream instead
		 * ({@link #resetForBudgetLocked}). An interim head taking the response's interim
		 * heads past {@link #MAX_INTERIM_BYTES} is not written: the upstream's stream is
		 * reset with {@code ENHANCE_YOUR_CALM}, and the stream is lost for the client
		 * ({@link #loseLocked}: no final head relayed, so the bad-gateway page).
		 */
		@Override
		public void onHeadersRead(ChannelHandlerContext ctx, int streamId, Http2Headers headers, int padding,
				boolean endStream) {
			Exchange exchange = exchange(streamId);
			if (exchange == null) {
				return;
			}
			int bytes = headerBytes(headers);
			if (interim(headers)) {
				exchange.interimBytes += bytes;
				if (exchange.interimBytes > MAX_INTERIM_BYTES) {
					NettyHttp2DemuxRelay.this.meters.resetForInterim();
					exchange.requestFlow.drop();
					loseLocked(exchange);
					resetLocked(this.leg.hop(), streamId, Http2Error.ENHANCE_YOUR_CALM.code());
					return;
				}
			}
			Shortfall shortfall = admitResponseBlockLocked(bytes);
			if (shortfall != null) {
				resetForBudgetLocked(exchange, shortfall);
				return;
			}
			exchange.continueAwaited = false;
			exchange.responseEnded |= endStream;
			exchange.responseFlow.block(headers, endStream);
		}

		@Override
		public int onDataRead(ChannelHandlerContext ctx, int streamId, ByteBuf data, int padding, boolean endStream) {
			measureLocked(this.leg.hop(), this.leg.window, data.readableBytes() + padding);
			Exchange exchange = exchange(streamId);
			if (exchange == null) {
				return data.readableBytes() + padding;
			}
			exchange.responseEnded |= endStream;
			exchange.responseFlow.data(data, endStream);
			// padding is consumed at once
			return padding;
		}

		@Override
		public void onPingAckRead(ChannelHandlerContext ctx, long data) {
			if (data == ROUND_TRIP_PAYLOAD) {
				this.leg.window.acked();
			}
		}

		@Override
		public void onRstStreamRead(ChannelHandlerContext ctx, int streamId, long errorCode) {
			resetByPeerLocked(this.leg.hop(), streamId);
			Exchange exchange = exchange(streamId);
			if (exchange == null) {
				return;
			}
			if (exchange.responseEnded) {
				// after the response, the reset tells the request's rest is not wanted
				resetAfterResponseLocked(exchange, errorCode);
				return;
			}
			exchange.reset = true;
			resetLocked(client(), exchange.id, errorCode);
		}

		/**
		 * The upstream's GOAWAY: the streams above its last stream id are refused once
		 * netty dropped them (it does right after this call), and the leg closes
		 * gracefully -- a GOAWAY of its own, then the close once the other streams ended
		 * -- as a task, run when netty is done with the GOAWAY.
		 */
		@Override
		public void onGoAwayRead(ChannelHandlerContext ctx, int lastStreamId, long errorCode, ByteBuf debugData) {
			for (Exchange exchange : this.leg.streams.values()) {
				if (exchange.legId > lastStreamId) {
					exchange.refused = true;
				}
			}
			Hop hop = this.leg.hop();
			ctx.executor().execute(hop::closeLocked);
		}

	}

	/**
	 * A leg's stream lifecycle: a waiting stream the leg opens stops waiting; a stream
	 * closed without the response's end -- the leg closed under it, say -- is lost for
	 * the client; a GOAWAY with an error code to the upstream fails the leg.
	 */
	private final class LegStreams extends Http2ConnectionAdapter {

		private final Leg leg;

		LegStreams(Leg leg) {
			this.leg = leg;
		}

		@Override
		public void onStreamAdded(Http2Stream stream) {
			Exchange exchange = this.leg.streams.get(stream.id());
			if (exchange != null) {
				stopWaitingLocked(exchange);
			}
		}

		@Override
		public void onStreamClosed(Http2Stream stream) {
			if (resetByCodecLocked(stream)) {
				NettyHttp2DemuxRelay.this.meters.resetForUpstream();
			}
			Exchange exchange = this.leg.streams.get(stream.id());
			if (exchange != null) {
				// nothing more reaches the upstream; what it sent goes on to the client
				exchange.requestFlow.drop();
				loseLocked(exchange);
			}
		}

		/**
		 * netty's codec sends the upstream a GOAWAY with the code of its connection
		 * error, the relay a GOAWAY {@code NO_ERROR} once the upstream's GOAWAY came: the
		 * former fails the leg, once.
		 */
		@Override
		public void onGoAwaySent(int lastStreamId, long errorCode, ByteBuf debugData) {
			if (errorCode != Http2Error.NO_ERROR.code() && !this.leg.failed) {
				this.leg.failed = true;
				NettyHttp2DemuxRelay.this.meters.legFailed();
			}
		}

	}

	/**
	 * Hands out the slots of the connection limit further legs open on, with the bytes
	 * they reserve.
	 */
	@FunctionalInterface
	interface LegSlots {

		/** A slot and the bytes for one more leg, or null when they do not fit. */
		ConnectionReserve.@Nullable Reservation tryAcquire();

	}

	/**
	 * Opens a leg's virtual connection and sees its hop built; a seam for tests to make a
	 * leg fail to open.
	 */
	interface LegSeam {

		/** Does nothing beyond what the session does. */
		LegSeam NONE = new LegSeam() {
		};

		/** Opens the leg's h2 connection; throwing fails the leg's opening. */
		default VirtualConnection open(TunnelSession session, String address) {
			return session.open(address, DataProxyServer.ALPN_H2);
		}

		/** Sees the leg's hop built; throwing fails the leg's configuration. */
		default void built(Hop hop) {
		}

	}

	static final class Builder {

		private @Nullable DuplexPipe pipe;

		private ConnectionHeadParser.@Nullable Head head;

		private @Nullable Router router;

		private @Nullable SessionRegistry sessions;

		private @Nullable AccessControl accessControl;

		private @Nullable RouteAuthenticator routeAuth;

		private @Nullable ErrorResponse errorResponse;

		private AccessLogger.@Nullable Connection access;

		private @Nullable H2DemuxMeters meters;

		private Router.@Nullable Route initialRoute;

		private @Nullable TunnelSession initialSession;

		private @Nullable InetAddress peer;

		private @Nullable TlsHandshake handshake;

		private ConnectionLimits.@Nullable KeepAliveDeadline deadline;

		private Duration streamWaitTimeout = Duration.ZERO;

		private Duration probeInterval = Duration.ZERO;

		private @Nullable Runnable onFinish;

		private @Nullable Runnable release;

		private @Nullable H2DemuxBudget budget;

		private @Nullable LegSlots legSlots;

		private LegSeam legSeam = LegSeam.NONE;

		private Builder() {
		}

		/** The client connection's pipe, past the connection head. */
		Builder pipe(DuplexPipe pipe) {
			this.pipe = pipe;
			return this;
		}

		/** The connection head read from the pipe, fed to the codec first. */
		Builder head(ConnectionHeadParser.Head head) {
			this.head = head;
			return this;
		}

		Builder router(Router router) {
			this.router = router;
			return this;
		}

		Builder sessions(SessionRegistry sessions) {
			this.sessions = sessions;
			return this;
		}

		Builder accessControl(AccessControl accessControl) {
			this.accessControl = accessControl;
			return this;
		}

		Builder routeAuth(RouteAuthenticator routeAuth) {
			this.routeAuth = routeAuth;
			return this;
		}

		Builder errorResponse(ErrorResponse errorResponse) {
			this.errorResponse = errorResponse;
			return this;
		}

		Builder access(AccessLogger.Connection access) {
			this.access = access;
			return this;
		}

		Builder meters(H2DemuxMeters meters) {
			this.meters = meters;
			return this;
		}

		/** The route and session resolved for the connection head's stream. */
		Builder initialRoute(Router.Route initialRoute, TunnelSession initialSession) {
			this.initialRoute = initialRoute;
			this.initialSession = initialSession;
			return this;
		}

		/** The address judged by the access control; null when unknown. */
		Builder peer(@Nullable InetAddress peer) {
			this.peer = peer;
			return this;
		}

		/**
		 * The handshake the data plane terminated; null (the default) for plaintext h2.
		 */
		Builder handshake(@Nullable TlsHandshake handshake) {
			this.handshake = handshake;
			return this;
		}

		/** The connection's deadline, disarmed once the relay finished. */
		Builder deadline(ConnectionLimits.KeepAliveDeadline deadline) {
			this.deadline = deadline;
			return this;
		}

		/**
		 * How long a stream may wait for its upstream's concurrency limit before it is
		 * refused; zero (the default) = as long as it takes.
		 */
		Builder streamWaitTimeout(Duration streamWaitTimeout) {
			this.streamWaitTimeout = streamWaitTimeout;
			return this;
		}

		/**
		 * The quiet after which a client that ended its side, responses owed, is probed
		 * with a PING; zero (the default) = never.
		 */
		Builder probeInterval(Duration probeInterval) {
			this.probeInterval = probeInterval;
			return this;
		}

		/** Runs once the relay finished. */
		Builder onFinish(Runnable onFinish) {
			this.onFinish = onFinish;
			return this;
		}

		/** Frees the connection's slot; runs after {@code onFinish}. */
		Builder release(Runnable release) {
			this.release = release;
			return this;
		}

		/** The queue budget the h2 demux relays share. */
		Builder budget(H2DemuxBudget budget) {
			this.budget = budget;
			return this;
		}

		/**
		 * The slots of the connection limit for the legs beyond the one the connection's
		 * own slot covers.
		 */
		Builder legSlots(LegSlots legSlots) {
			this.legSlots = legSlots;
			return this;
		}

		/** The seam of the legs' opening; the default does nothing beyond the session. */
		Builder legSeam(LegSeam legSeam) {
			this.legSeam = legSeam;
			return this;
		}

		NettyHttp2DemuxRelay build() {
			return new NettyHttp2DemuxRelay(this);
		}

	}

}
