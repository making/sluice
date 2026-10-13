package am.ik.sluice.server.proxy;

import java.io.ByteArrayInputStream;
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
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import am.ik.sluice.server.proxy.hop.Hop;
import am.ik.sluice.server.proxy.hop.Loop;
import am.ik.sluice.server.route.Router;
import am.ik.sluice.server.tunnel.SessionRegistry;
import am.ik.sluice.server.tunnel.TunnelSession;
import am.ik.sluice.tunnel.ConnectionLimit;
import am.ik.sluice.tunnel.DuplexPipe;
import am.ik.sluice.tunnel.StreamRelay;
import am.ik.sluice.tunnel.VirtualConnection;
import io.micrometer.core.instrument.Counter;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpStatusClass;
import io.netty.handler.codec.http2.Http2ConnectionAdapter;
import io.netty.handler.codec.http2.Http2ConnectionHandlerBuilder;
import io.netty.handler.codec.http2.Http2Error;
import io.netty.handler.codec.http2.Http2Exception;
import io.netty.handler.codec.http2.Http2FrameAdapter;
import io.netty.handler.codec.http2.Http2Headers;
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
 * client endpoint's flow controller, and the connection goes on. A stream of a
 * {@code force-http1} route is answered {@code 421} without a body, then reset with
 * {@code NO_ERROR} while its request is still being sent; it opens no leg. A head that is
 * no request (no {@code :method}, no {@code :path} but for a CONNECT, a CONNECT without
 * {@code :authority}) is reset with {@code PROTOCOL_ERROR}. A CONNECT stream is routed by
 * its {@code :authority} and logged with it as its path.
 * <p>
 * Streams of one route share the route's leg and keep their stream ids on it; with
 * {@code rewrite-host} the head's {@code :authority} becomes the route's address. Windows
 * are per hop: DATA read on one hop is written on the other ({@link Flow}: a copy, small
 * frames coalesced) and consumed -- its stream window granted back -- once written, so
 * what the relay holds per stream and direction stays within the stream window it
 * announced ({@link #STREAM_WINDOW}); the connection windows refill as DATA arrives
 * (netty's auto refill), so a stream held by its upstream or its client holds back no
 * other stream. Resets pass either way with their code; an upstream's reset after the
 * response's end follows that end to the client. A stream the client ends without its
 * request's end resets its upstream stream with {@code CANCEL}; one an early response
 * closed for the client still delivers the request's rest.
 * <p>
 * What the relay holds -- DATA and header blocks not yet written, both ways, and every
 * hop's writer queue -- is accounted after each action of its loop: the part past
 * {@link #CONNECTION_ALLOWANCE} is charged to the {@link H2DemuxBudget} every relay
 * shares, and past it with the budget exhausted the relay withholds the connection window
 * of every hop (each peer stops once the window it holds is spent), refuses a new stream
 * whose head does not fit ({@code REFUSED_STREAM}), resets a stream whose later header
 * block does not -- the client's trailers, the upstream's interim (1xx) heads, final head
 * and trailers, these once what heads for the client passed the client hop's read pause
 * ({@link #admitResponseBlockLocked}) -- ({@code ENHANCE_YOUR_CALM} to the client,
 * {@code CANCEL} to the upstream), and refuses the most recent waiting streams past the
 * allowance ({@link #refuseWaitingLocked}), until its hops drained below the allowance or
 * the budget has room. A header block is admitted at its size as HPACK counts it
 * ({@link #headerBytes}), whatever it took on the wire: one referring to the peer's
 * dynamic table, a few bytes read, is admitted at about what the relay writes for it. The
 * interim heads of one response may sum to {@link #MAX_INTERIM_BYTES}: the one past it is
 * not written, the upstream's stream is reset ({@code ENHANCE_YOUR_CALM}) and the stream
 * is lost for the client (the bad-gateway page). The bounds outside the budget:
 * {@link #UNBUDGETED_BYTES} per connection, {@link #RESPONSE_QUEUE_BYTES} per leg.
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
 * queued left the tunnel sender. The bytes each leg reads and writes count as the route's
 * tunnel bytes ({@code sluice.tunnel.bytes}), the session's traffic and the connection's
 * access log bytes. A stream netty's codec resets on a stream error of the client or of
 * an upstream, and a leg sending its upstream a GOAWAY with an error code, are counted
 * ({@link H2DemuxMeters}).
 * <p>
 * netty's codec answers the client's protocol errors itself; a header block that does not
 * decode ends the connection with a GOAWAY {@code COMPRESSION_ERROR}, logged as a request
 * line naming neither route, method nor path. The client endpoint's end -- EOF, error,
 * the codec closing it -- closes every leg. Once every hop's threads ended, the relay
 * finishes: the connection is logged closed and {@code onFinish} runs; {@code release}
 * follows once the frames the leg on the connection's own slot queued left the tunnel
 * sender too.
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
	 * The initial stream window the relay announces on both faces (netty's default, the
	 * RFC 9113 one): what it reads of a stream and has not written on the other hop yet,
	 * its consumed bytes not granted back, stays within it.
	 */
	static final int STREAM_WINDOW = 65535;

	/**
	 * The bytes a relay may queue without charging the shared budget: what it always
	 * admits, so a connection whose hops drain keeps flowing however exhausted the budget
	 * is.
	 */
	static final int CONNECTION_ALLOWANCE = 128 * 1024;

	/**
	 * The most one hop's writer queue holds outside the budget: the reader pauses past
	 * {@link Hop#DEFAULT_READ_PAUSE_BYTES} queued, by then unwritable for DATA (its high
	 * water mark is lower), and the upstreams' header blocks for the client hop are taken
	 * outside the budget only up to it ({@link #admitResponseBlockLocked}), so the queue
	 * passes the read pause by what netty answers to one read of the hop's own peer at
	 * most (acks, resets; under 32KiB in {@code HopFailureTest}), 64KiB here.
	 */
	static final long HOP_QUEUE_BYTES = Hop.DEFAULT_READ_PAUSE_BYTES + 64 * 1024;

	/**
	 * What a peer can still make the relay hold once the relay decided to withhold its
	 * connection window: the read under way when the budget ran out (the decision follows
	 * each read), then the connection window the peer holds -- at most
	 * {@link #STREAM_WINDOW}: netty's auto refill grants it back to its initial size,
	 * never more --, copied into the streams' backlogs, which round up by less than one
	 * {@link PendingPayload#GRANULE} each.
	 */
	static final long WITHHELD_ARRIVALS = Hop.MAX_READ_BYTES + STREAM_WINDOW
			+ (long) MAX_CONCURRENT_STREAMS * PendingPayload.GRANULE;

	/**
	 * The most one relay queues outside the shared budget: its allowance, what the client
	 * can still send once its connection window is withheld, and the client hop's writer
	 * queue -- what netty answers the client and the responses written but not yet sent.
	 * The responses' DATA reaches the queue within its writability; their header blocks
	 * within the hop's read pause, past it only within the allowance and the budget
	 * ({@link #admitResponseBlockLocked}).
	 */
	static final long UNBUDGETED_BYTES = CONNECTION_ALLOWANCE + WITHHELD_ARRIVALS + HOP_QUEUE_BYTES;

	/**
	 * The most one leg adds outside the shared budget: what its upstream can still send
	 * once the leg's connection window is withheld, and the leg hop's writer queue.
	 */
	static final long RESPONSE_QUEUE_BYTES = WITHHELD_ARRIVALS + HOP_QUEUE_BYTES;

	/** How long a hop's threads may take to end once its channel closed. */
	private static final Duration HOP_DONE_TIMEOUT = Duration.ofSeconds(30);

	private static final String HTTP_2 = "2";

	private static final String TUNNEL_BYTES = "sluice.tunnel.bytes";

	private final DuplexPipe pipe;

	private final byte[] seed;

	/** The stream of the connection head, routed to the initial route; -1 = none. */
	private int seedStreamId;

	private final Router router;

	private final SessionRegistry sessions;

	private final AccessControl accessControl;

	private final ErrorResponse errorResponse;

	private final AccessLogger.Connection access;

	private final H2DemuxMeters meters;

	private final Router.Route initialRoute;

	private final TunnelSession initialSession;

	private final @Nullable InetAddress peer;

	private final ConnectionLimits.KeepAliveDeadline deadline;

	/** How long a stream may wait for its upstream's concurrency limit; zero = no end. */
	private final Duration streamWaitTimeout;

	private final Runnable onFinish;

	private final Runnable release;

	private final H2DemuxBudget budget;

	private final LegSlots legSlots;

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

	/** The hops' connection windows are withheld; with the loop's lock. */
	private boolean withheld;

	/** The bytes the relay queued at the last review; with the loop's lock. */
	private long queued;

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
		this.errorResponse = Objects.requireNonNull(builder.errorResponse, "errorResponse is required");
		this.access = Objects.requireNonNull(builder.access, "access is required");
		this.meters = Objects.requireNonNull(builder.meters, "meters is required");
		this.initialRoute = Objects.requireNonNull(builder.initialRoute, "initialRoute is required");
		this.initialSession = Objects.requireNonNull(builder.initialSession, "initialSession is required");
		this.peer = builder.peer;
		this.deadline = Objects.requireNonNull(builder.deadline, "deadline is required");
		this.streamWaitTimeout = builder.streamWaitTimeout;
		if (this.streamWaitTimeout.isNegative()) {
			throw new IllegalArgumentException("streamWaitTimeout must not be negative");
		}
		this.onFinish = Objects.requireNonNull(builder.onFinish, "onFinish is required");
		this.release = Objects.requireNonNull(builder.release, "release is required");
		this.budget = Objects.requireNonNull(builder.budget, "budget is required");
		this.legSlots = Objects.requireNonNull(builder.legSlots, "legSlots is required");
		this.loop = new Loop("sluice-h2n",
				(source, cause) -> log.warn("h2 demux {}: {}", source, cause.toString(), cause));
		this.loop.afterSettle(this::reviewLocked);
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
					.codec(codec -> configure(codec, new Http2Settings().maxConcurrentStreams(MAX_CONCURRENT_STREAMS)))
					.onInactive(this::clientClosedLocked)
					.onWritten(NettyHttp2DemuxRelay::reviewed)
					.build();
				this.client = hop;
				hop.addConnectionListener(new ClientStreams());
				return hop;
			});
		}
		catch (RuntimeException | Error e) {
			this.loop.close();
			throw e;
		}
		watch(started, this::settle);
	}

	/**
	 * Both faces' codec: the settings plus the header list limit (netty's default is
	 * 8KiB), and header blocks encoded as never-indexed literals, so every block the
	 * relay writes decodes on its own whatever the peer's decoder saw before.
	 */
	private static void configure(Http2ConnectionHandlerBuilder codec, Http2Settings settings) {
		codec.initialSettings(settings.maxHeaderListSize(MAX_HEADER_LIST_SIZE))
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
	 * half-close and close stay the pipe's.
	 */
	private static DuplexPipe seeded(DuplexPipe pipe, byte[] seed) {
		InputStream source = new SequenceInputStream(new ByteArrayInputStream(seed), pipe.source());
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
			this.access.request("-", method == null ? "-" : method.toString(), "-", HTTP_2);
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
		if (route.forceHttp1()) {
			misdirectLocked(streamId, route, request, endStream);
			return;
		}
		int headBytes = headerBytes(headers);
		if (!admitLocked(headBytes)) {
			// past the allowance with the budget exhausted: safe for the client to retry
			resetClientLocked(streamId, Http2Error.REFUSED_STREAM);
			this.meters.refusedForBudget();
			return;
		}
		Leg leg = legLocked(route, session);
		if (leg == null) {
			resetClientLocked(streamId, Http2Error.REFUSED_STREAM);
			return;
		}
		if (route.rewriteHost()) {
			headers.authority(AsciiString.of(route.address()));
		}
		Exchange exchange = new Exchange(streamId, leg, client());
		exchange.host = host;
		exchange.request = request;
		exchange.requestEnded = endStream;
		exchange.responseFlow.onEndWritten(() -> responseWrittenLocked(exchange));
		this.exchanges.put(streamId, exchange);
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
		hop.encoder().writeHeaders(hop.ctx(), streamId, headers, 0, endStream, written);
		if (!written.isDone() && hop.connection().stream(streamId) == null) {
			// past the upstream's concurrency limit: buffered until a stream of the leg
			// closes
			waitLocked(exchange);
		}
		this.access.request(route.routeTag(), request.method(), request.path(), HTTP_2);
	}

	/**
	 * The exchange's stream waits in its leg's buffer; past the wait deadline it is
	 * refused. The deadline runs on the client hop's executor: its settle runs the task
	 * and then flushes the client's reset. With the loop's lock.
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
		dropWaitingLocked(exchange, Http2Error.CANCEL.code());
		resetClientLocked(exchange.id, Http2Error.REFUSED_STREAM);
		this.meters.refusedForWaitTimeout();
	}

	/**
	 * Drops a waiting stream from its leg's buffer: netty's buffering encoder removes it
	 * on a reset, completing its frames' promises and releasing its DATA, and writes
	 * nothing. With the loop's lock.
	 */
	private void dropWaitingLocked(Exchange exchange, long errorCode) {
		stopWaitingLocked(exchange);
		Hop leg = exchange.leg.hop();
		Http2Stream stream = leg.connection().stream(exchange.id);
		if (stream != null) {
			// opened meanwhile: the buffering encoder resets it for real
			this.resetsNotByCodec.add(stream);
		}
		leg.encoder().writeRstStream(leg.ctx(), exchange.id, errorCode, leg.ctx().newPromise());
	}

	/**
	 * Resets the exchange's stream for a header block that does not fit -- its connection
	 * past its allowance with the budget exhausted -- and cannot be refused without
	 * leaving the stream unfinished: {@code ENHANCE_YOUR_CALM} to the client,
	 * {@code CANCEL} to the upstream. With the loop's lock.
	 */
	private void resetForBudgetLocked(Exchange exchange) {
		exchange.reset = true;
		resetClientLocked(exchange.id, Http2Error.ENHANCE_YOUR_CALM);
		resetLocked(exchange.leg.hop(), exchange.id, Http2Error.CANCEL.code());
		this.meters.resetForBudget();
	}

	/** Whether a response header block is an interim (1xx) head. */
	private static boolean interim(Http2Headers headers) {
		CharSequence status = headers.status();
		return status != null && HttpStatusClass.valueOf(status) == HttpStatusClass.INFORMATIONAL;
	}

	/** Answers a refused stream with the page; with the loop's lock. */
	private void refuseLocked(int streamId, ConnectionHeadParser.Head.Request request, ErrorResponse.StreamPage page) {
		this.access.request("-", request.method(), request.path(), HTTP_2);
		answerLocked(streamId, page, false);
	}

	/**
	 * Answers a stream of a route reached over HTTP/1.1 only {@code 421}; a request still
	 * being sent is reset with {@code NO_ERROR} once the answer is written (RFC 9113
	 * 8.1). With the loop's lock.
	 */
	private void misdirectLocked(int streamId, Router.Route route, ConnectionHeadParser.Head.Request request,
			boolean endStream) {
		this.meters.misdirectedForHttp1();
		this.access.misdirected(route.routeTag(), request);
		answerLocked(streamId, new ErrorResponse.StreamPage(this.errorResponse.misdirectedHeaders(), new byte[0]),
				!endStream);
	}

	/**
	 * Answers the client's stream with the page, its body through the flow controller --
	 * held to the client's windows; with {@code endsRequest}, a request still being sent
	 * is then reset with {@code NO_ERROR} once the page's end is written (RFC 9113 8.1:
	 * no more of it is wanted). With the loop's lock.
	 */
	private void answerLocked(int streamId, ErrorResponse.StreamPage page, boolean endsRequest) {
		Hop hop = client();
		byte[] body = page.body();
		ChannelPromise end = endsRequest ? hop.newPromise(future -> {
			if (future.isSuccess()) {
				resetRequestLocked(streamId, Http2Error.NO_ERROR.code());
			}
		}) : hop.ctx().newPromise();
		hop.encoder()
			.writeHeaders(hop.ctx(), streamId, page.headers(), 0, body.length == 0,
					body.length == 0 ? end : hop.ctx().newPromise());
		if (body.length > 0) {
			hop.encoder().writeData(hop.ctx(), streamId, Unpooled.wrappedBuffer(body), 0, true, end);
		}
	}

	/**
	 * The leg taking the route's new streams: the open one, else a new one on a slot of
	 * the connection limit ({@link #acquireSlotLocked}); null when the limit has no slot
	 * left or the leg could not be opened. With the loop's lock.
	 */
	private @Nullable Leg legLocked(Router.Route route, TunnelSession session) {
		Leg leg = this.legs.get(route);
		if (leg != null && leg.takesStreamsLocked()) {
			return leg;
		}
		Runnable releaseSlot = acquireSlotLocked();
		if (releaseSlot == null) {
			this.meters.refusedForLegLimit();
			return null;
		}
		VirtualConnection connection;
		try {
			connection = session.open(route.address());
		}
		catch (RuntimeException e) {
			releaseSlot.run();
			log.warn("h2 demux: the leg of route {} did not open: {}", route.routeTag(), e.toString(), e);
			return null;
		}
		Leg opening = new Leg(route.routeTag(), this.meters.route(route.routeTag()),
				this.meters.writesHeldOf(route.routeTag()));
		try {
			Hop hop = Hop.builder()
				.name("sluice-h2n-leg-" + connection.connectionId())
				.loop(this.loop)
				.pipe(DuplexPipe.of(connection))
				.listener(built -> new LegListener(opening))
				.codec(codec -> {
					configure(codec, new Http2Settings().pushEnabled(false));
					// streams past the upstream's concurrency limit wait in netty's
					// StreamBufferingEncoder
					codec.encoderEnforceMaxConcurrentStreams(true);
				})
				.onInactive(() -> this.legs.remove(route, opening))
				.onWritten(NettyHttp2DemuxRelay::reviewed)
				.traffic(traffic(route, session))
				.build();
			opening.hop = hop;
			hop.withholdConnectionWindowLocked(this.withheld);
			hop.watchWrites(watchingWrites());
			hop.addConnectionListener(new LegStreams(opening));
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
		ConnectionLimit.Permit permit = this.legSlots.tryAcquire();
		return permit == null ? null : permit::release;
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

	/** The client endpoint closed: so do the legs; with the loop's lock. */
	private void clientClosedLocked() {
		for (Leg leg : this.opened) {
			leg.hop().abortLocked();
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
			answerLocked(exchange.id, this.errorResponse.badGatewayPage(exchange.host, exchange.request), true);
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
	 * Whether {@code bytes} more may be held: within the allowance, or charged to the
	 * budget now if it has room for the part beyond; with the loop's lock.
	 */
	private boolean admitLocked(long bytes) {
		long change = Math.max(0, queuedLocked() + bytes - CONNECTION_ALLOWANCE) - this.pooled;
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
	 * Whether an upstream's header block of {@code bytes} may be written on the client
	 * hop: while what heads for the client -- the hop's writer queue and the header
	 * blocks of the responses not yet in it -- stays within the hop's read pause, the
	 * bound that queue keeps outside the budget anyway ({@link #HOP_QUEUE_BYTES}), so a
	 * response is not held back for bytes queued the other way; past it, when
	 * {@link #admitLocked} admits it. With the loop's lock.
	 */
	private boolean admitResponseBlockLocked(long bytes) {
		long towardsClient = client().queuedBytes();
		for (Exchange exchange : this.exchanges.values()) {
			towardsClient += exchange.responseFlow.blockBytes;
		}
		return towardsClient + bytes <= Hop.DEFAULT_READ_PAUSE_BYTES || admitLocked(bytes);
	}

	/**
	 * The bytes the relay holds: every leg's backlog (what it read and has not written on
	 * the other hop yet, both ways) and every hop's writer queue. Updates the route
	 * gauges and lets go of the legs that closed holding nothing; with the loop's lock.
	 */
	private long queuedLocked() {
		Hop client = this.client;
		long queued = client == null ? 0 : client.queuedBytes();
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
	 * Reviews the accounts once the channels settled (the loop's hook): the share of the
	 * queued bytes beyond the allowance is charged to the budget or released; past the
	 * allowance with the budget exhausted, every hop's connection window is withheld --
	 * the peers stop sending DATA once the windows they hold are spent -- and the waiting
	 * streams past the allowance are refused ({@link #refuseWaitingLocked}); within the
	 * allowance again, or with the budget's room back, the windows withheld are granted.
	 * Whether it wrote anything; with the loop's lock.
	 */
	private boolean reviewLocked() {
		if (this.accountsClosed || this.client == null) {
			return false;
		}
		long queued = queuedLocked();
		this.queued = queued;
		settleShareLocked(queued);
		boolean wrote = false;
		if (queued > CONNECTION_ALLOWANCE && !this.budget.hasRoom()) {
			if (!this.withheld) {
				this.withheld = true;
				this.meters.windowWithheld();
				wrote |= withholdLocked(true);
			}
			this.budget.awaitRoom(this.budgetRoom);
			wrote |= refuseWaitingLocked();
		}
		else if (this.withheld) {
			this.withheld = false;
			this.budget.cancel(this.budgetRoom);
			wrote |= withholdLocked(false);
		}
		boolean watch = watchingWrites();
		client().watchWrites(watch);
		for (Leg leg : this.opened) {
			leg.hop().watchWrites(watch);
		}
		return wrote;
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

	/** Charges or releases what makes the share the part beyond the allowance. */
	private void settleShareLocked(long queued) {
		long share = Math.max(0, queued - CONNECTION_ALLOWANCE);
		long change = share - this.pooled;
		this.pooled = share;
		if (change > 0) {
			this.budget.charge(change);
		}
		else if (change < 0) {
			this.budget.release(-change);
		}
	}

	/** Withholds or grants every hop's connection window; whether it wrote. */
	private boolean withholdLocked(boolean withhold) {
		boolean wrote = client().withholdConnectionWindowLocked(withhold);
		for (Leg leg : this.opened) {
			wrote |= leg.hop().withholdConnectionWindowLocked(withhold);
		}
		return wrote;
	}

	/**
	 * While the windows are withheld, keeps the bytes held for streams waiting for an
	 * upstream's concurrency limit within the allowance: past it, the most recent waiting
	 * streams leave their leg's buffer and are refused ({@code REFUSED_STREAM}: their
	 * upstream never saw them, RFC 9113 8.7) until the rest fits. Their bytes drain only
	 * once a stream of their leg closed on the upstream, which may need the very request
	 * body the withheld window holds back; the open streams' bytes drain on their own.
	 * Whether a stream was refused; with the loop's lock.
	 */
	private boolean refuseWaitingLocked() {
		List<Exchange> waiting = new ArrayList<>();
		long bytes = 0;
		for (Exchange exchange : this.exchanges.values()) {
			if (exchange.waiting && !exchange.reset) {
				waiting.add(exchange);
				bytes += exchange.waitingBytes();
			}
		}
		if (bytes <= CONNECTION_ALLOWANCE) {
			return false;
		}
		waiting.sort(Comparator.comparingInt(exchange -> exchange.id));
		for (int i = waiting.size() - 1; i >= 0 && bytes > CONNECTION_ALLOWANCE; i--) {
			Exchange exchange = waiting.get(i);
			bytes -= exchange.waitingBytes();
			exchange.reset = true;
			exchange.refused = true;
			dropWaitingLocked(exchange, Http2Error.CANCEL.code());
			resetClientLocked(exchange.id, Http2Error.REFUSED_STREAM);
			this.meters.refusedWaiting();
		}
		return true;
	}

	/**
	 * Closes the accounts once the relay finished: its share of the budget goes back, its
	 * route gauges drop what it still counted, and no review runs any more.
	 */
	private void closeAccountsLocked() {
		this.accountsClosed = true;
		this.budget.cancel(this.budgetRoom);
		settleShareLocked(0);
		for (Leg leg : this.opened) {
			if (leg.backlog != 0) {
				log.warn("h2 demux: a leg of route {} finished with {} bytes accounted", leg.routeTag, leg.backlog);
			}
			forgottenLocked(leg);
		}
		this.opened.clear();
	}

	/**
	 * Waits on a virtual thread until the hop's threads ended, then runs {@code done},
	 * which settles the hop's share of {@link #pending}.
	 */
	private void watch(Hop hop, Runnable done) {
		Thread.ofVirtual().name(hop.name() + "-done").start(() -> {
			try {
				if (!hop.awaitDone(HOP_DONE_TIMEOUT)) {
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

	/** One route's leg: the hop over its virtual connection. */
	private static final class Leg {

		final String routeTag;

		final H2DemuxMeters.Route routeMeters;

		/** Set once the hop is built. */
		@Nullable Hop hop;

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

		Leg(String routeTag, H2DemuxMeters.Route routeMeters, LongAdder writesHeld) {
			this.routeTag = routeTag;
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

		/** Whether a new stream may go to the leg; with the loop's lock. */
		boolean takesStreamsLocked() {
			Hop hop = hop();
			return hop.open() && !hop.connection().goAwayReceived() && !hop.connection().goAwaySent();
		}

	}

	/** A client stream relayed to a leg; with the loop's lock. */
	private static final class Exchange {

		final int id;

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

		Exchange(int id, Leg leg, Hop client) {
			this.id = id;
			this.leg = leg;
			this.requestFlow = new Flow(this, client, leg.hop());
			this.responseFlow = new Flow(this, leg.hop(), client);
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

		private final Hop to;

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

		Flow(Exchange exchange, Hop from, Hop to) {
			this.exchange = exchange;
			this.from = from;
			this.to = to;
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
			int streamId = this.exchange.id;
			this.to.encoder()
				.writeData(this.to.ctx(), streamId, data, 0, endStream,
						this.to.newPromise(future -> dataWritten(future.isSuccess(), endStream)));
		}

		private void dataWritten(boolean success, boolean endStream) {
			int length = this.writing;
			account(-this.writingBytes);
			this.exchange.leg.writesHeld.decrement();
			this.writing = -1;
			this.writingBytes = 0;
			if (success) {
				consume(this.from, this.exchange.id, length);
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
				byte[] bytes = this.backlog.takeAll();
				boolean ends = this.backlogEnds;
				this.backlogEnds = false;
				account(-capacity);
				writeData(size == 0 ? Unpooled.EMPTY_BUFFER : Unpooled.wrappedBuffer(bytes, 0, size), size, capacity,
						ends);
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
				.writeHeaders(this.to.ctx(), this.exchange.id, headers, 0, endStream, this.to.newPromise(future -> {
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
				if (!admitLocked(headerBytes(headers))) {
					resetForBudgetLocked(exchange);
					return;
				}
				exchange.requestEnded |= endStream;
				exchange.requestFlow.block(headers, endStream);
				return;
			}
			if (streamId <= NettyHttp2DemuxRelay.this.highestStreamId) {
				return; // a later block of a stream the relay answered itself
			}
			NettyHttp2DemuxRelay.this.highestStreamId = streamId;
			routeLocked(streamId, headers, endStream);
		}

		@Override
		public int onDataRead(ChannelHandlerContext ctx, int streamId, ByteBuf data, int padding, boolean endStream) {
			Exchange exchange = NettyHttp2DemuxRelay.this.exchanges.get(streamId);
			if (exchange == null || exchange.reset) {
				return data.readableBytes() + padding;
			}
			exchange.requestEnded |= endStream;
			exchange.requestFlow.data(data, endStream);
			// padding is consumed at once
			return padding;
		}

		@Override
		public void onRstStreamRead(ChannelHandlerContext ctx, int streamId, long errorCode) {
			resetByPeerLocked(this.hop, streamId);
			Exchange exchange = NettyHttp2DemuxRelay.this.exchanges.get(streamId);
			if (exchange != null && !exchange.reset) {
				exchange.reset = true;
				// a waiting stream leaves the leg's buffer as netty closes the client's
				// stream right after this call (ClientStreams)
				resetLocked(exchange.leg.hop(), streamId, errorCode);
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
			Exchange exchange = NettyHttp2DemuxRelay.this.exchanges.remove(stream.id());
			if (exchange == null) {
				return;
			}
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
				resetLocked(exchange.leg.hop(), exchange.id, Http2Error.CANCEL.code());
			}
		}

		@Override
		public void onGoAwaySent(int lastStreamId, long errorCode, ByteBuf debugData) {
			if (errorCode == Http2Error.COMPRESSION_ERROR.code()) {
				// a header block that did not decode: nothing of it is known
				NettyHttp2DemuxRelay.this.access.request("-", "-", "-", HTTP_2);
			}
		}

	}

	/** A leg's frames: heads, DATA and resets go to the client. */
	private final class LegListener extends Http2FrameAdapter {

		private final Leg leg;

		LegListener(Leg leg) {
			this.leg = leg;
		}

		private @Nullable Exchange exchange(int streamId) {
			Exchange exchange = NettyHttp2DemuxRelay.this.exchanges.get(streamId);
			return exchange == null || exchange.leg != this.leg || exchange.reset ? null : exchange;
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
		 * ({@link #admitResponseBlockLocked}): past the client hop's read pause and the
		 * allowance with the budget exhausted, the stream is reset instead
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
					exchange.requestFlow.drop();
					loseLocked(exchange);
					resetLocked(this.leg.hop(), streamId, Http2Error.ENHANCE_YOUR_CALM.code());
					NettyHttp2DemuxRelay.this.meters.resetForInterim();
					return;
				}
			}
			if (!admitResponseBlockLocked(bytes)) {
				resetForBudgetLocked(exchange);
				return;
			}
			exchange.responseEnded |= endStream;
			exchange.responseFlow.block(headers, endStream);
		}

		@Override
		public int onDataRead(ChannelHandlerContext ctx, int streamId, ByteBuf data, int padding, boolean endStream) {
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
			resetLocked(client(), streamId, errorCode);
		}

		/**
		 * The upstream's GOAWAY: the streams above its last stream id are refused once
		 * netty dropped them (it does right after this call), and the leg closes
		 * gracefully -- a GOAWAY of its own, then the close once the other streams ended
		 * -- as a task, run when netty is done with the GOAWAY.
		 */
		@Override
		public void onGoAwayRead(ChannelHandlerContext ctx, int lastStreamId, long errorCode, ByteBuf debugData) {
			for (Exchange exchange : NettyHttp2DemuxRelay.this.exchanges.values()) {
				if (exchange.leg == this.leg && exchange.id > lastStreamId) {
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
			Exchange exchange = NettyHttp2DemuxRelay.this.exchanges.get(stream.id());
			if (exchange != null && exchange.leg == this.leg) {
				stopWaitingLocked(exchange);
			}
		}

		@Override
		public void onStreamClosed(Http2Stream stream) {
			if (resetByCodecLocked(stream)) {
				NettyHttp2DemuxRelay.this.meters.resetForUpstream();
			}
			Exchange exchange = NettyHttp2DemuxRelay.this.exchanges.get(stream.id());
			if (exchange != null && exchange.leg == this.leg) {
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

	/** Hands out the slots of the connection limit further legs open on. */
	@FunctionalInterface
	interface LegSlots {

		/** A slot for one more leg, or null when the limit has none left. */
		ConnectionLimit.@Nullable Permit tryAcquire();

	}

	static final class Builder {

		private @Nullable DuplexPipe pipe;

		private ConnectionHeadParser.@Nullable Head head;

		private @Nullable Router router;

		private @Nullable SessionRegistry sessions;

		private @Nullable AccessControl accessControl;

		private @Nullable ErrorResponse errorResponse;

		private AccessLogger.@Nullable Connection access;

		private @Nullable H2DemuxMeters meters;

		private Router.@Nullable Route initialRoute;

		private @Nullable TunnelSession initialSession;

		private @Nullable InetAddress peer;

		private ConnectionLimits.@Nullable KeepAliveDeadline deadline;

		private Duration streamWaitTimeout = Duration.ZERO;

		private @Nullable Runnable onFinish;

		private @Nullable Runnable release;

		private @Nullable H2DemuxBudget budget;

		private @Nullable LegSlots legSlots;

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

		NettyHttp2DemuxRelay build() {
			return new NettyHttp2DemuxRelay(this);
		}

	}

}
