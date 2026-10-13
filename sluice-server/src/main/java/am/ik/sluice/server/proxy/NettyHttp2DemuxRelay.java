package am.ik.sluice.server.proxy;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.SequenceInputStream;
import java.net.InetAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import am.ik.sluice.server.proxy.hop.Hop;
import am.ik.sluice.server.proxy.hop.Loop;
import am.ik.sluice.server.route.Router;
import am.ik.sluice.server.tunnel.SessionRegistry;
import am.ik.sluice.server.tunnel.TunnelSession;
import am.ik.sluice.tunnel.DuplexPipe;
import am.ik.sluice.tunnel.VirtualConnection;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
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
 * {@code rewrite-host} the head's {@code :authority} becomes the route's address. DATA
 * read on one hop is written on the other as a retained slice and consumed (its window
 * granted back) once written, so what the relay holds per stream is bounded by the window
 * it announced. Resets pass either way with their code; an upstream's reset after the
 * response's end follows that end to the client. A stream the client ends without its
 * request's end resets its upstream stream with {@code CANCEL}.
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
 * netty's codec answers the client's protocol errors itself; a header block that does not
 * decode ends the connection with a GOAWAY {@code COMPRESSION_ERROR}, logged as a request
 * line naming neither route, method nor path. The client endpoint's end -- EOF, error,
 * the codec closing it -- closes every leg. Once every hop's threads ended and the frames
 * the legs queued left the tunnel sender, the relay finishes: the connection is logged
 * closed, {@code onFinish} runs, then {@code release}.
 */
final class NettyHttp2DemuxRelay {

	private static final Logger log = LoggerFactory.getLogger(NettyHttp2DemuxRelay.class);

	/** The concurrent streams the relay advertises to the client. */
	static final int MAX_CONCURRENT_STREAMS = 100;

	/** The header list size limit of both faces, mirroring the connection head's. */
	static final int MAX_HEADER_LIST_SIZE = 64 * 1024;

	/** How long a hop's threads may take to end once its channel closed. */
	private static final Duration HOP_DONE_TIMEOUT = Duration.ofSeconds(30);

	private static final String HTTP_2 = "2";

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

	private final Loop loop;

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

	/** The client hop, plus each leg until its queued frames left; zero = finished. */
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
		this.loop = new Loop("sluice-h2n",
				(source, cause) -> log.warn("h2 demux {}: {}", source, cause.toString(), cause));
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
	 * relay writes decodes on its own whatever the peer's decoder saw before -- as the
	 * legacy relay's table-less encoding does.
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
		Leg leg = legLocked(route, session);
		if (leg == null) {
			resetClientLocked(streamId, Http2Error.REFUSED_STREAM);
			return;
		}
		if (route.rewriteHost()) {
			headers.authority(AsciiString.of(route.address()));
		}
		Exchange exchange = new Exchange(streamId, leg);
		exchange.host = host;
		exchange.request = request;
		exchange.requestEnded = endStream;
		this.exchanges.put(streamId, exchange);
		leg.routeMeters.trackedStreams().increment();
		Hop hop = leg.hop();
		ChannelPromise written = hop.newPromise(future -> {
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
	private static void dropWaitingLocked(Exchange exchange, long errorCode) {
		stopWaitingLocked(exchange);
		Hop leg = exchange.leg.hop();
		leg.encoder().writeRstStream(leg.ctx(), exchange.id, errorCode, leg.ctx().newPromise());
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
	 * The leg taking the route's new streams: the open one, else a new one; null when
	 * none could be opened. With the loop's lock.
	 */
	private @Nullable Leg legLocked(Router.Route route, TunnelSession session) {
		Leg leg = this.legs.get(route);
		if (leg != null && leg.takesStreamsLocked()) {
			return leg;
		}
		VirtualConnection connection = session.open(route.address());
		Leg opening = new Leg(this.meters.route(route.routeTag()));
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
				.build();
			opening.hop = hop;
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
			return null;
		}
		if (this.opened.isEmpty()) {
			this.access.connectionId(connection.connectionId());
		}
		this.opened.add(opening);
		this.legs.put(route, opening);
		this.pending.incrementAndGet();
		watch(opening.hop(), () -> {
			try {
				connection.close();
				session.remove(connection.connectionId());
			}
			finally {
				// the connection's slot covers what the leg still queues for the tunnel
				connection.afterQueued(this::settle);
			}
		});
		return opening;
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
		reset(client(), streamId, error.code());
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
			hop.encoder().writeRstStream(hop.ctx(), streamId, errorCode, hop.ctx().newPromise());
		}
	}

	/** Resets the hop's stream unless it is closed or gone; with the loop's lock. */
	private static void reset(Hop hop, int streamId, long errorCode) {
		Http2Stream stream = hop.connection().stream(streamId);
		if (stream == null || stream.state() == Http2Stream.State.CLOSED) {
			// netty would write a reset of an unknown stream all the same
			return;
		}
		hop.encoder().writeRstStream(hop.ctx(), streamId, errorCode, hop.ctx().newPromise());
	}

	/**
	 * The peer reset the hop's stream, which netty closes right after the listener: marks
	 * the reset as sent, so netty does not answer it with one of its own (RFC 9113 5.4.2)
	 * -- as it would with RST_STREAM {@code STREAM_CLOSED} when closing fails a write of
	 * the stream still waiting for its window. With the loop's lock.
	 */
	private static void resetByPeer(Hop hop, int streamId) {
		Http2Stream stream = hop.connection().stream(streamId);
		if (stream != null) {
			stream.resetSent();
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

	/** Writes DATA read on one hop to the other; the bytes consumed at once (padding). */
	private static int forward(Hop from, Hop to, int streamId, ByteBuf data, int padding, boolean endStream) {
		int length = data.readableBytes();
		to.encoder()
			.writeData(to.ctx(), streamId, data.retainedSlice(), 0, endStream,
					to.newPromise(future -> consume(from, streamId, length)));
		return padding;
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

	/** One of {@link #pending} settled; the last finishes the relay. */
	private void settle() {
		if (this.pending.decrementAndGet() == 0 && this.finished.compareAndSet(false, true)) {
			this.deadline.disarm();
			this.loop.close();
			this.access.close();
			try {
				this.onFinish.run();
			}
			finally {
				this.release.run();
			}
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

		final H2DemuxMeters.Route routeMeters;

		/** Set once the hop is built. */
		@Nullable Hop hop;

		/** The upstream's first SETTINGS has not arrived; with the loop's lock. */
		boolean awaitingSettings = true;

		Leg(H2DemuxMeters.Route routeMeters) {
			this.routeMeters = routeMeters;
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

		Exchange(int id, Leg leg) {
			this.id = id;
			this.leg = leg;
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
				if (!exchange.reset) {
					exchange.requestEnded |= endStream;
					Hop leg = exchange.leg.hop();
					leg.encoder().writeHeaders(leg.ctx(), streamId, headers, 0, endStream, leg.ctx().newPromise());
				}
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
			return forward(this.hop, exchange.leg.hop(), streamId, data, padding, endStream);
		}

		@Override
		public void onRstStreamRead(ChannelHandlerContext ctx, int streamId, long errorCode) {
			resetByPeer(this.hop, streamId);
			Exchange exchange = NettyHttp2DemuxRelay.this.exchanges.get(streamId);
			if (exchange != null && !exchange.reset) {
				exchange.reset = true;
				// a waiting stream leaves the leg's buffer as netty closes the client's
				// stream right after this call (ClientStreams)
				reset(exchange.leg.hop(), streamId, errorCode);
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
			Exchange exchange = NettyHttp2DemuxRelay.this.exchanges.remove(stream.id());
			if (exchange == null) {
				return;
			}
			exchange.leg.routeMeters.trackedStreams().decrement();
			if (exchange.waiting) {
				exchange.reset = true;
				dropWaitingLocked(exchange, Http2Error.CANCEL.code());
			}
			else if (!exchange.reset && !(exchange.requestEnded && exchange.responseEnded)) {
				exchange.reset = true;
				reset(exchange.leg.hop(), exchange.id, Http2Error.CANCEL.code());
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

		@Override
		public void onHeadersRead(ChannelHandlerContext ctx, int streamId, Http2Headers headers, int padding,
				boolean endStream) {
			Exchange exchange = exchange(streamId);
			if (exchange == null) {
				return;
			}
			exchange.responseEnded |= endStream;
			Hop client = client();
			client.encoder()
				.writeHeaders(client.ctx(), streamId, headers, 0, endStream, endStream
						? client.newPromise(future -> responseWrittenLocked(exchange)) : client.ctx().newPromise());
		}

		@Override
		public int onDataRead(ChannelHandlerContext ctx, int streamId, ByteBuf data, int padding, boolean endStream) {
			Exchange exchange = exchange(streamId);
			if (exchange == null) {
				return data.readableBytes() + padding;
			}
			exchange.responseEnded |= endStream;
			Hop from = this.leg.hop();
			Hop client = client();
			int length = data.readableBytes();
			client.encoder()
				.writeData(client.ctx(), streamId, data.retainedSlice(), 0, endStream, client.newPromise(future -> {
					consume(from, streamId, length);
					if (endStream) {
						responseWrittenLocked(exchange);
					}
				}));
			// padding is consumed at once
			return padding;
		}

		@Override
		public void onRstStreamRead(ChannelHandlerContext ctx, int streamId, long errorCode) {
			resetByPeer(this.leg.hop(), streamId);
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
			reset(client(), streamId, errorCode);
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
	 * the client.
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
			Exchange exchange = NettyHttp2DemuxRelay.this.exchanges.get(stream.id());
			if (exchange != null && exchange.leg == this.leg) {
				loseLocked(exchange);
			}
		}

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

		NettyHttp2DemuxRelay build() {
			return new NettyHttp2DemuxRelay(this);
		}

	}

}
