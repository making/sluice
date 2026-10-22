package am.ik.sluice.server.proxy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import am.ik.sluice.server.auth.Origin;
import am.ik.sluice.server.auth.RouteAuthenticator;
import am.ik.sluice.server.auth.Verdict;
import am.ik.sluice.server.route.Router;
import am.ik.sluice.server.tunnel.SessionRegistry;
import am.ik.sluice.server.tunnel.TunnelSession;
import am.ik.sluice.tunnel.DuplexPipe;
import am.ik.sluice.tunnel.StreamRelay;
import am.ik.sluice.tunnel.VirtualConnection;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Relays one plaintext HTTP/1.1 client connection over per-request legs: the authority of
 * every request head is resolved through the router and, when it belongs to another route
 * than the leg in charge, the outbound side switches to a virtual connection of that
 * route at the request boundary -- the requests of a keep-alive connection are strictly
 * sequential, and the switch waits for the outstanding response of the old leg to be
 * relayed completely before the new leg is opened, so responses never interleave. The
 * drained leg is closed once replaced, and the new leg's first request waits until the
 * frames still queued for the old one left the tunnel sender, so the connection's slot of
 * the connection limit never covers the send queues of two legs. The closure of the leg
 * in charge by its upstream ends the whole relay, as does any failure; the slot is
 * released once the relay ended and every leg's queued frames left. A leg in charge that
 * ends -- its dial failed, or its upstream closed or failed -- before any of a response
 * to its first request was relayed gets the client the bad-gateway page ({@code 502}) in
 * place of it; the connection then ends as after a refused request. A later request of
 * the leg lost the same way is not answered: the client sees its connection end, as it
 * would see the upstream's, and may retry the request. Each leg pumps its inbound side on
 * its own virtual thread for the lifetime of its virtual connection; a leg closed by the
 * relay leaves its tunnel session at once.
 * <p>
 * A leg in charge whose client withdrew ({@link TunnelSession#withdrawn()}) is retired at
 * the first request boundary -- no response owed, nothing of a next request read, the
 * connection not relayed verbatim, no request or response on it asked to close the
 * connection (that close ends the relay, as before) -- so the client's drain does not
 * wait on an idle connection: the relay closes the leg, and the next request opens a leg
 * on the route resolved anew. The decision is taken under the same lock as the start of a
 * request: a request head read after it goes to a new leg, one read before it holds the
 * leg.
 * <p>
 * A request asking for an upgrade, or a CONNECT, holds the parse -- the relay reads no
 * further client bytes -- until the leg's response to it: switched (101, a 2xx to
 * CONNECT), the connection is relayed verbatim on the leg in charge from then on;
 * declined, the parse resumes with the next request.
 * <p>
 * Every request head is checked against its route's credentials
 * ({@link RouteAuthenticator}), the first one included; the headers of the route's
 * credentials are removed from a head that passes unless the route forwards them, and the
 * headers its verdict adds are set. A head the check answers itself -- a refusal, the
 * browser sign-in's redirect -- ends the connection like a refused one.
 * <p>
 * A malformed request -- input the parser cannot follow, see {@link Http1RequestParser}
 * -- ends the connection at a request boundary: nothing of it or after it is relayed, the
 * responses to the requests before it are relayed in full, and then, unless a response to
 * the malformed request itself has begun meanwhile (its head was relayed and the upstream
 * answered early), a {@code 400} is written. A request refused -- no route, no session,
 * the access control, its route's credentials -- ends it the same way with its error
 * response. The relay then half-closes the connection and reads what the client still
 * sends for up to {@link #LINGER} (at most {@link #LINGER_LIMIT}) before closing, so the
 * client reads the answer rather than a reset.
 * <p>
 * Between requests the connection lives under its
 * {@link ConnectionLimits.KeepAliveDeadline}: once every response owed is relayed and
 * nothing of a next request was read, the idle timeout runs; the first bytes of the next
 * request stop it, and the head timeout runs until that head is complete. While a request
 * or response is in flight -- a response not yet complete, a request body still arriving
 * -- the stall timeout counts the time the relay waits on the client: a write to it not
 * completed, or a read of a request body -- not while the relay is held back from reading
 * by a leg that takes no more, nor while a {@code 100 Continue} is awaited (the request
 * asked for one, and neither a response head nor a body byte came since). Waiting for an
 * upstream's response does not count. Bytes read from the client and written to it count
 * as progress, a write as each of its pieces of
 * {@link ConnectionLimits.KeepAliveDeadline#WRITE_PIECE} completes. An upgrade puts the
 * connection under the verbatim idle timeout of the leg's route for good: bytes read from
 * the client and written to it count as activity. A response whose framing the tracker
 * cannot follow is relayed as an unframed one: it stays owed until the upstream closes
 * the connection, which ends the relay.
 * <p>
 * TCP tells a client that half-closed from one gone entirely only on a write: the second
 * write to a gone client fails, once its stack answered the first with a reset. So once
 * the client ended its side, while a response is owed and none of it was relayed, the
 * relay writes a {@code 100 Continue} whenever the connection stayed quiet -- nothing
 * written to the client, no request relayed -- for the probe interval; a failed write
 * ends the relay, failing its tunnel connection. A 1xx is the only thing HTTP/1.1 lets a
 * server write before a final response (RFC 9110 15.2); nothing can be added to a
 * response begun, to a response the tracker cannot follow, to an upgraded connection, or
 * towards an HTTP/1.0 request, which must not get a 1xx. One response gets at most
 * {@link #MAX_PROBES} probes.
 */
final class ReroutingRelay {

	private static final int BUFFER_SIZE = 64 * 1024;

	private static final String METRIC_NAME = "sluice.tunnel.bytes";

	/**
	 * What a probe writes; .kb/connection-limits.md "Half-close probes" tells why a 100.
	 */
	private static final byte[] PROBE = "HTTP/1.1 100 Continue\r\n\r\n".getBytes(StandardCharsets.US_ASCII);

	/**
	 * The probes one response may get: some clients fail a response after a few interim
	 * responses (Go's net/http before 1.24 at the sixth, the JDK HttpClient at the
	 * ninth), and the upstream may send its own.
	 */
	private static final int MAX_PROBES = 4;

	private static final Logger log = LoggerFactory.getLogger(ReroutingRelay.class);

	/** How long the client of a malformed request is read after the 400, at most. */
	static final Duration LINGER = Duration.ofSeconds(2);

	/** The most read from the client of a malformed request after the 400. */
	static final long LINGER_LIMIT = 1024 * 1024;

	private final DuplexPipe pipe;

	private final byte[] seedHead;

	private final Router router;

	private final SessionRegistry sessions;

	private final AccessControl accessControl;

	private final RouteAuthenticator routeAuth;

	private final ErrorResponse errorResponse;

	private final AccessLogger.Connection access;

	private final MeterRegistry meterRegistry;

	private final Router.Route initialRoute;

	private final TunnelSession initialSession;

	private final @Nullable InetAddress peer;

	/** How the connection reached the data plane, told by every relayed head. */
	private final Forwarding forwarding;

	private final Runnable onFinish;

	/** Frees the connection's slot; run once the legs' queued frames left. */
	private final Runnable release;

	private final ConnectionLimits.KeepAliveDeadline deadline;

	/** The quiet after which a half-closed client is probed; zero = never. */
	private final long probeIntervalNanos;

	/** When the client was last written to. */
	private volatile long lastClientWrite = System.nanoTime();

	/** When a request head was last relayed. */
	private volatile long lastRequest = System.nanoTime();

	/** Guards {@link #owed}, {@link #position} and {@link #starting}. */
	private final Object deadlineLock = new Object();

	/** Responses owed by the legs: requests relayed whose response is not complete. */
	private int owed;

	/**
	 * Where the outbound parse stood after the last bytes it was fed; written with
	 * {@link #deadlineLock}, read by the stall watch without it.
	 */
	private volatile Http1RequestParser.Position position = Http1RequestParser.Position.BETWEEN_REQUESTS;

	/** Threads writing to the client, or waiting to; the stall watch reads it. */
	private final AtomicInteger writers = new AtomicInteger();

	/** The outbound side waits in a read from the client. */
	private volatile boolean reading;

	/** The time {@link #waitsOnClient()} held, for the stall watch. */
	private final WaitClock clientWaits = new WaitClock(this::waitsOnClient);

	/**
	 * The last request head asked for {@code 100 Continue}, and neither a response head
	 * nor a body byte of it came since: the client may hold its body back meanwhile.
	 */
	private volatile boolean continueAwaited;

	/** The outbound side is feeding bytes read at a request boundary. */
	private boolean starting = true;

	/** Serializes the writes of the shared client stream. */
	private final ReentrantLock sinkLock = new ReentrantLock();

	/** Runs between a probe's decision and its write. */
	private final ProbeSeam probeSeam;

	/** Runs between the choice of a request head's leg and the head's relaying. */
	private final HeadSeam headSeam;

	/**
	 * A malformed request was rejected, or the leg in charge lost a request and the 502
	 * answered it: the legs relay nothing more to the client, nothing more is relayed to
	 * them, and their end leaves the client connection to the linger; set with
	 * {@link #sinkLock}.
	 */
	private volatile boolean rejected;

	private final List<Leg> legs = new CopyOnWriteArrayList<>();

	/** The outbound side plus one side per leg; the relay ends when it drains. */
	private final AtomicInteger pending = new AtomicInteger(1);

	private final AtomicBoolean finished = new AtomicBoolean();

	/** The relay was torn down ({@link #abort}): a leg's end is not answered. */
	private volatile boolean aborted;

	private boolean firstHead = true;

	/**
	 * The answer to a refused request head, written at the request boundary; nothing
	 * after the head is relayed. Outbound side only.
	 */
	private byte @Nullable [] refusal;

	@Nullable private volatile Leg current;

	ReroutingRelay(DuplexPipe pipe, ConnectionHeadParser.Head head, Router router, SessionRegistry sessions,
			AccessControl accessControl, RouteAuthenticator routeAuth, ErrorResponse errorResponse,
			AccessLogger.Connection access, MeterRegistry meterRegistry, Router.Route initialRoute,
			TunnelSession initialSession, @Nullable InetAddress peer, Forwarding forwarding,
			ConnectionLimits.KeepAliveDeadline deadline, Duration probeInterval, ProbeSeam probeSeam, HeadSeam headSeam,
			Runnable onFinish, Runnable release) {
		this.pipe = pipe;
		this.seedHead = head.bytes();
		this.router = router;
		this.sessions = sessions;
		this.accessControl = accessControl;
		this.routeAuth = routeAuth;
		this.errorResponse = errorResponse;
		this.access = access;
		this.meterRegistry = meterRegistry;
		this.initialRoute = initialRoute;
		this.initialSession = initialSession;
		this.peer = peer;
		this.forwarding = forwarding;
		this.onFinish = onFinish;
		this.release = release;
		this.deadline = deadline;
		this.probeIntervalNanos = probeInterval.toNanos();
		this.probeSeam = probeSeam;
		this.headSeam = headSeam;
		deadline.onExpiry(this::abort);
		deadline.watchStallClock(this.clientWaits::nanos, this::abort);
	}

	/**
	 * Whether the relay waits on the client: a write to it not completed, or a read of a
	 * request body that is due. Measured by {@link #clientWaits}, told every change of
	 * what it reads; never blocks.
	 */
	private boolean waitsOnClient() {
		return this.writers.get() > 0
				|| this.reading && this.position == Http1RequestParser.Position.IN_BODY && !this.continueAwaited;
	}

	/** Whether a thread waits to write to the client; for tests. */
	boolean sinkAwaited() {
		return this.sinkLock.hasQueuedThreads();
	}

	/**
	 * Starts the outbound side; the leg for the head consumed with the connection is
	 * opened by its event, further legs on demand.
	 */
	void start() {
		Thread.ofVirtual().name("sluice-reroute-out").start(this::pumpOutbound);
	}

	/**
	 * The outbound side: parses request heads, resolves each one's route, and relays the
	 * stream to the leg in charge, switching legs at request boundaries.
	 */
	private void pumpOutbound() {
		try {
			Http1RequestParser parser = new Http1RequestParser(new Http1RequestParser.Listener() {

				@Override
				public void head(byte[] head, @Nullable String host) {
					ReroutingRelay.this.onRequestHead(head, host);
				}

				@Override
				public void data(byte[] bytes, int offset, int length) {
					ReroutingRelay.this.onRequestData(bytes, offset, length);
				}
			});
			parser.feed(this.seedHead, 0, this.seedHead.length);
			if (this.refusal != null) {
				rejectRefused(this.refusal);
				return;
			}
			awaitUpgradeAnswers(parser);
			if (parser.position() == Http1RequestParser.Position.MALFORMED) {
				rejectMalformed(parser);
				return;
			}
			fed(parser.position());
			InputStream in = this.pipe.source();
			byte[] buffer = new byte[BUFFER_SIZE];
			int n;
			while ((n = readClient(in, buffer)) > 0) {
				if (this.rejected) {
					// a lost request was answered: nothing more is relayed
					linger();
					return;
				}
				if (!feeding()) {
					// the deadline passed before these bytes arrived: the connection is
					// being closed, and its request must not reach an upstream
					return;
				}
				parser.feed(buffer, 0, n);
				if (this.refusal != null) {
					rejectRefused(this.refusal);
					return;
				}
				awaitUpgradeAnswers(parser);
				if (parser.position() == Http1RequestParser.Position.MALFORMED) {
					rejectMalformed(parser);
					return;
				}
				fed(parser.position());
			}
			parser.finish();
			Leg leg = this.current;
			if (leg != null) {
				leg.halfClose();
				// the legs before it are drained: only this one can still owe responses
				probeWhileOwed(leg);
			}
		}
		catch (Exception e) {
			if (this.rejected) {
				// answered already: what the client still sends must not reset the answer
				log.debug("rerouting relay: relaying the client's requests failed after the answer; lingering", e);
				closeLegs();
				linger();
			}
			else {
				log.debug("rerouting relay: relaying the client's requests failed; aborting", e);
				abort();
			}
		}
		finally {
			settle();
		}
	}

	/** Reads from the client, marked for the stall watch; bytes read are progress. */
	private int readClient(InputStream in, byte[] buffer) throws IOException {
		this.reading = true;
		this.clientWaits.update();
		try {
			int n = in.read(buffer);
			if (n > 0) {
				this.deadline.progress(n);
			}
			return n;
		}
		finally {
			this.reading = false;
			this.clientWaits.update();
		}
	}

	/**
	 * After the client ended its side: probes it while {@code leg} owes a response, until
	 * the leg ended or relayed every response owed. A write failing throws, which ends
	 * the relay.
	 */
	private void probeWhileOwed(Leg leg) throws InterruptedException {
		if (this.probeIntervalNanos <= 0) {
			return;
		}
		int probes = 0;
		int completed = -1;
		long attempted = Long.MIN_VALUE;
		while (true) {
			synchronized (leg) {
				if (leg.ended || leg.completed >= leg.sent) {
					return;
				}
				if (leg.completed != completed) {
					// a response completed since: the next one starts its own count
					completed = leg.completed;
					probes = 0;
				}
				long quiet = System.nanoTime() - Math.max(Math.max(this.lastClientWrite, this.lastRequest), attempted);
				if (probes >= MAX_PROBES) {
					leg.wait(); // until a response completes or the leg ends
					continue;
				}
				if (quiet < this.probeIntervalNanos) {
					TimeUnit.NANOSECONDS.timedWait(leg, this.probeIntervalNanos - quiet);
					continue;
				}
			}
			attempted = System.nanoTime();
			this.sinkLock.lock();
			try {
				// the inbound side writes and tracks under this lock: the decision holds
				// until the probe is written
				if (!this.rejected && leg.probeable()) {
					this.probeSeam.decided(this);
					writeToClient(PROBE, 0, PROBE.length);
					probes++;
				}
			}
			finally {
				this.sinkLock.unlock();
			}
		}
	}

	/**
	 * While the parser holds after a request asking for an upgrade, waits for the leg's
	 * answer and reports it; a leg ending first counts as switched (verbatim), the relay
	 * ending with it.
	 */
	private void awaitUpgradeAnswers(Http1RequestParser parser) {
		while (parser.position() == Http1RequestParser.Position.AWAITING_UPGRADE) {
			Leg leg = this.current;
			parser.upgradeAnswered(leg == null || leg.awaitUpgradeAnswer());
		}
	}

	/**
	 * Bytes were read: at a request boundary they begin a request, which stops the idle
	 * timeout. {@code false} when a timeout passed first.
	 */
	private boolean feeding() {
		synchronized (this.deadlineLock) {
			if (this.position == Http1RequestParser.Position.BETWEEN_REQUESTS) {
				this.starting = true;
			}
			return updateDeadlineLocked();
		}
	}

	/**
	 * The bytes read were parsed; the parser now stands at {@code position}. A request
	 * body ending after its response retires a leg whose client withdrew.
	 */
	private void fed(Http1RequestParser.Position position) {
		synchronized (this.deadlineLock) {
			this.starting = false;
			this.position = position;
			this.clientWaits.update();
			updateDeadlineLocked();
		}
		Leg leg = this.current;
		if (leg != null) {
			retireIfWithdrawn(leg);
		}
	}

	/**
	 * Retires the leg in charge once its client withdrew, at a request boundary: no
	 * response owed, nothing of a next request read ({@link #starting}, set under the
	 * same lock before a request head is parsed), not relayed verbatim, no exchange on it
	 * closing the connection ({@link Leg#closing}). The leg stops being in charge -- the
	 * next request head opens a leg on the route resolved anew -- and is closed, its EOF
	 * not ending the relay. Runs on the leg's withdrawal, as a response of the leg
	 * completes, and as the parser reaches a boundary.
	 */
	private void retireIfWithdrawn(Leg leg) {
		if (!leg.session.withdrawn()) {
			return;
		}
		synchronized (this.deadlineLock) {
			if (this.current != leg || this.starting || this.owed > 0
					|| this.position != Http1RequestParser.Position.BETWEEN_REQUESTS || leg.closing) {
				return;
			}
			leg.replaced = true; // its EOF is the retirement, not the end
			this.current = null;
		}
		log.debug("rerouting relay: the client of connection {} (route {}) withdrew; retiring the leg",
				leg.connection.connectionId(), leg.route.routeTag());
		leg.retire();
	}

	/** A leg's responses owed changed by {@code delta}. */
	private void owedChanged(int delta) {
		if (delta == 0) {
			return;
		}
		synchronized (this.deadlineLock) {
			this.owed += delta;
			updateDeadlineLocked();
		}
	}

	/**
	 * Enters the phase the connection's state calls for; with {@link #deadlineLock}.
	 * {@code false} once a timeout passed.
	 */
	private boolean updateDeadlineLocked() {
		if (this.position == Http1RequestParser.Position.PASSTHROUGH) {
			// relayed verbatim on the leg in charge from now on
			Leg leg = this.current;
			return this.deadline.relayVerbatim(leg != null ? leg.route : this.initialRoute);
		}
		ConnectionLimits.KeepAliveDeadline.Phase phase;
		if (this.starting || this.owed > 0 || this.position == Http1RequestParser.Position.IN_BODY
				|| this.position == Http1RequestParser.Position.MALFORMED) {
			phase = ConnectionLimits.KeepAliveDeadline.Phase.BUSY;
		}
		else if (this.position == Http1RequestParser.Position.IN_HEAD) {
			phase = ConnectionLimits.KeepAliveDeadline.Phase.HEAD;
		}
		else {
			phase = ConnectionLimits.KeepAliveDeadline.Phase.IDLE;
		}
		return this.deadline.enter(phase);
	}

	/**
	 * Ends the connection at a malformed request: waits until the responses to the
	 * requests before it are relayed, writes the 400 unless a response to the malformed
	 * request has begun, closes the legs, and lingers.
	 */
	private void rejectMalformed(Http1RequestParser parser) {
		fed(Http1RequestParser.Position.MALFORMED);
		boolean inBody = parser.malformedInBody();
		if (!inBody) {
			ConnectionHeadParser.Head.@Nullable Request request = ConnectionHeadParser
				.http1RequestOf(parser.malformedHead());
			this.access.request(null, request);
		}
		// the requests before the malformed one: every one relayed, but its own head
		rejectAtBoundary(this.errorResponse.badRequest(), inBody);
	}

	/**
	 * Ends the connection at a refused request head: waits until the responses to the
	 * requests before it are relayed, writes the refusal, closes the legs, and lingers.
	 */
	private void rejectRefused(byte[] response) {
		// nothing more is parsed or relayed, as after a malformed request
		fed(Http1RequestParser.Position.MALFORMED);
		rejectAtBoundary(response, false);
	}

	/**
	 * Waits until the leg answered the requests before the rejected one -- every request
	 * it was sent, or all but the last when that is the rejected one ({@code sent}: its
	 * head was relayed, its body is malformed) --, writes the response unless a response
	 * to the rejected request has begun, closes the legs, and lingers.
	 */
	private void rejectAtBoundary(byte[] response, boolean sent) {
		Leg leg = this.current;
		int before = leg == null ? 0 : leg.awaitAnswered(sent);
		this.sinkLock.lock();
		try {
			this.rejected = true;
			if (leg == null || leg.atResponseStart(before)) {
				writeToClient(response, 0, response.length);
			}
		}
		finally {
			this.sinkLock.unlock();
		}
		closeLegs();
		linger();
	}

	/**
	 * Half-closes the client connection and reads what the client still sends, for up to
	 * {@link #LINGER} and {@link #LINGER_LIMIT}, then closes it: closing with unread
	 * bytes would reset the connection and discard the response before the client reads
	 * it. The half-close waits for a write to the client in progress: the 502 for a lost
	 * request is written on a leg's inbound side.
	 */
	private void linger() {
		this.sinkLock.lock();
		try {
			this.pipe.shutdownOutput();
		}
		catch (Exception e) {
			// a transport without half-close: the client closes after the response
		}
		finally {
			this.sinkLock.unlock();
		}
		Thread closer = Thread.ofVirtual().name("sluice-reroute-linger").start(() -> {
			try {
				Thread.sleep(LINGER);
				closePipe();
			}
			catch (InterruptedException e) {
				// the client closed first
			}
		});
		try {
			InputStream in = this.pipe.source();
			byte[] buffer = new byte[BUFFER_SIZE];
			long drained = 0;
			int n;
			while (drained < LINGER_LIMIT && (n = in.read(buffer)) > 0) {
				drained += n;
			}
		}
		catch (IOException | RuntimeException e) {
			// closed by the linger's end, the client, or a lost request's closer
		}
		finally {
			closer.interrupt();
			closePipe();
		}
	}

	private void closePipe() {
		try {
			this.pipe.close();
		}
		catch (Exception e) {
			// ignore
		}
	}

	private void onRequestHead(byte[] head, @Nullable String host) {
		if (this.refusal != null || this.rejected) {
			return; // a request refused or lost before: nothing after it is relayed
		}
		Resolved resolved;
		if (this.firstHead) {
			// resolved with the connection; the lookup must not run twice (it advances
			// the load balancing state)
			this.firstHead = false;
			resolved = new Resolved(this.initialRoute, this.initialSession);
		}
		else {
			resolved = resolveOrRespond(head, host);
			if (resolved == null) {
				return; // refused: the answer waits for the request boundary
			}
		}
		Router.Route route = resolved.route();
		Origin origin = this.forwarding.origin(ConnectionHeadParser.http1Headers(head),
				ConnectionHeadParser.http1HostOf(head));
		Verdict verdict = this.routeAuth.verify(route, ConnectionHeadParser.http1Headers(head), origin);
		if (!(verdict instanceof Verdict.Pass pass)) {
			this.access.refused(route, ConnectionHeadParser.http1RequestOf(head), verdict);
			this.refusal = this.errorResponse.refusal(ConnectionHeadParser.Head.http1(head, host), verdict);
			return;
		}
		Leg old = this.current;
		Leg leg;
		if (old != null && !old.ended && old.route.equals(route)) {
			leg = old;
		}
		else {
			if (old != null && !old.ended) {
				old.replaced = true; // its EOF from now on is the switch, not the end
				this.awaitDrained(old);
				if (this.rejected) {
					return; // it lost a request, answered 502: the connection ends
				}
				if (resolved.session().withdrawn()) {
					// its client began shutting down while the old leg finished: the
					// request goes to a client staying
					resolved = resolveOrRespond(head, host);
					if (resolved == null) {
						return;
					}
					route = resolved.route();
					verdict = this.routeAuth.verify(route, ConnectionHeadParser.http1Headers(head), origin);
					if (!(verdict instanceof Verdict.Pass passed)) {
						this.access.refused(route, ConnectionHeadParser.http1RequestOf(head), verdict);
						this.refusal = this.errorResponse.refusal(ConnectionHeadParser.Head.http1(head, host), verdict);
						return;
					}
					pass = passed;
				}
			}
			leg = new Leg(route, resolved.session());
			this.legs.add(leg);
			if (this.legs.size() == 1) {
				this.access.connectionId(leg.connection.connectionId());
			}
			this.current = leg; // visible to the old pump before its close below
			if (old != null && !old.ended) {
				old.retire(); // the switch ends the previous conversation
				old.awaitQueued();
			}
		}
		this.headSeam.routed(this);
		ConnectionHeadParser.Head.@Nullable Request request = ConnectionHeadParser.http1RequestOf(head);
		String expect = ConnectionHeadParser.http1HeaderOf(head, "expect");
		this.continueAwaited = expect != null && expect.toLowerCase(java.util.Locale.ROOT).contains("100-continue");
		this.clientWaits.update();
		if (closesConnection(head)) {
			leg.closing = true;
		}
		leg.requestSent(host, request, Http1RequestParser.isUpgrade(head));
		this.lastRequest = System.nanoTime();
		Forwarding.Headers forwarded = this.forwarding.headers(ConnectionHeadParser.http1Headers(head),
				ConnectionHeadParser.http1HostOf(head));
		Set<String> removed = new HashSet<>(pass.strip());
		removed.addAll(forwarded.removed());
		Map<String, String> added = new LinkedHashMap<>(pass.add());
		added.putAll(forwarded.added());
		byte[] stripped = ConnectionHeadRewriter.withHeaders(head, removed, added);
		byte[] relayed = route.rewriteHost() ? ConnectionHeadRewriter.rewriteHttp1(stripped, route.address())
				: stripped;
		leg.toUpstream(relayed, 0, relayed.length);
		if (request != null) {
			this.access.request(route, request);
		}
	}

	private void onRequestData(byte[] bytes, int offset, int length) {
		Leg leg = this.current;
		if (leg == null || this.refusal != null || this.rejected) {
			// no head relayed yet, or the request refused or lost: its body stays here
			return;
		}
		if (length > 0) {
			this.continueAwaited = false; // the client sends its body without the 100
			this.clientWaits.update();
		}
		leg.toUpstream(bytes, offset, length);
	}

	/**
	 * The route of a request head and the session serving it.
	 */
	private record Resolved(Router.Route route, TunnelSession session) {
	}

	/**
	 * Resolves the route of a later request head; on any refusal the error response is
	 * kept for the request boundary ({@link #refusal}), mirroring the connection-level
	 * refusals.
	 */
	private @Nullable Resolved resolveOrRespond(byte[] head, @Nullable String host) {
		Optional<Router.Route> route = this.router.lookup(host);
		if (route.isEmpty()) {
			return refuse(head, host, this.errorResponse.noRoute(ConnectionHeadParser.Head.http1(head, host)));
		}
		TunnelSession session = this.sessions.find(route.get().clientId()).orElse(null);
		if (session == null) {
			return refuse(head, host, this.errorResponse.noRoute(ConnectionHeadParser.Head.http1(head, host)));
		}
		String forwardedFor = ConnectionHeadParser.http1HeaderOf(head, ConnectionHeadParser.Head.FORWARDED_FOR);
		String forwarded = ConnectionHeadParser.http1HeaderOf(head, ConnectionHeadParser.Head.FORWARDED);
		if (!this.accessControl.allowed(route.get(), this.peer, forwardedFor, forwarded)) {
			return refuse(head, host, this.errorResponse.forbidden(ConnectionHeadParser.Head.http1(head, host)));
		}
		return new Resolved(route.get(), session);
	}

	private @Nullable Resolved refuse(byte[] head, @Nullable String host, byte[] response) {
		ConnectionHeadParser.Head.@Nullable Request request = ConnectionHeadParser.http1RequestOf(head);
		this.access.request(null, request);
		this.refusal = response;
		return null;
	}

	/**
	 * Writes to the client in pieces, each one progress; counted as a writer for the
	 * stall watch from before the sink lock is taken.
	 */
	private void writeToClient(byte[] bytes, int offset, int length) {
		OutputStream sink = this.pipe.sink();
		this.writers.incrementAndGet();
		this.clientWaits.update();
		this.sinkLock.lock();
		try {
			for (int written = 0; written < length;) {
				int piece = Math.min(ConnectionLimits.KeepAliveDeadline.WRITE_PIECE, length - written);
				sink.write(bytes, offset + written, piece);
				written += piece;
				this.deadline.progress(piece);
			}
			sink.flush();
			this.lastClientWrite = System.nanoTime();
		}
		catch (Exception e) {
			throw new IllegalStateException(e);
		}
		finally {
			this.sinkLock.unlock();
			this.writers.decrementAndGet();
			this.clientWaits.update();
		}
	}

	/**
	 * Waits until the leg relayed every response it owes; an ended leg qualifies.
	 */
	private void awaitDrained(Leg leg) {
		synchronized (leg) {
			while (!leg.ended && leg.completed < leg.sent) {
				try {
					leg.wait(1000);
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					return;
				}
			}
		}
	}

	/**
	 * A leg: the virtual connection of one route, alive from its first request head to
	 * its replacement or the end of the relay.
	 */
	private final class Leg {

		final Router.Route route;

		final TunnelSession session;

		final VirtualConnection connection;

		private final Counter inbound;

		private final Counter outbound;

		/** The response framing; driven by the inbound side under the sink lock. */
		private final ResponseTracker tracker;

		private volatile boolean ended;

		private volatile boolean replaced;

		/** A write towards the upstream failed; outbound side only. */
		private boolean sinkFailed;

		/**
		 * A request or response relayed on the leg asked to close the connection after
		 * its exchange: the upstream's close ends the relay, the leg is not retired.
		 */
		private volatile boolean closing;

		/** The requests whose final response has not begun, in order. */
		private final ArrayDeque<Sent> unanswered = new ArrayDeque<>();

		/** A request asking for an upgrade waits for its answer. */
		private boolean upgradeAwaited;

		/** The answer to the upgrade awaited, once known: switched or declined. */
		private @Nullable Boolean upgradeAnswer;

		private int sent;

		private int completed;

		Leg(Router.Route route, TunnelSession session) {
			this.route = route;
			this.session = session;
			this.connection = session.open(route.address(), DataProxyServer.ALPN_HTTP_1_1);
			this.inbound = meterRegistry.counter(METRIC_NAME, "direction", "inbound", "route", route.routeTag());
			this.outbound = meterRegistry.counter(METRIC_NAME, "direction", "outbound", "route", route.routeTag());
			this.tracker = new ResponseTracker(this);
			session.onWithdrawn(this.connection.connectionId(), () -> retireIfWithdrawn(this));
			pending.incrementAndGet();
			Thread.ofVirtual()
				.name("sluice-reroute-in-" + this.connection.connectionId())
				.start(() -> pumpInbound(this));
		}

		/**
		 * Relays request bytes towards the upstream. Once a write failed -- the virtual
		 * connection was closed or failed, or the tunnel stream ended, each of which ends
		 * its inbound side too -- the bytes are dropped: the leg's end is told by its
		 * inbound side ({@link #lose}), which may still owe the client a response.
		 */
		void toUpstream(byte[] bytes, int offset, int length) {
			if (this.sinkFailed) {
				return;
			}
			try {
				this.connection.sink().write(bytes, offset, length);
			}
			catch (Exception e) {
				this.sinkFailed = true;
				log.debug("rerouting relay: relaying a request to connection {} (route {}) failed; dropping its rest",
						this.connection.connectionId(), this.route.routeTag(), e);
				return;
			}
			this.inbound.increment(length);
			this.session.recordRelayed(length, StreamRelay.Direction.TO_REMOTE);
			access.bytes(length, StreamRelay.Direction.TO_REMOTE);
		}

		/** Half-closes the client side: no further requests follow. */
		void halfClose() {
			try {
				this.connection.sink().close();
			}
			catch (Exception e) {
				// the relay is ending anyway
			}
		}

		/** Closes the virtual connection, releasing the upstream. */
		void close() {
			this.connection.close();
		}

		/**
		 * Closes the virtual connection and lets it leave the session: the relay is done
		 * with the leg, though not with the client connection.
		 */
		void retire() {
			close();
			this.session.remove(this.connection.connectionId());
		}

		/**
		 * Waits until the frames queued for the closed leg left the tunnel sender, or the
		 * sender stopped and dropped them.
		 */
		void awaitQueued() {
			CountDownLatch left = new CountDownLatch(1);
			this.connection.afterQueued(left::countDown);
			try {
				left.await();
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}

		void requestSent(@Nullable String host, ConnectionHeadParser.Head.@Nullable Request request, boolean upgrade) {
			synchronized (this) {
				this.sent++;
				this.unanswered.addLast(new Sent(host, request, upgrade));
				if (upgrade) {
					this.upgradeAwaited = true;
					this.upgradeAnswer = null;
				}
			}
			owedChanged(1);
		}

		/**
		 * Whether a probe may be written now: a request awaits its final response, none
		 * of which was relayed, and the request is HTTP/1.1. With the sink lock held.
		 */
		boolean probeable() {
			synchronized (this) {
				Sent next = this.unanswered.peekFirst();
				return !this.ended && next != null && "1.1".equals(next.version()) && this.tracker.atResponseStart();
			}
		}

		/**
		 * The leg's first request while none of a response to it was relayed; null once a
		 * response has begun or completed. A request after it found the upstream
		 * connection reused, which its upstream may have closed as idle as the request
		 * came. With the sink lock held.
		 */
		@Nullable Sent firstUnanswered() {
			synchronized (this) {
				Sent first = this.unanswered.peekFirst();
				return first != null && this.completed == 0 && this.tracker.atResponseStart() ? first : null;
			}
		}

		/** The request the next final response answers; null for one sent unasked. */
		@Nullable Sent nextRequest() {
			synchronized (this) {
				return this.unanswered.pollFirst();
			}
		}

		/**
		 * Waits until the leg relayed the responses to the requests before a malformed
		 * one, or ended; returns how many requests those are: every request relayed, but
		 * the last one when the malformed request's head was relayed ({@code inBody}).
		 */
		int awaitAnswered(boolean inBody) {
			synchronized (this) {
				int before = inBody ? this.sent - 1 : this.sent;
				while (!this.ended && this.completed < before) {
					try {
						this.wait(1000);
					}
					catch (InterruptedException e) {
						Thread.currentThread().interrupt();
						break;
					}
				}
				return before;
			}
		}

		/**
		 * Whether exactly {@code before} responses completed and nothing of the next one
		 * was relayed, the leg not ended. With the sink lock held.
		 */
		boolean atResponseStart(int before) {
			synchronized (this) {
				return !this.ended && this.completed == before && this.tracker.atResponseStart();
			}
		}

		/** Reports the answer to the upgrade awaited, if one is. */
		void upgradeAnswered(boolean switched) {
			synchronized (this) {
				if (this.upgradeAwaited) {
					this.upgradeAwaited = false;
					this.upgradeAnswer = switched;
					this.notifyAll();
				}
			}
		}

		/**
		 * Waits for the answer to the upgrade awaited: whether it switched. The leg
		 * ending first counts as switched.
		 */
		boolean awaitUpgradeAnswer() {
			synchronized (this) {
				while (this.upgradeAnswer == null && !this.ended) {
					try {
						this.wait(1000);
					}
					catch (InterruptedException e) {
						Thread.currentThread().interrupt();
						return true;
					}
				}
				Boolean answer = this.upgradeAnswer;
				this.upgradeAnswer = null;
				return answer == null || answer;
			}
		}

		/** A response completed; one the upstream sent unasked counts for nothing. */
		void responseCompleted() {
			boolean counted;
			synchronized (this) {
				counted = this.completed < this.sent;
				if (counted) {
					this.completed++;
				}
				this.notifyAll();
			}
			if (counted) {
				owedChanged(-1);
			}
		}

		/** The leg ended: the responses it still owed never come. */
		void markEnded() {
			int lost;
			synchronized (this) {
				this.ended = true;
				lost = this.sent - this.completed;
				this.completed = this.sent;
				this.notifyAll();
			}
			owedChanged(-lost);
		}

	}

	/**
	 * Runs on the probing thread between a probe's decision and its write, with the
	 * client stream's lock held; a seam for tests to hold a probe there.
	 */
	@FunctionalInterface
	interface ProbeSeam {

		/** Does nothing. */
		ProbeSeam NONE = relay -> {
		};

		void decided(ReroutingRelay relay);

	}

	/**
	 * Whether a request or response head ends its connection after its exchange (RFC 9112
	 * 9.3): a {@code close} connection option, or HTTP/1.0 without {@code keep-alive}.
	 */
	static boolean closesConnection(byte[] head) {
		boolean keepAlive = false;
		for (String value : ConnectionHeadParser.http1HeaderValues(head, "connection")) {
			for (String option : value.split(",")) {
				String token = option.trim();
				if (token.equalsIgnoreCase("close")) {
					return true;
				}
				keepAlive |= token.equalsIgnoreCase("keep-alive");
			}
		}
		int eol = 0;
		while (eol < head.length && head[eol] != '\r' && head[eol] != '\n') {
			eol++;
		}
		String line = new String(head, 0, eol, StandardCharsets.ISO_8859_1);
		boolean http10 = line.startsWith("HTTP/1.0 ") || line.endsWith(" HTTP/1.0");
		return http10 && !keepAlive;
	}

	/**
	 * Runs on the outbound side between the choice of a request head's leg and the head's
	 * relaying, the request already started ({@link #starting}); a seam for tests to hold
	 * a head there.
	 */
	@FunctionalInterface
	interface HeadSeam {

		/** Does nothing. */
		HeadSeam NONE = relay -> {
		};

		void routed(ReroutingRelay relay);

	}

	/**
	 * A request relayed on a leg: its host, its request line (null for one that does not
	 * parse), and whether it asked for an upgrade.
	 */
	private record Sent(@Nullable String host, ConnectionHeadParser.Head.@Nullable Request request, boolean upgrade) {

		String method() {
			return this.request == null ? "" : this.request.method();
		}

		String version() {
			return this.request == null ? "" : this.request.version();
		}

	}

	/**
	 * The inbound side of a leg: relays response bytes to the client and tracks the
	 * response framing to know when the leg's conversation is drained.
	 */
	private void pumpInbound(Leg leg) {
		try {
			ResponseTracker tracker = leg.tracker;
			InputStream in = leg.connection.source();
			byte[] buffer = new byte[BUFFER_SIZE];
			int n;
			while ((n = in.read(buffer)) > 0) {
				this.sinkLock.lock();
				try {
					if (this.rejected) {
						return; // a malformed request ended the connection
					}
					// written and tracked at once: a probe sees the tracker's state of
					// the bytes written
					this.writeToClient(buffer, 0, n);
					tracker.data(buffer, 0, n);
				}
				finally {
					this.sinkLock.unlock();
				}
				leg.outbound.increment(n);
				leg.session.recordRelayed(n, StreamRelay.Direction.TO_LOCAL);
				this.access.bytes(n, StreamRelay.Direction.TO_LOCAL);
				// a response completed: the leg may be idle now
				retireIfWithdrawn(leg);
			}
			this.sinkLock.lock();
			try {
				tracker.ended();
			}
			finally {
				this.sinkLock.unlock();
			}
			if (this.current == leg && !leg.replaced && !this.rejected) {
				// the upstream of the leg in charge is gone; the client reconnects
				log.debug(
						"rerouting relay: the upstream of connection {} (route {}) in charge ended it; ending the relay",
						leg.connection.connectionId(), leg.route.routeTag());
				lose(leg);
			}
		}
		catch (Exception e) {
			if (!this.rejected) {
				log.debug("rerouting relay: relaying the responses of connection {} (route {}) failed; aborting",
						leg.connection.connectionId(), leg.route.routeTag(), e);
				if (this.current == leg && !leg.replaced) {
					lose(leg);
				}
				else {
					abort();
				}
			}
		}
		finally {
			leg.markEnded();
			settle();
		}
	}

	/**
	 * The leg in charge ended: its first request, when none of a response to it was
	 * relayed, is answered with the bad-gateway page, the legs are closed, and the client
	 * connection is half-closed and closed after {@link #LINGER} unless the outbound
	 * side's linger or the relay's end closes it first, so the client reads the answer
	 * rather than a reset. Otherwise, or should the answer fail, the relay is aborted: a
	 * later request lost unanswered ends the client connection unanswered, as its reused
	 * upstream connection did, so the client may retry it (RFC 9112 9.3.1). Nothing is
	 * done once a refusal was answered or the relay was aborted.
	 */
	private void lose(Leg leg) {
		this.sinkLock.lock();
		try {
			if (this.rejected || this.aborted) {
				// a refusal was answered meanwhile, and the linger ends the connection;
				// or
				// the relay was torn down, the leg with it
				return;
			}
			Sent lost = leg.firstUnanswered();
			if (lost == null) {
				abort();
				return;
			}
			this.rejected = true;
			byte[] answer = this.errorResponse.badGateway(lost.host(), lost.request());
			writeToClient(answer, 0, answer.length);
		}
		catch (Exception e) {
			log.debug("rerouting relay: answering the request connection {} (route {}) lost failed; aborting",
					leg.connection.connectionId(), leg.route.routeTag(), e);
			abort();
			return;
		}
		finally {
			this.sinkLock.unlock();
		}
		closeLegs();
		try {
			this.pipe.shutdownOutput();
		}
		catch (Exception e) {
			// a transport without half-close: the client closes after the response
		}
		Thread.ofVirtual().name("sluice-reroute-linger").start(() -> {
			try {
				Thread.sleep(LINGER);
			}
			catch (InterruptedException e) {
				// closed now
			}
			closePipe();
		});
	}

	private void closeLegs() {
		for (Leg leg : this.legs) {
			leg.close();
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
		this.deadline.disarm();
		for (Leg leg : this.legs) {
			leg.close();
			leg.session.remove(leg.connection.connectionId());
		}
		try {
			this.pipe.close();
		}
		catch (Exception e) {
			// ignore
		}
		this.access.close();
		this.onFinish.run();
		VirtualConnection.afterQueued(this.legs.stream().map(leg -> leg.connection).toList(), this.release);
	}

	/** Tears the relay down after a failure or a final error response. */
	private void abort() {
		this.aborted = true; // before the legs' close wakes their inbound sides
		closeLegs();
		try {
			this.pipe.close();
		}
		catch (Exception e) {
			// ignore
		}
	}

	/**
	 * Tracks the response framing of one leg to mark its conversation drained: a response
	 * is complete at the end of its body (Content-Length or last chunk), at its head when
	 * it has no body (a response to HEAD, 204, 304), at the end of an unframed body, or
	 * when it switches to a tunneled mode (101, a 2xx to CONNECT). Heads are read as
	 * ISO-8859-1. A response the tracker cannot follow -- a head over 64KiB or without a
	 * status code, an invalid Content-Length or chunk-size line, a chunk-size or trailer
	 * line over 64KiB -- is taken as unframed: it ends when the upstream closes, and a
	 * request awaiting an upgrade's answer is relayed verbatim. Exactly one completion is
	 * counted per response.
	 */
	private final class ResponseTracker {

		private enum Mode {

			HEAD, BODY, CHUNK_SIZE, CHUNK_DATA, CHUNK_CRLF, TRAILERS, UNFRAMED, PASS

		}

		private final Leg leg;

		private final ByteArrayOutputStream head = new ByteArrayOutputStream(512);

		private final ByteArrayOutputStream line = new ByteArrayOutputStream(64);

		private Mode mode = Mode.HEAD;

		private long remaining;

		ResponseTracker(Leg leg) {
			this.leg = leg;
		}

		void data(byte[] bytes, int offset, int length) {
			int i = 0;
			while (i < length) {
				switch (this.mode) {
					case HEAD -> {
						this.head.write(bytes[offset + i]);
						boolean lineEnded = bytes[offset + i] == '\n';
						i++;
						if (this.head.size() > Http1RequestParser.MAX_HEAD) {
							this.head.reset();
							this.untracked();
						}
						else if (lineEnded) {
							this.onHeadByte();
						}
					}
					case BODY -> {
						int n = (int) Math.min(this.remaining, length - i);
						i += n;
						this.remaining -= n;
						if (this.remaining == 0) {
							this.drain(Mode.HEAD);
						}
					}
					case CHUNK_SIZE -> {
						this.line.write(bytes[offset + i]);
						boolean lineEnded = bytes[offset + i] == '\n';
						i++;
						if (this.line.size() > Http1RequestParser.MAX_HEAD) {
							this.line.reset();
							this.untracked();
						}
						else if (lineEnded) {
							this.onChunkSize();
						}
					}
					case CHUNK_DATA -> {
						int n = (int) Math.min(this.remaining, length - i);
						i += n;
						this.remaining -= n;
						if (this.remaining == 0) {
							this.mode = Mode.CHUNK_CRLF;
						}
					}
					case CHUNK_CRLF -> {
						boolean lineEnded = bytes[offset + i] == '\n';
						i++;
						if (lineEnded) {
							this.mode = Mode.CHUNK_SIZE;
						}
					}
					case TRAILERS -> {
						this.line.write(bytes[offset + i]);
						boolean lineEnded = bytes[offset + i] == '\n';
						i++;
						if (this.line.size() > Http1RequestParser.MAX_HEAD) {
							this.line.reset();
							this.untracked();
						}
						else if (lineEnded && this.onTrailerLine()) {
							this.drain(Mode.HEAD);
						}
					}
					case UNFRAMED -> i++; // an unframed body ends at EOF only
					case PASS -> {
						return;
					}
				}
			}
		}

		/**
		 * At the start of a response: none of the next head arrived, and no response is
		 * in progress or untracked. An interim response the upstream relayed counts as
		 * begun only while its head is incomplete.
		 */
		boolean atResponseStart() {
			return this.mode == Mode.HEAD && this.head.size() == 0;
		}

		/**
		 * The upstream half-closed: an unframed or truncated response ends here; at a
		 * response start the tracker stays there, for the request owed to be answered 502
		 * ({@link #lose}).
		 */
		void ended() {
			if (this.mode != Mode.PASS && !this.atResponseStart()) {
				this.leg.responseCompleted();
				this.mode = Mode.PASS;
			}
		}

		private void onHeadByte() {
			byte[] bytes = this.head.toByteArray();
			int end = ConnectionHeadParser.headerEnd(bytes);
			if (end < 0) {
				return;
			}
			this.head.reset();
			// a response head, interim or final: no 100 Continue is awaited any more
			ReroutingRelay.this.continueAwaited = false;
			ReroutingRelay.this.clientWaits.update();
			int status = statusOf(bytes);
			if (status < 0) {
				this.untracked();
				return;
			}
			if (status >= 100 && status < 200 && status != 101) {
				return; // interim: the final response follows on the same stream
			}
			if (closesConnection(bytes)) {
				this.leg.closing = true;
			}
			Sent request = this.leg.nextRequest();
			@Nullable String method = request == null ? null : request.method();
			boolean switched = status == 101 || "CONNECT".equals(method) && status >= 200 && status < 300;
			if (request != null && request.upgrade()) {
				this.leg.upgradeAnswered(switched);
			}
			if (switched) {
				this.drain(Mode.PASS);
				return;
			}
			if ("HEAD".equals(method) || status == 204 || status == 304) {
				// no body, whatever its framing headers say (RFC 9112 6.3)
				this.drain(Mode.HEAD);
				return;
			}
			String contentLength = header(bytes, "content-length");
			if (contentLength != null) {
				try {
					long length = Long.parseLong(contentLength.trim());
					if (length == 0) {
						this.drain(Mode.HEAD);
					}
					else if (length > 0) {
						this.remaining = length;
						this.mode = Mode.BODY;
					}
					else {
						this.untracked();
					}
					return;
				}
				catch (NumberFormatException e) {
					this.untracked();
					return;
				}
			}
			String transferEncoding = header(bytes, "transfer-encoding");
			if (transferEncoding != null && transferEncoding.toLowerCase(java.util.Locale.ROOT).contains("chunked")) {
				this.mode = Mode.CHUNK_SIZE;
				return;
			}
			this.mode = Mode.UNFRAMED;
		}

		private void onChunkSize() {
			byte[] line = this.line.toByteArray();
			this.line.reset();
			String text = new String(line, StandardCharsets.ISO_8859_1);
			if (text.endsWith("\r\n")) {
				text = text.substring(0, text.length() - 2);
			}
			int semicolon = text.indexOf(';');
			String hex = (semicolon < 0 ? text : text.substring(0, semicolon)).trim();
			int size;
			try {
				size = Integer.parseUnsignedInt(hex, 16);
			}
			catch (NumberFormatException e) {
				size = -1;
			}
			if (size < 0) {
				this.untracked();
			}
			else if (size == 0) {
				this.mode = Mode.TRAILERS;
			}
			else {
				this.remaining = size;
				this.mode = Mode.CHUNK_DATA;
			}
		}

		private boolean onTrailerLine() {
			byte[] line = this.line.toByteArray();
			this.line.reset();
			return line.length == 2 && line[0] == '\r' && line[1] == '\n';
		}

		/**
		 * Takes the response in progress as unframed: it ends when the upstream closes;
		 * an upgrade awaited is relayed verbatim.
		 */
		private void untracked() {
			this.mode = Mode.UNFRAMED;
			this.leg.upgradeAnswered(true);
		}

		private void drain(Mode next) {
			this.mode = next;
			if (next == Mode.PASS) {
				// untracked from here on: an upgrade awaited is relayed verbatim
				this.leg.upgradeAnswered(true);
			}
			this.leg.responseCompleted();
		}

		private static int statusOf(byte[] head) {
			String text = new String(head, StandardCharsets.ISO_8859_1);
			int eol = text.indexOf("\r\n");
			String line = eol < 0 ? text : text.substring(0, eol);
			String[] parts = line.trim().split(" ");
			if (parts.length < 2) {
				return -1;
			}
			try {
				return Integer.parseInt(parts[1].trim());
			}
			catch (NumberFormatException e) {
				return -1;
			}
		}

		private static @Nullable String header(byte[] head, String name) {
			return ConnectionHeadParser.http1HeaderOf(head, name);
		}

	}

}
