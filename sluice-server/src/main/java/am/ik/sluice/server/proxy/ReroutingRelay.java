package am.ik.sluice.server.proxy;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
 * Relays one plaintext HTTP/1.1 client connection over per-request legs: the authority of
 * every request head is resolved through the router and, when it belongs to another route
 * than the leg in charge, the outbound side switches to a virtual connection of that
 * route at the request boundary -- the requests of a keep-alive connection are strictly
 * sequential, and the switch waits for the outstanding response of the old leg to be
 * relayed completely before the new leg is opened, so responses never interleave. The
 * drained leg is closed once replaced; the closure of the leg in charge by its upstream
 * ends the whole relay, as does any failure. Each leg pumps its inbound side on its own
 * virtual thread for the lifetime of its virtual connection.
 * <p>
 * Upgrades, CONNECT, and any structure the parser cannot follow are relayed verbatim on
 * the leg in charge from then on.
 */
final class ReroutingRelay {

	private static final int BUFFER_SIZE = 64 * 1024;

	private static final String METRIC_NAME = "sluice.tunnel.bytes";

	private final DuplexPipe pipe;

	private final byte[] seedHead;

	private final Router router;

	private final SessionRegistry sessions;

	private final AccessControl accessControl;

	private final ErrorResponse errorResponse;

	private final AccessLogger.Connection access;

	private final MeterRegistry meterRegistry;

	private final Router.Route initialRoute;

	private final TunnelSession initialSession;

	private final @Nullable InetAddress peer;

	private final Runnable onFinish;

	/** Serializes the writes of the shared client stream. */
	private final Object sinkLock = new Object();

	private final List<Leg> legs = new CopyOnWriteArrayList<>();

	/** The outbound side plus one side per leg; the relay ends when it drains. */
	private final AtomicInteger pending = new AtomicInteger(1);

	private final AtomicBoolean finished = new AtomicBoolean();

	private boolean firstHead = true;

	@Nullable private volatile Leg current;

	ReroutingRelay(DuplexPipe pipe, ConnectionHeadParser.Head head, Router router, SessionRegistry sessions,
			AccessControl accessControl, ErrorResponse errorResponse, AccessLogger.Connection access,
			MeterRegistry meterRegistry, Router.Route initialRoute, TunnelSession initialSession,
			@Nullable InetAddress peer, Runnable onFinish) {
		this.pipe = pipe;
		this.seedHead = head.bytes();
		this.router = router;
		this.sessions = sessions;
		this.accessControl = accessControl;
		this.errorResponse = errorResponse;
		this.access = access;
		this.meterRegistry = meterRegistry;
		this.initialRoute = initialRoute;
		this.initialSession = initialSession;
		this.peer = peer;
		this.onFinish = onFinish;
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
			InputStream in = this.pipe.source();
			byte[] buffer = new byte[BUFFER_SIZE];
			int n;
			while ((n = in.read(buffer)) > 0) {
				parser.feed(buffer, 0, n);
			}
			parser.finish();
			Leg leg = this.current;
			if (leg != null) {
				leg.halfClose();
			}
		}
		catch (Exception e) {
			abort();
		}
		finally {
			settle();
		}
	}

	private void onRequestHead(byte[] head, @Nullable String host) {
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
				return; // the error response was written and the relay aborted
			}
		}
		Router.Route route = resolved.route();
		Leg old = this.current;
		Leg leg;
		if (old != null && !old.ended && old.route.equals(route)) {
			leg = old;
		}
		else {
			if (old != null && !old.ended) {
				old.replaced = true; // its EOF from now on is the switch, not the end
				this.awaitDrained(old);
			}
			leg = new Leg(route, resolved.session());
			this.legs.add(leg);
			if (this.legs.size() == 1) {
				this.access.connectionId(leg.connection.connectionId());
			}
			this.current = leg; // visible to the old pump before its close below
			if (old != null && !old.ended) {
				old.close(); // the switch ends the previous conversation
			}
		}
		ConnectionHeadParser.Head.@Nullable Request request = ConnectionHeadParser.http1RequestOf(head);
		leg.requestSent(request == null ? null : request.method());
		byte[] relayed = route.rewriteHost() ? ConnectionHeadRewriter.rewriteHttp1(head, route.address()) : head;
		leg.toUpstream(relayed, 0, relayed.length);
		if (request != null) {
			this.access.request(route.routeTag(), request.method(), request.path(), request.version());
		}
	}

	private void onRequestData(byte[] bytes, int offset, int length) {
		Leg leg = this.current;
		if (leg == null) {
			return; // unreachable: a head always precedes the body
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
	 * written to the client and the relay aborted, mirroring the connection-level
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
		this.access.request("-", request == null ? "-" : request.method(), request == null ? "-" : request.path(),
				request == null ? "-" : request.version());
		this.writeToClient(response, 0, response.length);
		abort();
		return null;
	}

	private void writeToClient(byte[] bytes, int offset, int length) {
		OutputStream sink = this.pipe.sink();
		synchronized (this.sinkLock) {
			try {
				sink.write(bytes, offset, length);
				sink.flush();
			}
			catch (Exception e) {
				throw new IllegalStateException(e);
			}
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

		private volatile boolean ended;

		private volatile boolean replaced;

		private volatile @Nullable String method;

		private int sent;

		private int completed;

		Leg(Router.Route route, TunnelSession session) {
			this.route = route;
			this.session = session;
			this.connection = session.open(route.address());
			this.inbound = meterRegistry.counter(METRIC_NAME, "direction", "inbound", "route", route.routeTag());
			this.outbound = meterRegistry.counter(METRIC_NAME, "direction", "outbound", "route", route.routeTag());
			pending.incrementAndGet();
			Thread.ofVirtual()
				.name("sluice-reroute-in-" + this.connection.connectionId())
				.start(() -> pumpInbound(this));
		}

		/** Relays request bytes towards the upstream. */
		void toUpstream(byte[] bytes, int offset, int length) {
			try {
				this.connection.sink().write(bytes, offset, length);
			}
			catch (Exception e) {
				throw new IllegalStateException(e);
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

		void requestSent(@Nullable String method) {
			synchronized (this) {
				this.sent++;
				this.method = method;
			}
		}

		void responseCompleted() {
			synchronized (this) {
				this.completed++;
				this.notifyAll();
			}
		}

	}

	/**
	 * The inbound side of a leg: relays response bytes to the client and tracks the
	 * response framing to know when the leg's conversation is drained.
	 */
	private void pumpInbound(Leg leg) {
		try {
			ResponseTracker tracker = new ResponseTracker(leg);
			InputStream in = leg.connection.source();
			byte[] buffer = new byte[BUFFER_SIZE];
			int n;
			while ((n = in.read(buffer)) > 0) {
				this.writeToClient(buffer, 0, n);
				leg.outbound.increment(n);
				leg.session.recordRelayed(n, StreamRelay.Direction.TO_LOCAL);
				this.access.bytes(n, StreamRelay.Direction.TO_LOCAL);
				tracker.data(buffer, 0, n);
			}
			tracker.ended();
			if (this.current == leg && !leg.replaced) {
				// the upstream of the leg in charge is gone; the client reconnects
				abort();
			}
		}
		catch (Exception e) {
			abort();
		}
		finally {
			synchronized (leg) {
				leg.ended = true;
				leg.notifyAll();
			}
			settle();
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
		try {
			this.pipe.close();
		}
		catch (Exception e) {
			// ignore
		}
		this.access.close();
		this.onFinish.run();
	}

	/** Tears the relay down after a failure or a final error response. */
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

	/**
	 * Tracks the response framing of one leg to mark its conversation drained: a response
	 * is complete at the end of its body (Content-Length or last chunk), at the end of an
	 * unframed body, or when it switches to a tunneled mode; anything unrecognized drains
	 * too. Exactly one completion is counted per response.
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
						if (lineEnded) {
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
						if (lineEnded) {
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
						if (lineEnded && this.onTrailerLine()) {
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

		/** The upstream half-closed: an unframed or truncated response ends here. */
		void ended() {
			if (this.mode != Mode.PASS && !(this.mode == Mode.HEAD && this.head.size() == 0)) {
				this.leg.responseCompleted();
			}
			this.mode = Mode.PASS;
		}

		private void onHeadByte() {
			byte[] bytes = this.head.toByteArray();
			int end = ConnectionHeadParser.headerEnd(bytes);
			if (end < 0) {
				if (bytes.length > 64 * 1024 || !ConnectionHeadRewriter.isAscii(bytes)) {
					this.drain(Mode.PASS);
				}
				return;
			}
			this.head.reset();
			if (!ConnectionHeadRewriter.isAscii(bytes)) {
				this.drain(Mode.PASS);
				return;
			}
			int status = statusOf(bytes);
			String method = this.leg.method;
			if (status < 0) {
				this.drain(Mode.PASS);
				return;
			}
			if (status >= 100 && status < 200 && status != 101) {
				return; // interim: the final response follows on the same stream
			}
			if (status == 101 || upgrade(bytes) || "CONNECT".equals(method) && status >= 200 && status < 300
					|| "HEAD".equals(method) || status == 204 || status == 304) {
				this.drain(Mode.PASS);
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
						this.drain(Mode.PASS);
					}
					return;
				}
				catch (NumberFormatException e) {
					this.drain(Mode.PASS);
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
			String text = new String(line, StandardCharsets.US_ASCII);
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
				this.drain(Mode.PASS);
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

		private void drain(Mode next) {
			this.mode = next;
			this.leg.responseCompleted();
		}

		private static int statusOf(byte[] head) {
			String text = new String(head, StandardCharsets.US_ASCII);
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

		private static boolean upgrade(byte[] head) {
			String connection = header(head, "connection");
			return header(head, "upgrade") != null
					|| connection != null && connection.toLowerCase(java.util.Locale.ROOT).contains("upgrade");
		}

		private static @Nullable String header(byte[] head, String name) {
			return ConnectionHeadParser.http1HeaderOf(head, name);
		}

	}

}
