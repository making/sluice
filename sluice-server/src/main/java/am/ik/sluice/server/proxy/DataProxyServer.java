package am.ik.sluice.server.proxy;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PushbackInputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;
import java.util.List;
import javax.net.ssl.SNIMatcher;
import javax.net.ssl.SNIServerName;
import javax.net.ssl.SSLParameters;
import java.util.Optional;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import org.jspecify.annotations.Nullable;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Component;

import am.ik.sluice.server.auth.RouteAuthenticator;
import am.ik.sluice.server.config.SluiceServerProperties;
import am.ik.sluice.server.proxyprotocol.ProxyProtocolParser;
import am.ik.sluice.server.route.Router;
import am.ik.sluice.server.tunnel.Drainable;
import am.ik.sluice.server.tunnel.SessionRegistry;
import am.ik.sluice.server.tunnel.TunnelSession;
import am.ik.sluice.tunnel.DuplexPipe;
import am.ik.sluice.tunnel.StreamRelay;
import am.ik.sluice.tunnel.TcpKeepAlive;
import am.ik.sluice.tunnel.VirtualConnection;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Data plane: a TCP listener on the data port that routes each connection by the Host
 * header (HTTP/1.1) or {@code :authority} (HTTP/2 prior knowledge) of the request head.
 * Plaintext and TLS-terminated HTTP relays per request -- every HTTP/1.1 request head and
 * every HTTP/2 stream is routed anew, access-checked and checked against its route's
 * credentials, so keep-alive successors and coalesced streams reach the upstream of their
 * own route (see {@link ReroutingRelay} and {@link NettyHttp2DemuxRelay});
 * encrypted-passthrough and hostless connections relay verbatim over the virtual
 * connection of the resolved route, refused when the route requires credentials. A TLS
 * handshake (first byte {@code 0x16}) is either terminated locally when an SSL bundle is
 * configured (ALPN: h2 preferred over http/1.1 while the bytes an h2 relay reserves fit,
 * see {@link ConnectionLimits}), or relayed untouched in passthrough mode otherwise,
 * routed by the SNI host name of the ClientHello (the backend terminates TLS and presents
 * its own certificate). An h2 connection whose relay does not fit is answered with a
 * GOAWAY naming no stream processed. A connection past the connection limit given a place
 * of the overflow ({@link ConnectionLimits#enter}) is answered 503 with
 * {@code Retry-After} once its head is read, then closed.
 */
@Component
public class DataProxyServer implements SmartLifecycle, Drainable {

	private static final Logger log = LoggerFactory.getLogger(DataProxyServer.class);

	private static final int HEAD_LIMIT = 64 * 1024;

	private static final int TLS_HANDSHAKE = 0x16;

	static final String ALPN_H2 = "h2";

	static final String ALPN_HTTP_1_1 = "http/1.1";

	/** How long a rejected connection is drained for its peer to read the response. */
	private static final Duration LINGER = Duration.ofSeconds(2);

	/** The most a rejected connection drains before closing regardless. */
	private static final long LINGER_LIMIT = 1024 * 1024;

	private final Router router;

	private final SessionRegistry sessions;

	private final SluiceServerProperties properties;

	private static final String METRIC_NAME = "sluice.tunnel.bytes";

	private final AtomicInteger activeConnections = new AtomicInteger();

	private final MeterRegistry meterRegistry;

	private final H2DemuxMeters h2DemuxMeters;

	private final AccessLogger accessLogger;

	private final TaskExecutor taskExecutor;

	private final ObjectProvider<SSLContext> sslContext;

	private final ErrorResponse errorResponse;

	private final AccessControl accessControl;

	private final RouteAuthenticator routeAuth;

	private final ConnectionLimits limits;

	/** Handed to every HTTP/1.1 relay started from now on. */
	private volatile ReroutingRelay.ProbeSeam probeSeam = ReroutingRelay.ProbeSeam.NONE;

	/** The wait deadline every h2 demux relay started from now on gets. */
	private volatile Duration h2StreamWaitTimeout;

	/**
	 * Wraps the client pipe of every HTTP relay (HTTP/1.1 and h2) started from now on.
	 */
	private volatile UnaryOperator<DuplexPipe> clientPipeSeam = UnaryOperator.identity();

	/** Handed to every h2 demux relay started from now on. */
	private volatile NettyHttp2DemuxRelay.LegSeam legSeam = NettyHttp2DemuxRelay.LegSeam.NONE;

	private volatile boolean running;

	private volatile @Nullable ServerSocket serverSocket;

	DataProxyServer(Router router, SessionRegistry sessions, SluiceServerProperties properties,
			MeterRegistry meterRegistry, AccessLogger accessLogger, AccessControl accessControl,
			RouteAuthenticator routeAuth, @Qualifier("applicationTaskExecutor") TaskExecutor taskExecutor,
			ObjectProvider<SSLContext> sslContext, ErrorResponse errorResponse, ConnectionLimits limits) {
		this.router = router;
		this.sessions = sessions;
		this.properties = properties;
		this.meterRegistry = meterRegistry;
		this.h2DemuxMeters = new H2DemuxMeters(meterRegistry);
		this.accessLogger = accessLogger;
		this.accessControl = accessControl;
		this.routeAuth = routeAuth;
		this.taskExecutor = taskExecutor;
		this.sslContext = sslContext;
		this.errorResponse = errorResponse;
		this.limits = limits;
		this.h2StreamWaitTimeout = properties.limits().h2StreamWaitTimeout();
	}

	/**
	 * A routed connection: the transport socket, its {@link DuplexPipe} view, the parsed
	 * connection head (verbatim bytes plus the resolved route host), the connection peer
	 * -- the source of a parsed PROXY protocol header when one was present, the socket
	 * peer otherwise --, and the handshake the data plane terminated (null for plaintext
	 * and passthrough).
	 */
	record Connection(Socket socket, DuplexPipe pipe, ConnectionHeadParser.Head head, @Nullable InetAddress peer,
			@Nullable TlsHandshake handshake) {

		static Builder builder() {
			return new Builder();
		}

		static final class Builder {

			private @Nullable Socket socket;

			private ConnectionHeadParser.@Nullable Head head;

			private @Nullable InetAddress peer;

			private @Nullable TlsHandshake handshake;

			private Builder() {
			}

			/** The transport socket; the pipe is its view. */
			Builder socket(Socket socket) {
				this.socket = socket;
				return this;
			}

			Builder head(ConnectionHeadParser.Head head) {
				this.head = head;
				return this;
			}

			/** The connection peer; null (the default) = the socket peer. */
			Builder peer(@Nullable InetAddress peer) {
				this.peer = peer;
				return this;
			}

			/** The handshake the data plane terminated; null (the default) when none. */
			Builder handshake(@Nullable TlsHandshake handshake) {
				this.handshake = handshake;
				return this;
			}

			Connection build() {
				Socket socket = Objects.requireNonNull(this.socket, "socket is required");
				return new Connection(socket, DuplexPipe.of(socket),
						Objects.requireNonNull(this.head, "head is required"), this.peer, this.handshake);
			}

		}

	}

	@Override
	public void start() {
		try {
			ServerSocket socket = new ServerSocket();
			socket.setReuseAddress(true);
			socket.bind(new InetSocketAddress(InetAddress.getByName(this.properties.dataHost()),
					this.properties.dataPort()), 128);
			this.serverSocket = socket;
		}
		catch (Exception e) {
			throw new IllegalStateException("failed to bind data port %d".formatted(this.properties.dataPort()), e);
		}
		this.running = true;
		log.info("data plane listening on {}:{}", this.properties.dataHost(), this.properties.dataPort());
		this.taskExecutor.execute(this::acceptLoop);
	}

	private void acceptLoop() {
		ServerSocket serverSocket = Objects.requireNonNull(this.serverSocket, "server not started");
		while (this.running) {
			try {
				Socket socket = serverSocket.accept();
				AccessLogger.Connection access = this.accessLogger.accepted("data", socket);
				ConnectionLimits.Entry entry = this.limits.enter(access);
				if (entry == null) {
					// past the limit and its overflow: closed unread, before any thread
					// is spent on it
					this.close(socket);
					continue;
				}
				try {
					socket.setTcpNoDelay(true);
					TcpKeepAlive.enable(socket, this.properties.limits().tcpKeepAlive());
				}
				catch (IOException e) {
					// the client reset the connection before it was taken up (on macOS
					// the options fail on such a socket)
					log.debug("dropped an accepted connection: {}", e.toString());
					entry.release();
					this.close(socket);
					continue;
				}
				try {
					switch (entry) {
						case ConnectionLimits.Admission admission ->
							this.taskExecutor.execute(() -> this.handle(socket, access, admission));
						case ConnectionLimits.Overflow overflow ->
							this.taskExecutor.execute(() -> this.answerOverflow(socket, access, overflow));
					}
				}
				catch (RuntimeException e) {
					entry.release();
					this.close(socket);
					throw e;
				}
			}
			catch (Exception e) {
				if (this.running) {
					log.warn("accept failed: {}", e.toString());
				}
			}
		}
	}

	/**
	 * Serves one admitted connection. The admission is released exactly once: here on
	 * every path that does not hand it to a relay, by the relay otherwise, once it ended
	 * and the frames its virtual connections queued left the tunnel sender.
	 */
	private void handle(Socket socket, AccessLogger.Connection access, ConnectionLimits.Admission admission) {
		boolean relaying = false;
		try {
			admission.startHead(socket);
			Connection connection = this.transport(socket, access, admission);
			if (connection == null || !admission.endHead()) {
				access.close();
				this.close(socket);
				return;
			}
			relaying = this.relay(connection, access, this.limits.keepAliveDeadline(socket), admission);
			if (!relaying) {
				// access denied or no session: relay() answered with the error response
				// and closed the socket
				access.close();
			}
		}
		catch (Exception e) {
			log.debug("data connection failed: {}", admission.expired() ? "head timeout" : e.toString());
			access.close();
			this.close(socket);
		}
		finally {
			if (!relaying) {
				admission.release();
			}
		}
	}

	/**
	 * Answers a connection past the connection limit with the busy page -- once its TLS
	 * handshake (on a route the server terminates) and its head are read, in the protocol
	 * of the head -- and closes it; everything under the overflow deadline, which closes
	 * the socket wherever the connection is. A head that is no HTTP request -- a TLS
	 * passthrough ClientHello (the handshake is the upstream's, nothing the server could
	 * answer in), a RESP array -- is closed unanswered. The place is released on every
	 * path.
	 */
	private void answerOverflow(Socket socket, AccessLogger.Connection access, ConnectionLimits.Overflow overflow) {
		try {
			overflow.start(socket);
			Connection connection = this.transport(socket, access, overflow);
			if (connection == null || connection.head().undecodable()) {
				return;
			}
			byte[] response = this.errorResponse.busy(connection.head(), this.limits.overflowRetryAfter());
			if (response.length == 0) {
				return;
			}
			try {
				OutputStream out = connection.pipe().sink();
				out.write(response);
				out.flush();
				overflow.answered(connection.head().http2Connection() ? ALPN_H2 : "h1");
				// unread request bytes would reset the connection and discard the answer
				this.linger(connection);
			}
			finally {
				this.close(connection.socket());
			}
		}
		catch (Exception e) {
			log.debug("overflow connection failed: {}", overflow.expired() ? "overflow timeout" : e.toString());
		}
		finally {
			this.close(socket);
			overflow.release();
		}
	}

	/**
	 * Consumes the PROXY protocol header when enabled, peeks the first byte to detect a
	 * TLS handshake, terminates TLS when an SSL bundle is configured, routes by SNI in
	 * passthrough mode otherwise, and parses the connection head for routing. Claims the
	 * slot of the source address first -- the PROXY protocol source is known only once
	 * the header is parsed -- and returns null when the address is at its limit. A
	 * connection of the overflow ({@code entry} no {@link ConnectionLimits.Admission})
	 * claims nothing and is given http/1.1 in ALPN when it offers it.
	 */
	private @Nullable Connection transport(Socket socket, AccessLogger.Connection access, ConnectionLimits.Entry entry)
			throws Exception {
		ConnectionLimits.@Nullable Admission admission = entry instanceof ConnectionLimits.Admission admitted ? admitted
				: null;
		InputStream in = socket.getInputStream();
		InetAddress peer = null;
		if (this.properties.dataProxyProtocol()) {
			// the header is consumed here and never relayed: the remainder is parsed
			// exactly
			// as on a plain connection, and headerless connections pass untouched
			ProxyProtocolParser.Result proxied = new ProxyProtocolParser().parse(in);
			in = proxied.input();
			ProxyProtocolParser.Header header = proxied.header();
			if (header != null && header.source() != null) {
				peer = header.source();
				access.remote(peer.getHostAddress() + ":" + header.sourcePort());
			}
		}
		if (admission != null) {
			if (!admission.claim(peer != null ? peer : peerOf(socket))) {
				return null;
			}
			access.accept();
		}
		int first = in.read();
		if (first < 0) {
			return null;
		}
		// captured from the ClientHello server_name extension
		AtomicReference<String> sni = new AtomicReference<>();
		TlsHandshake handshake = null;
		if (first == TLS_HANDSHAKE) {
			// parse the ClientHello first: the route (resolved by SNI) decides between
			// termination and passthrough
			PushbackInputStream pushback = new PushbackInputStream(in);
			pushback.unread(first);
			SniHeadParser.Head hello = new SniHeadParser(HEAD_LIMIT).parse(pushback)
				.orElseThrow(() -> new IllegalArgumentException("unrecognized tls handshake"));
			// an unresolved route keeps the legacy behavior: terminate when a bundle is
			// configured, passthrough otherwise
			Optional<Router.Route> resolved = this.router.lookup(hello.sni());
			boolean passthrough = resolved.map(Router.Route::tlsPassthrough).orElseGet(() -> this.tlsFactory() == null);
			if (passthrough) {
				// relay the TLS records untouched; the upstream terminates TLS
				log.debug("tls passthrough; sni={}", hello.sni());
				access.transport("tls-passthrough");
				return Connection.builder()
					.socket(socket)
					.head(ConnectionHeadParser.Head.encrypted(hello.bytes(), hello.sni()))
					.peer(peer)
					.build();
			}
			SSLSocketFactory factory = this.tlsFactory();
			if (factory == null) {
				log.debug("no ssl bundle to terminate tls for sni={}", hello.sni());
				return null;
			}
			// the consumed ClientHello bytes are replayed into the ssl handshake
			SSLSocket ssl = (SSLSocket) factory.createSocket(socket, new ByteArrayInputStream(hello.bytes()), true);
			ssl.setUseClientMode(false);
			SSLParameters parameters = ssl.getSSLParameters();
			parameters.setSNIMatchers(List.<SNIMatcher>of(new SNIMatcher(0) {

				@Override
				public boolean matches(SNIServerName serverName) {
					sni.set(new String(serverName.getEncoded(), StandardCharsets.US_ASCII));
					return true;
				}
			}));
			ssl.setSSLParameters(parameters);
			// force-http1 routes prefer http/1.1 in ALPN so h1-only upstreams are reached
			// over h1 (the route is the SNI-resolved one above); a client offering only
			// h2 still gets h2, and the demux answers its streams 421 instead of
			// relaying them
			boolean forceHttp1 = resolved.map(Router.Route::forceHttp1).orElse(false);
			AtomicBoolean http1Offered = new AtomicBoolean();
			ssl.setHandshakeApplicationProtocolSelector((s, protocols) -> {
				http1Offered.set(protocols.contains(ALPN_HTTP_1_1));
				return admission == null ? overflowProtocol(protocols)
						: selectProtocol(protocols, forceHttp1, admission);
			});
			ssl.startHandshake();
			handshake = new TlsHandshake(hello.sni(), http1Offered.get());
			log.debug("tls handshake done; protocol={} sni={}", ssl.getApplicationProtocol(), sni.get());
			access.transport(ALPN_H2.equals(ssl.getApplicationProtocol()) ? "h2" : "h1");
			in = ssl.getInputStream();
			socket = ssl;
		}
		else {
			PushbackInputStream pushback = new PushbackInputStream(in);
			pushback.unread(first);
			in = pushback;
		}
		ConnectionHeadParser.Head head = new ConnectionHeadParser(HEAD_LIMIT).parse(in)
			.orElseThrow(() -> new IllegalArgumentException("unrecognized connection head"));
		access.transport(head.http2Connection() ? "h2" : "h1");
		// prefer the HTTP host; fall back to the SNI host name of the terminated
		// handshake (hostless protocols such as RESP carry no Host header)
		if (head.host() == null && sni.get() != null) {
			head = head.withHost(sni.get());
		}
		return Connection.builder().socket(socket).head(head).peer(peer).handshake(handshake).build();
	}

	/**
	 * The ALPN protocol of a terminated handshake: http/1.1 for a force-http1 route when
	 * offered; else h2 when offered and the bytes of its relay fit -- reserved here, so
	 * the connection is relayed as h2 with them --, http/1.1 when offered besides,
	 * declining h2; else h2, whose relay is refused once the head is read if its bytes
	 * still do not fit.
	 */
	private static String selectProtocol(List<String> protocols, boolean forceHttp1,
			ConnectionLimits.Admission admission) {
		boolean h1 = protocols.contains(ALPN_HTTP_1_1);
		if (forceHttp1) {
			return h1 ? ALPN_HTTP_1_1 : ALPN_H2;
		}
		if (!protocols.contains(ALPN_H2)) {
			return ALPN_HTTP_1_1;
		}
		if (admission.reserveH2Relay() || !h1) {
			return ALPN_H2;
		}
		admission.declineH2();
		return ALPN_HTTP_1_1;
	}

	/**
	 * The ALPN protocol of an overflow connection, only answered: http/1.1 when offered,
	 * else h2 when offered, else http/1.1.
	 */
	private static String overflowProtocol(List<String> protocols) {
		return !protocols.contains(ALPN_HTTP_1_1) && protocols.contains(ALPN_H2) ? ALPN_H2 : ALPN_HTTP_1_1;
	}

	private @Nullable SSLSocketFactory tlsFactory() {
		SSLContext context = this.sslContext.getIfAvailable();
		return context == null ? null : context.getSocketFactory();
	}

	/**
	 * Routes and relays the connection; true once a relay owns it, in which case the
	 * admission is released once the relay completed and the frames its virtual
	 * connections queued left the tunnel sender. False when the connection was answered
	 * with an error response and closed. Plaintext and TLS-terminated HTTP relays drive
	 * {@code deadline} between their requests; a verbatim relay puts it under the
	 * verbatim idle timeout of its route. An h2 connection is relayed only once the bytes
	 * of its relay are reserved.
	 */
	private boolean relay(Connection conn, AccessLogger.Connection access, ConnectionLimits.KeepAliveDeadline deadline,
			ConnectionLimits.Admission admission) {
		Runnable release = admission::release;
		if (conn.head().undecodable()) {
			// a connection error before any stream: nothing of it may be routed
			access.request("-", "-", "2");
			this.reject(conn, this.errorResponse.compressionError());
			return false;
		}
		if (malformedHttp1(conn.head())) {
			this.reject(conn, this.errorResponse.badRequest());
			return false;
		}
		Optional<Router.Route> route = this.router.lookup(conn.head().host());
		InetAddress peer = conn.peer() != null ? conn.peer() : peerOf(conn.socket());
		// a trusted proxy's forwarded-for entry replaces the peer as the judged address
		if (!this.accessControl.allowed(route.orElse(null), peer, conn.head().forwardedFor(),
				conn.head().forwarded())) {
			this.reject(conn, this.errorResponse.forbidden(conn.head()));
			return false;
		}
		TunnelSession session = route.map(r -> this.sessions.find(r.clientId()).orElse(null)).orElse(null);
		if (session == null) {
			this.reject(conn, this.errorResponse.noRoute(conn.head()));
			return false;
		}
		Router.Route route0 = route.get();
		access.route(route0.routeTag());
		ConnectionHeadParser.Head.Request request = conn.head().request();
		boolean http2 = conn.head().http2Connection();
		if ((request != null || http2) && !conn.head().encrypted()) {
			// plaintext HTTP: every request head / stream is routed; successors may
			// belong to another client's upstream (keep-alive, h2 multiplexing). An h2
			// connection is demultiplexed even when its first head is no request the
			// relay can route -- the relay resets that stream: relayed verbatim, its
			// later streams would bypass their own routing and access control
			if (http2 && !admission.reserveH2Relay()) {
				admission.rejectH2();
				this.reject(conn, this.errorResponse.connectionLimit());
				return false;
			}
			Runnable relayed = this.relayed(access);
			if (http2) {
				NettyHttp2DemuxRelay demux = NettyHttp2DemuxRelay.builder()
					.pipe(this.clientPipeSeam.apply(conn.pipe()))
					.head(conn.head())
					.router(this.router)
					.sessions(this.sessions)
					.accessControl(this.accessControl)
					.routeAuth(this.routeAuth)
					.errorResponse(this.errorResponse)
					.access(access)
					.meters(this.h2DemuxMeters)
					.initialRoute(route0, session)
					.handshake(conn.handshake())
					.peer(peer)
					.deadline(deadline)
					.streamWaitTimeout(this.h2StreamWaitTimeout)
					.probeInterval(this.properties.limits().halfCloseProbeInterval())
					.budget(this.limits.h2Budget())
					.legSlots(this.limits::tryAcquireLeg)
					.legSeam(this.legSeam)
					.onFinish(relayed)
					.release(release)
					.build();
				this.activeConnections.incrementAndGet();
				try {
					demux.start();
				}
				catch (RuntimeException e) {
					this.activeConnections.decrementAndGet();
					throw e;
				}
				return true;
			}
			ReroutingRelay rerouting = new ReroutingRelay(this.clientPipeSeam.apply(conn.pipe()), conn.head(),
					this.router, this.sessions, this.accessControl, this.routeAuth, this.errorResponse, access,
					this.meterRegistry, route0, session, peer, deadline,
					this.properties.limits().halfCloseProbeInterval(), this.probeSeam, relayed, release);
			this.activeConnections.incrementAndGet();
			try {
				rerouting.start();
			}
			catch (RuntimeException e) {
				this.activeConnections.decrementAndGet();
				throw e;
			}
			return true;
		}
		if (this.routeAuth.protects(route0)) {
			// relayed verbatim, no request of it could be checked: refused rather than
			// passed unchecked (a TLS passthrough connection gets no answer)
			access.refused(route0.routeTag(), request, 401, "auth-unreadable");
			this.reject(conn, this.errorResponse.unauthorized(conn.head(), List.of()));
			return false;
		}
		VirtualConnection connection = session.open(route0.address());
		access.connectionId(connection.connectionId());
		if (request != null) {
			access.request(request.method(), request.path(), request.version());
		}
		StreamRelay relay = StreamRelay
			.builder(ProgressPipe.of(conn.pipe(), deadline::progress), connection, session.sender())
			.prefix(conn.head().bytes())
			.listener(this.relayedBytes(route0, session, access))
			.onComplete(() -> {
				deadline.disarm();
				this.activeConnections.decrementAndGet();
				access.close();
				session.remove(connection.connectionId());
				// the slot covers what the connection still queues for the tunnel
				connection.afterQueued(release);
			})
			.build();
		deadline.onExpiry(relay::abandon);
		deadline.relayVerbatim(route0);
		this.activeConnections.incrementAndGet();
		try {
			relay.start();
		}
		catch (RuntimeException e) {
			deadline.disarm();
			this.activeConnections.decrementAndGet();
			throw e;
		}
		return true;
	}

	/**
	 * Whether the head is an HTTP/1.1 request whose request line is malformed: a Host
	 * header, yet no request line -- neither an h2 preface, a RESP array nor TLS
	 * passthrough, which are routed without one. Answered 400 rather than relayed
	 * verbatim, as the rerouting relay answers a malformed request.
	 */
	private static boolean malformedHttp1(ConnectionHeadParser.Head head) {
		byte[] bytes = head.bytes();
		return head.request() == null && !head.encrypted() && bytes.length > 0
				&& bytes[0] != ConnectionHeadParser.RESP_ARRAY && !ConnectionHeadParser.isHttp2(bytes)
				&& ConnectionHeadParser.http1HostOf(bytes) != null;
	}

	/**
	 * The once-only completion of a demultiplexing relay: the connection level of the
	 * access log and the active-connection count, shared by every leg. The relay releases
	 * the admission itself, once the frames its legs queued left.
	 */
	private Runnable relayed(AccessLogger.Connection access) {
		return () -> {
			this.activeConnections.decrementAndGet();
			access.close();
		};
	}

	/**
	 * Answers a rejected connection with the given error response, then half-closes and
	 * drains it before closing: closing with unread request bytes (a body, or HTTP/2
	 * frames sent after the head) would reset the connection and discard the response
	 * before the peer reads it.
	 */
	private void reject(Connection conn, byte[] response) {
		try {
			if (response.length > 0) {
				OutputStream out = conn.pipe().sink();
				out.write(response);
				out.flush();
				this.linger(conn);
			}
		}
		catch (Exception e) {
			log.debug("failed to write no-route response: {}", e.toString());
		}
		finally {
			this.close(conn.socket());
		}
	}

	private void linger(Connection conn) throws Exception {
		if (!(conn.socket() instanceof SSLSocket)) {
			// TLS has no half-close; the peer closes after the response instead
			conn.pipe().shutdownOutput();
		}
		long deadline = System.nanoTime() + LINGER.toNanos();
		long drained = 0;
		byte[] buffer = new byte[8192];
		InputStream in = conn.pipe().source();
		try {
			while (drained < LINGER_LIMIT) {
				long remaining = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
				if (remaining <= 0) {
					return;
				}
				conn.socket().setSoTimeout((int) remaining);
				int n = in.read(buffer);
				if (n < 0) {
					return;
				}
				drained += n;
			}
		}
		catch (SocketTimeoutException e) {
			// the peer kept the connection open past the linger period
		}
	}

	private StreamRelay.Listener relayedBytes(Router.Route route, TunnelSession session,
			AccessLogger.Connection access) {
		Counter inbound = this.meterRegistry.counter(METRIC_NAME, "direction", "inbound", "route", route.routeTag());
		Counter outbound = this.meterRegistry.counter(METRIC_NAME, "direction", "outbound", "route", route.routeTag());
		return new StreamRelay.Listener() {

			@Override
			public void onBytesRelayed(long count) {
			}

			@Override
			public void onBytesRelayed(long count, StreamRelay.Direction direction) {
				(direction == StreamRelay.Direction.TO_REMOTE ? inbound : outbound).increment(count);
				session.recordRelayed(count, direction);
				access.bytes(count, direction);
			}
		};
	}

	private static @Nullable InetAddress peerOf(Socket socket) {
		if (socket.getRemoteSocketAddress() instanceof InetSocketAddress remote) {
			return remote.getAddress();
		}
		return null;
	}

	private void close(Socket socket) {
		try {
			socket.close();
		}
		catch (Exception e) {
			// ignore
		}
	}

	/**
	 * Drain entry point: closes the listen socket so new connections are rejected while
	 * the relays in flight keep running on their own sockets; the lifecycle stop below
	 * repeats it harmlessly.
	 */
	@Override
	public void beginDrain() {
		stop();
	}

	@Override
	public void stop() {
		this.running = false;
		ServerSocket socket = this.serverSocket;
		if (socket != null) {
			try {
				socket.close();
			}
			catch (Exception e) {
				// ignore
			}
		}
	}

	@Override
	public boolean isRunning() {
		return this.running;
	}

	/** The meters of the h2 demux relays; for tests. */
	H2DemuxMeters h2DemuxMeters() {
		return this.h2DemuxMeters;
	}

	/** Sets the probe seam of the HTTP/1.1 relays started from now on; for tests. */
	void probeSeam(ReroutingRelay.ProbeSeam probeSeam) {
		this.probeSeam = probeSeam;
	}

	/**
	 * Sets the wait deadline of the h2 demux relays started from now on, in place of
	 * {@code sluice.limits.h2-stream-wait-timeout}; for tests.
	 */
	void h2StreamWaitTimeout(Duration h2StreamWaitTimeout) {
		this.h2StreamWaitTimeout = h2StreamWaitTimeout;
	}

	/**
	 * Wraps the client pipe of the HTTP relays (HTTP/1.1 and h2) started from now on in
	 * {@code clientPipeSeam}; for tests.
	 */
	void clientPipeSeam(UnaryOperator<DuplexPipe> clientPipeSeam) {
		this.clientPipeSeam = clientPipeSeam;
	}

	/** Sets the leg seam of the h2 demux relays started from now on; for tests. */
	void legSeam(NettyHttp2DemuxRelay.LegSeam legSeam) {
		this.legSeam = legSeam;
	}

	/**
	 * Number of connections currently relayed from the data plane listener.
	 */
	public int activeConnections() {
		return this.activeConnections.get();
	}

	public int boundPort() {
		ServerSocket socket = this.serverSocket;
		return socket == null ? -1 : socket.getLocalPort();
	}

}
