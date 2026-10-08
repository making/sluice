package am.ik.sluice.server.proxy;

import java.io.ByteArrayInputStream;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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

import am.ik.sluice.server.config.SluiceServerProperties;
import am.ik.sluice.server.route.Router;
import am.ik.sluice.server.tunnel.Drainable;
import am.ik.sluice.server.tunnel.SessionRegistry;
import am.ik.sluice.server.tunnel.TunnelSession;
import am.ik.sluice.tunnel.DuplexPipe;
import am.ik.sluice.tunnel.StreamRelay;
import am.ik.sluice.tunnel.VirtualConnection;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Data plane: a TCP listener on the data port that routes each connection by the Host
 * header (HTTP/1.1) or {@code :authority} (HTTP/2 prior knowledge) of the request head
 * and relays it, byte for byte, over a virtual connection of the matching tunnel session.
 * Because only the request head is inspected, WebSocket upgrades, h2c upgrade, and HTTP
 * keep-alive pass through transparently. A TLS handshake (first byte {@code 0x16}) is
 * either terminated locally when an SSL bundle is configured (ALPN: h2 preferred over
 * http/1.1), or relayed untouched in passthrough mode otherwise, routed by the SNI host
 * name of the ClientHello (the backend terminates TLS and presents its own certificate).
 */
@Component
public class DataProxyServer implements SmartLifecycle, Drainable {

	private static final Logger log = LoggerFactory.getLogger(DataProxyServer.class);

	private static final int HEAD_LIMIT = 64 * 1024;

	private static final int TLS_HANDSHAKE = 0x16;

	private static final String ALPN_H2 = "h2";

	private static final String ALPN_HTTP_1_1 = "http/1.1";

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

	private final AccessLogger accessLogger;

	private final TaskExecutor taskExecutor;

	private final ObjectProvider<SSLContext> sslContext;

	private final ErrorResponse errorResponse;

	private final AccessControl accessControl;

	private volatile boolean running;

	private volatile @Nullable ServerSocket serverSocket;

	DataProxyServer(Router router, SessionRegistry sessions, SluiceServerProperties properties,
			MeterRegistry meterRegistry, AccessLogger accessLogger, AccessControl accessControl,
			@Qualifier("applicationTaskExecutor") TaskExecutor taskExecutor, ObjectProvider<SSLContext> sslContext,
			ErrorResponse errorResponse) {
		this.router = router;
		this.sessions = sessions;
		this.properties = properties;
		this.meterRegistry = meterRegistry;
		this.accessLogger = accessLogger;
		this.accessControl = accessControl;
		this.taskExecutor = taskExecutor;
		this.sslContext = sslContext;
		this.errorResponse = errorResponse;
	}

	/**
	 * A routed connection: the transport socket, its {@link DuplexPipe} view, the parsed
	 * connection head (verbatim bytes plus the resolved route host), and the connection
	 * peer -- the source of a parsed PROXY protocol header when one was present, the
	 * socket peer otherwise.
	 */
	record Connection(Socket socket, DuplexPipe pipe, ConnectionHeadParser.Head head, @Nullable InetAddress peer) {

		static Connection of(Socket socket, ConnectionHeadParser.Head head, @Nullable InetAddress peer) {
			return new Connection(socket, DuplexPipe.of(socket), head, peer);
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
				socket.setTcpNoDelay(true);
				this.taskExecutor.execute(() -> this.handle(socket));
			}
			catch (Exception e) {
				if (this.running) {
					log.warn("accept failed: {}", e.toString());
				}
			}
		}
	}

	private void handle(Socket socket) {
		AccessLogger.Connection access = this.accessLogger.accepted("data", socket);
		try {
			Connection connection = this.transport(socket, access);
			if (connection == null) {
				access.close();
				this.close(socket);
				return;
			}
			if (!this.relay(connection, access)) {
				// access denied or no session: relay() answered with the error response
				// and closed the socket
				access.close();
			}
		}
		catch (Exception e) {
			log.debug("data connection failed: {}", e.toString());
			access.close();
			this.close(socket);
		}
	}

	/**
	 * Consumes the PROXY protocol header when enabled, peeks the first byte to detect a
	 * TLS handshake, terminates TLS when an SSL bundle is configured, routes by SNI in
	 * passthrough mode otherwise, and parses the connection head for routing.
	 */
	private @Nullable Connection transport(Socket socket, AccessLogger.Connection access) throws Exception {
		InputStream in = socket.getInputStream();
		InetAddress peer = null;
		if (this.properties.proxyProtocol()) {
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
		access.accept();
		int first = in.read();
		if (first < 0) {
			return null;
		}
		// captured from the ClientHello server_name extension
		AtomicReference<String> sni = new AtomicReference<>();
		if (first == TLS_HANDSHAKE) {
			// parse the ClientHello first: the route (resolved by SNI) decides between
			// termination and passthrough
			PushbackInputStream pushback = new PushbackInputStream(in);
			pushback.unread(first);
			SniHeadParser.Head hello = new SniHeadParser(HEAD_LIMIT).parse(pushback)
				.orElseThrow(() -> new IllegalArgumentException("unrecognized tls handshake"));
			// an unresolved route keeps the legacy behavior: terminate when a bundle is
			// configured, passthrough otherwise
			boolean passthrough = this.router.lookup(hello.sni())
				.map(Router.Route::tlsPassthrough)
				.orElseGet(() -> this.tlsFactory() == null);
			if (passthrough) {
				// relay the TLS records untouched; the upstream terminates TLS
				log.debug("tls passthrough; sni={}", hello.sni());
				access.transport("tls-passthrough");
				return Connection.of(socket, ConnectionHeadParser.Head.encrypted(hello.bytes(), hello.sni()), peer);
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
			ssl.setHandshakeApplicationProtocolSelector(
					(s, protocols) -> protocols.contains(ALPN_H2) ? ALPN_H2 : ALPN_HTTP_1_1);
			ssl.startHandshake();
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
		boolean h2 = head.request() != null && "2".equals(head.request().version());
		access.transport(h2 ? "h2" : "h1");
		// prefer the HTTP host; fall back to the SNI host name of the terminated
		// handshake (hostless protocols such as RESP carry no Host header)
		if (head.host() == null && sni.get() != null) {
			head = head.withHost(sni.get());
		}
		return Connection.of(socket, head, peer);
	}

	private @Nullable SSLSocketFactory tlsFactory() {
		SSLContext context = this.sslContext.getIfAvailable();
		return context == null ? null : context.getSocketFactory();
	}

	private boolean relay(Connection conn, AccessLogger.Connection access) {
		Optional<Router.Route> route = this.router.lookup(conn.head().host());
		InetAddress peer = conn.peer() != null ? conn.peer() : peerOf(conn.socket());
		// a trusted proxy's forwarded-for entry replaces the peer as the judged address
		if (!this.accessControl.allowed(route.orElse(null), peer, conn.head().forwardedFor())) {
			this.reject(conn, this.errorResponse.forbidden(conn.head()));
			return false;
		}
		TunnelSession session = route.map(r -> this.sessions.find(r.clientId()).orElse(null)).orElse(null);
		if (session == null) {
			this.reject(conn, this.errorResponse.noRoute(conn.head()));
			return false;
		}
		Router.Route route0 = route.get();
		VirtualConnection connection = session.open(route0.address());
		access.route(route0.routeTag()).connectionId(connection.connectionId());
		ConnectionHeadParser.Head.Request request = conn.head().request();
		if (request != null) {
			access.request(request.method(), request.path(), request.version());
		}
		// per-request host rewriting on the relayed stream; connections that must stay
		// verbatim (preserve-host, TLS passthrough) keep the one-shot head prefix
		HostRewritingPipe rewriting = route0.preserveHost() || conn.head().encrypted() ? null
				: HostRewritingPipe.of(conn.pipe(), conn.head(), route0.address());
		StreamRelay relay = StreamRelay
			.builder(rewriting != null ? rewriting : conn.pipe(), connection, session.sender())
			.prefix(rewriting != null ? null : conn.head().bytes())
			.listener(this.relayedBytes(route0, session, access))
			.onComplete(() -> {
				this.activeConnections.decrementAndGet();
				access.close();
				session.remove(connection.connectionId());
			})
			.build();
		this.activeConnections.incrementAndGet();
		try {
			relay.start();
		}
		catch (RuntimeException e) {
			this.activeConnections.decrementAndGet();
			throw e;
		}
		return true;
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
