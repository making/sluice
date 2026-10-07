package am.ik.sluice.server.proxy;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PushbackInputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
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

	private static final byte[] SERVICE_UNAVAILABLE = ("HTTP/1.1 503 Service Unavailable\r\n"
			+ "Content-Length: 0\r\nConnection: close\r\n\r\n")
		.getBytes(StandardCharsets.US_ASCII);

	private final Router router;

	private final SessionRegistry sessions;

	private final SluiceServerProperties properties;

	private static final String METRIC_NAME = "sluice.tunnel.bytes";

	private final AtomicInteger activeConnections = new AtomicInteger();

	private final MeterRegistry meterRegistry;

	private final AccessLogger accessLogger;

	private final TaskExecutor taskExecutor;

	private final ObjectProvider<SSLContext> sslContext;

	private volatile boolean running;

	private volatile @Nullable ServerSocket serverSocket;

	DataProxyServer(Router router, SessionRegistry sessions, SluiceServerProperties properties,
			MeterRegistry meterRegistry, AccessLogger accessLogger,
			@Qualifier("applicationTaskExecutor") TaskExecutor taskExecutor, ObjectProvider<SSLContext> sslContext) {
		this.router = router;
		this.sessions = sessions;
		this.properties = properties;
		this.meterRegistry = meterRegistry;
		this.accessLogger = accessLogger;
		this.taskExecutor = taskExecutor;
		this.sslContext = sslContext;
	}

	/**
	 * A routed connection: the transport socket, its {@link DuplexPipe} view, and the
	 * parsed connection head (verbatim bytes plus the resolved route host).
	 */
	record Connection(Socket socket, DuplexPipe pipe, ConnectionHeadParser.Head head) {

		static Connection of(Socket socket, ConnectionHeadParser.Head head) {
			return new Connection(socket, DuplexPipe.of(socket), head);
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
		access.accept();
		try {
			Connection connection = this.transport(socket, access);
			if (connection == null) {
				access.close();
				this.close(socket);
				return;
			}
			if (!this.relay(connection, access)) {
				// no session: relay() wrote the 503 and closed the socket
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
	 * Peeks the first byte to detect a TLS handshake, terminates TLS when an SSL bundle
	 * is configured, routes by SNI in passthrough mode otherwise, and parses the
	 * connection head for routing.
	 */
	private @Nullable Connection transport(Socket socket, AccessLogger.Connection access) throws Exception {
		InputStream in = socket.getInputStream();
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
				return Connection.of(socket, ConnectionHeadParser.Head.encrypted(hello.bytes(), hello.sni()));
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
		return Connection.of(socket, head);
	}

	private @Nullable SSLSocketFactory tlsFactory() {
		SSLContext context = this.sslContext.getIfAvailable();
		return context == null ? null : context.getSocketFactory();
	}

	private boolean relay(Connection conn, AccessLogger.Connection access) {
		Optional<Router.Route> route = this.router.lookup(conn.head().host());
		TunnelSession session = route.map(r -> this.sessions.find(r.clientId()).orElse(null)).orElse(null);
		if (session == null) {
			// a passthrough peer mid TLS handshake expects TLS records, not an HTTP error
			if (!conn.head().encrypted()) {
				try (OutputStream out = conn.pipe().sink()) {
					out.write(SERVICE_UNAVAILABLE);
					out.flush();
				}
				catch (Exception e) {
					log.debug("failed to write service unavailable: {}", e.toString());
				}
			}
			this.close(conn.socket());
			return false;
		}
		Router.Route route0 = route.get();
		VirtualConnection connection = session.open(route0.address());
		access.route(route0.routeTag()).connectionId(connection.connectionId());
		ConnectionHeadParser.Head.Request request = conn.head().request();
		if (request != null) {
			access.request(request.method(), request.path(), request.version());
		}
		byte[] head = route0.preserveHost() || conn.head().encrypted() ? conn.head().bytes()
				: ConnectionHeadRewriter.rewrite(conn.head(), route0.address());
		StreamRelay relay = StreamRelay.builder(conn.pipe(), connection, session.sender())
			.prefix(head)
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
