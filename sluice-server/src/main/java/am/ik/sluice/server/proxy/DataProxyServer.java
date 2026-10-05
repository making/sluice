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
 * keep-alive pass through transparently. When an SSL bundle is configured, connections
 * whose first byte is a TLS handshake are terminated locally (ALPN: h2 preferred over
 * http/1.1) before routing.
 */
@Component
public class DataProxyServer implements SmartLifecycle {

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

	private final Counter relayedBytes;

	private final TaskExecutor taskExecutor;

	private final ObjectProvider<SSLContext> sslContext;

	private volatile boolean running;

	private volatile @Nullable ServerSocket serverSocket;

	DataProxyServer(Router router, SessionRegistry sessions, SluiceServerProperties properties,
			MeterRegistry meterRegistry, @Qualifier("applicationTaskExecutor") TaskExecutor taskExecutor,
			ObjectProvider<SSLContext> sslContext) {
		this.router = router;
		this.sessions = sessions;
		this.properties = properties;
		this.taskExecutor = taskExecutor;
		this.sslContext = sslContext;
		this.relayedBytes = Counter.builder("sluice.tunnel.bytes")
			.tag("direction", "data")
			.description("Bytes relayed through the data plane")
			.register(meterRegistry);
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
		try {
			Connection connection = this.transport(socket);
			if (connection == null) {
				this.close(socket);
				return;
			}
			this.relay(connection);
		}
		catch (Exception e) {
			log.debug("data connection failed: {}", e.toString());
			this.close(socket);
		}
	}

	/**
	 * Peeks the first byte to detect a TLS handshake, terminates TLS when configured,
	 * then parses the connection head for routing.
	 */
	private @Nullable Connection transport(Socket socket) throws Exception {
		InputStream in = socket.getInputStream();
		int first = in.read();
		if (first < 0) {
			return null;
		}
		SSLSocketFactory factory = this.tlsFactory();
		if (first == TLS_HANDSHAKE && factory != null) {
			// the consumed byte is replayed into the ssl handshake via the consumed
			// stream
			SSLSocket ssl = (SSLSocket) factory.createSocket(socket,
					new ByteArrayInputStream(new byte[] { (byte) first }), true);
			ssl.setUseClientMode(false);
			ssl.setHandshakeApplicationProtocolSelector(
					(s, protocols) -> protocols.contains(ALPN_H2) ? ALPN_H2 : ALPN_HTTP_1_1);
			ssl.startHandshake();
			log.debug("tls handshake done; protocol={}", ssl.getApplicationProtocol());
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
		return Connection.of(socket, head);
	}

	private @Nullable SSLSocketFactory tlsFactory() {
		SSLContext context = this.sslContext.getIfAvailable();
		return context == null ? null : context.getSocketFactory();
	}

	private void relay(Connection conn) {
		Optional<Router.Route> route = this.router.lookup(conn.head().host());
		TunnelSession session = route.map(r -> this.sessions.find(r.clientId()).orElse(null)).orElse(null);
		if (session == null) {
			try (OutputStream out = conn.pipe().sink()) {
				out.write(SERVICE_UNAVAILABLE);
				out.flush();
			}
			catch (Exception e) {
				log.debug("failed to write service unavailable: {}", e.toString());
			}
			this.close(conn.socket());
			return;
		}
		VirtualConnection connection = session.open(route.get().address());
		StreamRelay relay = StreamRelay.builder(conn.pipe(), connection, session.sender())
			.prefix(conn.head().bytes())
			.listener(this.relayedBytes::increment)
			.onComplete(() -> session.remove(connection.connectionId()))
			.build();
		relay.start();
	}

	private void close(Socket socket) {
		try {
			socket.close();
		}
		catch (Exception e) {
			// ignore
		}
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

	public int boundPort() {
		ServerSocket socket = this.serverSocket;
		return socket == null ? -1 : socket.getLocalPort();
	}

}
