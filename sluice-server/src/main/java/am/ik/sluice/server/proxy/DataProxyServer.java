package am.ik.sluice.server.proxy;

import java.util.Optional;

import org.jspecify.annotations.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import am.ik.sluice.server.config.SluiceServerProperties;
import am.ik.sluice.server.route.Router;
import am.ik.sluice.server.tunnel.SessionRegistry;
import am.ik.sluice.server.tunnel.TunnelSession;
import am.ik.sluice.tunnel.SocketRelay;
import am.ik.sluice.tunnel.VirtualConnection;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Component;

/**
 * Data plane: a raw TCP listener on the data port that routes each connection by the Host
 * header of the first request and relays it, byte for byte, over a virtual connection of
 * the matching tunnel session. Because only the request head is inspected, WebSocket
 * upgrades and HTTP keep-alive pass through transparently.
 */
@Component
public class DataProxyServer implements SmartLifecycle {

	private static final Logger log = LoggerFactory.getLogger(DataProxyServer.class);

	private static final int HEAD_LIMIT = 64 * 1024;

	private static final byte[] SERVICE_UNAVAILABLE = ("HTTP/1.1 503 Service Unavailable\r\n"
			+ "Content-Length: 0\r\nConnection: close\r\n\r\n")
		.getBytes(StandardCharsets.US_ASCII);

	private final Router router;

	private final SessionRegistry sessions;

	private final SluiceServerProperties properties;

	private final MeterRegistry meterRegistry;

	private final Counter relayedBytes;

	private final TaskExecutor taskExecutor;

	private final AtomicLong sequence = new AtomicLong(1);

	private volatile boolean running;

	private volatile @Nullable ServerSocket serverSocket;

	public DataProxyServer(Router router, SessionRegistry sessions, SluiceServerProperties properties,
			MeterRegistry meterRegistry, @Qualifier("applicationTaskExecutor") TaskExecutor taskExecutor) {
		this.router = router;
		this.sessions = sessions;
		this.properties = properties;
		this.meterRegistry = meterRegistry;
		this.taskExecutor = taskExecutor;
		this.relayedBytes = Counter.builder("sluice.tunnel.bytes")
			.tag("direction", "data")
			.description("Bytes relayed through the data plane")
			.register(meterRegistry);
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
				this.taskExecutor.execute(() -> handle(socket));
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
			Optional<byte[]> head = readHead(socket.getInputStream());
			if (head.isEmpty()) {
				return;
			}
			Optional<Router.Route> route = this.router.lookup(hostOf(head.get()));
			TunnelSession session = route.map(r -> this.sessions.find(r.clientId()).orElse(null)).orElse(null);
			if (session == null) {
				try (OutputStream out = socket.getOutputStream()) {
					out.write(SERVICE_UNAVAILABLE);
					out.flush();
				}
				return;
			}
			VirtualConnection connection = session.open(route.get().address());
			SocketRelay relay = SocketRelay.builder(socket, connection, session.sender())
				.prefix(head.get())
				.listener(this.relayedBytes::increment)
				.onComplete(() -> session.remove(connection.connectionId()))
				.build();
			relay.start();
		}
		catch (Exception e) {
			log.debug("data connection failed: {}", e.toString());
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

	private static Optional<byte[]> readHead(InputStream in) {
		ByteArrayOutputStream buffer = new ByteArrayOutputStream(1024);
		byte[] chunk = new byte[1024];
		try {
			int n;
			while (buffer.size() < HEAD_LIMIT && (n = in.read(chunk)) > 0) {
				buffer.write(chunk, 0, n);
				byte[] bytes = buffer.toByteArray();
				if (endsWithDoubleCrlf(bytes)) {
					return Optional.of(bytes);
				}
			}
		}
		catch (Exception e) {
			return Optional.empty();
		}
		byte[] tail = buffer.size() == 0 ? null : buffer.toByteArray();
		if (tail == null || !endsWithDoubleCrlf(tail)) {
			return Optional.empty();
		}
		return Optional.of(tail);
	}

	private static boolean endsWithDoubleCrlf(byte @Nullable [] bytes) {
		if (bytes == null || bytes.length < 4) {
			return false;
		}
		int length = bytes.length;
		return bytes[length - 4] == '\r' && bytes[length - 3] == '\n' && bytes[length - 2] == '\r'
				&& bytes[length - 1] == '\n';
	}

	/**
	 * Extracts the Host header value from a request head; {@code null} when absent.
	 */
	static @Nullable String hostOf(byte[] head) {
		String text = new String(head, StandardCharsets.US_ASCII);
		String[] lines = text.split("\r\n");
		for (int i = 1; i < lines.length; i++) {
			int colon = lines[i].indexOf(':');
			if (colon <= 0) {
				continue;
			}
			if ("host".equalsIgnoreCase(lines[i].substring(0, colon))) {
				String value = lines[i].substring(colon + 1).trim();
				return value.isBlank() ? null : value;
			}
		}
		return null;
	}

}
