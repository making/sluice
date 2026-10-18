package am.ik.sluice.server.proxy;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.jspecify.annotations.Nullable;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Component;

import am.ik.sluice.server.config.SluiceServerProperties;
import am.ik.sluice.server.config.TcpPortRange;
import am.ik.sluice.server.route.Router;
import am.ik.sluice.server.tunnel.LogThrottle;
import am.ik.sluice.server.tunnel.SessionRegistry;
import am.ik.sluice.server.tunnel.TcpRouteListener;
import am.ik.sluice.server.tunnel.Drainable;
import am.ik.sluice.server.tunnel.TunnelSession;
import am.ik.sluice.tunnel.DuplexPipe;
import am.ik.sluice.tunnel.StreamRelay;
import am.ik.sluice.tunnel.TcpKeepAlive;
import am.ik.sluice.tunnel.VirtualConnection;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

/**
 * Data plane for tcp port routes: one TCP listener per port advertised by a client
 * ({@code listen-port} of an upstream), relaying every accepted connection verbatim -- no
 * head parsing, no rewriting -- over a virtual connection of the owning session, under
 * the verbatim idle timeout of the route. Listeners are bound on advertise and released
 * on session end; in-flight relays survive an unbind because each runs on its own
 * accepted socket.
 */
@Component
public class TcpPortGateway implements TcpRouteListener, SmartLifecycle, AutoCloseable, Drainable {

	private static final Logger log = LoggerFactory.getLogger(TcpPortGateway.class);

	private final Router router;

	private final SessionRegistry sessions;

	private final SluiceServerProperties properties;

	private final TaskExecutor taskExecutor;

	private static final String METRIC_NAME = "sluice.tunnel.bytes";

	private final MeterRegistry meterRegistry;

	private final AccessLogger accessLogger;

	private final AccessControl accessControl;

	private final ConnectionLimits limits;

	/** The period of {@link #portLog}. */
	static final Duration PORT_LOG_PERIOD = Duration.ofSeconds(10);

	/**
	 * The port lines (bind, unbind, takeover, rejection, bind failure) per
	 * {@link #PORT_LOG_PERIOD} logged at info / warn; the others log at debug.
	 */
	static final int PORT_LOG_PERMITS = 32;

	private final LogThrottle portLog;

	private final ConcurrentMap<Integer, BoundListener> listeners = new ConcurrentHashMap<>();

	private final ConcurrentMap<Integer, AtomicInteger> activeConnections = new ConcurrentHashMap<>();

	/**
	 * A bound tcp route listener as reported to observers.
	 *
	 * @param clientId client owning the listener
	 * @param activeConnections connections currently relayed from the port
	 */
	public record Binding(String clientId, int activeConnections) {

	}

	private final TcpPortRange tcpPortRange;

	private volatile boolean running;

	@Autowired
	TcpPortGateway(Router router, SessionRegistry sessions, SluiceServerProperties properties,
			AccessControl accessControl, @Qualifier("applicationTaskExecutor") TaskExecutor taskExecutor,
			MeterRegistry meterRegistry, AccessLogger accessLogger, ConnectionLimits limits) {
		this(router, sessions, properties, accessControl, taskExecutor, meterRegistry, accessLogger, limits,
				new LogThrottle(PORT_LOG_PERIOD, PORT_LOG_PERMITS, System::nanoTime));
	}

	TcpPortGateway(Router router, SessionRegistry sessions, SluiceServerProperties properties,
			AccessControl accessControl, TaskExecutor taskExecutor, MeterRegistry meterRegistry,
			AccessLogger accessLogger, ConnectionLimits limits, LogThrottle portLog) {
		this.portLog = portLog;
		this.router = router;
		this.sessions = sessions;
		this.properties = properties;
		this.accessControl = accessControl;
		this.tcpPortRange = TcpPortRange.parse(properties.tcpPortRange());
		this.taskExecutor = taskExecutor;
		this.meterRegistry = meterRegistry;
		this.accessLogger = accessLogger;
		this.limits = limits;
	}

	/**
	 * A bound listener: the owning client, its server socket, and the accept loop flag.
	 */
	private record BoundListener(String clientId, ServerSocket socket, AtomicBoolean running) {

		static Builder builder() {
			return new Builder();
		}

		void close() {
			this.running.set(false);
			try {
				this.socket.close();
			}
			catch (Exception e) {
				// ignore
			}
		}

		static final class Builder {

			private @Nullable String clientId;

			private @Nullable ServerSocket socket;

			private @Nullable AtomicBoolean running;

			private Builder() {
			}

			Builder clientId(String clientId) {
				this.clientId = clientId;
				return this;
			}

			Builder socket(ServerSocket socket) {
				this.socket = socket;
				return this;
			}

			Builder running(AtomicBoolean running) {
				this.running = running;
				return this;
			}

			BoundListener build() {
				return new BoundListener(Objects.requireNonNull(this.clientId, "clientId is required"),
						Objects.requireNonNull(this.socket, "socket is required"),
						Objects.requireNonNull(this.running, "running is required"));
			}

		}

	}

	@Override
	public synchronized Set<Integer> reconcile(String clientId, Set<Integer> listenPorts) {
		for (ConcurrentMap.Entry<Integer, BoundListener> entry : this.listeners.entrySet()) {
			if (entry.getValue().clientId().equals(clientId) && !listenPorts.contains(entry.getKey())) {
				int port = entry.getKey();
				this.listeners.remove(port, entry.getValue());
				entry.getValue().close();
				portLog(Level.INFO, "unbound tcp route port {} (client {})", port, clientId);
			}
		}
		Set<Integer> rejected = new TreeSet<>();
		for (Integer port : listenPorts) {
			if (!this.tcpPortRange.contains(port)) {
				portLog(Level.WARN, "tcp route port {} is outside tcp-port-range ({}); rejecting for client {}", port,
						this.properties.tcpPortRange(), clientId);
				rejected.add(port);
				continue;
			}
			BoundListener existing = this.listeners.get(port);
			if (existing != null) {
				if (existing.clientId().equals(clientId)) {
					continue; // already bound by a previous advertise of the same client
				}
				// deterministic ownership: the smaller client id owns the port on every
				// node. A gone owner always loses (the unbind of a dropped session may
				// race the advertise of the reconnected one)
				if (this.sessions.find(existing.clientId()).isPresent()
						&& existing.clientId().compareTo(clientId) < 0) {
					portLog(Level.WARN, "tcp route port {} is owned by client {}; rejecting for client {}", port,
							existing.clientId(), clientId);
					rejected.add(port);
					continue;
				}
				portLog(Level.INFO, "tcp route port {} taken over from client {} by client {}", port,
						existing.clientId(), clientId);
				this.listeners.remove(port, existing);
				existing.close();
			}
			if (!this.bind(clientId, port)) {
				rejected.add(port);
			}
		}
		return Collections.unmodifiableSet(rejected);
	}

	/**
	 * Logs one port line at {@code level} while the period's permits last, at debug
	 * after; the next line let through reports how many were demoted.
	 */
	private void portLog(Level level, String format, Object... args) {
		long unreported = this.portLog.tryAcquire();
		String note = unreported > 0
				? " (%d more tcp route port lines logged at debug since the last report)".formatted(unreported) : "";
		log.atLevel(unreported >= 0 ? level : Level.DEBUG).log(format + note, args);
	}

	private boolean bind(String clientId, int port) {
		// a stolen listener's socket may still be in teardown; retry briefly
		for (int attempt = 0;; attempt++) {
			try {
				ServerSocket socket = new ServerSocket();
				socket.setReuseAddress(true);
				socket.bind(new InetSocketAddress(InetAddress.getByName(this.properties.dataHost()), port), 128);
				BoundListener listener = BoundListener.builder()
					.clientId(clientId)
					.socket(socket)
					.running(new AtomicBoolean(true))
					.build();
				this.listeners.put(port, listener);
				Thread.ofVirtual().name("sluice-tcp-accept-" + port).start(() -> this.acceptLoop(port, listener));
				portLog(Level.INFO, "bound tcp route port {} for client {}", port, clientId);
				return true;
			}
			catch (Exception e) {
				if (attempt >= 2) {
					// a bind failure must not tear down the session; the port is reported
					// back
					portLog(Level.WARN, "failed to bind tcp route port {} for client {}: {}", port, clientId,
							e.toString());
					return false;
				}
				try {
					Thread.sleep(100);
				}
				catch (InterruptedException ie) {
					Thread.currentThread().interrupt();
					return false;
				}
			}
		}
	}

	private void acceptLoop(int port, BoundListener listener) {
		while (listener.running().get()) {
			try {
				Socket socket = listener.socket().accept();
				AccessLogger.Connection access = this.accessLogger.accepted("tcp:" + port, socket).transport("tcp");
				ConnectionLimits.Admission admission = this.limits.admit(access);
				if (admission == null) {
					// at the limit: closed unread, before any thread is spent on it
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
					log.debug("dropped an accepted connection on tcp route port {}: {}", port, e.toString());
					admission.release();
					this.close(socket);
					continue;
				}
				try {
					this.taskExecutor.execute(() -> this.relay(socket, port, access, admission));
				}
				catch (RuntimeException e) {
					admission.release();
					this.close(socket);
					throw e;
				}
			}
			catch (Exception e) {
				if (listener.running().get()) {
					log.warn("accept failed on tcp route port {}: {}", port, e.toString());
				}
			}
		}
	}

	/**
	 * Relays one admitted connection. The admission is released exactly once: here on
	 * every path that does not start the relay, otherwise once the relay completed and
	 * the frames its virtual connection queued left the tunnel sender.
	 */
	private void relay(Socket socket, int port, AccessLogger.Connection access, ConnectionLimits.Admission admission) {
		boolean relaying = false;
		try {
			InputStream source = socket.getInputStream();
			InetAddress peer = peerOf(socket);
			if (this.properties.proxyProtocol()) {
				// the header is consumed here and never relayed: the relay starts past
				// it, and headerless connections pass untouched. The parse is the only
				// head phase of a tcp route connection, so it alone has the deadline
				admission.startHead(socket);
				ProxyProtocolParser.Result proxied = new ProxyProtocolParser().parse(source);
				source = proxied.input();
				ProxyProtocolParser.Header header = proxied.header();
				if (header != null && header.source() != null) {
					peer = header.source();
					access.remote(peer.getHostAddress() + ":" + header.sourcePort());
				}
				if (!admission.endHead()) {
					this.close(socket);
					return;
				}
			}
			if (!admission.claim(peer)) {
				this.close(socket);
				return;
			}
			access.accept();
			Optional<Router.Route> route = this.router.lookupByPort(port);
			if (!this.accessControl.allowed(route.orElse(null), peer)) {
				this.close(socket);
				access.close();
				return;
			}
			TunnelSession session = route.map(r -> this.sessions.find(r.clientId()).orElse(null)).orElse(null);
			if (session == null) {
				this.close(socket);
				access.close();
				return;
			}
			Router.Route route0 = route.get();
			VirtualConnection connection = session.open(route0.address());
			access.route(route0.routeTag()).connectionId(connection.connectionId());
			AtomicInteger active = this.activeConnections.computeIfAbsent(port, p -> new AtomicInteger());
			ConnectionLimits.KeepAliveDeadline deadline = this.limits.keepAliveDeadline(socket);
			// reads continue from the proxy parser's position: past the consumed header,
			// or (no header present) from the first byte the probe pushed back
			StreamRelay relay = StreamRelay
				.builder(ProgressPipe.of(DuplexPipe.of(source, socket), deadline::progress), connection,
						session.sender())
				.listener(this.relayedBytes(route0, session, access))
				.onComplete(() -> {
					deadline.disarm();
					active.decrementAndGet();
					access.close();
					session.remove(connection.connectionId());
					// the slot covers what the connection still queues for the tunnel
					connection.afterQueued(admission::release);
				})
				.build();
			deadline.onExpiry(relay::abandon);
			deadline.relayVerbatim(route0);
			active.incrementAndGet();
			try {
				relay.start();
				relaying = true;
			}
			catch (RuntimeException e) {
				deadline.disarm();
				active.decrementAndGet();
				throw e;
			}
		}
		catch (Exception e) {
			log.debug("tcp route connection failed on port {}: {}", port,
					admission.expired() ? "head timeout" : e.toString());
			access.close();
			this.close(socket);
		}
		finally {
			if (!relaying) {
				admission.release();
			}
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
	 * Whether the given port currently has a bound listener.
	 */
	/**
	 * Currently bound tcp route listeners by listen port, ordered by port.
	 */
	public SortedMap<Integer, Binding> bindings() {
		SortedMap<Integer, Binding> bindings = new TreeMap<>();
		this.listeners.forEach((port, listener) -> {
			AtomicInteger active = this.activeConnections.get(port);
			bindings.put(port, new Binding(listener.clientId(), active == null ? 0 : active.get()));
		});
		return Collections.unmodifiableSortedMap(bindings);
	}

	public boolean isBound(int port) {
		return this.listeners.containsKey(port);
	}

	@Override
	public void close() {
		for (BoundListener listener : this.listeners.values()) {
			listener.close();
		}
		this.listeners.clear();
	}

	/**
	 * Listeners come and go with the advertise lifecycle, so start only marks the gateway
	 * running; {@link #stop()} releases every bound port (also invoked at drain start by
	 * the cluster lifecycle).
	 */
	@Override
	public void start() {
		this.running = true;
	}

	@Override
	public void beginDrain() {
		stop();
	}

	@Override
	public void stop() {
		this.running = false;
		this.close();
	}

	@Override
	public boolean isRunning() {
		return this.running;
	}

}
