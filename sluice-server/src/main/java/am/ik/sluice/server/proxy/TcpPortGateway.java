package am.ik.sluice.server.proxy;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jspecify.annotations.Nullable;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Component;

import am.ik.sluice.server.config.SluiceServerProperties;
import am.ik.sluice.server.config.TcpPortRange;
import am.ik.sluice.server.route.Router;
import am.ik.sluice.server.tunnel.SessionRegistry;
import am.ik.sluice.server.tunnel.TcpRouteListener;
import am.ik.sluice.server.tunnel.TunnelSession;
import am.ik.sluice.tunnel.DuplexPipe;
import am.ik.sluice.tunnel.StreamRelay;
import am.ik.sluice.tunnel.VirtualConnection;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Data plane for tcp port routes: one TCP listener per port advertised by a client
 * ({@code listen-port} of an upstream), relaying every accepted connection verbatim -- no
 * head parsing, no rewriting -- over a virtual connection of the owning session.
 * Listeners are bound on advertise and released on session end; in-flight relays survive
 * an unbind because each runs on its own accepted socket.
 */
@Component
public class TcpPortGateway implements TcpRouteListener, AutoCloseable {

	private static final Logger log = LoggerFactory.getLogger(TcpPortGateway.class);

	private final Router router;

	private final SessionRegistry sessions;

	private final SluiceServerProperties properties;

	private final TaskExecutor taskExecutor;

	private static final String METRIC_NAME = "sluice.tunnel.bytes";

	private final MeterRegistry meterRegistry;

	private final AccessLogger accessLogger;

	private final ConcurrentMap<Integer, BoundListener> listeners = new ConcurrentHashMap<>();

	private final TcpPortRange tcpPortRange;

	TcpPortGateway(Router router, SessionRegistry sessions, SluiceServerProperties properties,
			@Qualifier("applicationTaskExecutor") TaskExecutor taskExecutor, MeterRegistry meterRegistry,
			AccessLogger accessLogger) {
		this.router = router;
		this.sessions = sessions;
		this.properties = properties;
		this.tcpPortRange = TcpPortRange.parse(properties.tcpPortRange());
		this.taskExecutor = taskExecutor;
		this.meterRegistry = meterRegistry;
		this.accessLogger = accessLogger;
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
				log.info("unbound tcp route port {} (client {})", port, clientId);
			}
		}
		Set<Integer> rejected = new TreeSet<>();
		for (Integer port : listenPorts) {
			if (!this.tcpPortRange.contains(port)) {
				log.warn("tcp route port {} is outside tcp-port-range ({}); rejecting for client {}", port,
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
					log.warn("tcp route port {} is owned by client {}; rejecting for client {}", port,
							existing.clientId(), clientId);
					rejected.add(port);
					continue;
				}
				log.info("tcp route port {} taken over from client {} by client {}", port, existing.clientId(),
						clientId);
				this.listeners.remove(port, existing);
				existing.close();
			}
			if (!this.bind(clientId, port)) {
				rejected.add(port);
			}
		}
		return Collections.unmodifiableSet(rejected);
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
				log.info("bound tcp route port {} for client {}", port, clientId);
				return true;
			}
			catch (Exception e) {
				if (attempt >= 2) {
					// a bind failure must not tear down the session; the port is reported
					// back
					log.warn("failed to bind tcp route port {} for client {}: {}", port, clientId, e.toString());
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
				socket.setTcpNoDelay(true);
				this.taskExecutor.execute(() -> this.relay(socket, port));
			}
			catch (Exception e) {
				if (listener.running().get()) {
					log.warn("accept failed on tcp route port {}: {}", port, e.toString());
				}
			}
		}
	}

	private void relay(Socket socket, int port) {
		AccessLogger.Connection access = this.accessLogger.accepted("tcp:" + port, socket).transport("tcp");
		access.accept();
		try {
			Optional<Router.Route> route = this.router.lookupByPort(port);
			TunnelSession session = route.map(r -> this.sessions.find(r.clientId()).orElse(null)).orElse(null);
			if (session == null) {
				this.close(socket);
				access.close();
				return;
			}
			Router.Route route0 = route.get();
			VirtualConnection connection = session.open(route0.address());
			access.route(route0.routeTag()).connectionId(connection.connectionId());
			StreamRelay relay = StreamRelay.builder(DuplexPipe.of(socket), connection, session.sender())
				.listener(this.relayedBytes(route0, access))
				.onComplete(() -> {
					access.close();
					session.remove(connection.connectionId());
				})
				.build();
			relay.start();
		}
		catch (Exception e) {
			log.debug("tcp route connection failed on port {}: {}", port, e.toString());
			access.close();
			this.close(socket);
		}
	}

	private StreamRelay.Listener relayedBytes(Router.Route route, AccessLogger.Connection access) {
		Counter counter = this.meterRegistry.counter(METRIC_NAME, "direction", "data", "route", route.routeTag());
		return new StreamRelay.Listener() {

			@Override
			public void onBytesRelayed(long count) {
			}

			@Override
			public void onBytesRelayed(long count, StreamRelay.Direction direction) {
				counter.increment();
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
	 * Whether the given port currently has a bound listener.
	 */
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

}
