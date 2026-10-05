package am.ik.sluice.server.proxy;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import am.ik.sluice.server.config.SluiceServerProperties;
import am.ik.sluice.tunnel.StreamRelay;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.stereotype.Component;

/**
 * Connection and (head) request access log for the data plane, emitted to the
 * {@code sluice.access} logger as single-line key=value pairs; disable via
 * {@code sluice.access-log.enabled=false}. Byte totals are accumulated per direction and
 * the {@code close} line is emitted when the relay completes, so the counts are final.
 */
@Component
public class AccessLogger {

	private static final Logger log = LoggerFactory.getLogger("sluice.access");

	private final AtomicInteger sequence = new AtomicInteger();

	private final boolean enabled;

	private final boolean connEnabled;

	private final boolean requestEnabled;

	AccessLogger(SluiceServerProperties properties) {
		SluiceServerProperties.AccessLog accessLog = properties.accessLog();
		this.enabled = accessLog.enabled();
		Set<SluiceServerProperties.AccessLog.Type> types = accessLog.types();
		this.connEnabled = types.contains(SluiceServerProperties.AccessLog.Type.CONNECTION);
		this.requestEnabled = types.contains(SluiceServerProperties.AccessLog.Type.REQUEST);
	}

	/**
	 * Begins tracking an accepted connection; returns a no-op instance when disabled.
	 */
	public Connection accepted(String listener, Socket socket) {
		if (!this.enabled || (!this.connEnabled && !this.requestEnabled)) {
			return Connection.NOOP;
		}
		return new Connection(this.sequence.incrementAndGet(), listener, peerOf(socket), false, this.connEnabled,
				this.requestEnabled);
	}

	private static String peerOf(Socket socket) {
		try {
			if (socket.getRemoteSocketAddress() instanceof InetSocketAddress address) {
				return address.getHostString() + ":" + address.getPort();
			}
			return String.valueOf(socket.getRemoteSocketAddress());
		}
		catch (Exception e) {
			return "-";
		}
	}

	/**
	 * The mutable per-connection state; all lifecycle methods are safe to call from any
	 * relay thread, and the close line is emitted once.
	 */
	public static final class Connection {

		private static final Connection NOOP = new Connection(0, "-", "-", true, false, false);

		private final int id;

		private final String listener;

		private final String remote;

		private final boolean noop;

		private final boolean connEnabled;

		private final boolean requestEnabled;

		private final long startNanos = System.nanoTime();

		private final AtomicLong bytesIn = new AtomicLong();

		private final AtomicLong bytesOut = new AtomicLong();

		private String route = "-";

		private String transport = "-";

		private long connectionId = -1;

		private volatile boolean closed;

		private Connection(int id, String listener, String remote, boolean noop, boolean connEnabled,
				boolean requestEnabled) {
			this.id = id;
			this.listener = listener;
			this.remote = remote;
			this.noop = noop;
			this.connEnabled = connEnabled;
			this.requestEnabled = requestEnabled;
		}

		Connection transport(String transport) {
			this.transport = transport;
			return this;
		}

		Connection route(String routeTag) {
			this.route = routeTag;
			return this;
		}

		Connection connectionId(long connectionId) {
			this.connectionId = connectionId;
			return this;
		}

		public void bytes(long count, StreamRelay.Direction direction) {
			(direction == StreamRelay.Direction.TO_REMOTE ? this.bytesIn : this.bytesOut).addAndGet(count);
		}

		/**
		 * Logs the head request of the connection; later requests on a keep-alive
		 * connection are not parsed and not logged.
		 */
		public void request(String method, String path, String version) {
			if (!this.requestEnabled || this.noop || this.closed) {
				return;
			}
			log.info("type=req id={} route={} method={} path={} http={} remote={}", this.id, this.route, method, path,
					version, this.remote);
		}

		public void close() {
			if (!this.connEnabled || this.noop || this.closed) {
				return;
			}
			this.closed = true;
			log.info(
					"type=conn id={} event=close listener={} route={} transport={} remote={} connId={} bytesIn={} bytesOut={} durationMs={}",
					this.id, this.listener, this.route, this.transport, this.remote,
					this.connectionId < 0 ? "-" : this.connectionId, this.bytesIn.get(), this.bytesOut.get(),
					(System.nanoTime() - this.startNanos) / 1_000_000);
		}

	}

}
