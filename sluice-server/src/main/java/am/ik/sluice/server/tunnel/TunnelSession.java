package am.ik.sluice.server.tunnel;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import am.ik.sluice.server.route.Router;
import am.ik.sluice.tunnel.FrameWriter;
import am.ik.sluice.tunnel.SessionSender;
import am.ik.sluice.tunnel.VirtualConnection;
import am.ik.sluice.v1.proto.Frame;
import io.grpc.Status;

/**
 * State of one connected tunnel client: the multiplexed virtual connections and the
 * router registration.
 */
public final class TunnelSession implements AutoCloseable {

	private static final Logger log = LoggerFactory.getLogger(TunnelSession.class);

	private final String clientId;

	private final Router router;

	private final SessionSender sender;

	private final AtomicLong sequence = new AtomicLong(1);

	private final ConcurrentMap<Long, VirtualConnection> connections = new ConcurrentHashMap<>();

	private volatile boolean closed;

	TunnelSession(String clientId, Router router, SessionSender sender, SessionRegistry registry) {
		this.clientId = clientId;
		this.router = router;
		this.sender = sender;
		registry.register(this);
	}

	String clientId() {
		return this.clientId;
	}

	public FrameWriter sender() {
		return this.sender;
	}

	void start() {
		this.sender.start();
	}

	/**
	 * Handles one frame received from the client.
	 */
	void handle(Frame frame) {
		switch (frame.getType()) {
			case ADVERTISE -> {
				int registered = this.router.register(this.clientId, frame.getUpstreamsList());
				log.info("client {} advertised {} upstream(s)", this.clientId, registered);
			}
			case DATA -> {
				VirtualConnection connection = this.connections.get(frame.getConnId());
				if (connection != null) {
					connection.acceptData(frame.getPayload().toByteArray());
				}
			}
			case CLOSE -> {
				VirtualConnection connection = this.connections.get(frame.getConnId());
				if (connection != null) {
					connection.remoteClosed();
				}
			}
			case ERROR -> {
				VirtualConnection connection = this.connections.get(frame.getConnId());
				if (connection != null) {
					connection.remoteFailed(frame.getMessage());
				}
			}
			case CONNECT -> log.warn("unexpected CONNECT from client {}", this.clientId);
			case KEEPALIVE -> {
				// liveness no-op
			}
			default -> {
				// unknown future type: ignore
			}
		}
	}

	/**
	 * Opens a virtual connection to the given dial address and sends the CONNECT request
	 * to the client.
	 */
	public VirtualConnection open(String address) {
		long connectionId = this.sequence.getAndIncrement();
		VirtualConnection connection = new VirtualConnection(connectionId, this.sender);
		this.connections.put(connectionId, connection);
		this.sender.sendConnect(connectionId, address);
		return connection;
	}

	public void remove(long connectionId) {
		this.connections.remove(connectionId);
	}

	@Override
	public void close() {
		if (this.closed) {
			return;
		}
		this.closed = true;
		log.info("closing session for client {}", this.clientId);
		this.router.remove(this.clientId);
		for (VirtualConnection connection : List.copyOf(this.connections.values())) {
			connection.remoteFailed("tunnel session closed");
			connection.close();
		}
		this.connections.clear();
		this.sender.close();
	}

}
