package am.ik.sluice.server.tunnel;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.springframework.stereotype.Component;

import am.ik.sluice.server.config.SluiceServerProperties;
import io.grpc.Status;

/**
 * Registry of the currently connected tunnel sessions, keyed by client id, and of the
 * streams still waiting for their client's first {@code Advertise} (pending).
 * <p>
 * The pending streams are bounded: at most
 * {@code sluice.limits.max-pending-tunnel-streams} at once, and at most
 * {@code sluice.limits.max-pending-tunnel-streams-per-address} from one source address
 * when that is set. A stream beyond a bound is refused at {@link #admit}; a pending
 * stream frees its place once it is registered or removed. Once the drain began
 * ({@link #beginDrain()}) every stream is refused, so no session appears after the drain
 * took the pending ones.
 */
@Component
public class SessionRegistry {

	/** Why {@link #admit} refused a session; the stream is failed with its status. */
	public enum Refusal {

		/** The node is draining: no session appears after the drain began. */
		DRAINING("draining", Status.UNAVAILABLE.withDescription("node is draining")),

		/** {@code sluice.limits.max-pending-tunnel-streams} streams are pending. */
		LIMIT("limit",
				Status.RESOURCE_EXHAUSTED
					.withDescription("too many tunnel streams are waiting for their client's first Advertise")),

		/**
		 * {@code sluice.limits.max-pending-tunnel-streams-per-address} streams from the
		 * source address are pending.
		 */
		ADDRESS_LIMIT("address-limit", Status.RESOURCE_EXHAUSTED.withDescription(
				"too many tunnel streams from this address are waiting for their client's first Advertise"));

		private final String reason;

		private final Status status;

		Refusal(String reason, Status status) {
			this.reason = reason;
			this.status = status;
		}

		/** The {@code reason} tag of the refusal meter. */
		public String reason() {
			return this.reason;
		}

		/** The status the refused stream is failed with. */
		public Status status() {
			return this.status;
		}

	}

	private final ConcurrentMap<String, TunnelSession> sessions = new ConcurrentHashMap<>();

	private final int maxPending;

	/** {@code 0} = no limit per address beyond {@link #maxPending}. */
	private final int maxPendingPerAddress;

	/** Guards {@link #pending}, {@link #pendingPerAddress} and {@link #draining}. */
	private final Object pendingLock = new Object();

	/** Set by {@link #beginDrain()}; no session is admitted once it is. */
	private boolean draining;

	private final Set<TunnelSession> pending = new LinkedHashSet<>();

	/** Pending streams per source address; an entry lives while its count is > 0. */
	private final Map<String, Integer> pendingPerAddress = new HashMap<>();

	public SessionRegistry(SluiceServerProperties properties) {
		this.maxPending = properties.limits().maxPendingTunnelStreams();
		this.maxPendingPerAddress = properties.limits().maxPendingTunnelStreamsPerAddress();
	}

	/** The session serving the client id: the latest one that advertised. */
	public Optional<TunnelSession> find(String clientId) {
		return Optional.ofNullable(this.sessions.get(clientId));
	}

	/**
	 * Tracks a session whose client has not advertised yet, unless the drain began or a
	 * bound on the pending streams is reached: then the session is not tracked and the
	 * refusal is returned. A session without a source address is bounded only by the
	 * global limit.
	 */
	Optional<Refusal> admit(TunnelSession session) {
		String source = session.sourceAddress();
		synchronized (this.pendingLock) {
			if (this.draining) {
				return Optional.of(Refusal.DRAINING);
			}
			if (this.pending.size() >= this.maxPending) {
				return Optional.of(Refusal.LIMIT);
			}
			if (this.maxPendingPerAddress > 0 && !source.isEmpty()
					&& this.pendingPerAddress.getOrDefault(source, 0) >= this.maxPendingPerAddress) {
				return Optional.of(Refusal.ADDRESS_LIMIT);
			}
			if (this.pending.add(session) && !source.isEmpty()) {
				this.pendingPerAddress.merge(source, 1, Integer::sum);
			}
			return Optional.empty();
		}
	}

	/**
	 * Makes the session its client id's session; returns the one it replaced, which the
	 * caller closes.
	 */
	Optional<TunnelSession> register(TunnelSession session) {
		release(session);
		TunnelSession existing = this.sessions.put(session.clientId(), session);
		return existing == session ? Optional.empty() : Optional.ofNullable(existing);
	}

	public void remove(TunnelSession session) {
		release(session);
		this.sessions.remove(session.clientId(), session);
	}

	/** Frees the pending place of the session, if it holds one. */
	private void release(TunnelSession session) {
		String source = session.sourceAddress();
		synchronized (this.pendingLock) {
			if (this.pending.remove(session) && !source.isEmpty()) {
				this.pendingPerAddress.computeIfPresent(source, (key, count) -> count <= 1 ? null : count - 1);
			}
		}
	}

	/** Number of registered sessions; pending ones do not count. */
	public int count() {
		return this.sessions.size();
	}

	/** The registered sessions; pending ones are not among them. */
	public List<TunnelSession> all() {
		return List.copyOf(this.sessions.values());
	}

	/** Sessions whose client has not advertised yet; they serve no connection. */
	public List<TunnelSession> pending() {
		synchronized (this.pendingLock) {
			return List.copyOf(this.pending);
		}
	}

	/**
	 * Begins the drain: from now on {@link #admit} refuses every session
	 * ({@link Refusal#DRAINING}). Returns the pending sessions, which the caller closes;
	 * since the refusal and the snapshot are one step with the admission, every session
	 * ever admitted is among them or registered before. Idempotent.
	 */
	List<TunnelSession> beginDrain() {
		synchronized (this.pendingLock) {
			this.draining = true;
			return List.copyOf(this.pending);
		}
	}

	/** Pending streams from {@code source}. */
	int pending(String source) {
		synchronized (this.pendingLock) {
			return this.pendingPerAddress.getOrDefault(source, 0);
		}
	}

	/** Source addresses with a pending stream; entries never outlive their count. */
	int trackedAddresses() {
		synchronized (this.pendingLock) {
			return this.pendingPerAddress.size();
		}
	}

}
