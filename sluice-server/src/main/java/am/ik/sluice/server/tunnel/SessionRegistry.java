package am.ik.sluice.server.tunnel;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.jspecify.annotations.Nullable;
import org.springframework.stereotype.Component;

import am.ik.sluice.server.config.SluiceServerProperties;
import io.grpc.Status;

/**
 * Registry of the currently connected tunnel sessions, keyed by client id, and of the
 * streams still waiting for their client's first {@code Advertise} (pending).
 * <p>
 * The registered sessions are bounded: at most {@code sluice.limits.max-tunnel-sessions},
 * and at most {@code sluice.limits.max-tunnel-sessions-per-address} from one source
 * address when that is set. A session replacing its client id's registered one takes no
 * further place (none from its address when the replaced one has the same address). The
 * registration decides ({@link #register}); {@link #admit} already refuses a stream that
 * could not register at that moment. A session frees its place once it is removed or
 * replaced.
 * <p>
 * The pending streams are bounded too: at most
 * {@code sluice.limits.max-pending-tunnel-streams} at once, and at most
 * {@code sluice.limits.max-pending-tunnel-streams-per-address} from one source address
 * when that is set. A stream beyond a bound is refused at {@link #admit}; a pending
 * stream frees its place once it is registered, refused at its registration or removed.
 * Once the drain began ({@link #beginDrain()}) every stream is refused, so no session
 * appears after the drain took the pending ones.
 */
@Component
public class SessionRegistry {

	/** Why a session was refused; the stream is failed with its status. */
	public enum Refusal {

		/** The node is draining: no session appears after the drain began. */
		DRAINING("draining", Status.UNAVAILABLE.withDescription("node is draining")),

		/** {@code sluice.limits.max-tunnel-sessions} sessions are registered. */
		SESSION_LIMIT("session-limit",
				Status.RESOURCE_EXHAUSTED.withDescription("too many tunnel clients are connected to this node")),

		/**
		 * {@code sluice.limits.max-tunnel-sessions-per-address} sessions from the source
		 * address are registered.
		 */
		SESSION_ADDRESS_LIMIT("session-address-limit",
				Status.RESOURCE_EXHAUSTED
					.withDescription("too many tunnel clients from this address are connected to this node")),

		/** {@code sluice.limits.max-pending-tunnel-streams} streams are pending. */
		PENDING_LIMIT("pending-limit",
				Status.RESOURCE_EXHAUSTED
					.withDescription("too many tunnel streams are waiting for their client's first Advertise")),

		/**
		 * {@code sluice.limits.max-pending-tunnel-streams-per-address} streams from the
		 * source address are pending.
		 */
		PENDING_ADDRESS_LIMIT("pending-address-limit", Status.RESOURCE_EXHAUSTED.withDescription(
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

	/** The outcome of {@link #register}. */
	sealed interface Registration {

		/**
		 * The session is its client id's session; {@code replaced} is the one it
		 * replaced, which the caller closes.
		 */
		record Registered(Optional<TunnelSession> replaced) implements Registration {
		}

		/** The session is not registered; the caller fails its stream. */
		record Refused(Refusal refusal) implements Registration {
		}

	}

	/** Written under {@link #lock}; read without it by {@link #find} and the like. */
	private final ConcurrentMap<String, TunnelSession> sessions = new ConcurrentHashMap<>();

	private final int maxSessions;

	/** {@code 0} = no limit per address beyond {@link #maxSessions}. */
	private final int maxSessionsPerAddress;

	private final int maxPending;

	/** {@code 0} = no limit per address beyond {@link #maxPending}. */
	private final int maxPendingPerAddress;

	/**
	 * Guards the writes to {@link #sessions}, and {@link #sessionsPerAddress},
	 * {@link #pending}, {@link #pendingPerAddress} and {@link #draining}.
	 */
	private final Object lock = new Object();

	/** Registered sessions per source address; an entry lives while its count is > 0. */
	private final Map<String, Integer> sessionsPerAddress = new HashMap<>();

	/** Set by {@link #beginDrain()}; no session is admitted once it is. */
	private boolean draining;

	private final Set<TunnelSession> pending = new LinkedHashSet<>();

	/** Pending streams per source address; an entry lives while its count is > 0. */
	private final Map<String, Integer> pendingPerAddress = new HashMap<>();

	public SessionRegistry(SluiceServerProperties properties) {
		this.maxSessions = properties.limits().maxTunnelSessions();
		this.maxSessionsPerAddress = properties.limits().maxTunnelSessionsPerAddress();
		this.maxPending = properties.limits().maxPendingTunnelStreams();
		this.maxPendingPerAddress = properties.limits().maxPendingTunnelStreamsPerAddress();
	}

	/** The session serving the client id: the latest one that advertised. */
	public Optional<TunnelSession> find(String clientId) {
		return Optional.ofNullable(this.sessions.get(clientId));
	}

	/**
	 * Tracks a session whose client has not advertised yet, unless the drain began, the
	 * session could not register now ({@link #register}), or a bound on the pending
	 * streams is reached: then the session is not tracked and the refusal is returned. A
	 * session without a source address is bounded only by the global limits.
	 */
	Optional<Refusal> admit(TunnelSession session) {
		String source = session.sourceAddress();
		synchronized (this.lock) {
			if (this.draining) {
				return Optional.of(Refusal.DRAINING);
			}
			Optional<Refusal> sessionRefusal = sessionRefusal(session);
			if (sessionRefusal.isPresent()) {
				return sessionRefusal;
			}
			if (this.pending.size() >= this.maxPending) {
				return Optional.of(Refusal.PENDING_LIMIT);
			}
			if (this.maxPendingPerAddress > 0 && !source.isEmpty()
					&& this.pendingPerAddress.getOrDefault(source, 0) >= this.maxPendingPerAddress) {
				return Optional.of(Refusal.PENDING_ADDRESS_LIMIT);
			}
			if (this.pending.add(session)) {
				increment(this.pendingPerAddress, source);
			}
			return Optional.empty();
		}
	}

	/**
	 * Makes the session its client id's session, replacing the registered one, unless
	 * that would pass a bound on the registered sessions. Either way the session no
	 * longer holds a pending place.
	 */
	Registration register(TunnelSession session) {
		synchronized (this.lock) {
			release(session);
			Optional<Refusal> refusal = sessionRefusal(session);
			if (refusal.isPresent()) {
				return new Registration.Refused(refusal.get());
			}
			TunnelSession existing = this.sessions.put(session.clientId(), session);
			if (existing == session) {
				return new Registration.Registered(Optional.empty());
			}
			if (existing != null) {
				decrement(this.sessionsPerAddress, existing.sourceAddress());
			}
			increment(this.sessionsPerAddress, session.sourceAddress());
			return new Registration.Registered(Optional.ofNullable(existing));
		}
	}

	/**
	 * Why the session could not register now: past a bound once it replaced its client
	 * id's registered session, if any.
	 */
	private Optional<Refusal> sessionRefusal(TunnelSession session) {
		@Nullable TunnelSession existing = this.sessions.get(session.clientId());
		if (existing == session) {
			return Optional.empty();
		}
		if (existing == null && this.sessions.size() >= this.maxSessions) {
			return Optional.of(Refusal.SESSION_LIMIT);
		}
		String source = session.sourceAddress();
		boolean replacesFromSameAddress = existing != null && existing.sourceAddress().equals(source);
		if (this.maxSessionsPerAddress > 0 && !source.isEmpty() && !replacesFromSameAddress
				&& this.sessionsPerAddress.getOrDefault(source, 0) >= this.maxSessionsPerAddress) {
			return Optional.of(Refusal.SESSION_ADDRESS_LIMIT);
		}
		return Optional.empty();
	}

	public void remove(TunnelSession session) {
		synchronized (this.lock) {
			release(session);
			if (this.sessions.remove(session.clientId(), session)) {
				decrement(this.sessionsPerAddress, session.sourceAddress());
			}
		}
	}

	/** Frees the pending place of the session, if it holds one; under {@link #lock}. */
	private void release(TunnelSession session) {
		if (this.pending.remove(session)) {
			decrement(this.pendingPerAddress, session.sourceAddress());
		}
	}

	private static void increment(Map<String, Integer> counts, String source) {
		if (!source.isEmpty()) {
			counts.merge(source, 1, Integer::sum);
		}
	}

	private static void decrement(Map<String, Integer> counts, String source) {
		if (!source.isEmpty()) {
			counts.computeIfPresent(source, (key, count) -> count <= 1 ? null : count - 1);
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
		synchronized (this.lock) {
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
		synchronized (this.lock) {
			this.draining = true;
			return List.copyOf(this.pending);
		}
	}

	/** Pending streams from {@code source}. */
	int pending(String source) {
		synchronized (this.lock) {
			return this.pendingPerAddress.getOrDefault(source, 0);
		}
	}

	/** Source addresses with a pending stream; entries never outlive their count. */
	int trackedAddresses() {
		synchronized (this.lock) {
			return this.pendingPerAddress.size();
		}
	}

	/** Registered sessions from {@code source}. */
	int sessions(String source) {
		synchronized (this.lock) {
			return this.sessionsPerAddress.getOrDefault(source, 0);
		}
	}

	/** Source addresses with a registered session; entries never outlive their count. */
	int trackedSessionAddresses() {
		synchronized (this.lock) {
			return this.sessionsPerAddress.size();
		}
	}

}
