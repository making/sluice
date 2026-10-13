package am.ik.sluice.server.tunnel;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.springframework.stereotype.Component;

/**
 * Registry of the currently connected tunnel sessions, keyed by client id, and of the
 * streams still waiting for their client's first {@code Advertise}.
 */
@Component
public class SessionRegistry {

	private final ConcurrentMap<String, TunnelSession> sessions = new ConcurrentHashMap<>();

	private final Set<TunnelSession> pending = ConcurrentHashMap.newKeySet();

	/** The session serving the client id: the latest one that advertised. */
	public Optional<TunnelSession> find(String clientId) {
		return Optional.ofNullable(this.sessions.get(clientId));
	}

	/** Tracks a session whose client has not advertised yet. */
	void admit(TunnelSession session) {
		this.pending.add(session);
	}

	/**
	 * Makes the session its client id's session; returns the one it replaced, which the
	 * caller closes.
	 */
	Optional<TunnelSession> register(TunnelSession session) {
		this.pending.remove(session);
		TunnelSession existing = this.sessions.put(session.clientId(), session);
		return existing == session ? Optional.empty() : Optional.ofNullable(existing);
	}

	public void remove(TunnelSession session) {
		this.pending.remove(session);
		this.sessions.remove(session.clientId(), session);
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
		return List.copyOf(this.pending);
	}

}
