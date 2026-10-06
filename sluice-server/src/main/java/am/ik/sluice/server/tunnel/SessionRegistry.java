package am.ik.sluice.server.tunnel;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.springframework.stereotype.Component;

/**
 * Registry of the currently connected tunnel sessions, keyed by client id.
 */
@Component
public class SessionRegistry {

	private final ConcurrentMap<String, TunnelSession> sessions = new ConcurrentHashMap<>();

	public Optional<TunnelSession> find(String clientId) {
		return Optional.ofNullable(this.sessions.get(clientId));
	}

	void register(TunnelSession session) {
		TunnelSession existing = this.sessions.put(session.clientId(), session);
		if (existing != null && existing != session) {
			existing.close();
		}
	}

	public void remove(TunnelSession session) {
		this.sessions.remove(session.clientId(), session);
	}

	public int count() {
		return this.sessions.size();
	}

	public List<TunnelSession> all() {
		return List.copyOf(this.sessions.values());
	}

}
