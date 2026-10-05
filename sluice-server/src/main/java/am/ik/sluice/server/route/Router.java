package am.ik.sluice.server.route;

import org.jspecify.annotations.Nullable;

import java.util.Optional;

import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import am.ik.sluice.v1.proto.Upstream;

/**
 * Route table mapping public domains to client upstreams (the port of inlets'
 * pkg/router). Lookup falls back to the catch-all entry registered with an empty domain
 * and always returns the first registered target; no load balancing is performed,
 * mirroring the original behavior.
 */
@Component
public class Router {

	/**
	 * Resolved route: the client owning the upstream, its dial address, and whether the
	 * request Host header passes through unmodified.
	 */
	public record Route(String clientId, String address, boolean preserveHost) {
	}

	record Target(String clientId, String domain, String address, boolean preserveHost) {
	}

	private final ConcurrentMap<String, List<Target>> byDomain = new ConcurrentHashMap<>();

	private final ConcurrentMap<String, List<Target>> byClient = new ConcurrentHashMap<>();

	private final Object lock = new Object();

	/**
	 * (Re-)registers the upstreams announced by a client. Any previous entry for the
	 * client is replaced, so re-announcement after reconnect is idempotent.
	 * @return the number of registered targets, {@code 0} when nothing was registered
	 */
	public int register(String clientId, List<Upstream> upstreams) {
		List<Target> targets = new ArrayList<>();
		for (Upstream upstream : upstreams) {
			String domain = upstream.getHost();
			String address = addressOf(upstream.getTargetUrl()).orElse(null);
			if (address == null) {
				continue;
			}
			targets.add(new Target(clientId, domain, address, upstream.getPreserveHost()));
		}
		if (clientId == null || clientId.isBlank() || targets.isEmpty()) {
			return 0;
		}
		synchronized (this.lock) {
			removeLocked(clientId);
			for (Target target : targets) {
				this.byDomain.compute(target.domain(), (k, existing) -> append(existing, target));
			}
			this.byClient.put(clientId, List.copyOf(targets));
		}
		return targets.size();
	}

	/**
	 * Removes every route registered by the client.
	 */
	public void remove(String clientId) {
		synchronized (this.lock) {
			removeLocked(clientId);
		}
	}

	private void removeLocked(String clientId) {
		List<Target> old = this.byClient.remove(clientId);
		if (old == null) {
			return;
		}
		for (Target target : old) {
			this.byDomain.compute(target.domain(), (k, existing) -> without(existing, clientId));
		}
	}

	private static List<Target> append(List<Target> existing, Target target) {
		List<Target> copy = existing == null ? new ArrayList<>() : new ArrayList<>(existing);
		copy.add(target);
		return List.copyOf(copy);
	}

	private static List<Target> without(List<Target> existing, String clientId) {
		if (existing == null) {
			return List.of();
		}
		List<Target> copy = new ArrayList<>();
		for (Target target : existing) {
			if (!target.clientId().equals(clientId)) {
				copy.add(target);
			}
		}
		return List.copyOf(copy);
	}

	/**
	 * Resolves the route for the given Host header value. Exact match first, then the
	 * host without its port, then the catch-all entry.
	 */
	public Optional<Route> lookup(@Nullable String host) {
		for (String candidate : candidates(host)) {
			List<Target> targets = this.byDomain.get(candidate);
			if (targets != null && !targets.isEmpty()) {
				Target target = targets.get(0);
				return Optional.of(new Route(target.clientId(), target.address(), target.preserveHost()));
			}
		}
		return Optional.empty();
	}

	private static List<String> candidates(@Nullable String host) {
		if (host == null || host.isBlank()) {
			return List.of("");
		}
		int portSeparator = host.lastIndexOf(':');
		boolean hasPort = portSeparator > host.lastIndexOf(']'); // not an IPv6 suffix
		return hasPort ? List.of(host, host.substring(0, portSeparator), "") : List.of(host, "");
	}

	/**
	 * Number of clients with at least one route.
	 */
	public int clientCount() {
		return this.byClient.size();
	}

	/**
	 * Extracts {@code host[:port]} from an upstream URL; {@code null} when unparseable.
	 */
	static Optional<String> addressOf(String targetUrl) {
		if (targetUrl == null || targetUrl.isBlank()) {
			return Optional.empty();
		}
		try {
			URI uri = new URI(targetUrl.trim());
			String host = uri.getHost();
			if (host == null) {
				return Optional.empty();
			}
			int port = uri.getPort();
			return Optional.of(port > 0 ? host + ":" + port : host);
		}
		catch (URISyntaxException e) {
			return Optional.empty();
		}
	}

}
