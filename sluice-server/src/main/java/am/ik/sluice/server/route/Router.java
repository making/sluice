package am.ik.sluice.server.route;

import org.jspecify.annotations.Nullable;

import java.util.Optional;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import am.ik.sluice.v1.proto.Upstream;

/**
 * Route table mapping public domains and listen ports to client upstreams (the port of
 * inlets' pkg/router). An upstream with a listen port is a tcp route reached only through
 * that port; its host takes no part in Host / SNI routing. Every other upstream is an
 * http route keyed by its domain, and lookup falls back to the catch-all entry registered
 * with an empty domain. When several clients serve one key the configured load balancing
 * strategy picks the target.
 */
public class Router {

	/**
	 * Resolved route: the client owning the upstream, the domain the route is registered
	 * under, its dial address, whether the request Host header passes through unmodified,
	 * and whether TLS connections are relayed untouched ({@code tls-passthrough}) instead
	 * of terminated on the data plane.
	 */
	public record Route(String clientId, String domain, String address, int listenPort, boolean preserveHost,
			boolean tlsPassthrough) {

		/**
		 * The route identity used for metrics: the domain, falling back to the listen
		 * port (tcp routes may carry no domain) and finally the catch-all marker.
		 */
		public String routeTag() {
			if (!this.domain.isEmpty()) {
				return this.domain;
			}
			return this.listenPort > 0 ? "tcp:" + this.listenPort : "*";
		}

		public static Builder builder() {
			return new Builder();
		}

		public static final class Builder {

			private @Nullable String clientId;

			private @Nullable String domain;

			private @Nullable String address;

			private int listenPort;

			private boolean preserveHost = true;

			private boolean tlsPassthrough;

			private Builder() {
			}

			public Builder clientId(String clientId) {
				this.clientId = clientId;
				return this;
			}

			public Builder domain(String domain) {
				this.domain = domain;
				return this;
			}

			public Builder address(String address) {
				this.address = address;
				return this;
			}

			public Builder listenPort(int listenPort) {
				this.listenPort = listenPort;
				return this;
			}

			public Builder preserveHost(boolean preserveHost) {
				this.preserveHost = preserveHost;
				return this;
			}

			public Builder tlsPassthrough(boolean tlsPassthrough) {
				this.tlsPassthrough = tlsPassthrough;
				return this;
			}

			public Route build() {
				return new Route(Objects.requireNonNull(this.clientId, "clientId is required"),
						Objects.requireNonNull(this.domain, "domain is required"),
						Objects.requireNonNull(this.address, "address is required"), this.listenPort, this.preserveHost,
						this.tlsPassthrough);
			}

		}

	}

	/**
	 * The routes registered under one lookup key.
	 *
	 * @param key the domain ({@code ""} = catch-all) or the listen port of tcp routes
	 * @param candidates every registered route, in registration order
	 * @param preferred the route lookups resolve to when the load balancing is
	 * deterministic; {@code null} when the strategy rotates or randomizes
	 */
	public record RouteGroup(String key, List<Route> candidates, @Nullable Route preferred) {

		public RouteGroup {
			candidates = List.copyOf(candidates);
		}

		public static Builder builder() {
			return new Builder();
		}

		public static final class Builder {

			private @Nullable String key;

			private List<Route> candidates = List.of();

			private @Nullable Route preferred;

			private Builder() {
			}

			public Builder key(String key) {
				this.key = key;
				return this;
			}

			public Builder candidates(List<Route> candidates) {
				this.candidates = candidates;
				return this;
			}

			public Builder preferred(@Nullable Route preferred) {
				this.preferred = preferred;
				return this;
			}

			public RouteGroup build() {
				return new RouteGroup(Objects.requireNonNull(this.key, "key is required"), this.candidates,
						this.preferred);
			}

		}

	}

	record Target(String clientId, String domain, String address, boolean preserveHost, boolean tlsPassthrough,
			int listenPort) {

		static Builder builder() {
			return new Builder();
		}

		static final class Builder {

			private @Nullable String clientId;

			private @Nullable String domain;

			private @Nullable String address;

			private boolean preserveHost = true;

			private boolean tlsPassthrough;

			private int listenPort;

			private Builder() {
			}

			Builder clientId(String clientId) {
				this.clientId = clientId;
				return this;
			}

			Builder domain(String domain) {
				this.domain = domain;
				return this;
			}

			Builder address(String address) {
				this.address = address;
				return this;
			}

			Builder preserveHost(boolean preserveHost) {
				this.preserveHost = preserveHost;
				return this;
			}

			Builder tlsPassthrough(boolean tlsPassthrough) {
				this.tlsPassthrough = tlsPassthrough;
				return this;
			}

			Builder listenPort(int listenPort) {
				this.listenPort = listenPort;
				return this;
			}

			Target build() {
				return new Target(Objects.requireNonNull(this.clientId, "clientId is required"),
						Objects.requireNonNull(this.domain, "domain is required"),
						Objects.requireNonNull(this.address, "address is required"), this.preserveHost,
						this.tlsPassthrough, this.listenPort);
			}

		}

	}

	private final ConcurrentMap<String, List<Target>> byDomain = new ConcurrentHashMap<>();

	private final ConcurrentMap<Integer, List<Target>> byPort = new ConcurrentHashMap<>();

	private final ConcurrentMap<String, List<Target>> byClient = new ConcurrentHashMap<>();

	private final Object lock = new Object();

	private final LoadBalance httpLoadBalance;

	private final LoadBalance tcpLoadBalance;

	private final LoadBalanceStrategy httpStrategy;

	private final LoadBalanceStrategy tcpStrategy;

	/**
	 * The load balancing strategies default to {@link LoadBalance#SMALLEST_CLIENT_ID}
	 * (deterministic across nodes).
	 */
	public Router() {
		this(LoadBalance.SMALLEST_CLIENT_ID, LoadBalance.SMALLEST_CLIENT_ID);
	}

	public Router(LoadBalance httpLoadBalance, LoadBalance tcpLoadBalance) {
		this.httpLoadBalance = httpLoadBalance;
		this.tcpLoadBalance = tcpLoadBalance;
		this.httpStrategy = httpLoadBalance.instance();
		this.tcpStrategy = tcpLoadBalance.instance();
	}

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
			targets.add(Target.builder()
				.clientId(clientId)
				.domain(domain)
				.address(address)
				.preserveHost(upstream.getPreserveHost())
				.tlsPassthrough(upstream.getTlsPassthrough())
				.listenPort(upstream.getListenPort())
				.build());
		}
		if (clientId == null || clientId.isBlank() || targets.isEmpty()) {
			return 0;
		}
		synchronized (this.lock) {
			removeLocked(clientId);
			for (Target target : targets) {
				if (target.listenPort() > 0) {
					this.byPort.compute(target.listenPort(), (k, existing) -> append(existing, target));
				}
				else {
					this.byDomain.compute(target.domain(), (k, existing) -> append(existing, target));
				}
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
			if (target.listenPort() > 0) {
				this.byPort.compute(target.listenPort(), (k, existing) -> without(existing, clientId));
			}
			else {
				this.byDomain.compute(target.domain(), (k, existing) -> without(existing, clientId));
			}
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
	 * host without its port, then the catch-all entry. When several clients serve the
	 * same domain the configured http load balancing strategy picks the target (the
	 * default, smallest client id, is deterministic across nodes -- in fan-out mode every
	 * node holds every client).
	 */
	public Optional<Route> lookup(@Nullable String host) {
		for (String candidate : candidates(host)) {
			List<Target> targets = this.byDomain.get(candidate);
			if (targets != null && !targets.isEmpty()) {
				return Optional.of(toRoute(this.httpStrategy.pick(candidate, targets)));
			}
		}
		return Optional.empty();
	}

	/**
	 * Resolves the route for the given public listen port (raw TCP routing); the
	 * configured tcp load balancing strategy picks the target (see {@link #lookup}).
	 */
	public Optional<Route> lookupByPort(int port) {
		List<Target> targets = this.byPort.get(port);
		if (targets == null || targets.isEmpty()) {
			return Optional.empty();
		}
		return Optional.of(toRoute(this.tcpStrategy.pick(Integer.toString(port), targets)));
	}

	/**
	 * Explains which routes the given Host header value resolves to, following the same
	 * candidate order as {@link #lookup} but without advancing any load balancing state.
	 */
	public Optional<RouteGroup> resolve(@Nullable String host) {
		for (String candidate : candidates(host)) {
			List<Target> targets = this.byDomain.get(candidate);
			if (targets != null && !targets.isEmpty()) {
				return Optional.of(group(candidate, targets, this.httpLoadBalance));
			}
		}
		return Optional.empty();
	}

	/**
	 * Current http route table ordered by domain (the catch-all first).
	 */
	public List<RouteGroup> httpRoutes() {
		return this.byDomain.entrySet()
			.stream()
			.filter(entry -> !entry.getValue().isEmpty())
			.sorted(Map.Entry.comparingByKey())
			.map(entry -> group(entry.getKey(), entry.getValue(), this.httpLoadBalance))
			.toList();
	}

	/**
	 * Current tcp route table ordered by listen port.
	 */
	public List<RouteGroup> tcpRoutes() {
		return this.byPort.entrySet()
			.stream()
			.filter(entry -> !entry.getValue().isEmpty())
			.sorted(Comparator.comparing(Map.Entry::getKey))
			.map(entry -> group(Integer.toString(entry.getKey()), entry.getValue(), this.tcpLoadBalance))
			.toList();
	}

	public LoadBalance httpLoadBalance() {
		return this.httpLoadBalance;
	}

	public LoadBalance tcpLoadBalance() {
		return this.tcpLoadBalance;
	}

	private static RouteGroup group(String key, List<Target> targets, LoadBalance loadBalance) {
		// only the stateless deterministic strategy can be previewed without side effects
		Route preferred = loadBalance == LoadBalance.SMALLEST_CLIENT_ID
				? toRoute(LoadBalance.SMALLEST_CLIENT_ID.instance().pick(key, targets)) : null;
		return RouteGroup.builder()
			.key(key)
			.candidates(targets.stream().map(Router::toRoute).toList())
			.preferred(preferred)
			.build();
	}

	private static Route toRoute(Target target) {
		return Route.builder()
			.clientId(target.clientId())
			.domain(target.domain())
			.address(target.address())
			.listenPort(target.listenPort())
			.preserveHost(target.preserveHost())
			.tlsPassthrough(target.tlsPassthrough())
			.build();
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
