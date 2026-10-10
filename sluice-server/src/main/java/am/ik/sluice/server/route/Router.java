package am.ik.sluice.server.route;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import am.ik.sluice.v1.proto.Upstream;

/**
 * Route table mapping public domains and listen ports to client upstreams (the port of
 * inlets' pkg/router). An upstream with a listen port is a tcp route reached only through
 * that port; its host takes no part in Host / SNI routing. Every other upstream is an
 * http route keyed by its domain or, when it declares a host pattern, by that regular
 * expression; lookup tries the exact matches, then the patterns, and falls back to the
 * catch-all entry registered with an empty domain. When several clients serve one key the
 * configured load balancing strategy picks the target.
 */
public class Router {

	private static final Logger log = LoggerFactory.getLogger(Router.class);

	/**
	 * Resolved route: the client owning the upstream, the domain the route is registered
	 * under, its dial address, whether the request Host header is rewritten to the target
	 * ({@code rewrite-host}), whether TLS connections are relayed untouched
	 * ({@code tls-passthrough}) instead of terminated on the data plane, and the IP
	 * networks allowed to connect (empty = the server-wide allow list applies).
	 */
	public record Route(String clientId, String domain, String address, int listenPort, boolean rewriteHost,
			boolean tlsPassthrough, List<String> allowedCidrs) {

		public Route {
			allowedCidrs = allowedCidrs == null ? List.of() : List.copyOf(allowedCidrs);
		}

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

			private boolean rewriteHost;

			private boolean tlsPassthrough;

			private List<String> allowedCidrs = List.of();

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

			public Builder rewriteHost(boolean rewriteHost) {
				this.rewriteHost = rewriteHost;
				return this;
			}

			public Builder tlsPassthrough(boolean tlsPassthrough) {
				this.tlsPassthrough = tlsPassthrough;
				return this;
			}

			public Builder allowedCidrs(List<String> allowedCidrs) {
				this.allowedCidrs = allowedCidrs;
				return this;
			}

			public Route build() {
				return new Route(Objects.requireNonNull(this.clientId, "clientId is required"),
						Objects.requireNonNull(this.domain, "domain is required"),
						Objects.requireNonNull(this.address, "address is required"), this.listenPort, this.rewriteHost,
						this.tlsPassthrough, this.allowedCidrs);
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

	record Target(String clientId, String domain, String address, boolean rewriteHost, boolean tlsPassthrough,
			int listenPort, List<String> allowedCidrs, @Nullable Pattern hostPattern) {

		Target {
			allowedCidrs = allowedCidrs == null ? List.of() : List.copyOf(allowedCidrs);
		}

		static Builder builder() {
			return new Builder();
		}

		static final class Builder {

			private @Nullable String clientId;

			private @Nullable String domain;

			private @Nullable String address;

			private boolean rewriteHost;

			private boolean tlsPassthrough;

			private int listenPort;

			private List<String> allowedCidrs = List.of();

			private @Nullable Pattern hostPattern;

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

			Builder rewriteHost(boolean rewriteHost) {
				this.rewriteHost = rewriteHost;
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

			Builder allowedCidrs(List<String> allowedCidrs) {
				this.allowedCidrs = allowedCidrs;
				return this;
			}

			Builder hostPattern(@Nullable Pattern hostPattern) {
				this.hostPattern = hostPattern;
				return this;
			}

			Target build() {
				return new Target(Objects.requireNonNull(this.clientId, "clientId is required"),
						Objects.requireNonNull(this.domain, "domain is required"),
						Objects.requireNonNull(this.address, "address is required"), this.rewriteHost,
						this.tlsPassthrough, this.listenPort, this.allowedCidrs, this.hostPattern);
			}

		}

	}

	private final ConcurrentMap<String, List<Target>> byDomain = new ConcurrentHashMap<>();

	/**
	 * Host pattern routes keyed by the pattern source; keys are tried in their natural
	 * String order and entries are removed once their last target goes. The compiled
	 * pattern is carried by every target of the key.
	 */
	private final ConcurrentNavigableMap<String, List<Target>> byPattern = new ConcurrentSkipListMap<>();

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
			String address = addressOf(upstream.getTargetUrl()).orElse(null);
			if (address == null) {
				continue;
			}
			Pattern hostPattern = compiledPattern(upstream.getHostPattern(), upstream.getTargetUrl());
			targets.add(Target.builder()
				.clientId(clientId)
				.domain(hostPattern == null ? upstream.getHost() : upstream.getHostPattern())
				.address(address)
				.rewriteHost(upstream.getRewriteHost())
				.tlsPassthrough(upstream.getTlsPassthrough())
				.listenPort(upstream.getListenPort())
				.allowedCidrs(upstream.getAllowedCidrsList())
				.hostPattern(hostPattern)
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
				else if (target.hostPattern() != null) {
					this.byPattern.compute(target.domain(), (k, existing) -> append(existing, target));
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
	 * Compiles the declared host pattern; {@code null} when blank (the literal host
	 * applies) or invalid (logged, the literal host applies).
	 */
	private static @Nullable Pattern compiledPattern(String hostPattern, String targetUrl) {
		if (hostPattern == null || hostPattern.isEmpty()) {
			return null;
		}
		try {
			return Pattern.compile(hostPattern);
		}
		catch (PatternSyntaxException e) {
			log.warn("ignoring invalid host pattern [{}] of upstream {}: {}", hostPattern, targetUrl, e.getMessage());
			return null;
		}
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
			else if (target.hostPattern() != null) {
				// an empty list is dropped entirely so stale pattern keys go away
				this.byPattern.compute(target.domain(), (k, existing) -> {
					List<Target> remaining = without(existing, clientId);
					return remaining.isEmpty() ? null : remaining;
				});
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
	 * host without its port, then the host pattern routes (matched against the host
	 * without its port, whole match, keys in their natural String order), then the
	 * catch-all entry. When several clients serve the same domain the configured http
	 * load balancing strategy picks the target (the default, smallest client id, is
	 * deterministic across nodes -- in fan-out mode every node holds every client).
	 */
	public Optional<Route> lookup(@Nullable String host) {
		return match(host).map(candidate -> toRoute(this.httpStrategy.pick(candidate.key(), candidate.targets())));
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
		return match(host).map(candidate -> group(candidate.key(), candidate.targets(), this.httpLoadBalance));
	}

	/**
	 * Whether the key is a registered host pattern route.
	 */
	public boolean isPattern(String key) {
		return this.byPattern.containsKey(key);
	}

	private Optional<Match> match(@Nullable String host) {
		if (host != null && !host.isBlank()) {
			int portSeparator = host.lastIndexOf(':');
			boolean hasPort = portSeparator > host.lastIndexOf(']'); // not an IPv6 suffix
			String bareHost = hasPort ? host.substring(0, portSeparator) : host;
			for (String candidate : hasPort ? List.of(host, bareHost) : List.of(host)) {
				List<Target> targets = this.byDomain.get(candidate);
				if (targets != null && !targets.isEmpty()) {
					return Optional.of(new Match(candidate, targets));
				}
			}
			for (Map.Entry<String, List<Target>> entry : this.byPattern.entrySet()) {
				Pattern pattern = entry.getValue().get(0).hostPattern();
				if (pattern != null && pattern.matcher(bareHost).matches()) {
					return Optional.of(new Match(entry.getKey(), entry.getValue()));
				}
			}
		}
		List<Target> catchAll = this.byDomain.get("");
		if (catchAll == null || catchAll.isEmpty()) {
			return Optional.empty();
		}
		return Optional.of(new Match("", catchAll));
	}

	private record Match(String key, List<Target> targets) {
	}

	/**
	 * Current http route table ordered by key (the catch-all first); pattern routes are
	 * merged in by their pattern source.
	 */
	public List<RouteGroup> httpRoutes() {
		Map<String, List<Target>> merged = new TreeMap<>();
		this.byDomain.forEach((key, targets) -> {
			if (!targets.isEmpty()) {
				merged.put(key, targets);
			}
		});
		this.byPattern.forEach((key, targets) -> {
			if (!targets.isEmpty()) {
				merged.merge(key, targets, Router::concat);
			}
		});
		return merged.entrySet()
			.stream()
			.map(entry -> group(entry.getKey(), entry.getValue(), this.httpLoadBalance))
			.toList();
	}

	private static List<Target> concat(List<Target> first, List<Target> second) {
		List<Target> merged = new ArrayList<>(first);
		merged.addAll(second);
		return List.copyOf(merged);
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
			.rewriteHost(target.rewriteHost())
			.tlsPassthrough(target.tlsPassthrough())
			.allowedCidrs(target.allowedCidrs())
			.build();
	}

	/**
	 * Number of clients with at least one route.
	 */
	public int clientCount() {
		return this.byClient.size();
	}

	/**
	 * Extracts {@code host[:port]} from an upstream URL; {@code null} when unparseable.
	 * {@code wasm:} targets are component locators resolved by the client (file / oci /
	 * s3 / ...): they pass through verbatim as the dial address.
	 */
	static Optional<String> addressOf(String targetUrl) {
		if (targetUrl == null || targetUrl.isBlank()) {
			return Optional.empty();
		}
		if (targetUrl.startsWith("wasm:")) {
			return Optional.of(targetUrl.trim());
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
