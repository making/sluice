package am.ik.sluice.server.route;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
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
import java.util.concurrent.atomic.AtomicLong;
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
 * configured load balancing strategy picks the target, unless every one of them carries a
 * weight ({@code Upstream.weight}, a wicket's route its instances): then a request goes
 * to the one with the fewest requests in flight for its weight, ties taking turns by the
 * weights ({@link WeightedLeastInFlight}), whatever the strategy. A request is in flight
 * from its {@link #pick} until its relay closes the {@link Pick}.
 */
public class Router {

	private static final Logger log = LoggerFactory.getLogger(Router.class);

	/**
	 * The characters one host pattern match may inspect before it is abandoned as a
	 * non-match: a client-supplied pattern with catastrophic backtracking must not hold
	 * the event loop for the time its worst case takes.
	 */
	private static final int HOST_MATCH_BUDGET = 1_000_000;

	/** Stateless: what stands for several candidates without picking among them. */
	private static final LoadBalanceStrategy SMALLEST_CLIENT_ID = LoadBalance.SMALLEST_CLIENT_ID.instance();

	/**
	 * Resolved route: the client owning the upstream, the domain the route is registered
	 * under, its dial address, whether the request Host header is rewritten to the target
	 * ({@code rewrite-host}), whether TLS connections are relayed untouched
	 * ({@code tls-passthrough}) instead of terminated on the data plane, whether the data
	 * plane's TLS termination prefers http/1.1 in ALPN and answers an h2 stream 421
	 * ({@code force-http1}: the upstream speaks HTTP/1.1 only), the IP networks allowed
	 * to connect (empty = the server-wide allow list applies), how long a connection
	 * relayed verbatim may relay no byte either way before it is closed
	 * ({@code verbatim-idle-timeout}; zero = the route sets none, the server's default
	 * applies), and the literal host the client announced separately from the pattern key
	 * ({@code null} when it announced none), so a credential policy configured for that
	 * host applies to its host pattern too.
	 */
	public record Route(String clientId, String domain, String address, int listenPort, boolean rewriteHost,
			boolean tlsPassthrough, boolean forceHttp1, List<String> allowedCidrs, Duration verbatimIdleTimeout,
			@Nullable String literalHost) {

		public Route {
			allowedCidrs = allowedCidrs == null ? List.of() : List.copyOf(allowedCidrs);
			verbatimIdleTimeout = verbatimIdleTimeout == null || verbatimIdleTimeout.isNegative() ? Duration.ZERO
					: verbatimIdleTimeout;
			literalHost = literalHost == null || literalHost.isBlank() ? null : literalHost;
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

		/**
		 * The key the route is configured under ({@code sluice.route.*}) and gated by:
		 * {@code tcp:<port>} for a tcp route, else the domain or host pattern, {@code *}
		 * for the catch-all.
		 */
		public String routeKey() {
			if (this.listenPort > 0) {
				return "tcp:" + this.listenPort;
			}
			return this.domain.isEmpty() ? "*" : this.domain;
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

			private boolean forceHttp1;

			private List<String> allowedCidrs = List.of();

			private Duration verbatimIdleTimeout = Duration.ZERO;

			private @Nullable String literalHost;

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

			public Builder forceHttp1(boolean forceHttp1) {
				this.forceHttp1 = forceHttp1;
				return this;
			}

			public Builder allowedCidrs(List<String> allowedCidrs) {
				this.allowedCidrs = allowedCidrs;
				return this;
			}

			public Builder verbatimIdleTimeout(Duration verbatimIdleTimeout) {
				this.verbatimIdleTimeout = verbatimIdleTimeout;
				return this;
			}

			/**
			 * The literal host the client announced alongside the pattern key; blank or
			 * absent when it announced none.
			 */
			public Builder literalHost(@Nullable String literalHost) {
				this.literalHost = literalHost;
				return this;
			}

			public Route build() {
				return new Route(Objects.requireNonNull(this.clientId, "clientId is required"),
						Objects.requireNonNull(this.domain, "domain is required"),
						Objects.requireNonNull(this.address, "address is required"), this.listenPort, this.rewriteHost,
						this.tlsPassthrough, this.forceHttp1, this.allowedCidrs, this.verbatimIdleTimeout,
						this.literalHost);
			}

		}

	}

	/**
	 * The routes registered under one lookup key.
	 *
	 * @param key the domain ({@code ""} = catch-all) or the listen port of tcp routes
	 * @param candidates every registered route, in registration order
	 * @param preferred the route picks resolve to when the load balancing is
	 * deterministic; {@code null} when the candidates share the key by their weights, or
	 * the strategy rotates or randomizes
	 * @param weights the weight each candidate carries, in their order; {@code 0} = none
	 * @param inFlight the requests in flight to each candidate on this node, in their
	 * order; counted while the candidates share the key by their weights, else none
	 */
	public record RouteGroup(String key, List<Route> candidates, @Nullable Route preferred, List<Long> weights,
			List<Long> inFlight) {

		public RouteGroup {
			candidates = List.copyOf(candidates);
			weights = List.copyOf(weights);
			inFlight = List.copyOf(inFlight);
			if (weights.size() != candidates.size() || inFlight.size() != candidates.size()) {
				throw new IllegalArgumentException("%d weights and %d counts for %d candidates"
					.formatted(weights.size(), inFlight.size(), candidates.size()));
			}
		}

		/**
		 * Whether the candidates share the key by their weights: every one carries one.
		 */
		public boolean weighted() {
			return !this.weights.isEmpty() && !this.weights.contains(0L);
		}

		/**
		 * The sum of the candidates' weights.
		 */
		public long totalWeight() {
			return this.weights.stream().mapToLong(Long::longValue).sum();
		}

		public static Builder builder() {
			return new Builder();
		}

		public static final class Builder {

			private @Nullable String key;

			private List<Route> candidates = List.of();

			private @Nullable Route preferred;

			private @Nullable List<Long> weights;

			private @Nullable List<Long> inFlight;

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

			/**
			 * The weight of each candidate, in their order; unset, none carries one.
			 */
			public Builder weights(List<Long> weights) {
				this.weights = weights;
				return this;
			}

			/**
			 * The requests in flight to each candidate, in their order; unset, none.
			 */
			public Builder inFlight(List<Long> inFlight) {
				this.inFlight = inFlight;
				return this;
			}

			public RouteGroup build() {
				List<Long> none = this.candidates.stream().map(candidate -> 0L).toList();
				return new RouteGroup(Objects.requireNonNull(this.key, "key is required"), this.candidates,
						this.preferred, this.weights != null ? this.weights : none,
						this.inFlight != null ? this.inFlight : none);
			}

		}

	}

	/**
	 * The route {@link #pick} resolved a request to. While the candidates of its key
	 * share it by their weights, the request counts among those in flight to its
	 * candidate until the pick is closed: once, however often and from whichever thread.
	 * Its relay closes it once the request ended -- its response relayed or lost, the
	 * request refused or failed, a connection relayed verbatim closed --: a pick never
	 * closed holds its candidate's turn back for good.
	 */
	public static final class Pick implements AutoCloseable {

		private final Route route;

		/** The candidate's count until closed; null once closed, or never counted. */
		private @Nullable AtomicLong inFlight;

		private Pick(Route route, @Nullable AtomicLong inFlight) {
			this.route = route;
			this.inFlight = inFlight;
		}

		public Route route() {
			return this.route;
		}

		@Override
		public void close() {
			AtomicLong count;
			synchronized (this) {
				count = this.inFlight;
				this.inFlight = null;
			}
			if (count != null) {
				count.decrementAndGet();
			}
		}

	}

	/**
	 * A registered upstream; {@code weight} its share of its key among the targets
	 * carrying one, {@code 0} = none.
	 */
	record Target(String clientId, String domain, String address, boolean rewriteHost, boolean tlsPassthrough,
			boolean forceHttp1, int listenPort, List<String> allowedCidrs, Duration verbatimIdleTimeout,
			@Nullable Pattern hostPattern, @Nullable String literalHost, long weight) {

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

			private boolean forceHttp1;

			private int listenPort;

			private List<String> allowedCidrs = List.of();

			private Duration verbatimIdleTimeout = Duration.ZERO;

			private @Nullable Pattern hostPattern;

			private @Nullable String literalHost;

			private long weight;

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

			Builder forceHttp1(boolean forceHttp1) {
				this.forceHttp1 = forceHttp1;
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

			Builder verbatimIdleTimeout(Duration verbatimIdleTimeout) {
				this.verbatimIdleTimeout = verbatimIdleTimeout;
				return this;
			}

			Builder hostPattern(@Nullable Pattern hostPattern) {
				this.hostPattern = hostPattern;
				return this;
			}

			Builder literalHost(@Nullable String literalHost) {
				this.literalHost = literalHost;
				return this;
			}

			Builder weight(long weight) {
				this.weight = weight;
				return this;
			}

			Target build() {
				return new Target(Objects.requireNonNull(this.clientId, "clientId is required"),
						Objects.requireNonNull(this.domain, "domain is required"),
						Objects.requireNonNull(this.address, "address is required"), this.rewriteHost,
						this.tlsPassthrough, this.forceHttp1, this.listenPort, this.allowedCidrs,
						this.verbatimIdleTimeout, this.hostPattern, this.literalHost, this.weight);
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

	private final Balancing http;

	private final Balancing tcp;

	private final RouteGate gate;

	/**
	 * The load balancing strategies default to {@link LoadBalance#SMALLEST_CLIENT_ID}
	 * (deterministic across nodes).
	 */
	public Router() {
		this(LoadBalance.SMALLEST_CLIENT_ID, LoadBalance.SMALLEST_CLIENT_ID);
	}

	public Router(LoadBalance httpLoadBalance, LoadBalance tcpLoadBalance) {
		this(httpLoadBalance, tcpLoadBalance, RouteGate.alwaysOpen());
	}

	/**
	 * @param gate per-route gates; a shut route stops resolving and its requests fall
	 * through to the next candidate
	 */
	public Router(LoadBalance httpLoadBalance, LoadBalance tcpLoadBalance, RouteGate gate) {
		this.http = new Balancing(httpLoadBalance);
		this.tcp = new Balancing(tcpLoadBalance);
		this.gate = Objects.requireNonNull(gate, "gate is required");
	}

	/**
	 * (Re-)registers the upstreams announced by a client. Any previous entry for the
	 * client is replaced, so re-announcement after reconnect is idempotent, and an
	 * announcement without a target removes the client's routes.
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
				.forceHttp1(upstream.getForceHttp1())
				.listenPort(upstream.getListenPort())
				.allowedCidrs(upstream.getAllowedCidrsList())
				.verbatimIdleTimeout(verbatimIdleTimeoutOf(upstream))
				.hostPattern(hostPattern)
				.literalHost(upstream.getHost())
				.weight(Integer.toUnsignedLong(upstream.getWeight()))
				.build());
		}
		if (clientId == null || clientId.isBlank()) {
			return 0;
		}
		synchronized (this.lock) {
			List<Target> old = removeLocked(clientId);
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
			if (!targets.isEmpty()) {
				this.byClient.put(clientId, List.copyOf(targets));
			}
			// a key the client keeps keeps its state: the requests in flight to it, say
			forgetEmptiedLocked(old);
		}
		return targets.size();
	}

	/**
	 * The verbatim idle timeout an upstream declares; zero when it declares none. A value
	 * past the signed range of the unsigned field is taken as the longest one.
	 */
	private static Duration verbatimIdleTimeoutOf(Upstream upstream) {
		long millis = upstream.getVerbatimIdleTimeoutMillis();
		return Duration.ofMillis(millis < 0 ? Long.MAX_VALUE : millis);
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
			forgetEmptiedLocked(removeLocked(clientId));
		}
	}

	/** Removes the client's targets; those it had. */
	private List<Target> removeLocked(String clientId) {
		List<Target> old = this.byClient.remove(clientId);
		if (old == null) {
			return List.of();
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
		return old;
	}

	/**
	 * Each key of the targets left without a target: the load balancing lets go of its
	 * state.
	 */
	private void forgetEmptiedLocked(List<Target> old) {
		for (Target target : old) {
			if (target.listenPort() > 0) {
				if (isEmpty(this.byPort.get(target.listenPort()))) {
					this.tcp.forget(Integer.toString(target.listenPort()));
				}
			}
			else if (target.hostPattern() != null) {
				if (!this.byPattern.containsKey(target.domain())) {
					this.http.forget(target.domain());
				}
			}
			else if (isEmpty(this.byDomain.get(target.domain()))) {
				this.http.forget(target.domain());
			}
		}
	}

	private static boolean isEmpty(@Nullable List<Target> targets) {
		return targets == null || targets.isEmpty();
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
	 * Picks the route of a request with the given Host header value. Exact match first,
	 * then the host without its port, then the host pattern routes (matched against the
	 * host without its port, whole match, keys in their natural String order), then the
	 * catch-all entry. When several clients serve the same domain the configured http
	 * load balancing strategy picks the target (the default, smallest client id, is
	 * deterministic across nodes -- in fan-out mode every node holds every client),
	 * unless every one carries a weight: then the one with the fewest requests in flight
	 * for its weight, counting this one from now until the pick is closed.
	 */
	public Optional<Pick> pick(@Nullable String host) {
		return match(host).map(candidate -> this.http.pick(candidate.key(), candidate.targets()));
	}

	/**
	 * Picks the route of a connection to the given public listen port (raw TCP routing):
	 * the configured tcp load balancing strategy, or the weights (see {@link #pick}).
	 */
	public Optional<Pick> pickByPort(int port) {
		List<Target> targets = this.portTargets(port);
		return targets == null ? Optional.empty() : Optional.of(this.tcp.pick(Integer.toString(port), targets));
	}

	/**
	 * The route of the given Host header value, matched as {@link #pick} does, without
	 * picking: the load balancing moves on no further and counts nothing. When several
	 * clients serve the domain, the one of the smallest client id stands for them -- the
	 * one picked when the choice is deterministic.
	 */
	public Optional<Route> lookup(@Nullable String host) {
		return match(host).map(candidate -> toRoute(SMALLEST_CLIENT_ID.pick(candidate.key(), candidate.targets())));
	}

	/**
	 * The route of the given public listen port, without picking (see {@link #lookup}).
	 */
	public Optional<Route> lookupByPort(int port) {
		List<Target> targets = this.portTargets(port);
		return targets == null ? Optional.empty()
				: Optional.of(toRoute(SMALLEST_CLIENT_ID.pick(Integer.toString(port), targets)));
	}

	/** The targets of the listen port while its gate is open; null otherwise. */
	private @Nullable List<Target> portTargets(int port) {
		List<Target> targets = this.byPort.get(port);
		if (targets == null || targets.isEmpty() || !this.gate.isOpen("tcp:" + port)) {
			return null;
		}
		return targets;
	}

	/**
	 * Explains which routes the given Host header value resolves to, matched as
	 * {@link #pick} does but without advancing any load balancing state.
	 */
	public Optional<RouteGroup> resolve(@Nullable String host) {
		return match(host).map(candidate -> this.http.group(candidate.key(), candidate.targets()));
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
				List<Target> targets = this.opened(candidate, this.byDomain.get(candidate));
				if (targets != null) {
					return Optional.of(new Match(candidate, targets));
				}
			}
			for (Map.Entry<String, List<Target>> entry : this.byPattern.entrySet()) {
				List<Target> targets = this.opened(entry.getKey(), entry.getValue());
				Pattern pattern = entry.getValue().get(0).hostPattern();
				if (targets != null && pattern != null && matches(pattern, bareHost)) {
					return Optional.of(new Match(entry.getKey(), targets));
				}
			}
		}
		List<Target> catchAll = this.opened("*", this.byDomain.get(""));
		if (catchAll == null) {
			return Optional.empty();
		}
		return Optional.of(new Match("", catchAll));
	}

	/**
	 * Whether the host matches the pattern within the character budget; a pattern that
	 * exhausts it is a non-match, so the lookup falls through to the next candidate.
	 */
	private static boolean matches(Pattern pattern, String host) {
		try {
			return pattern.matcher(new BudgetedText(host, HOST_MATCH_BUDGET)).matches();
		}
		catch (BudgetedText.Exhausted e) {
			log.debug("host pattern [{}] gave up after {} character comparisons", pattern.pattern(), HOST_MATCH_BUDGET);
			return false;
		}
	}

	/**
	 * A {@link CharSequence} whose {@code charAt} stops the match once the budget is
	 * spent: the regex engine's backtracking is bounded by the characters it reads, not
	 * by the shapes it can take.
	 */
	private static final class BudgetedText implements CharSequence {

		private final CharSequence text;

		private final int[] remaining;

		BudgetedText(CharSequence text, int budget) {
			this(text, new int[] { budget });
		}

		private BudgetedText(CharSequence text, int[] remaining) {
			this.text = text;
			this.remaining = remaining;
		}

		@Override
		public int length() {
			return this.text.length();
		}

		@Override
		public char charAt(int index) {
			if (--this.remaining[0] < 0) {
				throw Exhausted.INSTANCE;
			}
			return this.text.charAt(index);
		}

		@Override
		public CharSequence subSequence(int start, int end) {
			// the budget is shared: a capture group's subsequence cannot spend its own
			return new BudgetedText(this.text.subSequence(start, end), this.remaining);
		}

		@Override
		public String toString() {
			return this.text.toString();
		}

		/** A control-flow signal, thrown once per exhausted match. */
		private static final class Exhausted extends RuntimeException {

			static final Exhausted INSTANCE = new Exhausted();

			private Exhausted() {
				super("the host pattern match budget is spent", null, false, false);
			}

		}

	}

	/**
	 * The candidates when the key's gate is open; {@code null} when the key is unknown or
	 * gated shut, so the lookup falls through to the next candidate.
	 */
	private @Nullable List<Target> opened(@Nullable String key, @Nullable List<Target> targets) {
		if (targets == null || targets.isEmpty() || !this.gate.isOpen(key == null || key.isEmpty() ? "*" : key)) {
			return null;
		}
		return targets;
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
		return merged.entrySet().stream().map(entry -> this.http.group(entry.getKey(), entry.getValue())).toList();
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
			.map(entry -> this.tcp.group(Integer.toString(entry.getKey()), entry.getValue()))
			.toList();
	}

	public LoadBalance httpLoadBalance() {
		return this.http.loadBalance;
	}

	public LoadBalance tcpLoadBalance() {
		return this.tcp.loadBalance;
	}

	/** The route gates; the console shuts and reopens them. */
	public RouteGate gate() {
		return this.gate;
	}

	/**
	 * Whether every target carries a weight: then they share their key by it.
	 */
	private static boolean weighted(List<Target> targets) {
		for (Target target : targets) {
			if (target.weight() == 0) {
				return false;
			}
		}
		return !targets.isEmpty();
	}

	/**
	 * How one route space, http or tcp, picks among the targets of a key: by the requests
	 * in flight for their weights when every one carries one, else by its strategy. Each
	 * keeps its own state.
	 */
	private static final class Balancing {

		private final LoadBalance loadBalance;

		private final LoadBalanceStrategy strategy;

		private final WeightedLeastInFlight weighted = new WeightedLeastInFlight();

		Balancing(LoadBalance loadBalance) {
			this.loadBalance = loadBalance;
			this.strategy = loadBalance.instance();
		}

		Pick pick(String key, List<Target> targets) {
			if (!weighted(targets)) {
				return new Pick(toRoute(this.strategy.pick(key, targets)), null);
			}
			WeightedLeastInFlight.Picked picked = this.weighted.pick(key, targets);
			return new Pick(toRoute(picked.target()), picked.inFlight());
		}

		void forget(String key) {
			this.strategy.forget(key);
			this.weighted.forget(key);
		}

		RouteGroup group(String key, List<Target> targets) {
			boolean weighted = weighted(targets);
			// only a deterministic winner can be previewed without side effects
			Route preferred = this.loadBalance == LoadBalance.SMALLEST_CLIENT_ID && !weighted
					? toRoute(SMALLEST_CLIENT_ID.pick(key, targets)) : null;
			RouteGroup.Builder group = RouteGroup.builder()
				.key(key)
				.candidates(targets.stream().map(Router::toRoute).toList())
				.preferred(preferred)
				.weights(targets.stream().map(Target::weight).toList());
			if (weighted) {
				group.inFlight(this.weighted.inFlight(key, targets));
			}
			return group.build();
		}

	}

	private static Route toRoute(Target target) {
		return Route.builder()
			.clientId(target.clientId())
			.domain(target.domain())
			.address(target.address())
			.listenPort(target.listenPort())
			.rewriteHost(target.rewriteHost())
			.tlsPassthrough(target.tlsPassthrough())
			.forceHttp1(target.forceHttp1())
			.allowedCidrs(target.allowedCidrs())
			.verbatimIdleTimeout(target.verbatimIdleTimeout())
			.literalHost(target.literalHost())
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
	 * Targets a wicket resolves itself pass through verbatim as the dial address:
	 * {@code wasm:} component locators (file / oci / s3 / ...) and {@code firecracker:}
	 * microVM specs.
	 */
	static Optional<String> addressOf(String targetUrl) {
		if (targetUrl == null || targetUrl.isBlank()) {
			return Optional.empty();
		}
		if (targetUrl.startsWith("wasm:") || targetUrl.startsWith("firecracker:")) {
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
