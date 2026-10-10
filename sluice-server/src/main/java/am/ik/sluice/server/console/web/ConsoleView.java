package am.ik.sluice.server.console.web;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedMap;
import java.util.stream.Collectors;

import org.jspecify.annotations.Nullable;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.availability.ApplicationAvailability;
import org.springframework.boot.availability.ReadinessState;
import org.springframework.boot.info.BuildProperties;
import org.springframework.boot.info.GitProperties;
import org.springframework.stereotype.Component;

import am.ik.sluice.server.cluster.NodeDirectory;
import am.ik.sluice.server.config.SluiceServerProperties;
import am.ik.sluice.server.console.ControlPlanePort;
import am.ik.sluice.server.proxy.DataProxyServer;
import am.ik.sluice.server.proxy.TcpPortGateway;
import am.ik.sluice.server.route.LoadBalance;
import am.ik.sluice.server.route.Router;
import am.ik.sluice.server.tunnel.ClusterLifecycle;
import am.ik.sluice.server.tunnel.SessionRegistry;
import am.ik.sluice.server.tunnel.TunnelSession;
import am.ik.sluice.v1.proto.Upstream;

/**
 * Assembles the console view model from the live server state. Every value is rendered as
 * display text here so the templates stay logic-less.
 */
@Component
class ConsoleView {

	private static final DateTimeFormatter CLOCK = DateTimeFormatter.ofPattern("HH:mm:ss", Locale.ROOT);

	private final SessionRegistry sessions;

	private final Router router;

	private final TcpPortGateway tcpPortGateway;

	private final DataProxyServer dataProxyServer;

	private final NodeDirectory nodeDirectory;

	private final ClusterLifecycle clusterLifecycle;

	private final ApplicationAvailability availability;

	private final SluiceServerProperties properties;

	private final ControlPlanePort controlPlanePort;

	private final ObjectProvider<BuildProperties> buildProperties;

	private final ObjectProvider<GitProperties> gitProperties;

	ConsoleView(SessionRegistry sessions, Router router, TcpPortGateway tcpPortGateway, DataProxyServer dataProxyServer,
			NodeDirectory nodeDirectory, ClusterLifecycle clusterLifecycle, ApplicationAvailability availability,
			SluiceServerProperties properties, ControlPlanePort controlPlanePort,
			ObjectProvider<BuildProperties> buildProperties, ObjectProvider<GitProperties> gitProperties) {
		this.sessions = sessions;
		this.router = router;
		this.tcpPortGateway = tcpPortGateway;
		this.dataProxyServer = dataProxyServer;
		this.nodeDirectory = nodeDirectory;
		this.clusterLifecycle = clusterLifecycle;
		this.availability = availability;
		this.properties = properties;
		this.controlPlanePort = controlPlanePort;
		this.buildProperties = buildProperties;
		this.gitProperties = gitProperties;
	}

	Status status() {
		String state = this.state();
		String label = switch (state) {
			case "draining" -> "Draining";
			case "warming" -> "Warming up";
			default -> "Accepting traffic";
		};
		return Status.builder()
			.nodeId(this.properties.node().id())
			.state(state)
			.stateLabel(label)
			.version(this.version())
			.uptime(Humanize.duration(Duration.ofMillis(ManagementFactory.getRuntimeMXBean().getUptime())))
			.refreshedAt(LocalTime.now().format(CLOCK))
			.build();
	}

	private String state() {
		if (this.clusterLifecycle.isDraining()) {
			return "draining";
		}
		return this.availability.getReadinessState() == ReadinessState.ACCEPTING_TRAFFIC ? "accepting" : "warming";
	}

	private String version() {
		BuildProperties build = this.buildProperties.getIfAvailable();
		GitProperties git = this.gitProperties.getIfAvailable();
		String version = build == null || build.getVersion() == null ? "" : build.getVersion();
		String commit = git == null || git.getShortCommitId() == null ? "" : git.getShortCommitId();
		if (commit.isEmpty()) {
			return version;
		}
		return version.isEmpty() ? commit : version + " (" + commit + ")";
	}

	FlowDrawing flow() {
		List<TunnelSession> live = this.liveSessions();
		FlowDrawing.Builder drawing = FlowDrawing.builder()
			.intake(FlowDrawing.Intake.builder()
				.port(":" + this.dataProxyServer.boundPort())
				.protocol(this.properties.dataTlsBundle() == null ? "HTTP" : "HTTP + TLS")
				.detail("Routed by Host header or SNI")
				.open(this.dataProxyServer.activeConnections()));
		this.tcpPortGateway.bindings()
			.forEach((port,
					binding) -> drawing.intake(FlowDrawing.Intake.builder()
						.port(":" + port)
						.protocol("TCP")
						.detail("Relayed to " + FlowDrawing.truncate(binding.clientId(), 18))
						.open(binding.activeConnections())));
		Map<String, Duties> duties = this.duties();
		live.forEach(session -> {
			Duties duty = duties.getOrDefault(session.clientId(), Duties.NONE);
			drawing.outlet(FlowDrawing.Outlet.builder()
				.clientId(session.clientId())
				.open(session.connectionCount())
				.serving(duty.serving().isEmpty() ? "" : "Serves " + String.join(", ", duty.serving()))
				.standby(duty.standby().isEmpty() ? "" : "Standby for " + String.join(", ", duty.standby())));
		});
		return drawing.openConnections(live.stream().mapToInt(TunnelSession::connectionCount).sum())
			.controlPort(this.controlPort())
			.state(this.state())
			.build();
	}

	private String controlPort() {
		return this.controlPlanePort.port().stream().mapToObj(port -> ":" + port).findFirst().orElse("");
	}

	List<Client> clients() {
		Instant now = Instant.now();
		Map<String, Router.RouteGroup> httpByKey = this.router.httpRoutes()
			.stream()
			.collect(Collectors.toMap(Router.RouteGroup::key, group -> group));
		SortedMap<Integer, TcpPortGateway.Binding> bindings = this.tcpPortGateway.bindings();
		return this.liveSessions()
			.stream()
			.map(session -> Client.builder()
				.clientId(session.clientId())
				.remoteAddress(session.remoteAddress())
				.connectedFor(Humanize.duration(Duration.between(session.connectedAt(), now)))
				.connectedAt(session.connectedAt().truncatedTo(ChronoUnit.SECONDS).toString())
				.openConnections(session.connectionCount())
				.connectionsOpened(session.connectionsOpened())
				.inbound(Humanize.bytes(session.bytesInbound()))
				.outbound(Humanize.bytes(session.bytesOutbound()))
				.draining(session.drained())
				.upstreams(session.upstreams()
					.stream()
					.map(upstream -> upstreamRow(upstream, session.rejectedPorts().contains(upstream.getListenPort()))
						.role(role(session.clientId(), upstream, httpByKey, bindings))
						.build())
					.toList())
				.build())
			.toList();
	}

	private static UpstreamRow.Builder upstreamRow(Upstream upstream, boolean rejected) {
		int listenPort = upstream.getListenPort();
		String kind = listenPort > 0 ? "TCP :" + listenPort : upstream.getTlsPassthrough() ? "TLS passthrough" : "HTTP";
		String note = rejected ? "Port not bound" : listenPort > 0 || upstream.getTlsPassthrough() ? ""
				: upstream.getRewriteHost() ? "Rewrites Host" : "Keeps Host";
		// force-http1 only takes effect on the TLS-terminated HTTP path
		if (!rejected && listenPort == 0 && !upstream.getTlsPassthrough() && upstream.getForceHttp1()) {
			note = note.isEmpty() ? "HTTP/1.1 only" : note + ", HTTP/1.1 only";
		}
		return UpstreamRow.builder()
			.host(routeKey(upstream))
			.target(upstream.getTargetUrl())
			.kind(kind)
			.note(note)
			.rejected(rejected);
	}

	/**
	 * Whether the client actually serves the upstream's route, or which client does.
	 */
	private String role(String clientId, Upstream upstream, Map<String, Router.RouteGroup> httpByKey,
			SortedMap<Integer, TcpPortGateway.Binding> bindings) {
		if (upstream.getListenPort() > 0) {
			TcpPortGateway.Binding binding = bindings.get(upstream.getListenPort());
			if (binding == null) {
				return "";
			}
			return binding.clientId().equals(clientId) ? "Serving" : "Standby, " + binding.clientId() + " serves";
		}
		Router.RouteGroup group = httpByKey.get(routeKey(upstream));
		if (group == null) {
			return "";
		}
		if (group.candidates().size() == 1) {
			return "Serving";
		}
		Router.Route preferred = group.preferred();
		if (preferred == null) {
			return this.router.httpLoadBalance() == LoadBalance.ROUND_ROBIN ? "In rotation" : "Picked at random";
		}
		return preferred.clientId().equals(clientId) ? "Serving" : "Standby, " + preferred.clientId() + " serves";
	}

	/**
	 * The route key the upstream is registered under: the host pattern when declared, the
	 * literal host otherwise.
	 */
	private static String routeKey(Upstream upstream) {
		return upstream.getHostPattern().isEmpty() ? upstream.getHost() : upstream.getHostPattern();
	}

	/**
	 * The routes each client serves and those it stands by for, keyed by client id.
	 */
	private Map<String, Duties> duties() {
		Map<String, Duties> duties = new HashMap<>();
		for (Router.RouteGroup group : this.router.httpRoutes()) {
			String key = group.key().isEmpty() ? "any host" : group.key();
			Router.Route preferred = group.preferred();
			for (Router.Route route : group.candidates()) {
				Duties duty = duties.computeIfAbsent(route.clientId(),
						id -> new Duties(new ArrayList<>(), new ArrayList<>()));
				boolean serving = preferred == null || preferred.clientId().equals(route.clientId())
						|| group.candidates().size() == 1;
				(serving ? duty.serving() : duty.standby()).add(key);
			}
		}
		this.tcpPortGateway.bindings()
			.forEach((port, binding) -> duties
				.computeIfAbsent(binding.clientId(), id -> new Duties(new ArrayList<>(), new ArrayList<>()))
				.serving()
				.add(":" + port));
		return duties;
	}

	/**
	 * Route keys a client serves or stands by for.
	 */
	private record Duties(List<String> serving, List<String> standby) {

		static final Duties NONE = new Duties(List.of(), List.of());

	}

	List<RouteRow> httpRoutes() {
		LoadBalance loadBalance = this.router.httpLoadBalance();
		return this.router.httpRoutes()
			.stream()
			.map(group -> new RouteRow(group.key(), candidates(group, loadBalance)))
			.toList();
	}

	List<PortRow> tcpRoutes() {
		LoadBalance loadBalance = this.router.tcpLoadBalance();
		SortedMap<Integer, TcpPortGateway.Binding> bindings = this.tcpPortGateway.bindings();
		return this.router.tcpRoutes()
			.stream()
			.map(group -> PortRow.builder()
				.port(group.key())
				.owner(Optional.ofNullable(bindings.get(Integer.parseInt(group.key())))
					.map(TcpPortGateway.Binding::clientId)
					.orElse(""))
				.candidates(candidates(group, loadBalance))
				.build())
			.toList();
	}

	Optional<Lookup> lookup(String host) {
		String trimmed = host.strip();
		if (trimmed.isEmpty()) {
			return Optional.empty();
		}
		return Optional.of(this.router.resolve(trimmed).map(group -> {
			String how = group.key().equals(trimmed) ? "Exact match" : group.key().isEmpty() ? "Catch-all route"
					: this.router.isPattern(group.key()) ? "Pattern match" : "Matched without the port";
			return Lookup.builder()
				.host(trimmed)
				.matched(group.key())
				.how(how)
				.candidates(candidates(group, this.router.httpLoadBalance()))
				.build();
		}).orElseGet(() -> Lookup.builder().host(trimmed).build()));
	}

	private static List<Candidate> candidates(Router.RouteGroup group, LoadBalance loadBalance) {
		boolean single = group.candidates().size() == 1;
		// listed in the order the strategy decides by, so the winner comes first
		Comparator<Router.Route> order = loadBalance == LoadBalance.SMALLEST_CLIENT_ID
				? Comparator.comparing(Router.Route::clientId) : (a, b) -> 0;
		return group.candidates().stream().sorted(order).map(route -> {
			String role;
			if (single || route.equals(group.preferred())) {
				role = "Serving";
			}
			else if (group.preferred() != null) {
				role = "Standby";
			}
			else {
				role = loadBalance == LoadBalance.ROUND_ROBIN ? "In rotation" : "Picked at random";
			}
			String note = route.tlsPassthrough() ? "TLS passthrough" : route.forceHttp1() ? "HTTP/1.1 only"
					: route.listenPort() > 0 || !route.rewriteHost() ? "" : "Rewrites Host";
			return Candidate.builder()
				.clientId(route.clientId())
				.address(route.address())
				.role(role)
				.note(note)
				.build();
		}).toList();
	}

	Cluster cluster() {
		String self = this.properties.node().id();
		return Cluster.builder()
			.enabled(this.properties.clusterEnabled())
			.membershipVersion(this.nodeDirectory.version())
			.nodes(this.nodeDirectory.nodes()
				.stream()
				.map(member -> NodeRow.builder()
					.nodeId(member.nodeId())
					.publicUrl(member.publicUrl())
					.self(member.nodeId().equals(self))
					.build())
				.toList())
			.build();
	}

	List<Setting> settings() {
		SluiceServerProperties.AccessLog accessLog = this.properties.accessLog();
		String tcpPortRange = this.properties.tcpPortRange();
		String tlsBundle = this.properties.dataTlsBundle();
		List<Setting> settings = new ArrayList<>(
				List.of(new Setting("Data plane", this.properties.dataHost() + ":" + this.dataProxyServer.boundPort()),
						new Setting("TLS termination", tlsBundle == null ? "Off" : "SSL bundle " + tlsBundle),
						new Setting("Control plane", this.controlPort()),
						new Setting("TCP port range", tcpPortRange.isBlank() ? "Any port" : tcpPortRange),
						new Setting("HTTP load balancing", label(this.router.httpLoadBalance())),
						new Setting("TCP load balancing", label(this.router.tcpLoadBalance())),
						new Setting("Access log",
								accessLog.enabled() ? accessLog.types()
									.stream()
									.map(type -> type.name().toLowerCase(Locale.ROOT))
									.sorted()
									.collect(Collectors.joining(", ")) : "Off")));
		if (this.properties.clusterEnabled()) {
			SluiceServerProperties.Cluster cluster = this.properties.cluster();
			settings.add(new Setting("Warm-up", Humanize.duration(cluster.warmup())));
			settings.add(new Setting("Drain grace", Humanize.duration(cluster.drainGrace())));
			settings.add(new Setting("Membership poll", Humanize.duration(cluster.membershipPoll())));
		}
		return List.copyOf(settings);
	}

	/**
	 * How the load balancing chooses among several candidates; empty for a single one.
	 */
	static String decision(List<Candidate> candidates) {
		if (candidates.size() < 2) {
			return "";
		}
		return switch (candidates.get(candidates.size() - 1).role()) {
			case "In rotation" -> "Requests rotate across these clients";
			case "Picked at random" -> "Each request goes to a client picked at random";
			default -> "Smallest client id wins";
		};
	}

	private static String label(LoadBalance loadBalance) {
		return switch (loadBalance) {
			case SMALLEST_CLIENT_ID -> "Smallest client id";
			case ROUND_ROBIN -> "Round robin";
			case RANDOM -> "Random";
		};
	}

	private List<TunnelSession> liveSessions() {
		return this.sessions.all().stream().sorted(Comparator.comparing(TunnelSession::clientId)).toList();
	}

	/**
	 * Header line: node identity and readiness.
	 *
	 * @param state readiness key used as a style hook ({@code accepting},
	 * {@code warming}, {@code draining})
	 */
	public record Status(String nodeId, String state, String stateLabel, String version, String uptime,
			String refreshedAt) {

		public static Builder builder() {
			return new Builder();
		}

		public static final class Builder {

			private @Nullable String nodeId;

			private @Nullable String state;

			private @Nullable String stateLabel;

			private String version = "";

			private @Nullable String uptime;

			private @Nullable String refreshedAt;

			private Builder() {
			}

			public Builder nodeId(String nodeId) {
				this.nodeId = nodeId;
				return this;
			}

			public Builder state(String state) {
				this.state = state;
				return this;
			}

			public Builder stateLabel(String stateLabel) {
				this.stateLabel = stateLabel;
				return this;
			}

			public Builder version(String version) {
				this.version = version;
				return this;
			}

			public Builder uptime(String uptime) {
				this.uptime = uptime;
				return this;
			}

			public Builder refreshedAt(String refreshedAt) {
				this.refreshedAt = refreshedAt;
				return this;
			}

			public Status build() {
				return new Status(Objects.requireNonNull(this.nodeId, "nodeId is required"),
						Objects.requireNonNull(this.state, "state is required"),
						Objects.requireNonNull(this.stateLabel, "stateLabel is required"), this.version,
						Objects.requireNonNull(this.uptime, "uptime is required"),
						Objects.requireNonNull(this.refreshedAt, "refreshedAt is required"));
			}

		}

	}

	/**
	 * One connected client.
	 */
	public record Client(String clientId, String remoteAddress, String connectedFor, String connectedAt,
			int openConnections, long connectionsOpened, String inbound, String outbound, boolean draining,
			List<UpstreamRow> upstreams) {

		public static Builder builder() {
			return new Builder();
		}

		public static final class Builder {

			private @Nullable String clientId;

			private String remoteAddress = "";

			private @Nullable String connectedFor;

			private @Nullable String connectedAt;

			private int openConnections;

			private long connectionsOpened;

			private String inbound = "0 B";

			private String outbound = "0 B";

			private boolean draining;

			private List<UpstreamRow> upstreams = List.of();

			private Builder() {
			}

			public Builder clientId(String clientId) {
				this.clientId = clientId;
				return this;
			}

			public Builder remoteAddress(String remoteAddress) {
				this.remoteAddress = remoteAddress;
				return this;
			}

			public Builder connectedFor(String connectedFor) {
				this.connectedFor = connectedFor;
				return this;
			}

			public Builder connectedAt(String connectedAt) {
				this.connectedAt = connectedAt;
				return this;
			}

			public Builder openConnections(int openConnections) {
				this.openConnections = openConnections;
				return this;
			}

			public Builder connectionsOpened(long connectionsOpened) {
				this.connectionsOpened = connectionsOpened;
				return this;
			}

			public Builder inbound(String inbound) {
				this.inbound = inbound;
				return this;
			}

			public Builder outbound(String outbound) {
				this.outbound = outbound;
				return this;
			}

			public Builder draining(boolean draining) {
				this.draining = draining;
				return this;
			}

			public Builder upstreams(List<UpstreamRow> upstreams) {
				this.upstreams = upstreams;
				return this;
			}

			public Client build() {
				return new Client(Objects.requireNonNull(this.clientId, "clientId is required"), this.remoteAddress,
						Objects.requireNonNull(this.connectedFor, "connectedFor is required"),
						Objects.requireNonNull(this.connectedAt, "connectedAt is required"), this.openConnections,
						this.connectionsOpened, this.inbound, this.outbound, this.draining,
						List.copyOf(this.upstreams));
			}

		}

	}

	/**
	 * One upstream advertised by a client.
	 *
	 * @param host public host ({@code ""} = catch-all)
	 * @param note short qualifier (host handling, or why the port is not bound)
	 * @param rejected whether the listen port was refused by this node
	 */
	public record UpstreamRow(String host, String target, String kind, String note, boolean rejected, String role) {

		public boolean catchAll() {
			return this.host.isEmpty();
		}

		public boolean serving() {
			return "Serving".equals(this.role);
		}

		public static Builder builder() {
			return new Builder();
		}

		public static final class Builder {

			private String host = "";

			private @Nullable String target;

			private @Nullable String kind;

			private String note = "";

			private boolean rejected;

			private String role = "";

			private Builder() {
			}

			public Builder role(String role) {
				this.role = role;
				return this;
			}

			public Builder host(String host) {
				this.host = host;
				return this;
			}

			public Builder target(String target) {
				this.target = target;
				return this;
			}

			public Builder kind(String kind) {
				this.kind = kind;
				return this;
			}

			public Builder note(String note) {
				this.note = note;
				return this;
			}

			public Builder rejected(boolean rejected) {
				this.rejected = rejected;
				return this;
			}

			public UpstreamRow build() {
				return new UpstreamRow(this.host, Objects.requireNonNull(this.target, "target is required"),
						Objects.requireNonNull(this.kind, "kind is required"), this.note, this.rejected, this.role);
			}

		}

	}

	/**
	 * The http routes registered under one domain.
	 *
	 * @param domain the domain ({@code ""} = catch-all)
	 */
	public record RouteRow(String domain, List<Candidate> candidates) {

		public boolean catchAll() {
			return this.domain.isEmpty();
		}

		public String decision() {
			return ConsoleView.decision(this.candidates);
		}

	}

	/**
	 * The tcp routes registered under one listen port.
	 *
	 * @param owner client whose listener is bound; empty when nothing is bound
	 */
	public record PortRow(String port, String owner, List<Candidate> candidates) {

		public boolean bound() {
			return !this.owner.isEmpty();
		}

		public String decision() {
			return ConsoleView.decision(this.candidates);
		}

		public static Builder builder() {
			return new Builder();
		}

		public static final class Builder {

			private @Nullable String port;

			private String owner = "";

			private List<Candidate> candidates = List.of();

			private Builder() {
			}

			public Builder port(String port) {
				this.port = port;
				return this;
			}

			public Builder owner(String owner) {
				this.owner = owner;
				return this;
			}

			public Builder candidates(List<Candidate> candidates) {
				this.candidates = candidates;
				return this;
			}

			public PortRow build() {
				return new PortRow(Objects.requireNonNull(this.port, "port is required"), this.owner,
						List.copyOf(this.candidates));
			}

		}

	}

	/**
	 * One registered target of a route.
	 *
	 * @param role how the load balancing treats it ({@code Serving}, {@code Standby},
	 * {@code In rotation}, {@code Picked at random})
	 */
	public record Candidate(String clientId, String address, String role, String note) {

		public boolean serving() {
			return "Serving".equals(this.role);
		}

		public static Builder builder() {
			return new Builder();
		}

		public static final class Builder {

			private @Nullable String clientId;

			private @Nullable String address;

			private @Nullable String role;

			private String note = "";

			private Builder() {
			}

			public Builder clientId(String clientId) {
				this.clientId = clientId;
				return this;
			}

			public Builder address(String address) {
				this.address = address;
				return this;
			}

			public Builder role(String role) {
				this.role = role;
				return this;
			}

			public Builder note(String note) {
				this.note = note;
				return this;
			}

			public Candidate build() {
				return new Candidate(Objects.requireNonNull(this.clientId, "clientId is required"),
						Objects.requireNonNull(this.address, "address is required"),
						Objects.requireNonNull(this.role, "role is required"), this.note);
			}

		}

	}

	/**
	 * Cluster membership as seen by this node.
	 */
	public record Cluster(boolean enabled, long membershipVersion, List<NodeRow> nodes) {

		public static Builder builder() {
			return new Builder();
		}

		public static final class Builder {

			private boolean enabled;

			private long membershipVersion;

			private List<NodeRow> nodes = List.of();

			private Builder() {
			}

			public Builder enabled(boolean enabled) {
				this.enabled = enabled;
				return this;
			}

			public Builder membershipVersion(long membershipVersion) {
				this.membershipVersion = membershipVersion;
				return this;
			}

			public Builder nodes(List<NodeRow> nodes) {
				this.nodes = nodes;
				return this;
			}

			public Cluster build() {
				return new Cluster(this.enabled, this.membershipVersion, List.copyOf(this.nodes));
			}

		}

	}

	/**
	 * One cluster member.
	 */
	public record NodeRow(String nodeId, String publicUrl, boolean self) {

		public static Builder builder() {
			return new Builder();
		}

		public static final class Builder {

			private @Nullable String nodeId;

			private String publicUrl = "";

			private boolean self;

			private Builder() {
			}

			public Builder nodeId(String nodeId) {
				this.nodeId = nodeId;
				return this;
			}

			public Builder publicUrl(String publicUrl) {
				this.publicUrl = publicUrl;
				return this;
			}

			public Builder self(boolean self) {
				this.self = self;
				return this;
			}

			public NodeRow build() {
				return new NodeRow(Objects.requireNonNull(this.nodeId, "nodeId is required"), this.publicUrl,
						this.self);
			}

		}

	}

	/**
	 * One effective configuration value.
	 */
	public record Setting(String name, String value) {

	}

	/**
	 * Result of a route lookup for a Host header value.
	 *
	 * @param matched the route key that matched ({@code ""} = catch-all); empty with no
	 * candidates when nothing matched
	 * @param how how the key matched the host
	 */
	public record Lookup(String host, String matched, String how, List<Candidate> candidates) {

		public boolean found() {
			return !this.candidates.isEmpty();
		}

		public boolean catchAll() {
			return this.found() && this.matched.isEmpty();
		}

		public String decision() {
			return ConsoleView.decision(this.candidates);
		}

		public static Builder builder() {
			return new Builder();
		}

		public static final class Builder {

			private @Nullable String host;

			private String matched = "";

			private String how = "";

			private List<Candidate> candidates = List.of();

			private Builder() {
			}

			public Builder host(String host) {
				this.host = host;
				return this;
			}

			public Builder matched(String matched) {
				this.matched = matched;
				return this;
			}

			public Builder how(String how) {
				this.how = how;
				return this;
			}

			public Builder candidates(List<Candidate> candidates) {
				this.candidates = candidates;
				return this;
			}

			public Lookup build() {
				return new Lookup(Objects.requireNonNull(this.host, "host is required"), this.matched, this.how,
						List.copyOf(this.candidates));
			}

		}

	}

}
