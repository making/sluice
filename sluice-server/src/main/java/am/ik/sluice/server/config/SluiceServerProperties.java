package am.ik.sluice.server.config;

import java.time.Duration;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import org.jspecify.annotations.Nullable;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import am.ik.sluice.server.route.LoadBalance;

/**
 * Configuration of the sluice server.
 *
 * @param tcpPortRange listen ports a client may claim for tcp routes (comma separated
 * single ports or {@code min-max} ranges, e.g. {@code 9000-9010,8080}; empty = any port)
 * @param proxyProtocol whether a PROXY protocol (v1 / v2) header preceding the payload is
 * parsed on the data plane: the header is stripped before routing / relay and its source
 * address becomes the connection peer (access control, access log); headerless
 * connections are unaffected, a malformed one fails the connection
 * @param httpLoadBalance load balancing applied when several clients serve the same
 * domain (http routes)
 * @param console management console settings (authentication)
 * @param tcpLoadBalance load balancing applied when several clients serve the same listen
 * port (tcp routes)
 */
@ConfigurationProperties("sluice")
public record SluiceServerProperties(String token, @Nullable String tokenFile, @DefaultValue("0.0.0.0") String dataHost,
		@DefaultValue("8000") int dataPort, @Nullable String dataTlsBundle,
		@DefaultValue("false") boolean proxyProtocol, @DefaultValue("") String tcpPortRange, AccessLog accessLog,
		AccessControl accessControl, Node node, Cluster cluster, Console console,
		@DefaultValue("smallest-client-id") LoadBalance httpLoadBalance,
		@DefaultValue("smallest-client-id") LoadBalance tcpLoadBalance) {

	public SluiceServerProperties {
		dataHost = dataHost == null || dataHost.isBlank() ? "0.0.0.0" : dataHost;
		accessLog = accessLog == null ? AccessLog.builder().build() : accessLog;
		accessControl = accessControl == null ? AccessControl.builder().build() : accessControl;
		node = node == null ? Node.builder().build() : node;
		cluster = cluster == null ? Cluster.builder().build() : cluster;
		console = console == null ? Console.builder().build() : console;
	}

	public boolean clusterEnabled() {
		return !this.cluster.nodes().isEmpty();
	}

	/**
	 * Identity of this server node within a cluster.
	 *
	 * @param id unique node id (defaults to the hostname); used in logs, metrics and the
	 * membership exchanged with clients
	 * @param publicUrl reachable-by-clients address of this node's control plane (e.g.
	 * {@code grpcs://sluice-0.tunnel.example.com}); empty = only reachable at its
	 * bootstrap address
	 */
	public record Node(@DefaultValue("") String id, @DefaultValue("") String publicUrl) {

		public Node {
			id = id == null || id.isBlank() ? defaultNodeId() : id;
			publicUrl = publicUrl == null ? "" : publicUrl;
		}

		/** Best-effort hostname; falls back to {@code unknown} when unresolvable. */
		public static String defaultNodeId() {
			try {
				String host = java.net.InetAddress.getLocalHost().getHostName();
				return host == null || host.isBlank() ? "unknown" : host;
			}
			catch (java.net.UnknownHostException e) {
				return "unknown";
			}
		}

		public static Builder builder() {
			return new Builder();
		}

		public static final class Builder {

			@Nullable private String id;

			@Nullable private String publicUrl;

			private Builder() {
			}

			public Builder id(String id) {
				this.id = id;
				return this;
			}

			public Builder publicUrl(String publicUrl) {
				this.publicUrl = publicUrl;
				return this;
			}

			public Node build() {
				return new Node(this.id == null ? "" : this.id, this.publicUrl == null ? "" : this.publicUrl);
			}

		}

	}

	/**
	 * Cluster settings.
	 *
	 * @param nodes cluster members as {@code nodeId=publicUrl} entries (comma separated;
	 * the {@code publicUrl} part is optional); empty = single-node mode
	 * @param warmup readiness stays down this long after start so clients can connect
	 * before the node enters load balancer rotation
	 * @param drainGrace how long to wait for in-flight virtual connections to finish
	 * during drain before closing the tunnel streams
	 * @param membershipPoll how often the membership is re-read from the
	 * {@code NodeDirectory} and pushed to connected clients on change
	 */
	public record Cluster(@DefaultValue("") List<String> nodes, @DefaultValue("10s") Duration warmup,
			@DefaultValue("10s") Duration drainGrace, @DefaultValue("10s") Duration membershipPoll) {

		public Cluster {
			nodes = nodes == null ? List.of() : List.copyOf(nodes);
			warmup = warmup == null || warmup.isNegative() ? Duration.ofSeconds(10) : warmup;
			drainGrace = drainGrace == null || drainGrace.isNegative() ? Duration.ofSeconds(10) : drainGrace;
			membershipPoll = membershipPoll == null || membershipPoll.isNegative() ? Duration.ofSeconds(10)
					: membershipPoll;
		}

		public static Builder builder() {
			return new Builder();
		}

		public static final class Builder {

			private List<String> nodes = List.of();

			@Nullable private Duration warmup;

			@Nullable private Duration drainGrace;

			@Nullable private Duration membershipPoll;

			private Builder() {
			}

			public Builder nodes(List<String> nodes) {
				this.nodes = nodes;
				return this;
			}

			public Builder warmup(Duration warmup) {
				this.warmup = warmup;
				return this;
			}

			public Builder drainGrace(Duration drainGrace) {
				this.drainGrace = drainGrace;
				return this;
			}

			public Builder membershipPoll(Duration membershipPoll) {
				this.membershipPoll = membershipPoll;
				return this;
			}

			public Cluster build() {
				return new Cluster(this.nodes, this.warmup == null ? Duration.ofSeconds(10) : this.warmup,
						this.drainGrace == null ? Duration.ofSeconds(10) : this.drainGrace,
						this.membershipPoll == null ? Duration.ofSeconds(10) : this.membershipPoll);
			}

		}

	}

	/**
	 * Access log settings.
	 *
	 * @param enabled whether access log lines are emitted to the {@code sluice.access}
	 * logger
	 * @param types event types to emit (comma separated in configuration): {@code conn}
	 * (connection accept/close), {@code request} (head request)
	 * @param rateLimit rate limit applied per line kind (syslog style: the first
	 * {@code maxRate} lines of each kind pass per {@code period}, further lines are
	 * suppressed and counted, and one summary line reports the suppressed count when the
	 * period rolls over)
	 */
	public record AccessLog(@DefaultValue("true") boolean enabled, @DefaultValue("connection,request") Set<Type> types,
			RateLimit rateLimit) {

		public enum Type {

			/** Connection accept/close lines. */
			CONNECTION,

			/** Head request lines. */
			REQUEST

		}

		public AccessLog {
			types = types == null || types.isEmpty() ? EnumSet.noneOf(Type.class) : EnumSet.copyOf(types);
			rateLimit = rateLimit == null ? new RateLimit(true, 10, Duration.ofSeconds(10)) : rateLimit;
		}

		public static Builder builder() {
			return new Builder();
		}

		public static final class Builder {

			@Nullable private Boolean enabled;

			@Nullable private Set<Type> types;

			@Nullable private RateLimit rateLimit;

			private Builder() {
			}

			public Builder enabled(boolean enabled) {
				this.enabled = enabled;
				return this;
			}

			public Builder types(Set<Type> types) {
				this.types = types;
				return this;
			}

			public Builder rateLimit(RateLimit rateLimit) {
				this.rateLimit = rateLimit;
				return this;
			}

			public AccessLog build() {
				return new AccessLog(this.enabled == null || this.enabled,
						this.types == null ? EnumSet.allOf(Type.class) : this.types,
						this.rateLimit == null ? RateLimit.builder().build() : this.rateLimit);
			}

		}

		/**
		 * Rate limit settings for the access log.
		 *
		 * @param enabled whether lines are rate limited (default true; disabled = every
		 * event logs)
		 * @param maxRate maximum lines emitted per line kind within one period; the line
		 * that reaches the limit is still emitted (syslog semantics)
		 * @param period window length, e.g. {@code 10s} / {@code 1m}
		 */
		public record RateLimit(@DefaultValue("true") boolean enabled, @DefaultValue("10") int maxRate,
				@DefaultValue("10s") Duration period) {

			public RateLimit {
				period = period == null || period.isNegative() || period.isZero() ? Duration.ofSeconds(10) : period;
				maxRate = Math.max(1, maxRate);
			}

			public static Builder builder() {
				return new Builder();
			}

			public static final class Builder {

				private boolean enabled = true;

				private int maxRate = 10;

				@Nullable private Duration period;

				private Builder() {
				}

				public Builder enabled(boolean enabled) {
					this.enabled = enabled;
					return this;
				}

				public Builder maxRate(int maxRate) {
					this.maxRate = maxRate;
					return this;
				}

				public Builder period(Duration period) {
					this.period = period;
					return this;
				}

				public RateLimit build() {
					return new RateLimit(this.enabled, this.maxRate,
							this.period == null ? Duration.ofSeconds(10) : this.period);
				}

			}

		}

	}

	/**
	 * Data plane IP access control.
	 *
	 * @param allowCidrs CIDRs / bare addresses allowed to connect on the data plane
	 * (comma separated in configuration); empty = every address is allowed. Overridden
	 * per route by the upstream's {@code allowed-cidrs}
	 * @param denyCidrs CIDRs / bare addresses rejected before any allow evaluation
	 */
	public record AccessControl(@DefaultValue List<String> allowCidrs, @DefaultValue List<String> denyCidrs) {

		public AccessControl {
			allowCidrs = allowCidrs == null ? List.of() : List.copyOf(allowCidrs);
			denyCidrs = denyCidrs == null ? List.of() : List.copyOf(denyCidrs);
		}

		public static Builder builder() {
			return new Builder();
		}

		public static final class Builder {

			private List<String> allowCidrs = List.of();

			private List<String> denyCidrs = List.of();

			private Builder() {
			}

			public Builder allowCidrs(List<String> allowCidrs) {
				this.allowCidrs = allowCidrs;
				return this;
			}

			public Builder denyCidrs(List<String> denyCidrs) {
				this.denyCidrs = denyCidrs;
				return this;
			}

			public AccessControl build() {
				return new AccessControl(this.allowCidrs, this.denyCidrs);
			}

		}

	}

	/**
	 * Management console settings.
	 *
	 * @param auth console authentication; the console is always authenticated, only the
	 * mechanism varies
	 */
	public record Console(Auth auth) {

		/**
		 * Console authentication mechanism.
		 *
		 * @param SIMPLE form login against {@code spring.security.user.*}
		 * @param OIDC OpenID Connect login against
		 * {@code spring.security.oauth2.client.*}
		 */
		public enum AuthType {

			/** Username / password form login. */
			SIMPLE,

			/** OpenID Connect login. */
			OIDC

		}

		/**
		 * Console authentication settings.
		 *
		 * @param type authentication mechanism ({@code simple} by default)
		 */
		public record Auth(@DefaultValue("simple") AuthType type) {

		}

		public static Builder builder() {
			return new Builder();
		}

		public static final class Builder {

			@Nullable private Auth auth;

			private Builder() {
			}

			public Builder auth(Auth auth) {
				this.auth = auth;
				return this;
			}

			public Console build() {
				return new Console(this.auth == null ? new Auth(AuthType.SIMPLE) : this.auth);
			}

		}

	}

	public static Builder builder() {
		return new Builder();
	}

	public static final class Builder {

		@Nullable private String token;

		@Nullable private String tokenFile;

		@Nullable private String dataHost;

		private int dataPort = 8000;

		@Nullable private String dataTlsBundle;

		private boolean proxyProtocol;

		private String tcpPortRange = "";

		@Nullable private AccessLog accessLog;

		private AccessControl accessControl = AccessControl.builder().build();

		@Nullable private Node node;

		@Nullable private Cluster cluster;

		@Nullable private Console console;

		private LoadBalance httpLoadBalance = LoadBalance.SMALLEST_CLIENT_ID;

		private LoadBalance tcpLoadBalance = LoadBalance.SMALLEST_CLIENT_ID;

		private Builder() {
		}

		public Builder token(String token) {
			this.token = token;
			return this;
		}

		public Builder tokenFile(String tokenFile) {
			this.tokenFile = tokenFile;
			return this;
		}

		public Builder dataHost(String dataHost) {
			this.dataHost = dataHost;
			return this;
		}

		public Builder dataPort(int dataPort) {
			this.dataPort = dataPort;
			return this;
		}

		public Builder dataTlsBundle(String dataTlsBundle) {
			this.dataTlsBundle = dataTlsBundle;
			return this;
		}

		public Builder proxyProtocol(boolean proxyProtocol) {
			this.proxyProtocol = proxyProtocol;
			return this;
		}

		public Builder tcpPortRange(String tcpPortRange) {
			this.tcpPortRange = tcpPortRange;
			return this;
		}

		public Builder accessLog(AccessLog accessLog) {
			this.accessLog = accessLog;
			return this;
		}

		public Builder accessControl(AccessControl accessControl) {
			this.accessControl = accessControl;
			return this;
		}

		public Builder node(Node node) {
			this.node = node;
			return this;
		}

		public Builder cluster(Cluster cluster) {
			this.cluster = cluster;
			return this;
		}

		public Builder console(Console console) {
			this.console = console;
			return this;
		}

		public Builder httpLoadBalance(LoadBalance httpLoadBalance) {
			this.httpLoadBalance = httpLoadBalance;
			return this;
		}

		public Builder tcpLoadBalance(LoadBalance tcpLoadBalance) {
			this.tcpLoadBalance = tcpLoadBalance;
			return this;
		}

		public SluiceServerProperties build() {
			return new SluiceServerProperties(this.token == null ? "" : this.token, this.tokenFile,
					this.dataHost == null ? "0.0.0.0" : this.dataHost, this.dataPort, this.dataTlsBundle,
					this.proxyProtocol, this.tcpPortRange,
					this.accessLog == null ? AccessLog.builder().build() : this.accessLog, this.accessControl,
					this.node == null ? Node.builder().build() : this.node,
					this.cluster == null ? Cluster.builder().build() : this.cluster,
					this.console == null ? Console.builder().build() : this.console, this.httpLoadBalance,
					this.tcpLoadBalance);
		}

	}

}
