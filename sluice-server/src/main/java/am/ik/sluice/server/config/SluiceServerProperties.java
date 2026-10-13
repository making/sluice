package am.ik.sluice.server.config;

import java.time.Duration;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import org.jspecify.annotations.Nullable;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.util.unit.DataSize;

import am.ik.sluice.server.route.LoadBalance;

/**
 * Configuration of the sluice server.
 *
 * @param tcpPortRange listen ports a client may claim for tcp routes (comma separated
 * single ports or {@code min-max} ranges, e.g. {@code 9000-9010,8080}; empty = any port)
 * @param caBundle SSL bundle naming the CA ({@code null} = console issuance disabled)
 * parsed on the data plane: the header is stripped before routing / relay and its source
 * address becomes the connection peer (access control, access log); headerless
 * connections are unaffected, a malformed one fails the connection
 * @param httpLoadBalance load balancing applied when several clients serve the same
 * domain (http routes)
 * @param console management console settings (authentication)
 * @param tcpLoadBalance load balancing applied when several clients serve the same listen
 * port (tcp routes)
 * @param limits data plane connection limits and the head deadline
 */
@ConfigurationProperties("sluice")
public record SluiceServerProperties(String token, @Nullable String tokenFile, @DefaultValue("0.0.0.0") String dataHost,
		@DefaultValue("8000") int dataPort, @Nullable String dataTlsBundle, @Nullable String caBundle,
		@DefaultValue("false") boolean proxyProtocol, @DefaultValue("") String tcpPortRange, AccessLog accessLog,
		AccessControl accessControl, Node node, Cluster cluster, Console console,
		@DefaultValue("smallest-client-id") LoadBalance httpLoadBalance,
		@DefaultValue("smallest-client-id") LoadBalance tcpLoadBalance, Limits limits) {

	public SluiceServerProperties {
		dataHost = dataHost == null || dataHost.isBlank() ? "0.0.0.0" : dataHost;
		accessLog = accessLog == null ? AccessLog.builder().build() : accessLog;
		accessControl = accessControl == null ? AccessControl.builder().build() : accessControl;
		node = node == null ? Node.builder().build() : node;
		cluster = cluster == null ? Cluster.builder().build() : cluster;
		console = console == null ? Console.builder().build() : console;
		limits = limits == null ? Limits.builder().build() : limits;
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
	 * @param trustedProxyCidrs CIDRs / bare addresses of proxying load balancers whose
	 * {@code X-Forwarded-For} is trusted: when the connection peer matches, the rightmost
	 * header entry (the one the proxy appended) is judged instead of the peer address
	 */
	public record AccessControl(@DefaultValue List<String> allowCidrs, @DefaultValue List<String> denyCidrs,
			@DefaultValue List<String> trustedProxyCidrs) {

		public AccessControl {
			allowCidrs = allowCidrs == null ? List.of() : List.copyOf(allowCidrs);
			denyCidrs = denyCidrs == null ? List.of() : List.copyOf(denyCidrs);
			trustedProxyCidrs = trustedProxyCidrs == null ? List.of() : List.copyOf(trustedProxyCidrs);
		}

		public static Builder builder() {
			return new Builder();
		}

		public static final class Builder {

			private List<String> allowCidrs = List.of();

			private List<String> denyCidrs = List.of();

			private List<String> trustedProxyCidrs = List.of();

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

			public Builder trustedProxyCidrs(List<String> trustedProxyCidrs) {
				this.trustedProxyCidrs = trustedProxyCidrs;
				return this;
			}

			public AccessControl build() {
				return new AccessControl(this.allowCidrs, this.denyCidrs, this.trustedProxyCidrs);
			}

		}

	}

	/**
	 * Data plane connection limits, shared by the data port and every tcp route port.
	 *
	 * @param maxConnections connections open at once (an h2 connection counts once per
	 * upstream connection open at once); {@code 0} = derived: {@code connectionMemory}
	 * over what one connection reserves at accept
	 * @param maxConnectionsPerAddress connections open at once from one source address
	 * (the PROXY protocol source when {@code proxy-protocol} is on, the socket peer
	 * otherwise); {@code 0} = no limit beyond {@code maxConnections}
	 * @param headTimeout deadline for the whole head phase of a connection (PROXY header,
	 * TLS handshake, request head), and for a later request head of a plaintext or
	 * TLS-terminated HTTP/1.1 connection left incomplete between requests
	 * @param idleTimeout how long a plaintext or TLS-terminated HTTP connection may idle
	 * between requests -- HTTP/1.1 with every response complete and no next request
	 * begun, h2 with no stream open -- before it is closed (h2: with a GOAWAY
	 * {@code NO_ERROR}); an upgraded connection has none (it is relayed verbatim: see
	 * {@code verbatimIdleTimeout})
	 * @param h2QueueBudget bytes the h2 demux relays may queue beyond each connection's
	 * own allowance, both ways, plus the connection window their clients and upstreams
	 * hold past 65535 bytes, shared by all of them, one connection taking at most 2MiB
	 * before its windows are withheld; a window grows past 65535 bytes only as far as the
	 * budget takes it; {@code 0} = an eighth of half the maximum heap
	 * @param h2StreamWaitTimeout how long an h2 stream may wait for a place within its
	 * upstream's {@code SETTINGS_MAX_CONCURRENT_STREAMS} before the relay refuses it
	 * ({@code REFUSED_STREAM}); {@code 0} = off
	 * @param halfCloseProbeInterval how long a plaintext or TLS-terminated HTTP
	 * connection whose client ended its side stays quiet, a response owed, before the
	 * relay probes whether the client is still there: an HTTP/1.1 one, none of the
	 * response relayed, with a {@code 100 Continue} (at most four per response, never to
	 * an HTTP/1.0 request), an h2 one with a PING; {@code 0} = off
	 * @param stallTimeout how long a request or response in flight on a plaintext or
	 * TLS-terminated HTTP connection may wait on its client -- to accept the relay's
	 * writes, to grant h2 window for them, or to send request bytes due -- without
	 * {@code stallMinBytes} moving before the connection is closed (h2: with a GOAWAY
	 * {@code ENHANCE_YOUR_CALM}); waiting for an upstream does not count
	 * @param stallMinBytes the bytes moved to or from the client that count as progress
	 * against {@code stallTimeout}: less within it is a stall (a minimum rate)
	 * @param tunnelStallTimeout how long a tunnel stream may hold frames queued for its
	 * client while not ready, the client taking none of them, before the session is
	 * failed -- its stream reset, its routes removed and the slots its connections hold
	 * freed; {@code 0} = off
	 * @param verbatimIdleTimeout how long a connection relayed verbatim -- TLS
	 * passthrough, a tcp route, a hostless connection, an upgraded HTTP/1.1 connection --
	 * may relay no byte in either direction before it is closed, for the routes whose
	 * upstream sets no {@code verbatim-idle-timeout} of its own; {@code 0} = off
	 * @param advertiseTimeout how long a tunnel stream may go without the client's first
	 * {@code Advertise} before it is failed ({@code DEADLINE_EXCEEDED}); until then the
	 * stream serves no connection and leaves the client id's live session in place
	 * @param maxPendingTunnelStreams tunnel streams whose client has not sent its first
	 * {@code Advertise} yet, at once; a stream beyond it is failed at once
	 * ({@code RESOURCE_EXHAUSTED}); a non-positive value falls back to the default
	 * @param maxPendingTunnelStreamsPerAddress the same from one source address (the gRPC
	 * peer); {@code 0} = no limit beyond {@code maxPendingTunnelStreams}
	 * @param connectionMemory bytes the data plane's connections may reserve for their
	 * worst-case buffering at once, the h2 queue budget aside: each connection reserves
	 * what its tunnel connection buffers at accept, an h2 connection what its relay holds
	 * outside the budget too, each further upstream connection of an h2 connection both;
	 * {@code 0} = half the maximum heap less the h2 queue budget and the tunnel budget,
	 * or no bound with {@code maxConnections} configured
	 * @param overflowConnections connections past the connection limit served at once far
	 * enough to be answered {@code 503} with {@code Retry-After} (on the data port: the
	 * TLS handshake of a terminated route, the head), then closed; a connection past them
	 * is closed at accept; {@code 0} = every connection past the limit is closed at
	 * accept
	 * @param overflowTimeout one deadline for all of an overflow connection -- PROXY
	 * header, TLS handshake, head, answer -- after which it is closed whatever it reached
	 * @param overflowRetryAfter the {@code Retry-After} of the {@code 503} answering an
	 * overflow connection, in whole seconds
	 * @param tunnelBudget bytes the tunnel connections may buffer beyond each
	 * connection's allowance, shared by all of them: the receive windows grown past the
	 * initial 64KiB, and the frames a connection queues for the tunnel stream past one;
	 * {@code 0} = an eighth of half the maximum heap
	 */
	public record Limits(@DefaultValue("0") int maxConnections, @DefaultValue("0") int maxConnectionsPerAddress,
			@DefaultValue("10s") Duration headTimeout, @DefaultValue("60s") Duration idleTimeout,
			@DefaultValue("0") DataSize h2QueueBudget, @DefaultValue("30s") Duration h2StreamWaitTimeout,
			@DefaultValue("1s") Duration halfCloseProbeInterval, @DefaultValue("60s") Duration stallTimeout,
			@DefaultValue("16KB") DataSize stallMinBytes, @DefaultValue("60s") Duration tunnelStallTimeout,
			@DefaultValue("0") Duration verbatimIdleTimeout, @DefaultValue("10s") Duration advertiseTimeout,
			@DefaultValue("256") int maxPendingTunnelStreams, @DefaultValue("0") int maxPendingTunnelStreamsPerAddress,
			@DefaultValue("1024") int maxTunnelSessions, @DefaultValue("0") int maxTunnelSessionsPerAddress,
			@DefaultValue("0") DataSize connectionMemory, @DefaultValue("8") int overflowConnections,
			@DefaultValue("5s") Duration overflowTimeout, @DefaultValue("5s") Duration overflowRetryAfter,
			@DefaultValue("0") DataSize tunnelBudget) {

		/** The default of {@code maxPendingTunnelStreams}. */
		public static final int DEFAULT_MAX_PENDING_TUNNEL_STREAMS = 256;

		/** The default of {@code maxTunnelSessions}. */
		public static final int DEFAULT_MAX_TUNNEL_SESSIONS = 1024;

		/** The default of {@code overflowConnections}. */
		public static final int DEFAULT_OVERFLOW_CONNECTIONS = 8;

		public Limits {
			maxConnections = Math.max(0, maxConnections);
			maxConnectionsPerAddress = Math.max(0, maxConnectionsPerAddress);
			headTimeout = headTimeout == null || headTimeout.isNegative() || headTimeout.isZero()
					? Duration.ofSeconds(10) : headTimeout;
			idleTimeout = idleTimeout == null || idleTimeout.isNegative() || idleTimeout.isZero()
					? Duration.ofSeconds(60) : idleTimeout;
			h2QueueBudget = h2QueueBudget == null || h2QueueBudget.isNegative() ? DataSize.ofBytes(0) : h2QueueBudget;
			h2StreamWaitTimeout = h2StreamWaitTimeout == null || h2StreamWaitTimeout.isNegative()
					? Duration.ofSeconds(30) : h2StreamWaitTimeout;
			halfCloseProbeInterval = halfCloseProbeInterval == null || halfCloseProbeInterval.isNegative()
					? Duration.ofSeconds(1) : halfCloseProbeInterval;
			stallTimeout = stallTimeout == null || stallTimeout.isNegative() || stallTimeout.isZero()
					? Duration.ofSeconds(60) : stallTimeout;
			stallMinBytes = stallMinBytes == null || stallMinBytes.toBytes() <= 0 ? DataSize.ofKilobytes(16)
					: stallMinBytes;
			tunnelStallTimeout = tunnelStallTimeout == null || tunnelStallTimeout.isNegative() ? Duration.ofSeconds(60)
					: tunnelStallTimeout;
			verbatimIdleTimeout = verbatimIdleTimeout == null || verbatimIdleTimeout.isNegative() ? Duration.ZERO
					: verbatimIdleTimeout;
			advertiseTimeout = advertiseTimeout == null || advertiseTimeout.isNegative() || advertiseTimeout.isZero()
					? Duration.ofSeconds(10) : advertiseTimeout;
			maxPendingTunnelStreams = maxPendingTunnelStreams > 0 ? maxPendingTunnelStreams
					: DEFAULT_MAX_PENDING_TUNNEL_STREAMS;
			maxPendingTunnelStreamsPerAddress = Math.max(0, maxPendingTunnelStreamsPerAddress);
			maxTunnelSessions = maxTunnelSessions > 0 ? maxTunnelSessions : DEFAULT_MAX_TUNNEL_SESSIONS;
			maxTunnelSessionsPerAddress = Math.max(0, maxTunnelSessionsPerAddress);
			connectionMemory = connectionMemory == null || connectionMemory.isNegative() ? DataSize.ofBytes(0)
					: connectionMemory;
			overflowConnections = Math.max(0, overflowConnections);
			overflowTimeout = overflowTimeout == null || overflowTimeout.isNegative() || overflowTimeout.isZero()
					? Duration.ofSeconds(5) : overflowTimeout;
			overflowRetryAfter = overflowRetryAfter == null || overflowRetryAfter.isNegative() ? Duration.ofSeconds(5)
					: overflowRetryAfter;
			tunnelBudget = tunnelBudget == null || tunnelBudget.isNegative() ? DataSize.ofBytes(0) : tunnelBudget;
		}

		public static Builder builder() {
			return new Builder();
		}

		public static final class Builder {

			private int maxConnections;

			private int maxConnectionsPerAddress;

			@Nullable private Duration headTimeout;

			@Nullable private Duration idleTimeout;

			@Nullable private DataSize h2QueueBudget;

			@Nullable private Duration h2StreamWaitTimeout;

			@Nullable private Duration halfCloseProbeInterval;

			@Nullable private Duration stallTimeout;

			@Nullable private DataSize stallMinBytes;

			@Nullable private Duration tunnelStallTimeout;

			private Duration verbatimIdleTimeout = Duration.ZERO;

			@Nullable private Duration advertiseTimeout;

			private int maxPendingTunnelStreams = DEFAULT_MAX_PENDING_TUNNEL_STREAMS;

			private int maxPendingTunnelStreamsPerAddress;

			private int maxTunnelSessions = DEFAULT_MAX_TUNNEL_SESSIONS;

			private int maxTunnelSessionsPerAddress;

			@Nullable private DataSize connectionMemory;

			private int overflowConnections = DEFAULT_OVERFLOW_CONNECTIONS;

			@Nullable private Duration overflowTimeout;

			@Nullable private Duration overflowRetryAfter;

			@Nullable private DataSize tunnelBudget;

			private Builder() {
			}

			public Builder maxConnections(int maxConnections) {
				this.maxConnections = maxConnections;
				return this;
			}

			public Builder maxConnectionsPerAddress(int maxConnectionsPerAddress) {
				this.maxConnectionsPerAddress = maxConnectionsPerAddress;
				return this;
			}

			public Builder headTimeout(Duration headTimeout) {
				this.headTimeout = headTimeout;
				return this;
			}

			public Builder idleTimeout(Duration idleTimeout) {
				this.idleTimeout = idleTimeout;
				return this;
			}

			public Builder h2QueueBudget(DataSize h2QueueBudget) {
				this.h2QueueBudget = h2QueueBudget;
				return this;
			}

			public Builder h2StreamWaitTimeout(Duration h2StreamWaitTimeout) {
				this.h2StreamWaitTimeout = h2StreamWaitTimeout;
				return this;
			}

			public Builder halfCloseProbeInterval(Duration halfCloseProbeInterval) {
				this.halfCloseProbeInterval = halfCloseProbeInterval;
				return this;
			}

			public Builder stallTimeout(Duration stallTimeout) {
				this.stallTimeout = stallTimeout;
				return this;
			}

			public Builder stallMinBytes(DataSize stallMinBytes) {
				this.stallMinBytes = stallMinBytes;
				return this;
			}

			public Builder tunnelStallTimeout(Duration tunnelStallTimeout) {
				this.tunnelStallTimeout = tunnelStallTimeout;
				return this;
			}

			public Builder verbatimIdleTimeout(Duration verbatimIdleTimeout) {
				this.verbatimIdleTimeout = verbatimIdleTimeout;
				return this;
			}

			public Builder advertiseTimeout(Duration advertiseTimeout) {
				this.advertiseTimeout = advertiseTimeout;
				return this;
			}

			public Builder maxPendingTunnelStreams(int maxPendingTunnelStreams) {
				this.maxPendingTunnelStreams = maxPendingTunnelStreams;
				return this;
			}

			public Builder maxPendingTunnelStreamsPerAddress(int maxPendingTunnelStreamsPerAddress) {
				this.maxPendingTunnelStreamsPerAddress = maxPendingTunnelStreamsPerAddress;
				return this;
			}

			public Builder maxTunnelSessions(int maxTunnelSessions) {
				this.maxTunnelSessions = maxTunnelSessions;
				return this;
			}

			public Builder maxTunnelSessionsPerAddress(int maxTunnelSessionsPerAddress) {
				this.maxTunnelSessionsPerAddress = maxTunnelSessionsPerAddress;
				return this;
			}

			public Builder connectionMemory(DataSize connectionMemory) {
				this.connectionMemory = connectionMemory;
				return this;
			}

			public Builder overflowConnections(int overflowConnections) {
				this.overflowConnections = overflowConnections;
				return this;
			}

			public Builder overflowTimeout(Duration overflowTimeout) {
				this.overflowTimeout = overflowTimeout;
				return this;
			}

			public Builder overflowRetryAfter(Duration overflowRetryAfter) {
				this.overflowRetryAfter = overflowRetryAfter;
				return this;
			}

			public Builder tunnelBudget(DataSize tunnelBudget) {
				this.tunnelBudget = tunnelBudget;
				return this;
			}

			public Limits build() {
				return new Limits(this.maxConnections, this.maxConnectionsPerAddress,
						this.headTimeout == null ? Duration.ofSeconds(10) : this.headTimeout,
						this.idleTimeout == null ? Duration.ofSeconds(60) : this.idleTimeout,
						this.h2QueueBudget == null ? DataSize.ofBytes(0) : this.h2QueueBudget,
						this.h2StreamWaitTimeout == null ? Duration.ofSeconds(30) : this.h2StreamWaitTimeout,
						this.halfCloseProbeInterval == null ? Duration.ofSeconds(1) : this.halfCloseProbeInterval,
						this.stallTimeout == null ? Duration.ofSeconds(60) : this.stallTimeout,
						this.stallMinBytes == null ? DataSize.ofKilobytes(16) : this.stallMinBytes,
						this.tunnelStallTimeout == null ? Duration.ofSeconds(60) : this.tunnelStallTimeout,
						this.verbatimIdleTimeout,
						this.advertiseTimeout == null ? Duration.ofSeconds(10) : this.advertiseTimeout,
						this.maxPendingTunnelStreams, this.maxPendingTunnelStreamsPerAddress, this.maxTunnelSessions,
						this.maxTunnelSessionsPerAddress,
						this.connectionMemory == null ? DataSize.ofBytes(0) : this.connectionMemory,
						this.overflowConnections,
						this.overflowTimeout == null ? Duration.ofSeconds(5) : this.overflowTimeout,
						this.overflowRetryAfter == null ? Duration.ofSeconds(5) : this.overflowRetryAfter,
						this.tunnelBudget == null ? DataSize.ofBytes(0) : this.tunnelBudget);
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

		@Nullable private String caBundle;

		private boolean proxyProtocol;

		private String tcpPortRange = "";

		@Nullable private AccessLog accessLog;

		private AccessControl accessControl = AccessControl.builder().build();

		@Nullable private Node node;

		@Nullable private Cluster cluster;

		@Nullable private Console console;

		private LoadBalance httpLoadBalance = LoadBalance.SMALLEST_CLIENT_ID;

		private LoadBalance tcpLoadBalance = LoadBalance.SMALLEST_CLIENT_ID;

		@Nullable private Limits limits;

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

		public Builder caBundle(String caBundle) {
			this.caBundle = caBundle;
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

		public Builder limits(Limits limits) {
			this.limits = limits;
			return this;
		}

		public SluiceServerProperties build() {
			return new SluiceServerProperties(this.token == null ? "" : this.token, this.tokenFile,
					this.dataHost == null ? "0.0.0.0" : this.dataHost, this.dataPort, this.dataTlsBundle, this.caBundle,
					this.proxyProtocol, this.tcpPortRange,
					this.accessLog == null ? AccessLog.builder().build() : this.accessLog, this.accessControl,
					this.node == null ? Node.builder().build() : this.node,
					this.cluster == null ? Cluster.builder().build() : this.cluster,
					this.console == null ? Console.builder().build() : this.console, this.httpLoadBalance,
					this.tcpLoadBalance, this.limits == null ? Limits.builder().build() : this.limits);
		}

	}

}
