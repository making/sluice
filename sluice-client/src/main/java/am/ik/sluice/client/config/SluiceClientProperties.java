package am.ik.sluice.client.config;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.jspecify.annotations.Nullable;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Configuration of the sluice client.
 *
 * @param serverUrl tunnel server endpoint, {@code grpc://host:port} or
 * {@code grpcs://host:port} for TLS
 * @param client client tunnel settings, including the upstream entries
 * @param token authentication token
 * @param tokenFile file to read the authentication token from
 * @param insecure skip TLS verification
 * @param tlsBundle name of the SSL bundle providing the client keystore (mTLS client
 * certificate) and the truststore for the {@code grpcs://} control plane connection
 * @param strictForwarding only dial upstreams present in the upstream map
 * @param keepAliveTime interval of the gRPC keepalive ping towards the server
 * @param keepAliveTimeout how long a keepalive ping answer may take before the channel is
 * torn down
 */
@ConfigurationProperties("sluice")
public record SluiceClientProperties(String serverUrl, @Nullable Client client, String token,
		@Nullable String tokenFile, @DefaultValue("false") boolean insecure, @Nullable String tlsBundle,
		@DefaultValue("true") boolean strictForwarding, @DefaultValue("30s") Duration keepAliveTime,
		@DefaultValue("10s") Duration keepAliveTimeout) {

	/** Default of {@code sluice.client.tunnel-stall-timeout}. */
	static final Duration DEFAULT_STALL_TIMEOUT = Duration.ofSeconds(60);

	/**
	 * Tunnel settings configured under {@code sluice.client}.
	 *
	 * @param id stable client identity sent as {@code x-sluice-id} on every tunnel
	 * stream; blank = a random id is generated once per process
	 * @param upstream upstream entries, bound from
	 * {@code sluice.client.upstream[n].{host,host-pattern,target,rewrite-host,force-http1,listen-port,allowed-cidrs,verbatim-idle-timeout}}
	 * @param maxConnections upstream connections open at once over every server node; a
	 * CONNECT beyond it is answered with an ERROR without dialing. {@code 0} = derived
	 * from the maximum heap so the worst-case buffering of every connection fits in half
	 * of it
	 * @param tunnelStallTimeout how long a tunnel stream may hold frames queued for the
	 * server while not ready, the server taking none of them, before the stream is
	 * cancelled -- its connections failed, the slots they hold freed -- and reconnected;
	 * {@code 0} = off
	 */
	public record Client(@DefaultValue("") String id, @DefaultValue List<Upstream> upstream,
			@DefaultValue("0") int maxConnections, @DefaultValue("60s") Duration tunnelStallTimeout) {

		public Client {
			id = id == null ? "" : id;
			upstream = upstream == null ? List.of() : List.copyOf(upstream);
			maxConnections = Math.max(0, maxConnections);
			tunnelStallTimeout = tunnelStallTimeout == null || tunnelStallTimeout.isNegative() ? DEFAULT_STALL_TIMEOUT
					: tunnelStallTimeout;
		}

		public static Builder builder() {
			return new Builder();
		}

		public static final class Builder {

			private String id = "";

			private List<Upstream> upstream = List.of();

			private int maxConnections;

			private Duration tunnelStallTimeout = DEFAULT_STALL_TIMEOUT;

			private Builder() {
			}

			public Builder id(String id) {
				this.id = id;
				return this;
			}

			public Builder upstream(List<Upstream> upstream) {
				this.upstream = upstream;
				return this;
			}

			public Builder maxConnections(int maxConnections) {
				this.maxConnections = maxConnections;
				return this;
			}

			public Builder tunnelStallTimeout(Duration tunnelStallTimeout) {
				this.tunnelStallTimeout = tunnelStallTimeout;
				return this;
			}

			public Client build() {
				return new Client(this.id, this.upstream, this.maxConnections, this.tunnelStallTimeout);
			}

		}

	}

	/** The configured connection limit; {@code 0} = derived. */
	public int maxConnections() {
		return this.client == null ? 0 : this.client.maxConnections();
	}

	/** The configured tunnel stall timeout; {@code 0} = off. */
	public Duration tunnelStallTimeout() {
		return this.client == null ? DEFAULT_STALL_TIMEOUT : this.client.tunnelStallTimeout();
	}

	/**
	 * Resolves the effective token, reading the token file when configured.
	 */
	public String tokenValue() {
		if (this.tokenFile != null && !this.tokenFile.isBlank()) {
			try {
				// trailing new-lines are stripped to keep the setup foolproof
				return Files.readString(Path.of(this.tokenFile), StandardCharsets.UTF_8).stripTrailing();
			}
			catch (Exception e) {
				throw new IllegalStateException("unable to load token file: " + this.tokenFile, e);
			}
		}
		return this.token == null ? "" : this.token;
	}

	/**
	 * The configured upstream targets in declaration order, duplicates preserved: several
	 * upstreams may share one host (e.g. an http route and a tcp route with a listen
	 * port) and every entry stays dialable; the first match wins for identical host:port
	 * pairs.
	 */
	public List<String> upstreamTargets() {
		if (this.client == null) {
			return List.of();
		}
		return this.client.upstream().stream().map(Upstream::target).toList();
	}

	/**
	 * The configured upstreams in the proto form carried by the ADVERTISE frame.
	 */
	public List<am.ik.sluice.v1.proto.Upstream> toProtoUpstreams() {
		if (this.client == null) {
			return List.of();
		}
		return this.client.upstream()
			.stream()
			.map(upstream -> am.ik.sluice.v1.proto.Upstream.newBuilder()
				.setHost(upstream.host())
				.setHostPattern(upstream.hostPattern())
				.setTargetUrl(upstream.target())
				.setRewriteHost(upstream.rewriteHost())
				.setTlsPassthrough(upstream.tlsPassthrough())
				.setForceHttp1(upstream.forceHttp1())
				.setListenPort(upstream.listenPort())
				.addAllAllowedCidrs(upstream.allowedCidrs())
				.setVerbatimIdleTimeoutMillis(upstream.verbatimIdleTimeoutMillis())
				.build())
			.toList();
	}

	public static Builder builder() {
		return new Builder();
	}

	public static final class Builder {

		@Nullable private String serverUrl;

		private String clientId = "";

		private final List<Upstream> upstreams = new ArrayList<>();

		private int maxConnections;

		private Duration tunnelStallTimeout = DEFAULT_STALL_TIMEOUT;

		@Nullable private String token;

		@Nullable private String tokenFile;

		private boolean insecure;

		@Nullable private String tlsBundle;

		private boolean strictForwarding = true;

		private Duration keepAliveTime = Duration.ofSeconds(30);

		private Duration keepAliveTimeout = Duration.ofSeconds(10);

		private Builder() {
		}

		public Builder serverUrl(String serverUrl) {
			this.serverUrl = serverUrl;
			return this;
		}

		public Builder clientId(String clientId) {
			this.clientId = clientId;
			return this;
		}

		public Builder upstream(Upstream upstream) {
			this.upstreams.add(upstream);
			return this;
		}

		public Builder maxConnections(int maxConnections) {
			this.maxConnections = maxConnections;
			return this;
		}

		public Builder tunnelStallTimeout(Duration tunnelStallTimeout) {
			this.tunnelStallTimeout = tunnelStallTimeout;
			return this;
		}

		public Builder token(String token) {
			this.token = token;
			return this;
		}

		public Builder tokenFile(String tokenFile) {
			this.tokenFile = tokenFile;
			return this;
		}

		public Builder insecure(boolean insecure) {
			this.insecure = insecure;
			return this;
		}

		public Builder tlsBundle(@Nullable String tlsBundle) {
			this.tlsBundle = tlsBundle;
			return this;
		}

		public Builder strictForwarding(boolean strictForwarding) {
			this.strictForwarding = strictForwarding;
			return this;
		}

		public Builder keepAliveTime(Duration keepAliveTime) {
			this.keepAliveTime = keepAliveTime;
			return this;
		}

		public Builder keepAliveTimeout(Duration keepAliveTimeout) {
			this.keepAliveTimeout = keepAliveTimeout;
			return this;
		}

		public SluiceClientProperties build() {
			return new SluiceClientProperties(Objects.requireNonNull(this.serverUrl, "serverUrl is required"),
					Client.builder()
						.id(this.clientId)
						.upstream(List.copyOf(this.upstreams))
						.maxConnections(this.maxConnections)
						.tunnelStallTimeout(this.tunnelStallTimeout)
						.build(),
					this.token == null ? "" : this.token, this.tokenFile, this.insecure, this.tlsBundle,
					this.strictForwarding, this.keepAliveTime, this.keepAliveTimeout);
		}

	}

}
