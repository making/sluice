package am.ik.sluice.client.config;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
 * @param strictForwarding only dial upstreams present in the upstream map
 */
@ConfigurationProperties("sluice")
public record SluiceClientProperties(String serverUrl, @Nullable Client client, String token,
		@Nullable String tokenFile, @DefaultValue("false") boolean insecure,
		@DefaultValue("true") boolean strictForwarding) {

	/**
	 * Tunnel settings configured under {@code sluice.client}.
	 *
	 * @param upstream upstream entries, bound from
	 * {@code sluice.client.upstream[n].{host,target,preserve-host}}
	 */
	public record Client(@DefaultValue List<Upstream> upstream) {

		public Client {
			upstream = upstream == null ? List.of() : List.copyOf(upstream);
		}

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
	 * The configured upstreams as a host to target map, preserving declaration order (a
	 * blank host is the catch-all entry).
	 */
	public Map<String, String> upstreamMap() {
		Map<String, String> map = new LinkedHashMap<>();
		if (this.client == null) {
			return map;
		}
		for (Upstream upstream : this.client.upstream()) {
			map.put(upstream.host(), upstream.target());
		}
		return map;
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
				.setTargetUrl(upstream.target())
				.setPreserveHost(upstream.preserveHost())
				.setTlsPassthrough(upstream.tlsPassthrough())
				.build())
			.toList();
	}

	public static Builder builder() {
		return new Builder();
	}

	public static final class Builder {

		@Nullable private String serverUrl;

		private final List<Upstream> upstreams = new ArrayList<>();

		@Nullable private String token;

		@Nullable private String tokenFile;

		private boolean insecure;

		private boolean strictForwarding = true;

		private Builder() {
		}

		public Builder serverUrl(String serverUrl) {
			this.serverUrl = serverUrl;
			return this;
		}

		public Builder upstream(Upstream upstream) {
			this.upstreams.add(upstream);
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

		public Builder strictForwarding(boolean strictForwarding) {
			this.strictForwarding = strictForwarding;
			return this;
		}

		public SluiceClientProperties build() {
			return new SluiceClientProperties(Objects.requireNonNull(this.serverUrl, "serverUrl is required"),
					new Client(List.copyOf(this.upstreams)), this.token == null ? "" : this.token, this.tokenFile,
					this.insecure, this.strictForwarding);
		}

	}

}
