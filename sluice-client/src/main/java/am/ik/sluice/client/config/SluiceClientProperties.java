package am.ik.sluice.client.config;

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
 * @param upstream comma separated {@code host=targetUrl} pairs
 * @param token authentication token
 * @param tokenFile file to read the authentication token from
 * @param insecure skip TLS verification
 * @param strictForwarding only dial upstreams present in the upstream map
 */
@ConfigurationProperties("sluice")
public record SluiceClientProperties(String serverUrl, String upstream, String token, @Nullable String tokenFile,
		@DefaultValue("false") boolean insecure, @DefaultValue("true") boolean strictForwarding) {

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

	public static Builder builder() {
		return new Builder();
	}

	public static final class Builder {

		@Nullable private String serverUrl;

		@Nullable private String upstream;

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

		public Builder upstream(String upstream) {
			this.upstream = upstream;
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
					Objects.requireNonNull(this.upstream, "upstream is required"), this.token == null ? "" : this.token,
					this.tokenFile, this.insecure, this.strictForwarding);
		}

	}

}
