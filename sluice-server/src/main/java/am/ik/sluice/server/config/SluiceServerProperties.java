package am.ik.sluice.server.config;

import org.jspecify.annotations.Nullable;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Configuration of the sluice server.
 */
@ConfigurationProperties("sluice")
public record SluiceServerProperties(String token, @Nullable String tokenFile, @DefaultValue("0.0.0.0") String dataHost,
		@DefaultValue("8000") int dataPort) {

	public SluiceServerProperties {
		dataHost = dataHost == null || dataHost.isBlank() ? "0.0.0.0" : dataHost;
	}

	public static Builder builder() {
		return new Builder();
	}

	public static final class Builder {

		@Nullable private String token;

		@Nullable private String tokenFile;

		@Nullable private String dataHost;

		private int dataPort = 8000;

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

		public SluiceServerProperties build() {
			return new SluiceServerProperties(this.token == null ? "" : this.token, this.tokenFile,
					this.dataHost == null ? "0.0.0.0" : this.dataHost, this.dataPort);
		}

	}

}
