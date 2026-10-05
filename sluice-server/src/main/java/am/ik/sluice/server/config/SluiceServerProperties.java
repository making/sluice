package am.ik.sluice.server.config;

import org.jspecify.annotations.Nullable;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Configuration of the sluice server.
 *
 * @param tcpPortRange listen ports a client may claim for tcp routes (comma separated
 * single ports or {@code min-max} ranges, e.g. {@code 9000-9010,8080}; empty = any port)
 */
@ConfigurationProperties("sluice")
public record SluiceServerProperties(String token, @Nullable String tokenFile, @DefaultValue("0.0.0.0") String dataHost,
		@DefaultValue("8000") int dataPort, @Nullable String dataTlsBundle, @DefaultValue("") String tcpPortRange) {

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

		@Nullable private String dataTlsBundle;

		private String tcpPortRange = "";

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

		public Builder tcpPortRange(String tcpPortRange) {
			this.tcpPortRange = tcpPortRange;
			return this;
		}

		public SluiceServerProperties build() {
			return new SluiceServerProperties(this.token == null ? "" : this.token, this.tokenFile,
					this.dataHost == null ? "0.0.0.0" : this.dataHost, this.dataPort, this.dataTlsBundle,
					this.tcpPortRange);
		}

	}

}
