package am.ik.sluice.server.config;

import java.time.Duration;
import java.util.EnumSet;
import java.util.Set;

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
		@DefaultValue("8000") int dataPort, @Nullable String dataTlsBundle, @DefaultValue("") String tcpPortRange,
		AccessLog accessLog) {

	public SluiceServerProperties {
		dataHost = dataHost == null || dataHost.isBlank() ? "0.0.0.0" : dataHost;
		accessLog = accessLog == null ? AccessLog.builder().build() : accessLog;
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

		@Nullable private AccessLog accessLog;

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

		public Builder accessLog(AccessLog accessLog) {
			this.accessLog = accessLog;
			return this;
		}

		public SluiceServerProperties build() {
			return new SluiceServerProperties(this.token == null ? "" : this.token, this.tokenFile,
					this.dataHost == null ? "0.0.0.0" : this.dataHost, this.dataPort, this.dataTlsBundle,
					this.tcpPortRange, this.accessLog == null ? AccessLog.builder().build() : this.accessLog);
		}

	}

}
