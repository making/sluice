package am.ik.sluice.client.config;

import java.util.Objects;

/**
 * A single upstream entry: the public host the server routes requests for (blank means
 * catch-all), the target URL to dial, whether the request Host header passes through, and
 * whether TLS connections are relayed untouched ({@code tls-passthrough}) instead of
 * terminated on the data plane.
 */
public record Upstream(String host, String target, boolean preserveHost, boolean tlsPassthrough) {

	public Upstream {
		host = host == null ? "" : host.trim();
		target = normalizeTarget(target);
	}

	/**
	 * Creates an entry that preserves the Host header (the default).
	 */
	public static Upstream of(String host, String target) {
		return new Upstream(host, target, true, false);
	}

	private static String normalizeTarget(String target) {
		String trimmed = target == null ? "" : target.trim();
		boolean hasScheme = trimmed.matches("[a-zA-Z][a-zA-Z0-9+.\\-]*://.*");
		return hasScheme || trimmed.isEmpty() ? trimmed : "http://" + trimmed;
	}

	public static Builder builder() {
		return new Builder();
	}

	public static final class Builder {

		private String host = "";

		private String target = "";

		private boolean preserveHost = true;

		private boolean tlsPassthrough;

		private Builder() {
		}

		public Builder host(String host) {
			this.host = host;
			return this;
		}

		public Builder target(String target) {
			this.target = target;
			return this;
		}

		public Builder preserveHost(boolean preserveHost) {
			this.preserveHost = preserveHost;
			return this;
		}

		public Builder tlsPassthrough(boolean tlsPassthrough) {
			this.tlsPassthrough = tlsPassthrough;
			return this;
		}

		public Upstream build() {
			return new Upstream(this.host, Objects.requireNonNull(this.target, "target is required"), this.preserveHost,
					this.tlsPassthrough);
		}

	}

}
