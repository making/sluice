package am.ik.sluice.client.config;

import java.util.Objects;

/**
 * A single upstream entry: the public host the server routes requests for (blank means
 * catch-all), the target URL to dial, and whether the request Host header is preserved.
 */
public record Upstream(String host, String target, boolean preserveHost) {

	public Upstream {
		host = host == null ? "" : host.trim();
		target = normalizeTarget(target);
	}

	/**
	 * Creates an entry that preserves the Host header (the default).
	 */
	public static Upstream of(String host, String target) {
		return new Upstream(host, target, true);
	}

	private static String normalizeTarget(String target) {
		String trimmed = target == null ? "" : target.trim();
		boolean hasScheme = trimmed.startsWith("http://") || trimmed.startsWith("https://");
		return hasScheme || trimmed.isEmpty() ? trimmed : "http://" + trimmed;
	}

	public static Builder builder() {
		return new Builder();
	}

	public static final class Builder {

		private String host = "";

		private String target = "";

		private boolean preserveHost = true;

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

		public Upstream build() {
			return new Upstream(this.host, Objects.requireNonNull(this.target, "target is required"),
					this.preserveHost);
		}

	}

}
