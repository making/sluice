package am.ik.sluice.client.config;

import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * A single upstream entry: the public host the server routes requests for (blank means
 * catch-all) or, instead of it, a regular expression the request host is matched against
 * ({@code host-pattern}; whole match, without the port), the target URL to dial, whether
 * the request Host header is rewritten to the target ({@code rewrite-host}), whether TLS
 * connections are relayed untouched ({@code tls-passthrough}) instead of terminated on
 * the data plane, the public port for raw TCP routing ({@code listen-port}; 0 disables
 * it), and the CIDRs / bare addresses allowed to connect on the data plane
 * ({@code allowed-cidrs}; empty = the server-wide allow list).
 */
public record Upstream(String host, @DefaultValue("") String hostPattern, String target,
		@DefaultValue("false") boolean rewriteHost, boolean tlsPassthrough, int listenPort,
		@DefaultValue List<String> allowedCidrs) {

	public Upstream {
		host = host == null ? "" : host.trim();
		hostPattern = hostPattern == null ? "" : hostPattern.trim();
		if (!hostPattern.isEmpty()) {
			try {
				Pattern.compile(hostPattern);
			}
			catch (PatternSyntaxException e) {
				throw new IllegalArgumentException("invalid host-pattern: " + hostPattern, e);
			}
		}
		target = normalizeTarget(target);
		listenPort = Math.max(0, listenPort);
		allowedCidrs = allowedCidrs == null ? List.of() : List.copyOf(allowedCidrs);
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

		private String hostPattern = "";

		private String target = "";

		private boolean rewriteHost;

		private boolean tlsPassthrough;

		private int listenPort;

		private List<String> allowedCidrs = List.of();

		private Builder() {
		}

		public Builder host(String host) {
			this.host = host;
			return this;
		}

		public Builder hostPattern(String hostPattern) {
			this.hostPattern = hostPattern;
			return this;
		}

		public Builder target(String target) {
			this.target = target;
			return this;
		}

		public Builder rewriteHost(boolean rewriteHost) {
			this.rewriteHost = rewriteHost;
			return this;
		}

		public Builder tlsPassthrough(boolean tlsPassthrough) {
			this.tlsPassthrough = tlsPassthrough;
			return this;
		}

		public Builder listenPort(int listenPort) {
			this.listenPort = listenPort;
			return this;
		}

		public Builder allowedCidrs(List<String> allowedCidrs) {
			this.allowedCidrs = allowedCidrs;
			return this;
		}

		public Upstream build() {
			return new Upstream(this.host, this.hostPattern, Objects.requireNonNull(this.target, "target is required"),
					this.rewriteHost, this.tlsPassthrough, this.listenPort, this.allowedCidrs);
		}

	}

}
