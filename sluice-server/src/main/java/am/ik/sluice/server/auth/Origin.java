package am.ik.sluice.server.auth;

import java.util.regex.Pattern;

import org.jspecify.annotations.Nullable;

/**
 * The origin a client addressed a request to, as the relay knows it: whether the client
 * used https -- the data plane terminated TLS, or a trusted proxy stated
 * {@code X-Forwarded-Proto: https} -- and the host, with its port if it named one. The
 * browser sign-in builds its redirect URI and the page it returns to from it.
 *
 * @param secure whether the client used https
 * @param authority the host and port the client named; null when none
 */
public record Origin(boolean secure, @Nullable String authority) {

	/** An origin the relay does not know: no sign-in begins at it. */
	public static final Origin UNKNOWN = new Origin(false, null);

	/** A DNS name or IPv4 address, or a bracketed IPv6 literal, with an optional port. */
	private static final Pattern AUTHORITY = Pattern
		.compile("(?:[A-Za-z0-9](?:[A-Za-z0-9.-]{0,251}[A-Za-z0-9])?|\\[[0-9A-Fa-f:.]{2,45}\\])(?::[0-9]{1,5})?");

	/**
	 * The origin as a URI, {@code https://host[:port]}; null when its authority is absent
	 * or no well-formed host and port.
	 */
	public @Nullable String uri() {
		String authority = this.authority;
		if (authority == null || !AUTHORITY.matcher(authority).matches()) {
			return null;
		}
		return (this.secure ? "https://" : "http://") + authority;
	}

}
