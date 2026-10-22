package am.ik.sluice.server.proxy;

import java.net.InetAddress;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.SequencedMap;
import java.util.Set;

import org.jspecify.annotations.Nullable;

import am.ik.sluice.server.auth.Origin;
import am.ik.sluice.server.auth.RequestHeaders;

/**
 * How a client connection reached the data plane, told to the upstream by the forwarding
 * headers of every request relayed on it (HTTP/1.1 and h2), so the upstream can rebuild
 * the URL the client used although TLS ended here or the host was rewritten
 * ({@code rewrite-host}): {@code X-Forwarded-Proto} ({@code https} on a TLS-terminated
 * connection, {@code http} otherwise), {@code X-Forwarded-Host} (the {@code Host} /
 * {@code :authority} the client sent), {@code X-Forwarded-Port} (the port of that host,
 * else the default port of the protocol) and {@code X-Forwarded-For} (the peer appended
 * to the chain). A peer not trusted as a proxy ({@code trusted-proxy-cidrs}) cannot claim
 * them: what it sent of the first three, of {@code X-Forwarded-Prefix} and of
 * {@code Forwarded} is dropped. A trusted proxy's values are kept and the missing ones
 * added, the port defaulting by the protocol it stated. The same values make the
 * {@link #origin} the browser sign-in returns to.
 *
 * @param secure whether the data plane terminated TLS for the connection
 * @param peer the connection peer; null when unknown
 * @param trustedPeer whether the peer is a trusted proxy
 */
record Forwarding(boolean secure, @Nullable InetAddress peer, boolean trustedPeer) {

	static final String PROTO = "x-forwarded-proto";

	static final String HOST = "x-forwarded-host";

	static final String PORT = "x-forwarded-port";

	static final String FOR = "x-forwarded-for";

	/** Dropped from an untrusted peer's requests besides the headers set. */
	private static final List<String> UNTRUSTED = List.of(PROTO, HOST, PORT, "x-forwarded-prefix", "forwarded");

	/**
	 * The forwarding headers of a request.
	 * @param request the request's headers as the client sent them
	 * @param host the {@code Host} / {@code :authority} the client sent; null when none
	 */
	Headers headers(RequestHeaders request, @Nullable String host) {
		Set<String> removed = new LinkedHashSet<>();
		SequencedMap<String, String> added = new LinkedHashMap<>();
		if (!this.trustedPeer) {
			removed.addAll(UNTRUSTED);
		}
		String proto = this.trustedPeer ? first(request.values(PROTO)) : null;
		if (proto == null) {
			proto = this.secure ? "https" : "http";
			added.put(PROTO, proto);
		}
		String forwardedHost = this.trustedPeer ? first(request.values(HOST)) : null;
		if (forwardedHost == null && host != null && !host.isBlank()) {
			forwardedHost = host.trim();
			added.put(HOST, forwardedHost);
		}
		if (!this.trustedPeer || first(request.values(PORT)) == null) {
			added.put(PORT, portOf(forwardedHost, proto));
		}
		InetAddress peer = this.peer;
		if (peer != null) {
			List<String> chain = request.values(FOR).stream().filter(value -> !value.isBlank()).toList();
			String address = peer.getHostAddress();
			added.put(FOR, chain.isEmpty() ? address : String.join(", ", chain) + ", " + address);
		}
		removed.addAll(added.keySet());
		return new Headers(removed, added);
	}

	/**
	 * The origin the client addressed: https when TLS ended here or a trusted proxy
	 * stated it, and the host the client sent -- a trusted proxy's
	 * {@code X-Forwarded-Host}, with its {@code X-Forwarded-Port} when the host names
	 * none and the port is not the protocol's default.
	 * @param request the request's headers as the client sent them
	 * @param host the {@code Host} / {@code :authority} the client sent; null when none
	 */
	Origin origin(RequestHeaders request, @Nullable String host) {
		String proto = this.trustedPeer ? first(request.values(PROTO)) : null;
		boolean secure = proto == null ? this.secure : "https".equalsIgnoreCase(proto);
		String authority = this.trustedPeer ? first(request.values(HOST)) : null;
		if (authority == null) {
			authority = host == null || host.isBlank() ? null : host.trim();
		}
		else {
			String port = first(request.values(PORT));
			if (port != null && explicitPort(authority) == null && !port.equals(secure ? "443" : "80")) {
				authority = authority + ":" + port;
			}
		}
		return new Origin(secure, authority);
	}

	/** The first element of a header's values; null when none. */
	private static @Nullable String first(List<String> values) {
		for (String value : values) {
			int comma = value.indexOf(',');
			String element = (comma < 0 ? value : value.substring(0, comma)).trim();
			if (!element.isEmpty()) {
				return element;
			}
		}
		return null;
	}

	/** The port of the host, else the default port of the protocol. */
	private static String portOf(@Nullable String host, String proto) {
		String port = host == null ? null : explicitPort(host);
		if (port != null) {
			return port;
		}
		return "https".equals(proto.toLowerCase(Locale.ROOT)) ? "443" : "80";
	}

	/** The port the host names; null when it names none. */
	private static @Nullable String explicitPort(String host) {
		int colon = host.lastIndexOf(':');
		// not an IPv6 literal's colon
		if (colon > host.lastIndexOf(']') && colon < host.length() - 1
				&& host.substring(colon + 1).chars().allMatch(c -> c >= '0' && c <= '9')) {
			return host.substring(colon + 1);
		}
		return null;
	}

	/**
	 * The forwarding headers of a request: the lowercase names whose fields are removed,
	 * and the fields added after them.
	 */
	record Headers(Set<String> removed, SequencedMap<String, String> added) {

	}

}
