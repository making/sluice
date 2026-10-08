package am.ik.sluice.server.proxy;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import org.jspecify.annotations.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import am.ik.sluice.server.config.SluiceServerProperties;
import am.ik.sluice.server.route.Router;

/**
 * Data plane IP access control (the HAProxy {@code tcp-request connection} counterpart):
 * the server-wide deny list is a blacklist applied to every connection, the allow side
 * uses the route's {@code allowed-cidrs} when it has one and the server-wide allow list
 * otherwise; an empty effective allow list means every address passes. An unreadable peer
 * address is allowed through (fail open). When the peer is a configured trusted proxy,
 * the rightmost {@code X-Forwarded-For} entry it appended is judged instead of the peer
 * address -- entries further left are client-supplied and part of the chain of trust
 * only. A request carrying the RFC 7239 {@code Forwarded} header is judged by its
 * rightmost literal {@code for=} instead: the proxy's explicit statement wins over
 * {@code X-Forwarded-For}.
 */
@Component
public class AccessControl {

	private static final Logger log = LoggerFactory.getLogger(AccessControl.class);

	/** A literal IPv4/IPv6 address (hostnames and obfuscated forms are excluded). */
	private static final Pattern LITERAL = Pattern.compile("[0-9a-fA-F.:]+");

	private final List<Cidr> deny;

	private final List<Cidr> allow;

	private final List<Cidr> trustedProxies;

	/** Compiled per-route allow lists, keyed by the configured literals. */
	private final ConcurrentHashMap<List<String>, List<Cidr>> routeCidrs = new ConcurrentHashMap<>();

	AccessControl(SluiceServerProperties properties) {
		SluiceServerProperties.AccessControl accessControl = properties.accessControl();
		this.deny = compile("deny-cidrs", accessControl.denyCidrs());
		this.allow = compile("allow-cidrs", accessControl.allowCidrs());
		this.trustedProxies = compile("trusted-proxy-cidrs", accessControl.trustedProxyCidrs());
	}

	public boolean allowed(Router.@Nullable Route route, @Nullable InetAddress peer, @Nullable String forwardedFor) {
		return this.allowed(route, peer, forwardedFor, null);
	}

	/**
	 * Whether a connection from the peer to the route may pass.
	 */
	public boolean allowed(Router.@Nullable Route route, @Nullable InetAddress peer) {
		return this.allowed(route, peer, null, null);
	}

	/**
	 * Whether a connection to the route may pass, judged by the address the connection is
	 * entitled to claim: the rightmost {@code forwardedFor} entry when the peer is a
	 * trusted proxy, the peer address itself otherwise. The deny-then-allow order is
	 * unchanged.
	 */
	public boolean allowed(Router.@Nullable Route route, @Nullable InetAddress peer, @Nullable String forwardedFor,
			@Nullable String forwarded) {
		InetAddress claimed = this.claimedAddress(peer, forwardedFor, forwarded);
		if (claimed == null) {
			return true;
		}
		for (Cidr cidr : this.deny) {
			if (cidr.matches(claimed)) {
				return false;
			}
		}
		List<String> configured = route == null ? List.of() : route.allowedCidrs();
		List<Cidr> allow = configured.isEmpty() ? this.allow
				: this.routeCidrs.computeIfAbsent(configured, cidrs -> compile("allowed-cidrs", cidrs));
		for (Cidr cidr : allow) {
			if (cidr.matches(claimed)) {
				return true;
			}
		}
		return allow.isEmpty();
	}

	/**
	 * The address the connection may claim: the rightmost {@code Forwarded} {@code for=}
	 * literal when the peer is a trusted proxy and carried the header, else the rightmost
	 * forwarded-for entry, the peer itself otherwise (also when no proxy is trusted, no
	 * header is present, or no entry is a literal address -- a per-connection DNS lookup
	 * is avoided by falling back).
	 */
	private @Nullable InetAddress claimedAddress(@Nullable InetAddress peer, @Nullable String forwardedFor,
			@Nullable String forwarded) {
		if (peer == null || this.trustedProxies.isEmpty()) {
			return peer;
		}
		for (Cidr trustedProxy : this.trustedProxies) {
			if (!trustedProxy.matches(peer)) {
				continue;
			}
			if (forwarded != null) {
				InetAddress literal = rightmostForwardedLiteral(forwarded);
				if (literal != null) {
					return literal;
				}
			}
			if (forwardedFor == null) {
				break;
			}
			String candidate = forwardedFor.substring(forwardedFor.lastIndexOf(',') + 1).trim();
			if (candidate.isEmpty()) {
				break;
			}
			try {
				return InetAddress.getByName(candidate);
			}
			catch (Exception e) {
				log.debug("ignoring malformed forwarded-for entry {}: {}", candidate, e.toString());
				break;
			}
		}
		return peer;
	}

	/**
	 * The rightmost {@code for=} parameter of a {@code Forwarded} header value that is a
	 * literal IP address; elements with a missing, obfuscated, or named {@code for=} are
	 * skipped per the RFC 7239 grammar.
	 */
	static @Nullable InetAddress rightmostForwardedLiteral(String forwarded) {
		String[] elements = forwarded.split(",");
		for (int i = elements.length - 1; i >= 0; i--) {
			for (String param : elements[i].split(";")) {
				int eq = param.indexOf('=');
				if (eq <= 0 || !"for".equalsIgnoreCase(param.substring(0, eq).trim())) {
					continue;
				}
				InetAddress literal = literalAddress(param.substring(eq + 1).trim());
				if (literal != null) {
					return literal;
				}
			}
		}
		return null;
	}

	/**
	 * The address of a {@code for=} value: quoted or bare, bracketed IPv6, optional port.
	 */
	private static @Nullable InetAddress literalAddress(String value) {
		String host = value;
		if (host.startsWith("\"") && host.endsWith("\"") && host.length() >= 2) {
			host = host.substring(1, host.length() - 1);
		}
		if (host.startsWith("[")) {
			int close = host.indexOf(']');
			if (close < 0) {
				return null;
			}
			host = host.substring(1, close);
		}
		else {
			int colon = host.indexOf(':');
			if (colon >= 0 && colon == host.lastIndexOf(':')) {
				host = host.substring(0, colon); // ipv4:port
			}
		}
		if (host.isEmpty() || !LITERAL.matcher(host).matches()) {
			return null;
		}
		try {
			return InetAddress.getByName(host);
		}
		catch (Exception e) {
			return null;
		}
	}

	private static List<Cidr> compile(String name, List<String> cidrs) {
		List<Cidr> compiled = new ArrayList<>(cidrs.size());
		for (String cidr : cidrs) {
			Optional<Cidr> parsed = Cidr.of(cidr);
			if (parsed.isEmpty()) {
				log.warn("ignoring malformed {} entry: {}", name, cidr);
			}
			else {
				compiled.add(parsed.get());
			}
		}
		return List.copyOf(compiled);
	}

}
