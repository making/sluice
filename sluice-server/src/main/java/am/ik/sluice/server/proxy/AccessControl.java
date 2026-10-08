package am.ik.sluice.server.proxy;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

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
 * only.
 */
@Component
public class AccessControl {

	private static final Logger log = LoggerFactory.getLogger(AccessControl.class);

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

	/**
	 * Whether a connection from the peer to the route may pass.
	 */
	public boolean allowed(Router.@Nullable Route route, @Nullable InetAddress peer) {
		return this.allowed(route, peer, null);
	}

	/**
	 * Whether a connection to the route may pass, judged by the address the connection is
	 * entitled to claim: the rightmost {@code forwardedFor} entry when the peer is a
	 * trusted proxy, the peer address itself otherwise. The deny-then-allow order is
	 * unchanged.
	 */
	public boolean allowed(Router.@Nullable Route route, @Nullable InetAddress peer, @Nullable String forwardedFor) {
		InetAddress claimed = this.claimedAddress(peer, forwardedFor);
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
	 * The address the connection may claim: the rightmost forwarded entry when the peer
	 * is a trusted proxy, the peer itself otherwise (also when no proxy is trusted, no
	 * header is present, or the entry is not a literal address -- a per-connection DNS
	 * lookup is avoided by falling back).
	 */
	private @Nullable InetAddress claimedAddress(@Nullable InetAddress peer, @Nullable String forwardedFor) {
		if (peer == null || forwardedFor == null || this.trustedProxies.isEmpty()) {
			return peer;
		}
		for (Cidr trustedProxy : this.trustedProxies) {
			if (!trustedProxy.matches(peer)) {
				continue;
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
