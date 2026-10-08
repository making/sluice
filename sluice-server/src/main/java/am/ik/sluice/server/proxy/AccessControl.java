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
 * address is allowed through (fail open).
 */
@Component
public class AccessControl {

	private static final Logger log = LoggerFactory.getLogger(AccessControl.class);

	private final List<Cidr> deny;

	private final List<Cidr> allow;

	/** Compiled per-route allow lists, keyed by the configured literals. */
	private final ConcurrentHashMap<List<String>, List<Cidr>> routeCidrs = new ConcurrentHashMap<>();

	AccessControl(SluiceServerProperties properties) {
		SluiceServerProperties.AccessControl accessControl = properties.accessControl();
		this.deny = compile("deny-cidrs", accessControl.denyCidrs());
		this.allow = compile("allow-cidrs", accessControl.allowCidrs());
	}

	/**
	 * Whether a connection from the peer to the route may pass.
	 */
	public boolean allowed(Router.@Nullable Route route, @Nullable InetAddress peer) {
		if (peer == null) {
			return true;
		}
		for (Cidr cidr : this.deny) {
			if (cidr.matches(peer)) {
				return false;
			}
		}
		List<String> configured = route == null ? List.of() : route.allowedCidrs();
		List<Cidr> allow = configured.isEmpty() ? this.allow
				: this.routeCidrs.computeIfAbsent(configured, cidrs -> compile("allowed-cidrs", cidrs));
		for (Cidr cidr : allow) {
			if (cidr.matches(peer)) {
				return true;
			}
		}
		return allow.isEmpty();
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
