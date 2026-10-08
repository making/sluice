package am.ik.sluice.server.proxy;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import am.ik.sluice.server.config.SluiceServerProperties;
import am.ik.sluice.server.route.Router;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class AccessControlTest {

	private static AccessControl rules(List<String> allow, List<String> deny) {
		return new AccessControl(SluiceServerProperties.builder()
			.accessControl(new SluiceServerProperties.AccessControl(allow, deny))
			.build());
	}

	private static Router.Route route(String... allowedCidrs) {
		return Router.Route.builder()
			.clientId("c1")
			.domain("demo.local")
			.address("127.0.0.1:3000")
			.allowedCidrs(List.of(allowedCidrs))
			.build();
	}

	private static InetAddress peer(String address) {
		try {
			return InetAddress.getByName(address);
		}
		catch (UnknownHostException e) {
			throw new IllegalStateException(e);
		}
	}

	@Test
	void allowsEveryAddressWhenNoRuleIsConfigured() {
		AccessControl accessControl = rules(List.of(), List.of());
		assertThat(accessControl.allowed(route(), peer("203.0.113.7"))).isTrue();
	}

	@Test
	void allowsEveryAddressWhenOnlyDenyIsConfiguredAndThePeerIsNotListed() {
		AccessControl accessControl = rules(List.of(), List.of("10.0.0.0/8"));
		assertThat(accessControl.allowed(null, peer("203.0.113.7"))).isTrue();
	}

	@Test
	void denyListRejectsTheMatchingPeerBeforeAnyAllow() {
		AccessControl accessControl = rules(List.of("0.0.0.0/0"), List.of("203.0.113.0/24"));
		assertThat(accessControl.allowed(null, peer("203.0.113.7"))).isFalse();
		assertThat(accessControl.allowed(null, peer("198.51.100.7"))).isTrue();
	}

	@Test
	void allowListAdmitsTheMatchingPeerAndRejectsTheOthers() {
		AccessControl accessControl = rules(List.of("10.0.0.0/8"), List.of());
		assertThat(accessControl.allowed(null, peer("10.1.2.3"))).isTrue();
		assertThat(accessControl.allowed(null, peer("203.0.113.7"))).isFalse();
	}

	@Test
	void routeAllowedCidrsReplaceTheGlobalAllowList() {
		AccessControl accessControl = rules(List.of("10.0.0.0/8"), List.of());
		Router.Route locked = route("192.168.0.0/16");
		// inside the global allow but outside the route's list
		assertThat(accessControl.allowed(locked, peer("10.1.2.3"))).isFalse();
		// inside the route's list
		assertThat(accessControl.allowed(locked, peer("192.168.1.2"))).isTrue();
		// another route without its own list keeps the global allow
		assertThat(accessControl.allowed(route(), peer("10.1.2.3"))).isTrue();
	}

	@Test
	void denyListWinsOverTheRouteAllowedCidrs() {
		AccessControl accessControl = rules(List.of(), List.of("192.168.0.0/16"));
		assertThat(accessControl.allowed(route("192.168.0.0/16"), peer("192.168.1.2"))).isFalse();
	}

	@Test
	void aNullRouteIsJudgedByTheGlobalListsOnly() {
		AccessControl accessControl = rules(List.of("127.0.0.0/8"), List.of());
		assertThat(accessControl.allowed(null, peer("127.0.0.1"))).isTrue();
		assertThat(accessControl.allowed(null, peer("192.168.1.2"))).isFalse();
	}

	@Test
	void ipv4MappedPeerMatchesTheIpv4Network() {
		AccessControl accessControl = rules(List.of("127.0.0.0/8"), List.of());
		assertThat(accessControl.allowed(null, peer("::ffff:127.0.0.1"))).isTrue();
		assertThat(accessControl.allowed(null, peer("::ffff:1.2.3.4"))).isFalse();
	}

	@Test
	void anIpv4MappedNetworkLiteralIsJudgedAsIpv4() {
		// the JDK normalizes ::ffff:a.b.c.d literals to plain IPv4 addresses
		AccessControl accessControl = rules(List.of("::ffff:127.0.0.1"), List.of());
		assertThat(accessControl.allowed(null, peer("127.0.0.1"))).isTrue();
		assertThat(accessControl.allowed(null, peer("127.0.0.2"))).isFalse();
	}

	@Test
	void ipv6NetworkMatchesTheIpv6Peer() {
		AccessControl accessControl = rules(List.of("2001:db8::/32"), List.of());
		assertThat(accessControl.allowed(null, peer("2001:db8::1"))).isTrue();
		assertThat(accessControl.allowed(null, peer("2001:db9::1"))).isFalse();
	}

	@Test
	void aBareAddressIsAPrefixOfItsFullLength() {
		AccessControl accessControl = rules(List.of("127.0.0.1", "::1"), List.of());
		assertThat(accessControl.allowed(null, peer("127.0.0.1"))).isTrue();
		assertThat(accessControl.allowed(null, peer("127.0.0.2"))).isFalse();
		assertThat(accessControl.allowed(null, peer("::1"))).isTrue();
	}

	@Test
	void hostBitsBeyondThePrefixAreMaskedAway() {
		AccessControl accessControl = rules(List.of("10.1.2.3/8"), List.of());
		assertThat(accessControl.allowed(null, peer("10.255.255.255"))).isTrue();
		assertThat(accessControl.allowed(null, peer("11.0.0.1"))).isFalse();
	}

	@Test
	void malformedEntriesAreIgnoredInsteadOfRejectingEverything() {
		AccessControl accessControl = rules(List.of("not-an-address", "10.0.0.0/8", "10.0.0.0/33"), List.of());
		assertThat(accessControl.allowed(null, peer("10.1.2.3"))).isTrue();
		assertThat(accessControl.allowed(null, peer("203.0.113.7"))).isFalse();
	}

	@Test
	void anUnreadablePeerAddressIsAllowedThrough() {
		AccessControl accessControl = rules(List.of("10.0.0.0/8"), List.of("203.0.113.0/24"));
		assertThat(accessControl.allowed(null, (InetAddress) null)).isTrue();
		assertThat(accessControl.allowed(route("10.0.0.0/8"), null)).isTrue();
	}

	@Test
	void aPeerOutsideEveryRouteListIsRejectedWhenTheRouteRestricts() {
		AccessControl accessControl = rules(List.of(), List.of());
		assertThat(accessControl.allowed(route("10.0.0.0/8"), peer("203.0.113.7"))).isFalse();
	}

}
