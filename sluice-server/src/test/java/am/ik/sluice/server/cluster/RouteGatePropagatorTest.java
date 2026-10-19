package am.ik.sluice.server.cluster;

import java.util.List;

import am.ik.sluice.server.auth.TokenValidator;
import am.ik.sluice.server.cluster.NodeDirectory.NodeMember;
import am.ik.sluice.server.config.SluiceServerProperties;
import org.junit.jupiter.api.Test;

import org.springframework.boot.ssl.SslBundle;
import org.springframework.boot.ssl.SslBundles;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RouteGatePropagatorTest {

	private static TokenValidator token() {
		return new TokenValidator(SluiceServerProperties.builder().token("t").build());
	}

	private static SslBundles bundles() {
		SslBundles bundles = mock(SslBundles.class);
		when(bundles.getBundle(anyString())).thenReturn(mock(SslBundle.class));
		return bundles;
	}

	@Test
	void singleNodePropagatesNowhere() {
		NodeDirectory directory = new NodeDirectory() {

			@Override
			public long version() {
				return 0;
			}

			@Override
			public List<NodeMember> nodes() {
				return List.of(new NodeMember("self", ""));
			}

		};
		RouteGatePropagator propagator = new RouteGatePropagator(directory,
				SluiceServerProperties.builder().node(SluiceServerProperties.Node.builder().id("self").build()).build(),
				token(), bundles());
		// no peers: nothing to dial, no channel kept
		propagator.propagate("demo.local", false);
		propagator.destroy();
	}

	@Test
	void peersWithoutAPublicUrlAreSkipped() {
		NodeDirectory directory = new NodeDirectory() {

			@Override
			public long version() {
				return 1;
			}

			@Override
			public List<NodeMember> nodes() {
				// the self entry with a reachable address is skipped by id, the peer
				// without a public url cannot be dialed
				return List.of(new NodeMember("self", "grpc://127.0.0.1:1"), new NodeMember("peer", ""));
			}

		};
		RouteGatePropagator propagator = new RouteGatePropagator(directory,
				SluiceServerProperties.builder().node(SluiceServerProperties.Node.builder().id("self").build()).build(),
				token(), bundles());
		propagator.propagate("demo.local", true);
		propagator.destroy();
	}

	@Test
	void peerAddressIsParsedHostAndPort() {
		assertThat(RouteGatePropagator.HostPort.parse("grpc://node-1:8001"))
			.isEqualTo(new RouteGatePropagator.HostPort("node-1", 8001));
		assertThat(RouteGatePropagator.HostPort.parse("grpcs://node-2"))
			.isEqualTo(new RouteGatePropagator.HostPort("node-2", 443));
		assertThat(RouteGatePropagator.HostPort.parse("grpc://node-3"))
			.isEqualTo(new RouteGatePropagator.HostPort("node-3", 80));
	}

}
