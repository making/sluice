package am.ik.sluice.client.config;

import java.util.Map;

import org.junit.jupiter.api.Test;

import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.StandardEnvironment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for the structured {@code sluice.client.upstream[n]} binding and the test
 * facing builder.
 */
class SluiceClientPropertiesTest {

	@Test
	void builderCompletesSchemeAndDefaultsRewriteHostToFalse() {
		SluiceClientProperties properties = SluiceClientProperties.builder()
			.serverUrl("grpc://127.0.0.1:8001")
			.upstream(Upstream.builder().host("demo.local").target("127.0.0.1:8080").build())
			.build();
		assertThat(properties.upstreamTargets()).containsExactly("http://127.0.0.1:8080");
		assertThat(properties.toProtoUpstreams()).singleElement().satisfies(upstream -> {
			assertThat(upstream.getHost()).isEqualTo("demo.local");
			assertThat(upstream.getTargetUrl()).isEqualTo("http://127.0.0.1:8080");
			assertThat(upstream.getRewriteHost()).isFalse();
		});
	}

	@Test
	void builderKeepsExplicitSchemeAndRewriteHostFlag() {
		SluiceClientProperties properties = SluiceClientProperties.builder()
			.serverUrl("grpc://127.0.0.1:8001")
			.upstream(Upstream.builder().host("a.local").target("https://192.0.2.1:8443").build())
			.upstream(Upstream.builder().host("").target("example.com:80").rewriteHost(true).build())
			.build();
		assertThat(properties.upstreamTargets()).containsExactly("https://192.0.2.1:8443", "http://example.com:80");
		assertThat(properties.toProtoUpstreams().get(1).getRewriteHost()).isTrue();
	}

	@Test
	void upstreamsSharingOneHostBothSurvive() {
		SluiceClientProperties properties = SluiceClientProperties.builder()
			.serverUrl("grpc://127.0.0.1:8001")
			.upstream(Upstream.builder().host("dual.local").target("http://127.0.0.1:8080").build())
			.upstream(Upstream.builder().host("dual.local").target("tcp://127.0.0.1:9000").listenPort(15000).build())
			.build();
		assertThat(properties.upstreamTargets()).containsExactly("http://127.0.0.1:8080", "tcp://127.0.0.1:9000");
		assertThat(properties.toProtoUpstreams()).hasSize(2);
	}

	@Test
	void indexedPropertiesBindIntoNestedRecords() {
		Map<String, Object> source = Map.of("sluice.server-url", "grpc://127.0.0.1:8001",
				"sluice.client.upstream[0].host", "demo.local", "sluice.client.upstream[0].target", "127.0.0.1:8080",
				"sluice.client.upstream[1].host", "", "sluice.client.upstream[1].target", "https://fallback:8443",
				"sluice.client.upstream[1].rewrite-host", "true");
		StandardEnvironment environment = new StandardEnvironment();
		environment.getPropertySources().addFirst(new MapPropertySource("test", source));
		SluiceClientProperties properties = Binder.get(environment)
			.bind("sluice", Bindable.of(SluiceClientProperties.class))
			.get();
		assertThat(properties.client()).isNotNull();
		assertThat(properties.client().upstream()).hasSize(2);
		assertThat(properties.upstreamTargets()).containsExactly("http://127.0.0.1:8080", "https://fallback:8443");
		assertThat(properties.toProtoUpstreams().get(1).getRewriteHost()).isTrue();
	}

	@Test
	void boundUpstreamWithoutRewriteHostDefaultsToFalse() {
		Map<String, Object> source = Map.of("sluice.server-url", "grpc://127.0.0.1:8001",
				"sluice.client.upstream[0].host", "demo.local", "sluice.client.upstream[0].target",
				"https://httpbingo.org");
		StandardEnvironment environment = new StandardEnvironment();
		environment.getPropertySources().addFirst(new MapPropertySource("test", source));
		SluiceClientProperties properties = Binder.get(environment)
			.bind("sluice", Bindable.of(SluiceClientProperties.class))
			.get();
		assertThat(properties.toProtoUpstreams().get(0).getRewriteHost()).isFalse();
	}

	@Test
	void missingClientSectionYieldsEmptyUpstreams() {
		SluiceClientProperties properties = SluiceClientProperties.builder().serverUrl("grpc://127.0.0.1:8001").build();
		assertThat(properties.upstreamTargets()).isEmpty();
		assertThat(properties.toProtoUpstreams()).isEmpty();
	}

	@Test
	void hostPatternIsCarriedToTheAdvertisedUpstream() {
		SluiceClientProperties properties = SluiceClientProperties.builder()
			.serverUrl("grpc://127.0.0.1:8001")
			.upstream(Upstream.builder().hostPattern("svc-.*\\.local").target("http://127.0.0.1:8080").build())
			.upstream(Upstream.builder().host("demo.local").target("http://127.0.0.1:8081").build())
			.build();
		assertThat(properties.toProtoUpstreams()).satisfies(upstreams -> {
			assertThat(upstreams.get(0).getHostPattern()).isEqualTo("svc-.*\\.local");
			assertThat(upstreams.get(0).getHost()).isEmpty();
			assertThat(upstreams.get(1).getHostPattern()).isEmpty();
		});
	}

	@Test
	void hostPatternBindsFromIndexedPropertiesAndIsTrimmed() {
		Map<String, Object> source = Map.of("sluice.server-url", "grpc://127.0.0.1:8001",
				"sluice.client.upstream[0].host-pattern", " svc-.*\\.local ", "sluice.client.upstream[0].target",
				"http://127.0.0.1:8080");
		StandardEnvironment environment = new StandardEnvironment();
		environment.getPropertySources().addFirst(new MapPropertySource("test", source));
		SluiceClientProperties properties = Binder.get(environment)
			.bind("sluice", Bindable.of(SluiceClientProperties.class))
			.get();
		assertThat(properties.toProtoUpstreams().get(0).getHostPattern()).isEqualTo("svc-.*\\.local");
	}

	@Test
	void invalidHostPatternFailsTheBinding() {
		Upstream.Builder builder = Upstream.builder().hostPattern("[(").target("http://127.0.0.1:8080");
		assertThatThrownBy(builder::build).isInstanceOf(IllegalArgumentException.class);
	}

}
