package am.ik.sluice.client.tunnel;

import java.util.List;
import java.util.Map;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import am.ik.sluice.client.config.FilterConfig;
import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.client.config.Upstream;
import am.ik.sluice.client.filter.Filters;
import am.ik.sluice.client.filter.LlmUsageFilter;
import am.ik.sluice.client.filter.UpstreamFilter;
import am.ik.sluice.client.filter.UpstreamFilterFactory;
import am.ik.sluice.client.filter.UpstreamFilterPipe;
import am.ik.sluice.tunnel.ConnectionLimit;
import am.ik.sluice.tunnel.TunnelBudget;
import am.ik.sluice.v1.proto.Connect;

import org.springframework.util.unit.DataSize;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Which connection passes which filter: routes may share a target, and differ by their
 * filters; what cannot pass them is refused.
 */
class RouteFiltersTest {

	private static final FilterConfig USAGE = FilterConfig.builder().name(LlmUsageFilter.NAME).build();

	private static final String TARGET = "http://127.0.0.1:8080";

	private static final String ADDRESS = "127.0.0.1:8080";

	private final SimpleMeterRegistry meters = new SimpleMeterRegistry();

	private RouteFilters routeFilters(Upstream... upstreams) {
		Filters filters = Filters.builder().meters(this.meters).budget(TunnelBudget.unlimited()).build();
		return RouteFilters.of(List.of(upstreams), filters, this.meters);
	}

	private static Upstream.Builder route(String host) {
		return Upstream.builder().host(host).target(TARGET);
	}

	private static Connect connect(String route, String protocol) {
		return Connect.newBuilder().setAddress(ADDRESS).setRoute(route).setApplicationProtocol(protocol).build();
	}

	@Test
	void routesSharingATargetEachKeepTheirFilters() {
		RouteFilters routeFilters = routeFilters(route("filtered.test").filters(List.of(USAGE)).build(),
				route("plain.test").build(), Upstream.builder().target(TARGET).filters(List.of(USAGE)).build());

		assertThat(routeFilters.any()).isTrue();
		assertThat(routeFilters.of(connect("filtered.test", "http/1.1"))).isNotNull();
		assertThat(routeFilters.of(connect("*", "http/1.1"))).isNotNull();
		assertThat(routeFilters.of(connect("plain.test", "http/1.1"))).isNull();
		assertThat(routeFilters.of(connect("plain.test", "h2"))).isNull();
		assertThat(routeFilters.of(connect("plain.test", ""))).isNull();
	}

	/** Relayed verbatim or as h2, a connection could not be read: it does not pass. */
	@Test
	void whatAFilteredRouteCannotReadIsRefused() {
		RouteFilters routeFilters = routeFilters(route("filtered.test").filters(List.of(USAGE)).build());

		for (String protocol : List.of("h2", "")) {
			assertThatThrownBy(() -> routeFilters.of(connect("filtered.test", protocol)))
				.isInstanceOf(IllegalStateException.class)
				.hasMessageContaining("no HTTP/1.1 they could read");
		}
		assertThat(this.meters.get("sluice.client.filter.refused").tag("route", "filtered.test").counter().count())
			.isEqualTo(2);
	}

	/** A server naming no route: the address tells, if the routes at it agree. */
	@Test
	void withoutARouteTheAddressTellsWhenItCan() {
		assertThat(routeFilters(route("a.test").filters(List.of(USAGE)).build()).of(connect("", "http/1.1")))
			.isNotNull();
		assertThat(routeFilters(route("a.test").build(), route("b.test").build()).of(connect("", "h2"))).isNull();

		RouteFilters differing = routeFilters(route("a.test").filters(List.of(USAGE)).build(), route("b.test").build());
		assertThatThrownBy(() -> differing.of(connect("", "http/1.1"))).isInstanceOf(IllegalStateException.class)
			.hasMessageContaining("needs a newer sluice-server");
	}

	@Test
	void whatARouteCannotBeGivenStopsTheClient() {
		assertThatThrownBy(() -> routeFilters(route("a.test").filters(List.of(USAGE)).build(), route("a.test").build()))
			.isInstanceOf(IllegalArgumentException.class)
			.hasMessage("route a.test to " + TARGET + " is given twice with different filters");
		assertThatThrownBy(() -> routeFilters(
				route("a.test").filters(List.of(FilterConfig.builder().name("nope").build())).build()))
			.isInstanceOf(IllegalArgumentException.class)
			.hasMessageContaining("unknown filter nope");
	}

	private double limitOf(SluiceClientProperties properties, List<UpstreamFilterFactory> factories) {
		SimpleMeterRegistry registry = new SimpleMeterRegistry();
		TunnelClient.builder()
			.properties(properties)
			.taskExecutor(Runnable::run)
			.meterRegistry(registry)
			.filterFactories(factories)
			.build();
		return registry.get("sluice.connections.limit").gauge().value();
	}

	/**
	 * What a filtered connection holds counts in the derived limit; a plugin's VMs,
	 * native memory, do not, but for what the host keeps of a connection's heads.
	 */
	@Test
	void theDerivedConnectionLimitCountsWhatFiltersHold() {
		long maxHeap = Runtime.getRuntime().maxMemory();
		long budget = TunnelBudget.derive(maxHeap);
		SluiceClientProperties plain = SluiceClientProperties.builder()
			.serverUrl("grpc://127.0.0.1:1")
			.upstream(route("a.test").build())
			.build();
		assertThat(limitOf(plain, List.of())).isEqualTo(ConnectionLimit.deriveFromHeap(maxHeap, budget));

		SluiceClientProperties filtered = SluiceClientProperties.builder()
			.serverUrl("grpc://127.0.0.1:1")
			.upstream(route("a.test").filters(List.of(USAGE)).build())
			.build();
		long connectionBytes = ConnectionLimit.CONNECTION_BYTES + UpstreamFilterPipe.CONNECTION_BYTES;
		assertThat(limitOf(filtered, List.of()))
			.isEqualTo(ConnectionLimit.derive(maxHeap / 2 - budget, connectionBytes));

		SluiceClientProperties plugin = SluiceClientProperties.builder()
			.serverUrl("grpc://127.0.0.1:1")
			.filter("custom",
					FilterConfig.builder()
						.plugin("src/test/resources/wasm/http_headers.wasm")
						.vms(2)
						.maxMemory(DataSize.ofMegabytes(8))
						.build())
			.upstream(route("a.test").filters(List.of(FilterConfig.builder().name("custom").build())).build())
			.build();
		assertThat(limitOf(plugin, List.of()))
			.isEqualTo(ConnectionLimit.derive(maxHeap / 2 - budget, connectionBytes + 256 * 1024));

		SluiceClientProperties configured = SluiceClientProperties.builder()
			.serverUrl("grpc://127.0.0.1:1")
			.maxConnections(7)
			.upstream(route("a.test").filters(List.of(USAGE)).build())
			.build();
		assertThat(limitOf(configured, List.of())).isEqualTo(7);
	}

	/** A custom build's filter is used by name, with its settings per route. */
	@Test
	void aFactoryMakesAFilterForEachUseWithItsSettings() {
		UpstreamFilterFactory factory = new UpstreamFilterFactory() {

			@Override
			public String name() {
				return "custom";
			}

			@Override
			public UpstreamFilter create(Context context) {
				assertThat(context.route()).isIn("a.test", "b.test");
				assertThat(context.settings().args()).isEqualTo(
						context.route().equals("a.test") ? Map.of("shared", "1", "own", "a") : Map.of("shared", "1"));
				return (request, headers) -> null;
			}

		};
		SluiceClientProperties properties = SluiceClientProperties.builder()
			.serverUrl("grpc://127.0.0.1:1")
			.filter("custom", FilterConfig.builder().args(Map.of("shared", "1")).build())
			.upstream(route("a.test")
				.filters(List.of(FilterConfig.builder().name("custom").args(Map.of("own", "a")).build()))
				.build())
			.upstream(route("b.test").filters(List.of(FilterConfig.builder().name("custom").build())).build())
			.build();

		limitOf(properties, List.of(factory));
	}

}
