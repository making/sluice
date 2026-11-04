package am.ik.sluice.client.tunnel;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.jspecify.annotations.Nullable;

import am.ik.sluice.client.config.FilterConfig;
import am.ik.sluice.client.config.Upstream;
import am.ik.sluice.client.filter.Filters;
import am.ik.sluice.client.filter.UpstreamFilter;
import am.ik.sluice.client.filter.UpstreamFilterPipe;
import am.ik.sluice.v1.proto.Connect;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * The filters of each route of the client, and which of them a connection passes: it is
 * opened for a route ({@code Connect.route}), as routes may share a target and differ by
 * their filters. A filtered route takes HTTP/1.1 connections only: what the server relays
 * verbatim, or as h2, could not be read, so it is refused rather than passed unseen.
 */
final class RouteFilters {

	private static final String HTTP_1_1 = "http/1.1";

	private final List<Entry> entries;

	private final boolean filtered;

	private final long reservedBytes;

	private final long connectionBytes;

	private final @Nullable MeterRegistry meters;

	/** One upstream of the client; a null filter for one without any. */
	private record Entry(String route, LocalConnector.UpstreamEndpoint endpoint, List<FilterConfig> uses,
			@Nullable UpstreamFilter filter) {
	}

	private RouteFilters(List<Entry> entries, @Nullable Filters filters, @Nullable MeterRegistry meters) {
		this.entries = List.copyOf(entries);
		this.filtered = entries.stream().anyMatch(entry -> entry.filter() != null);
		this.reservedBytes = filters == null ? 0 : filters.reservedBytes();
		this.connectionBytes = !this.filtered || filters == null ? 0
				: UpstreamFilterPipe.CONNECTION_BYTES + filters.pluginConnectionBytes();
		this.meters = meters;
	}

	/** No route has a filter. */
	static RouteFilters none() {
		return new RouteFilters(List.of(), null, null);
	}

	/**
	 * The filters of the upstreams.
	 * @throws IllegalArgumentException when a filter is unknown or invalid, or a route
	 * and target are given twice with different filters: the later would never be used
	 */
	static RouteFilters of(List<Upstream> upstreams, Filters filters, MeterRegistry meters) {
		List<Entry> entries = new ArrayList<>();
		for (Upstream upstream : upstreams) {
			LocalConnector.UpstreamEndpoint endpoint = LocalConnector.UpstreamEndpoint.of(upstream.target());
			if (endpoint == null) {
				continue;
			}
			String route = upstream.routeKey();
			for (Entry entry : entries) {
				if (entry.route().equals(route) && entry.endpoint().equals(endpoint)
						&& !entry.uses().equals(upstream.filters())) {
					throw new IllegalArgumentException(
							"route " + route + " to " + upstream.target() + " is given twice with different filters");
				}
			}
			entries.add(new Entry(route, endpoint, upstream.filters(),
					upstream.filters().isEmpty() ? null : filters.forRoute(route, upstream.filters())));
		}
		return new RouteFilters(entries, filters, meters);
	}

	/** Whether any route has a filter. */
	boolean any() {
		return this.filtered;
	}

	/** The most the plugins' instances hold. */
	long reservedBytes() {
		return this.reservedBytes;
	}

	/** The most a connection holds for filters, on top of what any connection does. */
	long connectionBytes() {
		return this.connectionBytes;
	}

	/**
	 * The filter a connection passes; null when its route has none.
	 * @throws IllegalStateException when its route has filters it cannot pass: the
	 * connection is refused
	 */
	@Nullable UpstreamFilter of(Connect connect) {
		if (!this.filtered) {
			return null;
		}
		String address = connect.getAddress();
		List<Entry> candidates = this.entries.stream().filter(entry -> entry.endpoint().matches(address)).toList();
		Entry entry = entryOf(connect.getRoute(), address, candidates);
		if (entry == null || entry.filter() == null) {
			return null;
		}
		if (!HTTP_1_1.equals(connect.getApplicationProtocol())) {
			throw refused(entry.route(), "not-http1",
					"route " + entry.route() + " has filters, and this connection is " + "no HTTP/1.1 they could read");
		}
		return entry.filter();
	}

	private @Nullable Entry entryOf(String route, String address, List<Entry> candidates) {
		if (!route.isEmpty()) {
			return candidates.stream().filter(entry -> entry.route().equals(route)).findFirst().orElse(null);
		}
		// a server before Connect.route: its address tells, if all at it agree
		if (candidates.stream().map(Entry::uses).distinct().count() > 1) {
			throw refused("unknown", "no-route", "routes to " + address + " differ by their filters, and the server "
					+ "does not name the route of a connection (Connect.route): this client needs a newer sluice-server");
		}
		return candidates.isEmpty() ? null : candidates.get(0);
	}

	private IllegalStateException refused(String route, String reason, String message) {
		Counter.builder("sluice.client.filter.refused")
			.description("Connections of a route with filters refused, as they could not pass them")
			.tag("route", route)
			.tag("reason", reason)
			.register(Objects.requireNonNull(this.meters, "meters"))
			.increment();
		return new IllegalStateException(message);
	}

}
