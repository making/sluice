package am.ik.sluice.server.proxy;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * The meters shared by every h2 demux relay of one data proxy: per route, the bytes
 * queued for the leg writers of all live legs -- one gauge per route, so the series count
 * follows the routes, not the connections -- and, for tests, the number of times a leg
 * writer woke up and the streams the relays track.
 */
final class H2DemuxMeters {

	/** Name of the per-route gauge of the bytes queued for the leg writers. */
	static final String QUEUED_GAUGE_NAME = "sluice.h2demux.queued.bytes";

	private final MeterRegistry registry;

	private final Map<String, Route> routes = new ConcurrentHashMap<>();

	H2DemuxMeters(MeterRegistry registry) {
		this.registry = registry;
	}

	MeterRegistry registry() {
		return this.registry;
	}

	/** The meters of the route, registered on first use. */
	Route route(String routeTag) {
		return this.routes.computeIfAbsent(routeTag, tag -> {
			AtomicLong queued = new AtomicLong();
			Gauge.builder(QUEUED_GAUGE_NAME, queued, AtomicLong::doubleValue)
				.tag("route", tag)
				.strongReference(true)
				.register(this.registry);
			return new Route(queued, new LongAdder(), new LongAdder());
		});
	}

	/** The wakeups of the route's leg writers so far; for tests. */
	long writerWakeups(String routeTag) {
		Route route = this.routes.get(routeTag);
		return route == null ? 0 : route.writerWakeups().sum();
	}

	/** The streams of the route the live relays still track; for tests. */
	long trackedStreams(String routeTag) {
		Route route = this.routes.get(routeTag);
		return route == null ? 0 : route.trackedStreams().sum();
	}

	/**
	 * The meters of one route: the bytes queued for its legs' writers, the writers'
	 * wakeups, and the streams tracked.
	 */
	record Route(AtomicLong queuedBytes, LongAdder writerWakeups, LongAdder trackedStreams) {
	}

}
