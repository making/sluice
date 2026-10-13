package am.ik.sluice.server.proxy;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * The meters shared by every h2 demux relay of one data proxy: per route, the bytes
 * queued for the leg writers of all live legs -- one gauge per route, so the series count
 * follows the routes, not the connections --, the streams refused for want of queue
 * budget or of a slot for a new leg or while waiting for an upstream's concurrency limit,
 * the streams a relay reset for want of queue budget or for a client's or an upstream's
 * stream error, the streams answered 421 for a route reached over HTTP/1.1 only, the legs
 * ended by their upstream's connection error, the times a relay withheld the client's
 * connection window, and, for tests, the number of times a leg writer woke up and the
 * streams the relays track.
 */
final class H2DemuxMeters {

	/** Name of the per-route gauge of the bytes queued for the leg writers. */
	static final String QUEUED_GAUGE_NAME = "sluice.h2demux.queued.bytes";

	private final MeterRegistry registry;

	private final Map<String, Route> routes = new ConcurrentHashMap<>();

	private final Counter refusedBudget;

	private final Counter refusedLegLimit;

	private final Counter refusedWaiting;

	private final Counter resetBudget;

	private final Counter resetProtocol;

	private final Counter resetUpstream;

	private final Counter misdirectedHttp1;

	private final Counter legsFailed;

	private final Counter windowWithheld;

	H2DemuxMeters(MeterRegistry registry) {
		this.registry = registry;
		this.refusedBudget = refused(registry, "budget");
		this.refusedLegLimit = refused(registry, "leg-limit");
		this.refusedWaiting = refused(registry, "waiting");
		this.resetBudget = reset(registry, "budget");
		this.resetProtocol = reset(registry, "protocol");
		this.resetUpstream = reset(registry, "upstream");
		this.misdirectedHttp1 = Counter.builder("sluice.h2demux.streams.misdirected")
			.tag("reason", "force-http1")
			.description("h2 streams answered 421 Misdirected Request: their route's upstream is reached over"
					+ " HTTP/1.1 only (force-http1)")
			.register(registry);
		this.legsFailed = Counter.builder("sluice.h2demux.legs.failed")
			.description("h2 demux legs ended by their upstream's connection error, with a GOAWAY to the upstream;"
					+ " the other legs of the connection go on")
			.register(registry);
		this.windowWithheld = Counter.builder("sluice.h2demux.window.withheld")
			.description("Times an h2 demux relay withheld the client's connection window: its queue was past its"
					+ " allowance and the shared budget exhausted")
			.register(registry);
	}

	private static Counter refused(MeterRegistry registry, String reason) {
		return Counter.builder("sluice.h2demux.streams.refused")
			.tag("reason", reason)
			.description("h2 streams refused (REFUSED_STREAM) for want of queue budget or of a slot for a new leg, or"
					+ " while waiting for an upstream's concurrency limit, to free the queue of a connection whose"
					+ " window is withheld")
			.register(registry);
	}

	private static Counter reset(MeterRegistry registry, String reason) {
		return Counter.builder("sluice.h2demux.streams.reset")
			.tag("reason", reason)
			.description("h2 streams an h2 demux relay reset on its own account, both ways")
			.register(registry);
	}

	/** A stream refused: its head did not fit the queue budget. */
	void refusedForBudget() {
		this.refusedBudget.increment();
	}

	/**
	 * A stream refused: its route needed a new leg and the connection limit is reached.
	 */
	void refusedForLegLimit() {
		this.refusedLegLimit.increment();
	}

	/**
	 * A stream refused while waiting for its upstream's concurrency limit: the client's
	 * connection window was withheld and the waiting streams held more than the
	 * connection's allowance.
	 */
	void refusedWaiting() {
		this.refusedWaiting.increment();
	}

	/** A stream reset: its trailers did not fit the queue budget. */
	void resetForBudget() {
		this.resetBudget.increment();
	}

	/** A stream reset: the client sent a stream error on it (RFC 9113 5.4.2). */
	void resetForProtocol() {
		this.resetProtocol.increment();
	}

	/** A stream reset: its upstream sent a stream error on it (RFC 9113 5.4.2). */
	void resetForUpstream() {
		this.resetUpstream.increment();
	}

	/**
	 * A stream answered 421: its route's upstream is reached over HTTP/1.1 only.
	 */
	void misdirectedForHttp1() {
		this.misdirectedHttp1.increment();
	}

	/** A leg ended: its upstream made a connection error (RFC 9113 5.4.1). */
	void legFailed() {
		this.legsFailed.increment();
	}

	/** A relay withheld the client's connection window. */
	void windowWithheld() {
		this.windowWithheld.increment();
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
