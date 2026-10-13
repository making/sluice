package am.ik.sluice.server.proxy;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * The meters shared by every h2 demux relay of one data proxy: per route, the bytes the
 * relays hold of the route's streams -- one gauge per route, so the series count follows
 * the routes, not the connections --, the streams refused for want of queue budget or of
 * a slot for a new leg, or while waiting for an upstream's concurrency limit (to free a
 * connection whose window is withheld, or past the wait deadline), the streams reset for
 * want of queue budget, for an upstream's interim heads past their limit, or on a stream
 * error of the client or of an upstream, the streams answered 421 for a route reached
 * over HTTP/1.1 only, the legs ended by a connection error, the times a relay withheld
 * the client's connection window, and, for tests, the DATA writes netty holds, the
 * streams the relays track and the legs they hold.
 */
final class H2DemuxMeters {

	/** Name of the per-route gauge of the bytes the relays hold of a route's streams. */
	static final String QUEUED_GAUGE_NAME = "sluice.h2demux.queued.bytes";

	private final MeterRegistry registry;

	private final Map<String, Route> routes = new ConcurrentHashMap<>();

	/** Per route, the DATA writes netty holds for the relays; for tests. */
	private final Map<String, LongAdder> writesHeld = new ConcurrentHashMap<>();

	private final Counter refusedBudget;

	private final Counter refusedLegLimit;

	private final Counter refusedWaiting;

	private final Counter refusedWaitTimeout;

	private final Counter resetBudget;

	private final Counter resetInterim;

	private final Counter resetProtocol;

	private final Counter resetUpstream;

	private final Counter legsFailed;

	private final Counter misdirectedHttp1;

	private final Counter windowWithheld;

	H2DemuxMeters(MeterRegistry registry) {
		this.registry = registry;
		this.refusedBudget = refused(registry, "budget");
		this.refusedLegLimit = refused(registry, "leg-limit");
		this.refusedWaiting = refused(registry, "waiting");
		this.refusedWaitTimeout = refused(registry, "wait-timeout");
		this.resetBudget = reset(registry, "budget");
		this.resetInterim = reset(registry, "interim");
		this.resetProtocol = reset(registry, "protocol");
		this.resetUpstream = reset(registry, "upstream");
		this.legsFailed = Counter.builder("sluice.h2demux.legs.failed")
			.description("h2 demux legs ended by a connection error, with a GOAWAY of its code to the upstream; the"
					+ " other legs of the connection go on")
			.register(registry);
		this.misdirectedHttp1 = Counter.builder("sluice.h2demux.streams.misdirected")
			.tag("reason", "force-http1")
			.description("h2 streams answered 421 Misdirected Request: their route's upstream is reached over"
					+ " HTTP/1.1 only (force-http1)")
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
					+ " window is withheld or past the wait deadline")
			.register(registry);
	}

	private static Counter reset(MeterRegistry registry, String reason) {
		return Counter.builder("sluice.h2demux.streams.reset")
			.tag("reason", reason)
			.description("h2 streams an h2 demux relay reset on its own account: for want of queue budget, for an"
					+ " upstream's interim heads past their limit, or on a stream error of the client or of an upstream")
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

	/**
	 * A stream refused: it waited for its upstream's concurrency limit past
	 * {@code sluice.limits.h2-stream-wait-timeout}.
	 */
	void refusedForWaitTimeout() {
		this.refusedWaitTimeout.increment();
	}

	/** A stream reset: a header block of it did not fit the queue budget. */
	void resetForBudget() {
		this.resetBudget.increment();
	}

	/**
	 * A stream reset towards its upstream: the upstream's interim (1xx) heads passed the
	 * limit of one response's.
	 */
	void resetForInterim() {
		this.resetInterim.increment();
	}

	/**
	 * A client stream reset by the relay's codec on a stream error of the client (RFC
	 * 9113 5.4.2).
	 */
	void resetForProtocol() {
		this.resetProtocol.increment();
	}

	/**
	 * A leg stream reset by the relay's codec on a stream error of its upstream (RFC 9113
	 * 5.4.2).
	 */
	void resetForUpstream() {
		this.resetUpstream.increment();
	}

	/** A leg sent its upstream a GOAWAY with an error code (RFC 9113 5.4.1). */
	void legFailed() {
		this.legsFailed.increment();
	}

	/**
	 * A stream answered 421: its route's upstream is reached over HTTP/1.1 only.
	 */
	void misdirectedForHttp1() {
		this.misdirectedHttp1.increment();
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

	/**
	 * The counter of the DATA writes the relays hand netty for the route and netty has
	 * not completed yet.
	 */
	LongAdder writesHeldOf(String routeTag) {
		return this.writesHeld.computeIfAbsent(routeTag, tag -> new LongAdder());
	}

	/** The DATA writes netty holds for the route; for tests. */
	long writesHeld(String routeTag) {
		LongAdder held = this.writesHeld.get(routeTag);
		return held == null ? 0 : held.sum();
	}

	/** The streams of the route the live relays still track; for tests. */
	long trackedStreams(String routeTag) {
		Route route = this.routes.get(routeTag);
		return route == null ? 0 : route.trackedStreams().sum();
	}

	/** The legs of the route the live relays hold; for tests. */
	long legs(String routeTag) {
		Route route = this.routes.get(routeTag);
		return route == null ? 0 : route.legs().sum();
	}

	/**
	 * The meters of one route: the bytes the relays hold of it, the streams tracked and
	 * the legs held.
	 */
	record Route(AtomicLong queuedBytes, LongAdder trackedStreams, LongAdder legs) {
	}

}
