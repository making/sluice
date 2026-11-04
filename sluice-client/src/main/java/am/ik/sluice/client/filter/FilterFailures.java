package am.ik.sluice.client.filter;

import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Counts what fails in one filter of a route, as
 * {@code sluice.client.filter.failures{route,filter,phase}}. A request may make a filter
 * fail, so its lines are bounded: one per 10s, telling how many went unlogged.
 */
final class FilterFailures {

	private static final Logger log = LoggerFactory.getLogger(FilterFailures.class);

	private static final long LOG_INTERVAL_NANOS = 10_000_000_000L;

	private final MeterRegistry meters;

	private final String route;

	private final String filter;

	private final AtomicLong unlogged = new AtomicLong();

	private final AtomicLong logged = new AtomicLong(System.nanoTime() - LOG_INTERVAL_NANOS);

	FilterFailures(MeterRegistry meters, String route, String filter) {
		this.meters = meters;
		this.route = route;
		this.filter = filter;
	}

	void count(String phase, Throwable failure) {
		Counter.builder("sluice.client.filter.failures")
			.description("Calls of a route's filter that failed")
			.tag("route", this.route)
			.tag("filter", this.filter)
			.tag("phase", phase)
			.register(this.meters)
			.increment();
		long last = this.logged.get();
		long now = System.nanoTime();
		if (now - last >= LOG_INTERVAL_NANOS && this.logged.compareAndSet(last, now)) {
			log.warn("filter {} of route {} failed on a {} ({} more since the last line): {}", this.filter, this.route,
					phase, this.unlogged.getAndSet(0), failure.toString());
		}
		else {
			this.unlogged.incrementAndGet();
		}
	}

}
