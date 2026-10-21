package am.ik.sluice.server.livelog;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.jspecify.annotations.Nullable;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import am.ik.sluice.server.config.SluiceServerProperties;

/**
 * Where the access log lines this node emits enter the live access log; the access logger
 * publishes, {@link FlumeFeed} takes them off. Every line goes to a ring of the recent
 * ones, the backfill once someone starts watching; while the flume asks for lines, those
 * passing its filter also go to a bounded queue its feed drains. Neither ever blocks the
 * relay thread logging: a line past a full queue is dropped and counted, and the feed
 * reports the count as a gap. With the live access log off, lines are not even kept.
 */
@Component
public class LiveLogTap {

	/** The recent lines kept for the backfill; a power of two. */
	static final int RING_SIZE = 512;

	/** The lines the feed may lag behind before lines are dropped. */
	static final int QUEUE_CAPACITY = 4096;

	private final boolean enabled;

	private final AtomicLong sequence = new AtomicLong();

	private final AtomicReferenceArray<@Nullable Line> ring = new AtomicReferenceArray<>(RING_SIZE);

	private final BlockingQueue<Line> queue;

	private final AtomicLong dropped = new AtomicLong();

	/** The filter the flume feeds by; {@code null} while it asks for nothing. */
	private volatile @Nullable Filter feeding;

	/** The lines of the backfill the queue may hold again, skipped there. */
	private volatile Set<Long> backfilled = Set.of();

	private volatile long backfillBoundary;

	@Autowired
	LiveLogTap(SluiceServerProperties properties) {
		this(properties.liveLog().enabled(), QUEUE_CAPACITY);
	}

	/**
	 * A tap keeping the lines when {@code enabled}, queueing at most
	 * {@code queueCapacity} for the feed.
	 */
	public LiveLogTap(boolean enabled, int queueCapacity) {
		this.enabled = enabled;
		this.queue = new ArrayBlockingQueue<>(queueCapacity);
	}

	/** A tap keeping nothing, for an access logger outside the live access log. */
	public static LiveLogTap off() {
		return new LiveLogTap(false, 1);
	}

	/** Takes a line the access logger emitted. */
	public void publish(AccessEvent event) {
		if (!this.enabled) {
			return;
		}
		long seq = this.sequence.incrementAndGet();
		Line line = new Line(seq, System.currentTimeMillis(), event);
		this.ring.set((int) (seq & (RING_SIZE - 1)), line);
		// read after the ring write: a feed starting meanwhile finds the line in the
		// ring, or this finds the feed started
		Filter filter = this.feeding;
		if (filter != null && filter.passes(event) && !this.queue.offer(line)) {
			this.dropped.incrementAndGet();
		}
	}

	/**
	 * Starts feeding the lines passing the filter, or changes the filter; returns the
	 * recent lines past {@code afterSeq} passing it, oldest first, to send ahead of the
	 * queue.
	 */
	public List<Line> start(long afterSeq, Filter filter) {
		this.queue.clear();
		this.dropped.set(0);
		this.feeding = filter;
		long boundary = this.sequence.get();
		List<Line> backfill = new ArrayList<>();
		for (int i = 0; i < RING_SIZE; i++) {
			Line line = this.ring.get(i);
			if (line != null && line.seq() > afterSeq && line.seq() <= boundary && filter.passes(line.event())) {
				backfill.add(line);
			}
		}
		backfill.sort(Comparator.comparingLong(Line::seq));
		Set<Long> seqs = new HashSet<>();
		backfill.forEach(line -> seqs.add(line.seq()));
		this.backfilled = seqs;
		this.backfillBoundary = boundary;
		return backfill;
	}

	/** Stops feeding: the queue is emptied, and lines stay in the ring only. */
	public void stop() {
		this.feeding = null;
		this.queue.clear();
		this.dropped.set(0);
		this.backfilled = Set.of();
	}

	public boolean feeding() {
		return this.feeding != null;
	}

	/**
	 * The next line to feed, waiting at most {@code timeout}; {@code null} when none
	 * came.
	 */
	public @Nullable Line poll(Duration timeout) throws InterruptedException {
		long deadline = System.nanoTime() + timeout.toNanos();
		while (true) {
			Line line = this.queue.poll(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
			if (line == null || line.seq() > this.backfillBoundary || !this.backfilled.contains(line.seq())) {
				return line;
			}
		}
	}

	/** The lines dropped since the last call. */
	public long takeDropped() {
		return this.dropped.getAndSet(0);
	}

	/**
	 * A line as fed: its place in this node's run, when it was logged, and the line.
	 */
	public record Line(long seq, long timeMillis, AccessEvent event) {

	}

	/**
	 * The lines the flume asks for; an empty set passes everything.
	 */
	public record Filter(Set<String> routeKeys, Set<String> kinds) {

		public static final Filter ALL = new Filter(Set.of(), Set.of());

		public Filter {
			routeKeys = Set.copyOf(routeKeys);
			kinds = Set.copyOf(kinds);
		}

		boolean passes(AccessEvent event) {
			return (this.routeKeys.isEmpty() || this.routeKeys.contains(event.routeKey()))
					&& (this.kinds.isEmpty() || this.kinds.contains(event.kind()));
		}

	}

}
