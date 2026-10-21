package am.ik.sluice.server.livelog;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import am.ik.sluice.server.config.SluiceServerProperties;
import am.ik.sluice.server.tunnel.LogThrottle;
import am.ik.sluice.v1.proto.AccessFilter;
import am.ik.sluice.v1.proto.FeedControl;
import am.ik.sluice.v1.proto.FeedGap;
import am.ik.sluice.v1.proto.FeedHello;
import am.ik.sluice.v1.proto.FeedMessage;

/**
 * This node's feed stream to the flume: dialed out at start and redialed with a backoff
 * whenever it ends, so neither the startup nor the data plane depends on the flume being
 * up. Idle while nobody watches; on the flume's start it sends the recent lines past the
 * flume's position, then the lines the access logger emits, as fast as the stream is
 * ready for them.
 */
@Component
public class FlumeFeed implements SmartLifecycle {

	private static final Logger log = LoggerFactory.getLogger(FlumeFeed.class);

	static final Duration MIN_BACKOFF = Duration.ofSeconds(1);

	static final Duration MAX_BACKOFF = Duration.ofSeconds(30);

	/** How long the feed waits on the queue before it looks at its stream again. */
	private static final Duration POLL = Duration.ofMillis(250);

	private final FlumeConnection flume;

	private final LiveLogTap tap;

	private final String nodeId;

	/** Names this run: the flume tells it apart from the node's earlier ones. */
	private final String bootId = UUID.randomUUID().toString();

	private final LogThrottle failures = new LogThrottle(Duration.ofMinutes(5), System::nanoTime);

	private volatile boolean running;

	private volatile @Nullable Thread thread;

	private volatile @Nullable Session session;

	FlumeFeed(FlumeConnection flume, LiveLogTap tap, SluiceServerProperties properties) {
		this.flume = flume;
		this.tap = tap;
		this.nodeId = properties.node().id();
	}

	@Override
	public void start() {
		this.running = true;
		if (this.flume.enabled()) {
			this.thread = Thread.ofVirtual().name("flume-feed").start(this::run);
		}
	}

	@Override
	public void stop() {
		this.running = false;
		Session current = this.session;
		if (current != null) {
			current.cancel();
		}
		Thread feeder = this.thread;
		if (feeder != null) {
			feeder.interrupt();
		}
	}

	@Override
	public boolean isRunning() {
		return this.running;
	}

	private void run() {
		Duration backoff = MIN_BACKOFF;
		while (this.running) {
			long began = System.nanoTime();
			try {
				this.feed();
			}
			catch (InterruptedException e) {
				return;
			}
			catch (RuntimeException e) {
				long suppressed = this.failures.tryAcquire();
				if (suppressed >= 0) {
					log.warn("live access log: the feed to {} ended: {}{}", this.flume.url(), e.toString(),
							suppressed > 0 ? " (" + suppressed + " more since)" : "");
				}
			}
			finally {
				this.tap.stop();
				this.session = null;
			}
			// a stream that held a while had the flume up: retry soon
			if (System.nanoTime() - began > MAX_BACKOFF.toNanos()) {
				backoff = MIN_BACKOFF;
			}
			try {
				TimeUnit.MILLISECONDS.sleep(backoff.toMillis());
			}
			catch (InterruptedException e) {
				return;
			}
			backoff = backoff.multipliedBy(2).compareTo(MAX_BACKOFF) > 0 ? MAX_BACKOFF : backoff.multipliedBy(2);
		}
	}

	/** One feed stream, until it ends; ends with an exception unless stopped. */
	private void feed() throws InterruptedException {
		Session current = new Session();
		this.session = current;
		this.flume.stub().feed(current);
		current.send(FeedMessage.newBuilder()
			.setHello(FeedHello.newBuilder().setNodeId(this.nodeId).setBootId(this.bootId))
			.build());
		ArrayDeque<LiveLogTap.Line> backfill = new ArrayDeque<>();
		while (this.running) {
			RuntimeException failure = current.failure();
			if (failure != null) {
				throw failure;
			}
			FeedControl control = current.takeControl();
			if (control != null) {
				backfill.clear();
				if (control.hasStart()) {
					backfill.addAll(this.tap.start(control.getStart().getAfterNodeSeq(),
							filterOf(control.getStart().getFilter())));
				}
				else {
					this.tap.stop();
				}
				continue;
			}
			if (!this.tap.feeding()) {
				current.await(MAX_BACKOFF);
				continue;
			}
			if (!current.ready()) {
				current.await(POLL);
				continue;
			}
			if (!backfill.isEmpty()) {
				current.send(eventOf(backfill.poll()));
				continue;
			}
			long dropped = this.tap.takeDropped();
			if (dropped > 0) {
				current.send(FeedMessage.newBuilder().setGap(FeedGap.newBuilder().setDropped(dropped)).build());
				continue;
			}
			LiveLogTap.Line line = this.tap.poll(POLL);
			if (line != null) {
				current.send(eventOf(line));
			}
		}
		current.cancel();
	}

	private static LiveLogTap.Filter filterOf(AccessFilter filter) {
		return new LiveLogTap.Filter(new HashSet<>(filter.getRouteKeysList()), new HashSet<>(filter.getKindsList()));
	}

	private static FeedMessage eventOf(LiveLogTap.Line line) {
		am.ik.sluice.v1.proto.AccessEvent.Builder event = am.ik.sluice.v1.proto.AccessEvent.newBuilder()
			.setNodeSeq(line.seq())
			.setTimeUnixMillis(line.timeMillis())
			.setKind(line.event().kind())
			.setRouteKey(line.event().routeKey());
		for (AccessEvent.Field field : line.event().fields()) {
			event.addFields(am.ik.sluice.v1.proto.AccessEvent.Field.newBuilder()
				.setName(field.name())
				.setValue(Objects.requireNonNullElse(field.value(), "-")));
		}
		return FeedMessage.newBuilder().setEvent(event).build();
	}

	/**
	 * The state of one feed stream as its callbacks leave it, for the feeding thread to
	 * act on: the latest control, readiness, the end.
	 */
	private static final class Session implements ClientResponseObserver<FeedMessage, FeedControl> {

		private final ReentrantLock lock = new ReentrantLock();

		private final Condition changed = this.lock.newCondition();

		private final AtomicReference<@Nullable FeedControl> control = new AtomicReference<>();

		private volatile @Nullable ClientCallStreamObserver<FeedMessage> requests;

		private volatile @Nullable RuntimeException failure;

		@Override
		public void beforeStart(ClientCallStreamObserver<FeedMessage> requests) {
			this.requests = requests;
			requests.setOnReadyHandler(this::signal);
		}

		@Override
		public void onNext(FeedControl value) {
			this.control.set(value);
			this.signal();
		}

		@Override
		public void onError(Throwable t) {
			this.failure = t instanceof RuntimeException runtime ? runtime : new IllegalStateException(t);
			this.signal();
		}

		@Override
		public void onCompleted() {
			this.failure = new IllegalStateException("the flume ended the feed");
			this.signal();
		}

		@Nullable FeedControl takeControl() {
			return this.control.getAndSet(null);
		}

		@Nullable RuntimeException failure() {
			return this.failure;
		}

		boolean ready() {
			ClientCallStreamObserver<FeedMessage> current = this.requests;
			return current != null && current.isReady();
		}

		void send(FeedMessage message) {
			Objects.requireNonNull(this.requests, "the stream has not started").onNext(message);
		}

		void cancel() {
			ClientCallStreamObserver<FeedMessage> current = this.requests;
			if (current != null) {
				current.cancel("the node stops feeding", null);
			}
		}

		/** Waits for a control, readiness or the end, at most {@code timeout}. */
		void await(Duration timeout) throws InterruptedException {
			this.lock.lock();
			try {
				if (this.control.get() == null && this.failure == null) {
					this.changed.await(timeout.toNanos(), TimeUnit.NANOSECONDS);
				}
			}
			finally {
				this.lock.unlock();
			}
		}

		private void signal() {
			this.lock.lock();
			try {
				this.changed.signalAll();
			}
			finally {
				this.lock.unlock();
			}
		}

	}

}
