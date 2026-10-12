package am.ik.sluice.server.proxy;

import java.net.InetAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.stereotype.Component;

import am.ik.sluice.server.config.SluiceServerProperties;
import am.ik.sluice.server.route.Router;
import am.ik.sluice.tunnel.ConnectionLimit;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * The limits every data plane connection lives under, shared by the data port and every
 * tcp route port: a cap on the connections open at once, a cap per source address, and
 * one deadline for the whole head phase; the deadlines between and during the requests of
 * a relayed HTTP connection, and the idle deadline of a connection relayed verbatim
 * ({@link KeepAliveDeadline}); plus the queue budget the h2 demux relays share.
 * <p>
 * The connection cap counts virtual connections: an h2 connection whose streams reach
 * several routes takes one more slot per further leg ({@link #tryAcquireLeg()}). An unset
 * cap is derived so that the worst case of every slot -- the buffering of one virtual
 * connection ({@link ConnectionLimit#CONNECTION_BYTES}) plus what an h2 demux relay
 * queues outside the budget ({@link Http2DemuxRelay#UNBUDGETED_BYTES}) and what one of
 * its legs holds for the client ({@link Http2DemuxRelay#RESPONSE_QUEUE_BYTES}) -- and the
 * budget fit in half the maximum heap.
 * <p>
 * A connection is admitted at accept ({@link #admit}) and holds its {@link Admission}
 * until it ended and the frames its virtual connections queued left the tunnel sender (a
 * stream that stays not ready keeps them, and the slots, until the session ends);
 * {@link Admission#release()} frees its slots and may be called on every exit path, only
 * the first call counts. A connection over a limit is closed without being read.
 * Rejections are counted as {@code sluice.data.connections.rejected} by {@code reason}
 * ({@code limit}, {@code address-limit}, {@code head-timeout}) and logged through the
 * access log's rate limit. Connections closed for idling between requests count as
 * {@code sluice.data.connections.idle.closed}, those closed for stalling an exchange as
 * {@code sluice.data.connections.stall.closed}, connections relayed verbatim closed for
 * relaying nothing as {@code sluice.data.connections.verbatim.idle.closed}.
 */
@Component
public class ConnectionLimits implements DisposableBean {

	private static final Logger log = LoggerFactory.getLogger(ConnectionLimits.class);

	private static final String REJECTED = "sluice.data.connections.rejected";

	private final ConnectionLimit global;

	private final H2DemuxBudget h2Budget;

	private final int maxPerAddress;

	private final Duration headTimeout;

	private final Duration idleTimeout;

	private final Duration stallTimeout;

	private final long stallMinBytes;

	/** The verbatim idle timeout of the routes that set none; zero = off. */
	private final Duration verbatimIdleTimeout;

	/** Open connections per source address; an entry lives while its count is > 0. */
	private final ConcurrentMap<InetAddress, Integer> perAddress = new ConcurrentHashMap<>();

	private final ScheduledThreadPoolExecutor deadlines;

	private final Counter rejectedLimit;

	private final Counter rejectedAddressLimit;

	private final Counter rejectedHeadTimeout;

	private final Counter idleClosed;

	private final Counter stallClosed;

	private final Counter verbatimIdleClosed;

	ConnectionLimits(SluiceServerProperties properties, MeterRegistry meterRegistry) {
		SluiceServerProperties.Limits limits = properties.limits();
		long maxHeap = Runtime.getRuntime().maxMemory();
		long budget = limits.h2QueueBudget().toBytes() > 0 ? limits.h2QueueBudget().toBytes() : deriveBudget(maxHeap);
		this.h2Budget = new H2DemuxBudget(budget);
		this.global = new ConnectionLimit(
				limits.maxConnections() > 0 ? limits.maxConnections() : deriveMaxConnections(maxHeap, budget));
		int perAddress = limits.maxConnectionsPerAddress();
		this.maxPerAddress = perAddress > 0 ? Math.min(perAddress, this.global.max()) : this.global.max();
		this.headTimeout = limits.headTimeout();
		this.idleTimeout = limits.idleTimeout();
		this.stallTimeout = limits.stallTimeout();
		this.stallMinBytes = limits.stallMinBytes().toBytes();
		this.verbatimIdleTimeout = limits.verbatimIdleTimeout();
		this.deadlines = new ScheduledThreadPoolExecutor(1,
				Thread.ofPlatform().name("sluice-deadline").daemon().factory());
		// a cancelled deadline leaves the queue at once instead of after the timeout
		this.deadlines.setRemoveOnCancelPolicy(true);
		this.rejectedLimit = rejected(meterRegistry, "limit");
		this.rejectedAddressLimit = rejected(meterRegistry, "address-limit");
		this.rejectedHeadTimeout = rejected(meterRegistry, "head-timeout");
		this.idleClosed = Counter.builder("sluice.data.connections.idle.closed")
			.description("Data plane connections closed after idling between requests for the idle timeout")
			.register(meterRegistry);
		this.stallClosed = Counter.builder("sluice.data.connections.stall.closed")
			.description("Data plane connections closed after an exchange in flight waited on its client "
					+ "for the stall timeout without progress")
			.register(meterRegistry);
		this.verbatimIdleClosed = Counter.builder("sluice.data.connections.verbatim.idle.closed")
			.description("Data plane connections relayed verbatim closed after relaying no byte in either direction "
					+ "for the verbatim idle timeout")
			.register(meterRegistry);
		Gauge.builder("sluice.data.connections.limit", this.global, ConnectionLimit::max)
			.description("Data plane connections allowed open at once")
			.register(meterRegistry);
		Gauge.builder("sluice.data.connections.admitted", this.global, ConnectionLimit::open)
			.description("Data plane connections and further h2 demux legs holding a slot of the connection limit")
			.register(meterRegistry);
		Gauge.builder("sluice.h2demux.budget.bytes", this.h2Budget, H2DemuxBudget::max)
			.description("Bytes the h2 demux relays may queue beyond their per-connection allowance, shared")
			.register(meterRegistry);
		Gauge.builder("sluice.h2demux.budget.used.bytes", this.h2Budget, H2DemuxBudget::used)
			.description("Bytes the h2 demux relays queue beyond their per-connection allowance")
			.register(meterRegistry);
		log.info(
				"data plane connection limit {} ({}), per address {}, head timeout {}, idle timeout {}, stall timeout {} per {}B, verbatim idle timeout {}, h2 queue budget {}KiB ({})",
				this.global.max(),
				limits.maxConnections() > 0 ? "configured"
						: "derived: (half of max heap %dMiB - h2 queue budget) / %dKiB per connection"
							.formatted(maxHeap >> 20, connectionBytes() >> 10),
				this.maxPerAddress, this.headTimeout, this.idleTimeout, this.stallTimeout, this.stallMinBytes,
				this.verbatimIdleTimeout.isZero() ? "off (unless a route sets one)" : this.verbatimIdleTimeout,
				budget >> 10,
				limits.h2QueueBudget().toBytes() > 0 ? "configured" : "derived: an eighth of half of max heap");
	}

	/**
	 * The worst case of one slot: a virtual connection plus an h2 relay's own queue and
	 * one leg's response queue.
	 */
	static long connectionBytes() {
		return ConnectionLimit.CONNECTION_BYTES + Http2DemuxRelay.UNBUDGETED_BYTES
				+ Http2DemuxRelay.RESPONSE_QUEUE_BYTES;
	}

	/** The h2 queue budget an unset value derives: an eighth of half the heap. */
	static long deriveBudget(long maxHeap) {
		return maxHeap / 2 / 8;
	}

	/**
	 * The connections whose worst case ({@link #connectionBytes()}) fits in half the heap
	 * besides the h2 queue budget; at least 1.
	 */
	static int deriveMaxConnections(long maxHeap, long budget) {
		return ConnectionLimit.derive(maxHeap / 2 - budget, connectionBytes());
	}

	private static Counter rejected(MeterRegistry meterRegistry, String reason) {
		return Counter.builder(REJECTED)
			.tag("reason", reason)
			.description("Data plane connections closed by a connection limit or the head deadline")
			.register(meterRegistry);
	}

	/**
	 * Admits an accepted connection, or returns null when the limit is reached: the
	 * caller closes the socket without reading it.
	 */
	@Nullable Admission admit(AccessLogger.Connection access) {
		ConnectionLimit.Permit permit = this.global.tryAcquire();
		if (permit == null) {
			this.rejectedLimit.increment();
			access.reject("limit");
			return null;
		}
		return new Admission(permit, access);
	}

	/**
	 * A slot for one more leg of an h2 demux relay beyond the one its connection's own
	 * slot covers, or null when the limit is reached: the stream needing it is refused.
	 */
	ConnectionLimit.@Nullable Permit tryAcquireLeg() {
		return this.global.tryAcquire();
	}

	/**
	 * The deadline of a connection after its head phase: between the requests of a
	 * relayed HTTP connection, or while it is relayed verbatim; busy and arming nothing
	 * until the relay watches it or enters another phase. Its expiry closes
	 * {@code socket}, the connection's transport.
	 */
	KeepAliveDeadline keepAliveDeadline(Socket socket) {
		return new KeepAliveDeadline(socket);
	}

	/**
	 * The verbatim idle timeout of {@code route}: its own when it sets one, the server's
	 * default otherwise; zero = none.
	 */
	Duration verbatimIdleTimeout(Router.Route route) {
		Duration own = route.verbatimIdleTimeout();
		return own.isZero() ? this.verbatimIdleTimeout : own;
	}

	/** Deadlines scheduled and not yet passed or cancelled; for tests. */
	int scheduledDeadlines() {
		return this.deadlines.getQueue().size();
	}

	/** The queue budget shared by the h2 demux relays. */
	H2DemuxBudget h2Budget() {
		return this.h2Budget;
	}

	/** Connections open at once, at most. */
	int max() {
		return this.global.max();
	}

	/** Connections from one source address open at once, at most. */
	int maxPerAddress() {
		return this.maxPerAddress;
	}

	/** Connections holding a slot. */
	int admitted() {
		return this.global.open();
	}

	/** Connections from {@code address} holding a slot. */
	int admitted(InetAddress address) {
		return this.perAddress.getOrDefault(address, 0);
	}

	/** Source addresses with an open connection; entries never outlive their count. */
	int trackedAddresses() {
		return this.perAddress.size();
	}

	@Override
	public void destroy() {
		this.deadlines.shutdownNow();
	}

	/** The slots and the head deadline of one admitted connection. */
	final class Admission {

		private static final int HEAD = 0;

		/** The head completed, or the connection ended, before the deadline. */
		private static final int SERVED = 1;

		/** The deadline passed first and closed the socket. */
		private static final int EXPIRED = 2;

		private final ConnectionLimit.Permit permit;

		private final AccessLogger.Connection access;

		private final AtomicBoolean released = new AtomicBoolean();

		/** The claimed source address; set once. */
		private final AtomicReference<@Nullable InetAddress> address = new AtomicReference<>();

		private final AtomicReference<@Nullable ScheduledFuture<?>> deadline = new AtomicReference<>();

		/** {@link #HEAD}, then {@link #SERVED} or {@link #EXPIRED}; changes once. */
		private final AtomicInteger phase = new AtomicInteger(HEAD);

		private Admission(ConnectionLimit.Permit permit, AccessLogger.Connection access) {
			this.permit = permit;
			this.access = access;
		}

		/**
		 * Claims a slot of the source address; false when the address is at its limit, in
		 * which case the caller closes the connection without reading further. A null
		 * address (an unconnected socket) is not limited.
		 */
		boolean claim(@Nullable InetAddress source) {
			if (source == null) {
				return true;
			}
			boolean[] claimed = new boolean[1];
			ConnectionLimits.this.perAddress.compute(source, (key, count) -> {
				int current = count == null ? 0 : count;
				if (current >= ConnectionLimits.this.maxPerAddress) {
					return count;
				}
				claimed[0] = true;
				return current + 1;
			});
			if (!claimed[0]) {
				ConnectionLimits.this.rejectedAddressLimit.increment();
				this.access.reject("address-limit");
				return false;
			}
			if (!this.address.compareAndSet(null, source)) {
				// claimed twice: the first claim holds the slot
				unclaim(source);
			}
			else if (this.released.get() && this.address.compareAndSet(source, null)) {
				// released meanwhile, before release() could see the address
				unclaim(source);
			}
			return true;
		}

		/**
		 * Starts the head deadline: once it passes before {@link #endHead()}, the socket
		 * is closed, which fails the reads of the head phase wherever they wait.
		 */
		void startHead(Socket socket) {
			ScheduledFuture<?> future = ConnectionLimits.this.deadlines.schedule(() -> expire(socket),
					ConnectionLimits.this.headTimeout.toNanos(), TimeUnit.NANOSECONDS);
			this.deadline.set(future);
		}

		private void expire(Socket socket) {
			if (!this.phase.compareAndSet(HEAD, EXPIRED)) {
				return;
			}
			ConnectionLimits.this.rejectedHeadTimeout.increment();
			this.access.reject("head-timeout");
			try {
				socket.close();
			}
			catch (Exception e) {
				// ignore
			}
		}

		/**
		 * Ends the head phase; false when the deadline passed first (the socket is
		 * closed). The relay that follows has no deadline.
		 */
		boolean endHead() {
			cancelDeadline();
			return this.phase.compareAndSet(HEAD, SERVED) || this.phase.get() == SERVED;
		}

		/** Whether the head deadline closed the connection. */
		boolean expired() {
			return this.phase.get() == EXPIRED;
		}

		private void cancelDeadline() {
			ScheduledFuture<?> future = this.deadline.getAndSet(null);
			if (future != null) {
				future.cancel(false);
			}
		}

		/** Frees the slots and cancels the deadline; only the first call counts. */
		void release() {
			if (!this.released.compareAndSet(false, true)) {
				return;
			}
			// a deadline racing the cancel finds the phase over and leaves the socket be
			this.phase.compareAndSet(HEAD, SERVED);
			cancelDeadline();
			InetAddress source = this.address.getAndSet(null);
			if (source != null) {
				unclaim(source);
			}
			this.permit.release();
		}

		private void unclaim(InetAddress source) {
			ConnectionLimits.this.perAddress.computeIfPresent(source, (key, count) -> count <= 1 ? null : count - 1);
		}

	}

	/**
	 * The deadline of a relayed HTTP connection after its head phase, driven by the relay
	 * through {@link #enter(Phase)}: the idle timeout while the connection idles between
	 * requests ({@link Phase#IDLE}), the head timeout while a later request head is
	 * incomplete ({@link Phase#HEAD}), and while a request or response is in flight
	 * ({@link Phase#BUSY}) the stall timeout of a relay watched through
	 * {@link #watchStalls}, none otherwise. A phase entered anew starts its timeout over;
	 * entering the current phase keeps the running one. Once a timeout passes, the
	 * relay's teardown runs on a virtual thread of its own -- never on the shared
	 * deadline thread, as it may write -- and the transport socket is closed after it, or
	 * after {@link #TEARDOWN_GRACE} at the latest should the teardown block on a write
	 * the peer does not read. {@link #disarm()} ends the deadline for good: the relay
	 * ended.
	 * <p>
	 * A connection relayed verbatim -- from its start, or once an HTTP/1.1 connection
	 * left HTTP (an upgrade) -- enters {@link Phase#VERBATIM} for good through
	 * {@link #relayVerbatim}: the verbatim idle timeout of its route runs from the last
	 * {@link #progress(long)} reported, a byte read from or written to the client, and
	 * its expiry tears the relay down as above.
	 * <p>
	 * The stall timeout counts only the time the relay waits on its client: busy, the
	 * deadline reads the relay's clock of that time {@link #STALL_SAMPLES} times per
	 * stall timeout ({@link #watchStallClock}) and adds what it advanced since the
	 * previous reading. A relay that can only tell whether it waits right now
	 * ({@link #watchStalls}) is sampled instead: the time between two samples counts when
	 * both found it waiting, so a wait shorter than a sample interval may go unseen, a
	 * wait spanning two samples counts whole, and a sample that finds the relay between
	 * two waits loses both intervals around it. Progress -- the stall minimum of bytes
	 * moved to or from the client, reported through {@link #progress(long)} -- starts the
	 * count over, and so does the busy phase entered anew. Once the count reaches the
	 * stall timeout the relay's stall teardown runs as above. The first reading after the
	 * count starts only sets where it counts from, so a stall is cut between the stall
	 * timeout and two sample intervals past it.
	 */
	final class KeepAliveDeadline {

		/** The most a teardown may take before the transport is closed under it. */
		static final Duration TEARDOWN_GRACE = Duration.ofSeconds(1);

		/** How often per stall timeout a busy relay is asked whether it waits. */
		static final int STALL_SAMPLES = 8;

		/**
		 * The most a relay writes to the client at once: a blocking write tells nothing
		 * until it returns, so a large one is made in pieces, each reported as progress.
		 */
		static final int WRITE_PIECE = 16 * 1024;

		/** What the connection is doing, which decides its deadline. */
		enum Phase {

			/**
			 * A request or response is in flight: the stall timeout when the relay is
			 * watched, none otherwise.
			 */
			BUSY,

			/** Between requests: the idle timeout. */
			IDLE,

			/** A later request head is incomplete: the head timeout. */
			HEAD,

			/**
			 * Relayed verbatim, for good: the verbatim idle timeout when the route has
			 * one, none otherwise.
			 */
			VERBATIM

		}

		private final Socket socket;

		private final Object lock = new Object();

		private Phase phase = Phase.BUSY;

		/** Bumped by every change, so a timeout that fires stale does nothing. */
		private long generation;

		private @Nullable ScheduledFuture<?> future;

		private boolean expired;

		private boolean disarmed;

		private volatile Runnable teardown = () -> {
		};

		/** The time the relay waited on its client so far; null = no stall timeout. */
		private volatile @Nullable LongSupplier clientWaitNanos;

		private volatile Runnable stallTeardown = () -> {
		};

		/** When progress was last reported; read by the verbatim idle timeout. */
		private volatile long lastProgress = System.nanoTime();

		/** The verbatim idle timeout running, in nanoseconds; with the lock. */
		private long verbatimIdleNanos;

		/** Bytes moved since the last progress counted. */
		private final AtomicLong moved = new AtomicLong();

		/** Bumped by every progress counted. */
		private final AtomicLong progresses = new AtomicLong();

		/** The time waited on the client since the count started; with the lock. */
		private long waitedNanos;

		/** {@link #clientWaitNanos} at the last sample; with the lock. */
		private long clientWaitAtLastSample;

		/**
		 * Whether a sample since the count started set {@link #clientWaitAtLastSample}.
		 */
		private boolean sampledSinceArmed;

		/** {@link #progresses} at the last sample; with the lock. */
		private long progressesAtLastSample;

		private KeepAliveDeadline(Socket socket) {
			this.socket = socket;
		}

		/**
		 * The relay's teardown, run once the idle, head or verbatim idle timeout passes.
		 */
		void onExpiry(Runnable teardown) {
			this.teardown = teardown;
		}

		/**
		 * Subjects the busy phase to the stall timeout, sampling the relay:
		 * {@code waitsOnClient} tells whether the relay waits on its client right now --
		 * asked on the shared deadline thread, it must not block --, and {@code teardown}
		 * runs once the stall timeout passed. Starts the count when the connection is
		 * busy already.
		 */
		void watchStalls(BooleanSupplier waitsOnClient, Runnable teardown) {
			watchStallClock(sampled(waitsOnClient), teardown);
		}

		/**
		 * Subjects the busy phase to the stall timeout: {@code clientWaitNanos} tells the
		 * time the relay waited on its client so far, the wait in progress included --
		 * read on the shared deadline thread, it must not block --, and {@code teardown}
		 * runs once the stall timeout passed. Starts the count when the connection is
		 * busy already.
		 */
		void watchStallClock(LongSupplier clientWaitNanos, Runnable teardown) {
			synchronized (this.lock) {
				this.clientWaitNanos = clientWaitNanos;
				this.stallTeardown = teardown;
				if (!this.expired && !this.disarmed && this.phase == Phase.BUSY) {
					cancelLocked();
					armStallLocked();
				}
			}
		}

		/**
		 * Reports {@code bytes} moved to or from the client: any starts the verbatim idle
		 * timeout over, every stall minimum of them the stall count.
		 */
		void progress(long bytes) {
			if (bytes <= 0) {
				return;
			}
			this.lastProgress = System.nanoTime();
			if (this.moved.addAndGet(bytes) >= ConnectionLimits.this.stallMinBytes) {
				this.moved.set(0);
				this.progresses.incrementAndGet();
			}
		}

		/**
		 * Enters {@code next}; {@code false} once a timeout passed, in which case the
		 * caller drops what it was about to relay: the connection is being closed.
		 */
		boolean enter(Phase next) {
			synchronized (this.lock) {
				if (this.expired) {
					return false;
				}
				if (this.disarmed || next == this.phase || this.phase == Phase.VERBATIM) {
					return true;
				}
				cancelLocked();
				this.phase = next;
				if (next == Phase.BUSY) {
					armStallLocked();
				}
				else {
					long armed = this.generation;
					Duration timeout = next == Phase.IDLE ? ConnectionLimits.this.idleTimeout
							: ConnectionLimits.this.headTimeout;
					this.future = ConnectionLimits.this.deadlines.schedule(() -> expire(armed), timeout.toNanos(),
							TimeUnit.NANOSECONDS);
				}
				return true;
			}
		}

		/**
		 * Enters {@link Phase#VERBATIM} for good: the connection is relayed verbatim on
		 * {@code route} from now on, and torn down once no progress was reported for the
		 * route's verbatim idle timeout ({@link ConnectionLimits#verbatimIdleTimeout});
		 * nothing is armed when it has none. Entering it again keeps the running timeout.
		 * {@code false} once a timeout passed, in which case the caller drops what it was
		 * about to relay: the connection is being closed.
		 */
		boolean relayVerbatim(Router.Route route) {
			synchronized (this.lock) {
				if (this.expired) {
					return false;
				}
				if (this.disarmed || this.phase == Phase.VERBATIM) {
					return true;
				}
				cancelLocked();
				this.phase = Phase.VERBATIM;
				Duration timeout = ConnectionLimits.this.verbatimIdleTimeout(route);
				if (!timeout.isZero()) {
					this.verbatimIdleNanos = saturatedNanos(timeout);
					this.lastProgress = System.nanoTime();
					scheduleVerbatimCheckLocked(this.generation, this.verbatimIdleNanos);
				}
				return true;
			}
		}

		private void scheduleVerbatimCheckLocked(long armed, long delayNanos) {
			this.future = ConnectionLimits.this.deadlines.schedule(() -> checkVerbatim(armed), delayNanos,
					TimeUnit.NANOSECONDS);
		}

		/**
		 * Tears down once no progress was reported for the verbatim idle timeout; checks
		 * again when it is due otherwise.
		 */
		private void checkVerbatim(long armed) {
			synchronized (this.lock) {
				if (this.expired || this.disarmed || armed != this.generation) {
					return;
				}
				long idle = System.nanoTime() - this.lastProgress;
				if (idle < this.verbatimIdleNanos) {
					scheduleVerbatimCheckLocked(armed, this.verbatimIdleNanos - idle);
					return;
				}
				this.expired = true;
				this.future = null;
			}
			ConnectionLimits.this.verbatimIdleClosed.increment();
			tearDown(this.teardown);
		}

		private static long saturatedNanos(Duration duration) {
			try {
				return duration.toNanos();
			}
			catch (ArithmeticException e) {
				return Long.MAX_VALUE;
			}
		}

		/** Cancels the deadline for good; later phases arm nothing. */
		void disarm() {
			synchronized (this.lock) {
				this.disarmed = true;
				this.phase = Phase.BUSY;
				cancelLocked();
			}
		}

		/** Whether a timeout passed. */
		boolean expired() {
			synchronized (this.lock) {
				return this.expired;
			}
		}

		private void cancelLocked() {
			this.generation++;
			ScheduledFuture<?> scheduled = this.future;
			this.future = null;
			if (scheduled != null) {
				scheduled.cancel(false);
			}
		}

		/** Starts the stall count of a watched relay; with the lock. */
		private void armStallLocked() {
			if (this.clientWaitNanos == null) {
				return;
			}
			this.moved.set(0);
			this.waitedNanos = 0;
			this.sampledSinceArmed = false;
			this.progressesAtLastSample = this.progresses.get();
			scheduleSampleLocked(this.generation);
		}

		private void scheduleSampleLocked(long armed) {
			long interval = Math.max(TimeUnit.MILLISECONDS.toNanos(1),
					ConnectionLimits.this.stallTimeout.toNanos() / STALL_SAMPLES);
			this.future = ConnectionLimits.this.deadlines.schedule(() -> sample(armed), interval, TimeUnit.NANOSECONDS);
		}

		/**
		 * Reads the relay's clock and counts what it advanced since the last sample; past
		 * the stall timeout, tears down.
		 */
		private void sample(long armed) {
			LongSupplier clock = this.clientWaitNanos;
			// read outside the lock: the relay enters phases under its own locks
			long clientWait = clock != null ? clock.getAsLong() : 0;
			synchronized (this.lock) {
				if (this.expired || this.disarmed || armed != this.generation) {
					return;
				}
				long progresses = this.progresses.get();
				if (progresses != this.progressesAtLastSample) {
					this.progressesAtLastSample = progresses;
					this.waitedNanos = 0;
				}
				else if (this.sampledSinceArmed) {
					this.waitedNanos += Math.max(0, clientWait - this.clientWaitAtLastSample);
				}
				this.clientWaitAtLastSample = clientWait;
				this.sampledSinceArmed = true;
				if (this.waitedNanos < ConnectionLimits.this.stallTimeout.toNanos()) {
					scheduleSampleLocked(armed);
					return;
				}
				this.expired = true;
				this.future = null;
			}
			ConnectionLimits.this.stallClosed.increment();
			tearDown(this.stallTeardown);
		}

		/**
		 * The clock of a relay sampled through {@code waitsOnClient}: advances by the
		 * time between two readings that both found the relay waiting.
		 */
		private static LongSupplier sampled(BooleanSupplier waitsOnClient) {
			return new LongSupplier() {

				private long waited;

				private long lastReading;

				private boolean waitingAtLastReading;

				@Override
				public synchronized long getAsLong() {
					boolean waiting = waitsOnClient.getAsBoolean();
					long now = System.nanoTime();
					if (waiting && this.waitingAtLastReading) {
						this.waited += now - this.lastReading;
					}
					this.waitingAtLastReading = waiting;
					this.lastReading = now;
					return this.waited;
				}

			};
		}

		private void expire(long armed) {
			Phase passed;
			synchronized (this.lock) {
				if (this.expired || this.disarmed || armed != this.generation) {
					return;
				}
				this.expired = true;
				this.future = null;
				passed = this.phase;
			}
			(passed == Phase.IDLE ? ConnectionLimits.this.idleClosed : ConnectionLimits.this.rejectedHeadTimeout)
				.increment();
			tearDown(this.teardown);
		}

		/**
		 * Runs the teardown on a virtual thread, then closes the transport; after the
		 * grace at the latest.
		 */
		private void tearDown(Runnable teardown) {
			ScheduledFuture<?> backstop = ConnectionLimits.this.deadlines.schedule(this::closeSocket,
					TEARDOWN_GRACE.toNanos(), TimeUnit.NANOSECONDS);
			Thread.ofVirtual().name("sluice-deadline-teardown").start(() -> {
				try {
					teardown.run();
				}
				finally {
					closeSocket();
					backstop.cancel(false);
				}
			});
		}

		private void closeSocket() {
			try {
				this.socket.close();
			}
			catch (Exception e) {
				// ignore
			}
		}

	}

}
