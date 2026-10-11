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
import java.util.concurrent.atomic.AtomicReference;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.stereotype.Component;

import am.ik.sluice.server.config.SluiceServerProperties;
import am.ik.sluice.tunnel.ConnectionLimit;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * The limits every data plane connection lives under, shared by the data port and every
 * tcp route port: a cap on the connections open at once, a cap per source address, and
 * one deadline for the whole head phase; the deadlines between the requests of a relayed
 * HTTP connection ({@link KeepAliveDeadline}); plus the queue budget the h2 demux relays
 * share.
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
 * {@code sluice.data.connections.idle.closed}.
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

	/** Open connections per source address; an entry lives while its count is > 0. */
	private final ConcurrentMap<InetAddress, Integer> perAddress = new ConcurrentHashMap<>();

	private final ScheduledThreadPoolExecutor deadlines;

	private final Counter rejectedLimit;

	private final Counter rejectedAddressLimit;

	private final Counter rejectedHeadTimeout;

	private final Counter idleClosed;

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
				"data plane connection limit {} ({}), per address {}, head timeout {}, idle timeout {}, h2 queue budget {}KiB ({})",
				this.global.max(),
				limits.maxConnections() > 0 ? "configured"
						: "derived: (half of max heap %dMiB - h2 queue budget) / %dKiB per connection"
							.formatted(maxHeap >> 20, connectionBytes() >> 10),
				this.maxPerAddress, this.headTimeout, this.idleTimeout, budget >> 10,
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
	 * A disarmed deadline for the requests after the head phase of a relayed HTTP
	 * connection; its expiry closes {@code socket}, the connection's transport.
	 */
	KeepAliveDeadline keepAliveDeadline(Socket socket) {
		return new KeepAliveDeadline(socket);
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
	 * through {@link #enter(Phase)}: none while a request or response is in flight
	 * ({@link Phase#BUSY}), the idle timeout while the connection idles between requests
	 * ({@link Phase#IDLE}), the head timeout while a later request head is incomplete
	 * ({@link Phase#HEAD}). A phase entered anew starts its timeout over; entering the
	 * current phase keeps the running one. Once a timeout passes, the relay's teardown
	 * runs on a virtual thread of its own -- never on the shared deadline thread, as it
	 * may write -- and the transport socket is closed after it, or after
	 * {@link #TEARDOWN_GRACE} at the latest should the teardown block on a write the peer
	 * does not read. {@link #disarm()} ends the deadline for good: the relay ended, or
	 * left HTTP (an upgrade).
	 */
	final class KeepAliveDeadline {

		/** The most a teardown may take before the transport is closed under it. */
		static final Duration TEARDOWN_GRACE = Duration.ofSeconds(1);

		/** What the connection is doing, which decides its deadline. */
		enum Phase {

			/** A request or response is in flight: no deadline. */
			BUSY,

			/** Between requests: the idle timeout. */
			IDLE,

			/** A later request head is incomplete: the head timeout. */
			HEAD

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

		private KeepAliveDeadline(Socket socket) {
			this.socket = socket;
		}

		/** The relay's teardown, run once a timeout passes. */
		void onExpiry(Runnable teardown) {
			this.teardown = teardown;
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
				if (this.disarmed || next == this.phase) {
					return true;
				}
				cancelLocked();
				this.phase = next;
				if (next != Phase.BUSY) {
					long armed = this.generation;
					Duration timeout = next == Phase.IDLE ? ConnectionLimits.this.idleTimeout
							: ConnectionLimits.this.headTimeout;
					this.future = ConnectionLimits.this.deadlines.schedule(() -> expire(armed), timeout.toNanos(),
							TimeUnit.NANOSECONDS);
				}
				return true;
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
			ScheduledFuture<?> backstop = ConnectionLimits.this.deadlines.schedule(this::closeSocket,
					TEARDOWN_GRACE.toNanos(), TimeUnit.NANOSECONDS);
			Runnable teardown = this.teardown;
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
