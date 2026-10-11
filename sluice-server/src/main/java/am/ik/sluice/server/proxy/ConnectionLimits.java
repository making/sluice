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
 * one deadline for the whole head phase.
 * <p>
 * A connection is admitted at accept ({@link #admit}) and holds its {@link Admission}
 * until it ends; {@link Admission#release()} frees its slots and may be called on every
 * exit path, only the first call counts. A connection over a limit is closed without
 * being read. Rejections are counted as {@code sluice.data.connections.rejected} by
 * {@code reason} ({@code limit}, {@code address-limit}, {@code head-timeout}) and logged
 * through the access log's rate limit.
 */
@Component
public class ConnectionLimits implements DisposableBean {

	private static final Logger log = LoggerFactory.getLogger(ConnectionLimits.class);

	private static final String REJECTED = "sluice.data.connections.rejected";

	private final ConnectionLimit global;

	private final int maxPerAddress;

	private final Duration headTimeout;

	/** Open connections per source address; an entry lives while its count is > 0. */
	private final ConcurrentMap<InetAddress, Integer> perAddress = new ConcurrentHashMap<>();

	private final ScheduledThreadPoolExecutor deadlines;

	private final Counter rejectedLimit;

	private final Counter rejectedAddressLimit;

	private final Counter rejectedHeadTimeout;

	ConnectionLimits(SluiceServerProperties properties, MeterRegistry meterRegistry) {
		SluiceServerProperties.Limits limits = properties.limits();
		this.global = new ConnectionLimit(ConnectionLimit.resolve(limits.maxConnections()));
		int perAddress = limits.maxConnectionsPerAddress();
		this.maxPerAddress = perAddress > 0 ? Math.min(perAddress, this.global.max()) : this.global.max();
		this.headTimeout = limits.headTimeout();
		this.deadlines = new ScheduledThreadPoolExecutor(1,
				Thread.ofPlatform().name("sluice-head-deadline").daemon().factory());
		// a cancelled deadline leaves the queue at once instead of after the timeout
		this.deadlines.setRemoveOnCancelPolicy(true);
		this.rejectedLimit = rejected(meterRegistry, "limit");
		this.rejectedAddressLimit = rejected(meterRegistry, "address-limit");
		this.rejectedHeadTimeout = rejected(meterRegistry, "head-timeout");
		Gauge.builder("sluice.data.connections.limit", this.global, ConnectionLimit::max)
			.description("Data plane connections allowed open at once")
			.register(meterRegistry);
		Gauge.builder("sluice.data.connections.admitted", this.global, ConnectionLimit::open)
			.description("Data plane connections holding a slot of the connection limit")
			.register(meterRegistry);
		log.info("data plane connection limit {} ({}), per address {}, head timeout {}", this.global.max(),
				limits.maxConnections() > 0 ? "configured"
						: "derived: half of max heap %dMiB / %dKiB per connection"
							.formatted(Runtime.getRuntime().maxMemory() >> 20, ConnectionLimit.CONNECTION_BYTES >> 10),
				this.maxPerAddress, this.headTimeout);
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

}
