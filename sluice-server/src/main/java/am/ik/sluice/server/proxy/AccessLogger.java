package am.ik.sluice.server.proxy;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import am.ik.sluice.server.config.SluiceServerProperties;
import am.ik.sluice.tunnel.StreamRelay;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Connection and (head) request access log for the data plane, emitted to the
 * {@code sluice.access} logger as single-line key=value pairs; disable via
 * {@code sluice.access-log.enabled=false}. Byte totals are accumulated per direction and
 * the {@code close} line is emitted when the relay completes, so the counts are final.
 * <p>
 * When {@code sluice.access-log.rate-limit.enabled=true} (the default), each line kind
 * ({@code conn-accept} / {@code conn-close} / {@code request}) is rate limited the same
 * way as UNIX syslog: the first {@code maxRate} lines of each kind within a
 * {@code period} are emitted, further lines are suppressed and counted, and one
 * {@code type=ratelimit} summary line reports the suppressed count when the next period
 * starts.
 */
@Component
public class AccessLogger {

	private static final Logger log = LoggerFactory.getLogger("sluice.access");

	private final AtomicInteger sequence = new AtomicInteger();

	private final boolean enabled;

	private final boolean connEnabled;

	private final boolean requestEnabled;

	private final @Nullable RateLimiter connAcceptLimiter;

	private final @Nullable RateLimiter connCloseLimiter;

	private final @Nullable RateLimiter requestLimiter;

	@Autowired
	AccessLogger(SluiceServerProperties properties) {
		this(properties, System::nanoTime);
	}

	AccessLogger(SluiceServerProperties properties, LongSupplier clock) {
		SluiceServerProperties.AccessLog accessLog = properties.accessLog();
		this.enabled = accessLog.enabled();
		Set<SluiceServerProperties.AccessLog.Type> types = accessLog.types();
		this.connEnabled = types.contains(SluiceServerProperties.AccessLog.Type.CONNECTION);
		this.requestEnabled = types.contains(SluiceServerProperties.AccessLog.Type.REQUEST);
		SluiceServerProperties.AccessLog.RateLimit rateLimit = accessLog.rateLimit();
		if (rateLimit.enabled()) {
			this.connAcceptLimiter = RateLimiter.builder()
				.kind("conn-accept")
				.maxRate(rateLimit.maxRate())
				.period(rateLimit.period())
				.clock(clock)
				.build();
			this.connCloseLimiter = RateLimiter.builder()
				.kind("conn-close")
				.maxRate(rateLimit.maxRate())
				.period(rateLimit.period())
				.clock(clock)
				.build();
			this.requestLimiter = RateLimiter.builder()
				.kind("request")
				.maxRate(rateLimit.maxRate())
				.period(rateLimit.period())
				.clock(clock)
				.build();
		}
		else {
			this.connAcceptLimiter = null;
			this.connCloseLimiter = null;
			this.requestLimiter = null;
		}
	}

	/**
	 * Begins tracking an accepted connection; returns a no-op instance when disabled.
	 */
	public Connection accepted(String listener, Socket socket) {
		if (!this.enabled || (!this.connEnabled && !this.requestEnabled)) {
			return Connection.NOOP;
		}
		return Connection.builder()
			.owner(this)
			.id(this.sequence.incrementAndGet())
			.listener(listener)
			.remote(peerOf(socket))
			.connEnabled(this.connEnabled)
			.requestEnabled(this.requestEnabled)
			.build();
	}

	private static String peerOf(Socket socket) {
		try {
			if (socket.getRemoteSocketAddress() instanceof InetSocketAddress address) {
				return address.getHostString() + ":" + address.getPort();
			}
			return String.valueOf(socket.getRemoteSocketAddress());
		}
		catch (Exception e) {
			return "-";
		}
	}

	private static boolean allows(@Nullable RateLimiter limiter) {
		return limiter == null || limiter.tryAcquire();
	}

	boolean allowAccept() {
		return allows(this.connAcceptLimiter);
	}

	boolean allowClose() {
		return allows(this.connCloseLimiter);
	}

	boolean allowRequest() {
		return allows(this.requestLimiter);
	}

	/**
	 * The mutable per-connection state; all lifecycle methods are safe to call from any
	 * relay thread, and the close line is emitted once.
	 */
	public static final class Connection {

		private static final Connection NOOP = Connection.builder().noop().build();

		private final @Nullable AccessLogger owner;

		private final int id;

		private final String listener;

		private final String remote;

		private final boolean noop;

		private final boolean connEnabled;

		private final boolean requestEnabled;

		private final long startNanos = System.nanoTime();

		private final AtomicLong bytesIn = new AtomicLong();

		private final AtomicLong bytesOut = new AtomicLong();

		private String route = "-";

		private String transport = "-";

		private long connectionId = -1;

		private volatile boolean closed;

		private Connection(Builder builder) {
			this.owner = builder.owner;
			this.id = builder.id;
			this.listener = builder.listener;
			this.remote = builder.remote;
			this.noop = builder.noop;
			this.connEnabled = builder.connEnabled;
			this.requestEnabled = builder.requestEnabled;
		}

		static Builder builder() {
			return new Builder();
		}

		Connection transport(String transport) {
			this.transport = transport;
			return this;
		}

		Connection route(String routeTag) {
			this.route = routeTag;
			return this;
		}

		Connection connectionId(long connectionId) {
			this.connectionId = connectionId;
			return this;
		}

		public void bytes(long count, StreamRelay.Direction direction) {
			(direction == StreamRelay.Direction.TO_REMOTE ? this.bytesIn : this.bytesOut).addAndGet(count);
		}

		/**
		 * Logs the head request of the connection; later requests on a keep-alive
		 * connection are not parsed and not logged.
		 */
		public void request(String method, String path, String version) {
			if (!this.requestEnabled || this.noop || this.closed || this.owner == null || !this.owner.allowRequest()) {
				return;
			}
			log.info("type=req id={} route={} method={} path={} http={} remote={}", this.id, this.route, method, path,
					version, this.remote);
		}

		/** Logs the connection acceptance, before route and transport are known. */
		public void accept() {
			if (!this.connEnabled || this.noop || this.owner == null || !this.owner.allowAccept()) {
				return;
			}
			log.info("type=conn id={} event=accept listener={} remote={}", this.id, this.listener, this.remote);
		}

		public void close() {
			if (!this.connEnabled || this.noop || this.closed || this.owner == null || !this.owner.allowClose()) {
				return;
			}
			this.closed = true;
			log.info(
					"type=conn id={} event=close listener={} route={} transport={} remote={} connId={} bytesIn={} bytesOut={} durationMs={}",
					this.id, this.listener, this.route, this.transport, this.remote,
					this.connectionId < 0 ? "-" : this.connectionId, this.bytesIn.get(), this.bytesOut.get(),
					(System.nanoTime() - this.startNanos) / 1_000_000);
		}

		static final class Builder {

			@Nullable private AccessLogger owner;

			private int id;

			private String listener = "-";

			private String remote = "-";

			private boolean noop;

			private boolean connEnabled = true;

			private boolean requestEnabled = true;

			private Builder() {
			}

			Builder owner(AccessLogger owner) {
				this.owner = owner;
				return this;
			}

			Builder id(int id) {
				this.id = id;
				return this;
			}

			Builder listener(String listener) {
				this.listener = listener;
				return this;
			}

			Builder remote(String remote) {
				this.remote = remote;
				return this;
			}

			Builder connEnabled(boolean connEnabled) {
				this.connEnabled = connEnabled;
				return this;
			}

			Builder requestEnabled(boolean requestEnabled) {
				this.requestEnabled = requestEnabled;
				return this;
			}

			/** Marks the connection as the shared no-op instance template. */
			Builder noop() {
				this.noop = true;
				return this;
			}

			Connection build() {
				return new Connection(this);
			}

		}

	}

	/**
	 * Syslog-style rate limiter for one access log line kind: the first {@code maxRate}
	 * lines of the period pass, the rest are counted, and a single summary line reports
	 * the suppressed count when the period rolls over.
	 */
	static final class RateLimiter {

		private final Logger logger;

		private final String kind;

		private final int maxRate;

		private final long periodNanos;

		private final LongSupplier clock;

		private long windowStart;

		private long count;

		private long suppressed;

		private RateLimiter(Builder builder) {
			this.logger = builder.logger;
			this.kind = builder.kind;
			this.maxRate = builder.maxRate;
			this.periodNanos = builder.period.toNanos();
			this.clock = builder.clock;
		}

		static Builder builder() {
			return new Builder();
		}

		/**
		 * Records one log attempt; {@code true} means the caller should emit the line.
		 */
		synchronized boolean tryAcquire() {
			long now = this.clock.getAsLong();
			if (now - this.windowStart >= this.periodNanos) {
				this.report();
				this.windowStart = now;
				this.count = 0;
				this.suppressed = 0;
			}
			this.count++;
			if (this.count <= this.maxRate) {
				return true;
			}
			this.suppressed++;
			return false;
		}

		private void report() {
			if (this.suppressed > 0) {
				this.logger.info("type=ratelimit kind={} suppressed={} periodMs={}", this.kind, this.suppressed,
						this.periodNanos / 1_000_000);
			}
		}

		static final class Builder {

			private Logger logger = AccessLogger.log;

			private String kind = "-";

			private int maxRate = 10;

			private Duration period = Duration.ofSeconds(10);

			private LongSupplier clock = System::nanoTime;

			private Builder() {
			}

			Builder kind(String kind) {
				this.kind = Objects.requireNonNull(kind, "kind is required");
				return this;
			}

			Builder maxRate(int maxRate) {
				this.maxRate = Math.max(1, maxRate);
				return this;
			}

			Builder period(Duration period) {
				this.period = period == null || period.isZero() || period.isNegative() ? Duration.ofSeconds(10)
						: period;
				return this;
			}

			Builder clock(LongSupplier clock) {
				this.clock = Objects.requireNonNull(clock, "clock is required");
				return this;
			}

			RateLimiter build() {
				return new RateLimiter(this);
			}

		}

	}

}
