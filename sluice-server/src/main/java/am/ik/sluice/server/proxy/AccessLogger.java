package am.ik.sluice.server.proxy;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import am.ik.sluice.server.auth.Verdict;
import am.ik.sluice.server.config.SluiceServerProperties;
import am.ik.sluice.server.route.Router;
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
 * Which lines are emitted follows {@code sluice.access-log.enabled} / {@code types}, or a
 * route's {@code sluice.route.access-log.[<route key>]} for a line of that route: a
 * request line its request's route, a close or reject line its connection's route once
 * known. A data port connection's accept line precedes its routing and follows the
 * server-wide settings; a tcp route port's follows its route's.
 * <p>
 * When {@code sluice.access-log.rate-limit.enabled=true} (the default), each line kind
 * ({@code conn-accept} / {@code conn-close} / {@code conn-reject} / {@code request}) is
 * rate limited the same way as UNIX syslog: the first {@code maxRate} lines of each kind
 * within a {@code period} are emitted, further lines are suppressed and counted, and one
 * {@code type=ratelimit} summary line reports the suppressed count when the next period
 * starts.
 */
@Component
public class AccessLogger {

	private static final Logger log = LoggerFactory.getLogger("sluice.access");

	private final AtomicInteger sequence = new AtomicInteger();

	private final Policy policy;

	private final Map<String, Policy> routePolicies;

	private final @Nullable RateLimiter connAcceptLimiter;

	private final @Nullable RateLimiter connCloseLimiter;

	private final @Nullable RateLimiter connRejectLimiter;

	private final @Nullable RateLimiter requestLimiter;

	@Autowired
	AccessLogger(SluiceServerProperties properties) {
		this(properties, System::nanoTime);
	}

	AccessLogger(SluiceServerProperties properties, LongSupplier clock) {
		SluiceServerProperties.AccessLog accessLog = properties.accessLog();
		this.policy = Policy.of(accessLog.enabled(), accessLog.types());
		Map<String, Policy> routePolicies = new HashMap<>();
		properties.route()
			.accessLog()
			.forEach((key, route) -> routePolicies.put(normalize(key),
					Policy.of(route.enabled() == null ? accessLog.enabled() : route.enabled(),
							route.types() == null ? accessLog.types() : route.types())));
		this.routePolicies = Map.copyOf(routePolicies);
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
			this.connRejectLimiter = RateLimiter.builder()
				.kind("conn-reject")
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
			this.connRejectLimiter = null;
			this.requestLimiter = null;
		}
	}

	/**
	 * Begins tracking an accepted connection; returns a no-op instance when no line is
	 * emitted, neither server-wide nor for any route.
	 */
	public Connection accepted(String listener, Socket socket) {
		if (this.policy.none() && this.routePolicies.values().stream().allMatch(Policy::none)) {
			return Connection.NOOP;
		}
		return Connection.builder()
			.owner(this)
			.id(this.sequence.incrementAndGet())
			.listener(listener)
			.remote(peerOf(socket))
			.policy(this.policy)
			.build();
	}

	/**
	 * The lines emitted for the route, the server-wide ones when it configures none.
	 * @param routeKey the route key: the domain or host pattern, {@code tcp:<port>},
	 * {@code *} or empty for the catch-all
	 */
	Policy policyOf(String routeKey) {
		return this.routePolicies.getOrDefault(normalize(routeKey), this.policy);
	}

	private Policy policyOf(Router.@Nullable Route route) {
		return route == null ? this.policy : this.policyOf(route.routeKey());
	}

	/**
	 * The event types the route configures in place of the server-wide ones, comma
	 * separated, {@code off} for none; empty when it configures none.
	 */
	public String describeRoute(String routeKey) {
		Policy policy = this.routePolicies.get(normalize(routeKey));
		return policy == null ? "" : policy.describe();
	}

	private static String normalize(String key) {
		return key == null || key.isBlank() ? "*" : key.strip();
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

	boolean allowReject() {
		return allows(this.connRejectLimiter);
	}

	boolean allowRequest() {
		return allows(this.requestLimiter);
	}

	/**
	 * The line kinds emitted: connection accept, close and reject, request.
	 */
	record Policy(boolean accept, boolean close, boolean reject, boolean request) {

		static final Policy NONE = new Policy(false, false, false, false);

		static Policy of(boolean enabled, Set<SluiceServerProperties.AccessLog.Type> types) {
			if (!enabled) {
				return NONE;
			}
			return new Policy(types.contains(SluiceServerProperties.AccessLog.Type.ACCEPT),
					types.contains(SluiceServerProperties.AccessLog.Type.CLOSE),
					types.contains(SluiceServerProperties.AccessLog.Type.REJECT),
					types.contains(SluiceServerProperties.AccessLog.Type.REQUEST));
		}

		boolean none() {
			return !this.accept && !this.close && !this.reject && !this.request;
		}

		String describe() {
			StringJoiner kinds = new StringJoiner(", ");
			if (this.accept) {
				kinds.add("accept");
			}
			if (this.close) {
				kinds.add("close");
			}
			if (this.reject) {
				kinds.add("reject");
			}
			if (this.request) {
				kinds.add("request");
			}
			return none() ? "off" : kinds.toString();
		}

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

		private String remote;

		private final boolean noop;

		private Policy policy;

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
			this.policy = builder.policy;
		}

		static Builder builder() {
			return new Builder();
		}

		Connection transport(String transport) {
			this.transport = transport;
			return this;
		}

		/**
		 * Overrides the peer of the connection (the source of a PROXY protocol header,
		 * say); lines emitted afterwards report it.
		 */
		Connection remote(String remote) {
			this.remote = remote;
			return this;
		}

		/**
		 * Names the route the connection is served by: the close line reports it, and it
		 * and the reject line follow its settings.
		 */
		Connection route(Router.Route route) {
			this.route = route.routeTag();
			return this.routeKey(route.routeKey());
		}

		/**
		 * Puts the connection's lines from now on under the settings of the route key,
		 * for a connection whose route key is known before its route resolves (a tcp
		 * route port's).
		 */
		Connection routeKey(String routeKey) {
			if (this.owner != null) {
				this.policy = this.owner.policyOf(routeKey);
			}
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
		 * connection are logged by the rerouting relay with their own route.
		 */
		void request(ConnectionHeadParser.Head.Request request) {
			this.logRequest(this.route, this.policy, request.method(), request.path(), request.version(),
					request.userAgent());
		}

		/**
		 * Logs a request head with the route it resolved to, which differs from the
		 * connection's when the request is rerouted, under that route's settings; a
		 * {@code null} route (a request routed nowhere) is logged as {@code -} under the
		 * server-wide ones, a {@code null} request (a head that is no well-formed
		 * request) with {@code -} for each of its parts.
		 */
		void request(Router.@Nullable Route route, ConnectionHeadParser.Head.@Nullable Request request) {
			if (request == null) {
				this.request(route, "-", "-", "-", null);
			}
			else {
				this.request(route, request.method(), request.path(), request.version(), request.userAgent());
			}
		}

		/**
		 * Logs a request head from its parts, for a head that is no well-formed request
		 * yet has some of them (an h2 head without {@code :path}, say).
		 */
		void request(Router.@Nullable Route route, String method, String path, String version,
				@Nullable String userAgent) {
			if (this.owner == null) {
				return;
			}
			this.logRequest(tagOf(route), this.owner.policyOf(route), method, path, version, userAgent);
		}

		private void logRequest(String routeTag, Policy policy, String method, String path, String version,
				@Nullable String userAgent) {
			if (!policy.request() || this.noop || this.closed || this.owner == null || !this.owner.allowRequest()) {
				return;
			}
			log.info("type=req id={} route={} method={} path={} http={} remote={} userAgent={}", this.id, routeTag,
					method, path, version, this.remote, quoted(userAgent));
		}

		private static String tagOf(Router.@Nullable Route route) {
			return route == null ? "-" : route.routeTag();
		}

		/**
		 * Logs an h2 request head the relay answered {@code 421 Misdirected Request}
		 * itself: its route's upstream is reached over HTTP/1.1 only.
		 */
		void misdirected(Router.Route route, ConnectionHeadParser.Head.Request request) {
			this.refused(route, request, 421, "force-http1");
		}

		/**
		 * Logs a request head refused for its route's credentials: {@code 401} with
		 * {@code reason=auth-missing} / {@code auth-invalid}, {@code 503} with
		 * {@code reason=auth-busy} / {@code auth-unavailable}; nothing for a verdict that
		 * passes.
		 */
		void refused(Router.Route route, ConnectionHeadParser.Head.@Nullable Request request, Verdict verdict) {
			switch (verdict) {
				case Verdict.Unauthorized unauthorized ->
					this.refused(route, request, 401, "auth-" + unauthorized.reason());
				case Verdict.Busy busy -> this.refused(route, request, 503, "auth-busy");
				case Verdict.Unavailable unavailable -> this.refused(route, request, 503, "auth-unavailable");
				case Verdict.Pass pass -> {
				}
			}
		}

		/**
		 * Logs a request head the relay answered itself with {@code status} instead of
		 * relaying it, for {@code reason}.
		 */
		void refused(Router.Route route, ConnectionHeadParser.Head.@Nullable Request request, int status,
				String reason) {
			if (this.noop || this.closed || this.owner == null || !this.owner.policyOf(route).request()
					|| !this.owner.allowRequest()) {
				return;
			}
			log.info("type=req id={} route={} method={} path={} http={} remote={} status={} reason={} userAgent={}",
					this.id, route.routeTag(), request == null ? "-" : request.method(),
					request == null ? "-" : request.path(), request == null ? "-" : request.version(), this.remote,
					status, reason, quoted(request == null ? null : request.userAgent()));
		}

		/**
		 * A free-text value as a logfmt value: {@code -} when absent, else in double
		 * quotes with quotes and backslashes backslash-escaped and control characters as
		 * Java unicode escapes, so it stays one field on one line.
		 */
		static String quoted(@Nullable String value) {
			if (value == null) {
				return "-";
			}
			StringBuilder quoted = new StringBuilder(value.length() + 2).append('"');
			for (int i = 0; i < value.length(); i++) {
				char c = value.charAt(i);
				if (c == '"' || c == '\\') {
					quoted.append('\\').append(c);
				}
				else if (Character.isISOControl(c)) {
					quoted.append(String.format("\\u%04x", (int) c));
				}
				else {
					quoted.append(c);
				}
			}
			return quoted.append('"').toString();
		}

		/** Logs the connection acceptance, before route and transport are known. */
		public void accept() {
			if (!this.policy.accept() || this.noop || this.owner == null || !this.owner.allowAccept()) {
				return;
			}
			log.info("type=conn id={} event=accept listener={} remote={}", this.id, this.listener, this.remote);
		}

		/**
		 * Logs the connection as rejected before it was served (a connection limit, the
		 * head deadline) instead of the close line, which is not emitted afterwards --
		 * neither when the reject line is not emitted.
		 */
		void reject(String reason) {
			if (this.noop || this.closed || this.owner == null) {
				return;
			}
			this.closed = true;
			if (!this.policy.reject() || !this.owner.allowReject()) {
				return;
			}
			log.info("type=conn id={} event=reject listener={} remote={} reason={} durationMs={}", this.id,
					this.listener, this.remote, reason, (System.nanoTime() - this.startNanos) / 1_000_000);
		}

		/**
		 * Logs the connection as rejected, as {@link #reject(String)} does, yet answered
		 * with an HTTP {@code status} in its transport (a connection past the connection
		 * limit answered 503).
		 */
		void rejectAnswered(String reason, int status) {
			if (this.noop || this.closed || this.owner == null) {
				return;
			}
			this.closed = true;
			if (!this.policy.reject() || !this.owner.allowReject()) {
				return;
			}
			log.info(
					"type=conn id={} event=reject listener={} remote={} reason={} transport={} status={} durationMs={}",
					this.id, this.listener, this.remote, reason, this.transport, status,
					(System.nanoTime() - this.startNanos) / 1_000_000);
		}

		public void close() {
			if (!this.policy.close() || this.noop || this.closed || this.owner == null || !this.owner.allowClose()) {
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

			private Policy policy = Policy.NONE;

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

			Builder policy(Policy policy) {
				this.policy = policy;
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
