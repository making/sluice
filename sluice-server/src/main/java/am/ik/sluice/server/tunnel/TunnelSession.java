package am.ik.sluice.server.tunnel;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.jspecify.annotations.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import am.ik.sluice.server.route.Router;
import am.ik.sluice.tunnel.FrameWriter;
import am.ik.sluice.tunnel.PeerBuilds;
import am.ik.sluice.tunnel.SessionSender;
import am.ik.sluice.tunnel.StreamRelay;
import am.ik.sluice.tunnel.TunnelBudget;
import am.ik.sluice.tunnel.VirtualConnection;
import am.ik.sluice.v1.proto.Advertise;
import am.ik.sluice.v1.proto.AdvertiseAck;
import am.ik.sluice.v1.proto.Connect;
import am.ik.sluice.v1.proto.Frame;
import am.ik.sluice.v1.proto.Node;
import am.ik.sluice.v1.proto.PeerBuild;
import am.ik.sluice.v1.proto.Upstream;
import io.grpc.Status;

/**
 * State of one connected tunnel client: the multiplexed virtual connections and the
 * router registration.
 * <p>
 * A session becomes its client id's session ({@link SessionRegistry#find}) only with the
 * client's first {@code Advertise}, replacing the previous one then; until that it is
 * pending ({@link SessionRegistry#pending()}): it serves no connection and leaves the
 * previous session and its routes in place. A stream whose first {@code Advertise} does
 * not arrive within the advertise timeout is failed with {@code DEADLINE_EXCEEDED}.
 * <p>
 * The receive path ({@link #handle(Frame)}) never waits on anything slow: ADVERTISE is
 * applied on the session's own serial executor (registration, route registration and port
 * binding can take a while), and connections accept DATA without blocking. Every
 * connection runs per-connection flow control: a client whose {@code Advertise} announces
 * no receive window is rejected ({@link #WINDOW_REQUIRED}).
 * <p>
 * The first advertise registers the session unless that would pass a bound on the
 * registered sessions ({@link SessionRegistry#register}): then the stream is failed with
 * the refusal's status.
 * <p>
 * The session's own log lines (established, build, advertised routes, closing, and its
 * warnings) are logged at info / warn when the stream took a permit of the stream log at
 * its start, at debug otherwise, so streams opened and closed quickly cannot flood the
 * log; an advertise repeating the stream's routes and their outcome is logged at debug,
 * one changing them takes a permit of its own.
 */
public final class TunnelSession implements AutoCloseable {

	private static final Logger log = LoggerFactory.getLogger(TunnelSession.class);

	/**
	 * The status description of a tunnel stream failed because the client announced no
	 * per-connection receive window; names the first builds that announce one.
	 */
	public static final String WINDOW_REQUIRED = "the client announced no per-connection receive window "
			+ "(Advertise.connection_window); this server requires sluice-client 046cd21 "
			+ "or sluice-wasmlet f76388d (2026-10-12) or later";

	private final String clientId;

	private final String nodeId;

	private final String remoteAddress;

	private final String sourceAddress;

	private final Instant connectedAt = Instant.now();

	private final AtomicLong bytesInbound = new AtomicLong();

	private final AtomicLong bytesOutbound = new AtomicLong();

	private volatile List<Upstream> upstreams = List.of();

	private volatile Set<Integer> rejectedPorts = Set.of();

	private final Router router;

	private final SessionSender sender;

	private final TcpRouteListener tcpRoutes;

	private final SessionRegistry registry;

	private final AtomicLong sequence = new AtomicLong(1);

	private final ConcurrentMap<Long, VirtualConnection> connections = new ConcurrentHashMap<>();

	private static Set<Integer> listenPorts(List<Upstream> upstreams) {
		return upstreams.stream()
			.map(Upstream::getListenPort)
			.filter(port -> port > 0)
			.collect(Collectors.toUnmodifiableSet());
	}

	private final AtomicBoolean closed = new AtomicBoolean();

	/**
	 * Makes the closed check and the registration of the first advertise one step, so a
	 * session closing meanwhile is never registered after its removal.
	 */
	private final Object registration = new Object();

	/** Whether the session is registered; touched on {@link #advertiser} only. */
	private boolean inRegistry;

	/**
	 * True until the first {@code Advertise} or the advertise deadline, whichever comes
	 * first: the one that flips it decides the stream's fate.
	 */
	private final AtomicBoolean awaitingAdvertise = new AtomicBoolean(true);

	/** Fails the stream at the advertise timeout; interrupted by the first advertise. */
	private volatile @Nullable Thread advertiseDeadline;

	private volatile boolean drained;

	/**
	 * The client's per-connection receive window from its latest {@code Advertise}; 0
	 * until the first one, and a session serves no connection before that.
	 */
	private volatile long clientWindow;

	/**
	 * The build the client announced in its latest {@code Advertise}, sanitized; null
	 * when it announced none (it predates the field) or has not advertised yet.
	 */
	private volatile @Nullable PeerBuild clientBuild;

	/** Whether an {@code Advertise} arrived; written after the fields it carries. */
	private volatile boolean advertised;

	/** This server's build, sent in every {@code AdvertiseAck}. */
	private final PeerBuild serverBuild;

	/**
	 * Applies ADVERTISE off the receive path, in order; the route removal of
	 * {@link #close()} runs here too, after the queued advertises.
	 */
	private final ExecutorService advertiser;

	private final Duration closeTimeout;

	private final Runnable windowUpdateReceived;

	/** What the connections' windows past the initial one are charged to. */
	private final TunnelBudget budget;

	/** Runs whenever a connection's writer starts waiting for the client's credit. */
	private final Runnable creditWaited;

	private final Duration advertiseTimeout;

	private final Runnable advertiseTimedOut;

	private final Consumer<TunnelSession> registered;

	private final Consumer<SessionRegistry.Refusal> refused;

	private final LogThrottle streamLog;

	/**
	 * Whether the stream took a permit of {@link #streamLog} at its start: its lines are
	 * logged at info / warn, else at debug.
	 */
	private volatile boolean logged = true;

	private TunnelSession(Builder builder) {
		this.clientId = Objects.requireNonNull(builder.clientId, "clientId is required");
		this.nodeId = builder.nodeId;
		this.remoteAddress = builder.remoteAddress;
		this.sourceAddress = builder.sourceAddress;
		this.router = Objects.requireNonNull(builder.router, "router is required");
		this.sender = Objects.requireNonNull(builder.sender, "sender is required");
		this.tcpRoutes = Objects.requireNonNull(builder.tcpRoutes, "tcpRoutes is required");
		this.registry = Objects.requireNonNull(builder.registry, "registry is required");
		this.closeTimeout = builder.closeTimeout;
		this.windowUpdateReceived = builder.windowUpdateReceived;
		this.budget = builder.budget == null ? TunnelBudget.unlimited() : builder.budget;
		this.creditWaited = builder.creditWaited;
		this.advertiseTimeout = builder.advertiseTimeout;
		this.advertiseTimedOut = builder.advertiseTimedOut;
		this.registered = builder.registered;
		this.refused = builder.refused;
		this.streamLog = builder.streamLog;
		this.serverBuild = builder.serverBuild;
		this.advertiser = Executors.newSingleThreadExecutor(
				task -> Thread.ofVirtual().name("sluice-advertise-" + this.clientId).unstarted(task));
	}

	public static Builder builder() {
		return new Builder();
	}

	public static final class Builder {

		private @Nullable String clientId;

		private String nodeId = "";

		private String remoteAddress = "";

		private String sourceAddress = "";

		private @Nullable Router router;

		private @Nullable SessionSender sender;

		private @Nullable SessionRegistry registry;

		private @Nullable TcpRouteListener tcpRoutes;

		private Duration closeTimeout = Duration.ofSeconds(5);

		private Duration advertiseTimeout = Duration.ofSeconds(10);

		private Runnable advertiseTimedOut = () -> {
		};

		private Consumer<TunnelSession> registered = session -> {
		};

		private Consumer<SessionRegistry.Refusal> refused = refusal -> {
		};

		private LogThrottle streamLog = LogThrottle.unlimited();

		private Runnable windowUpdateReceived = () -> {
		};

		private @Nullable TunnelBudget budget;

		private Runnable creditWaited = () -> {
		};

		private PeerBuild serverBuild = PeerBuild.newBuilder()
			.setImplementation(ServerBuildConfiguration.IMPLEMENTATION)
			.build();

		private Builder() {
		}

		public Builder clientId(String clientId) {
			this.clientId = clientId;
			return this;
		}

		public Builder nodeId(String nodeId) {
			this.nodeId = nodeId;
			return this;
		}

		public Builder remoteAddress(String remoteAddress) {
			this.remoteAddress = remoteAddress;
			return this;
		}

		/**
		 * The client's source address (its IP address, no port), under which the registry
		 * bounds the pending streams per address; empty = unknown, bounded only by the
		 * global limit.
		 */
		public Builder sourceAddress(String sourceAddress) {
			this.sourceAddress = sourceAddress;
			return this;
		}

		public Builder router(Router router) {
			this.router = router;
			return this;
		}

		public Builder sender(SessionSender sender) {
			this.sender = sender;
			return this;
		}

		public Builder registry(SessionRegistry registry) {
			this.registry = registry;
			return this;
		}

		public Builder tcpRoutes(TcpRouteListener tcpRoutes) {
			this.tcpRoutes = tcpRoutes;
			return this;
		}

		/** This server's build, sent in every {@code AdvertiseAck}. */
		public Builder serverBuild(PeerBuild serverBuild) {
			this.serverBuild = serverBuild;
			return this;
		}

		/**
		 * How long the stream may go without the client's first {@code Advertise} before
		 * it is failed; counted from {@link TunnelSession#start()}.
		 */
		public Builder advertiseTimeout(Duration advertiseTimeout) {
			this.advertiseTimeout = advertiseTimeout;
			return this;
		}

		/**
		 * Runs when the stream is failed for a missing {@code Advertise}; a metrics hook.
		 */
		public Builder advertiseTimedOut(Runnable advertiseTimedOut) {
			this.advertiseTimedOut = advertiseTimedOut;
			return this;
		}

		/**
		 * Runs once the session is registered (its first advertise), before its routes
		 * are; on the session's advertise thread.
		 */
		public Builder registered(Consumer<TunnelSession> registered) {
			this.registered = registered;
			return this;
		}

		/**
		 * Runs when the first advertise is refused registration, before the stream is
		 * failed with the refusal's status; on the session's advertise thread. A metrics
		 * and log hook.
		 */
		public Builder refused(Consumer<SessionRegistry.Refusal> refused) {
			this.refused = refused;
			return this;
		}

		/**
		 * The permits for the session's own log lines, shared by the sessions of a
		 * server; every line is logged by default.
		 */
		Builder streamLog(LogThrottle streamLog) {
			this.streamLog = streamLog;
			return this;
		}

		/** Runs for every WINDOW_UPDATE received; a metrics hook. */
		public Builder windowUpdateReceived(Runnable windowUpdateReceived) {
			this.windowUpdateReceived = windowUpdateReceived;
			return this;
		}

		/**
		 * What the connections' receive windows past the initial one are charged to;
		 * unlimited by default.
		 */
		public Builder budget(TunnelBudget budget) {
			this.budget = budget;
			return this;
		}

		/** Runs whenever a connection's writer starts waiting for the client's credit. */
		public Builder creditWaited(Runnable creditWaited) {
			this.creditWaited = creditWaited;
			return this;
		}

		/**
		 * How long {@link TunnelSession#close()} waits for a pending advertise and the
		 * route removal queued behind it.
		 */
		Builder closeTimeout(Duration closeTimeout) {
			this.closeTimeout = closeTimeout;
			return this;
		}

		public TunnelSession build() {
			return new TunnelSession(this);
		}

	}

	/**
	 * The status description of a tunnel stream failed because the client sent no
	 * {@code Advertise} within {@code timeout}.
	 */
	public static String advertiseTimeoutDescription(Duration timeout) {
		return "the client sent no Advertise within " + timeout.toMillis() + "ms";
	}

	/** How long the stream may go without the client's first {@code Advertise}. */
	public Duration advertiseTimeout() {
		return this.advertiseTimeout;
	}

	public String clientId() {
		return this.clientId;
	}

	public FrameWriter sender() {
		return this.sender;
	}

	/**
	 * How long the session's stream has held frames queued for the client, not ready,
	 * with none taken; zero while it is not stalled.
	 */
	public Duration stalled() {
		return this.sender.stalled();
	}

	/**
	 * Peer address ({@code host:port}) of the client's tunnel stream; empty when unknown.
	 */
	public String remoteAddress() {
		return this.remoteAddress;
	}

	/** Source address (IP, no port) of the client; empty when unknown. */
	public String sourceAddress() {
		return this.sourceAddress;
	}

	public Instant connectedAt() {
		return this.connectedAt;
	}

	/**
	 * Upstreams announced by the latest {@code Advertise}; empty until the first one.
	 */
	public List<Upstream> upstreams() {
		return this.upstreams;
	}

	/**
	 * Listen ports of the latest {@code Advertise} that were not bound.
	 */
	public Set<Integer> rejectedPorts() {
		return this.rejectedPorts;
	}

	/**
	 * Whether the client sent its {@code Advertise}; until then its build and window are
	 * not known.
	 */
	public boolean advertised() {
		return this.advertised;
	}

	/**
	 * The build the client announced in its latest {@code Advertise}, with its strings
	 * bounded ({@link PeerBuilds#sanitize}); empty when the client announced none (it
	 * predates the field) or has not advertised yet ({@link #advertised()}).
	 */
	public Optional<PeerBuild> clientBuild() {
		return Optional.ofNullable(this.clientBuild);
	}

	/**
	 * Admits the session as pending ({@link SessionRegistry#admit}), then starts the
	 * sender and the advertise deadline. A refused session starts nothing, holds no
	 * thread and logs nothing; the caller fails its stream with the refusal's status.
	 */
	Optional<SessionRegistry.Refusal> start() {
		Optional<SessionRegistry.Refusal> refusal = this.registry.admit(this);
		if (refusal.isPresent()) {
			return refusal;
		}
		long unreported = this.streamLog.tryAcquire();
		this.logged = unreported >= 0;
		log.atLevel(level(Level.INFO))
			.log("tunnel stream established for client {}{}", this.clientId, demoted(unreported));
		this.sender.start();
		this.advertiseDeadline = Thread.ofVirtual()
			.name("sluice-advertise-deadline-" + this.clientId)
			.start(this::awaitAdvertise);
		return Optional.empty();
	}

	/**
	 * Fails the stream unless the first {@code Advertise} arrived within the timeout; an
	 * interrupt (the advertise, or close) ends the wait.
	 */
	private void awaitAdvertise() {
		try {
			TimeUnit.NANOSECONDS.sleep(this.advertiseTimeout.toNanos());
		}
		catch (InterruptedException e) {
			return;
		}
		if (!this.awaitingAdvertise.compareAndSet(true, false) || this.closed.get()) {
			return;
		}
		String description = advertiseTimeoutDescription(this.advertiseTimeout);
		log.atLevel(level(Level.WARN))
			.log("client {} ({}): {}; failing its tunnel stream", this.clientId, this.remoteAddress, description);
		this.advertiseTimedOut.run();
		this.sender.fail(Status.DEADLINE_EXCEEDED.withDescription(description).asRuntimeException());
		close();
	}

	/**
	 * Handles one frame received from the client; never blocks on a slow consumer.
	 */
	void handle(Frame frame) {
		switch (frame.getBodyCase()) {
			case ADVERTISE -> handleAdvertise(frame.getAdvertise());
			case DATA -> {
				am.ik.sluice.v1.proto.Data data = frame.getData();
				VirtualConnection connection = this.connections.get(data.getConnId());
				if (connection != null) {
					connection.acceptData(data.getPayload().toByteArray());
				}
			}
			case WINDOW_UPDATE -> {
				this.windowUpdateReceived.run();
				VirtualConnection connection = this.connections.get(frame.getWindowUpdate().getConnId());
				if (connection != null) {
					connection.windowUpdate(frame.getWindowUpdate());
				}
			}
			case CLOSE -> {
				VirtualConnection connection = this.connections.get(frame.getClose().getConnId());
				if (connection != null) {
					connection.remoteClosed();
				}
			}
			case ERROR -> {
				am.ik.sluice.v1.proto.Error error = frame.getError();
				VirtualConnection connection = this.connections.get(error.getConnId());
				if (connection != null) {
					connection.remoteFailed(error.getMessage());
				}
			}
			case CONNECT -> log.warn("unexpected CONNECT from client {}", this.clientId);
			case KEEP_ALIVE -> {
				// liveness no-op
			}
			default -> {
				// empty or unknown future body: ignore
			}
		}
	}

	/**
	 * Applies the client's window and build, then registers the session and its routes
	 * off the receive path; rejects a client announcing no window. Ignored once the
	 * session is closed -- after a rejection, so a client sending its advertise again
	 * before it sees the failure is rejected once -- or once the advertise deadline
	 * passed.
	 */
	private void handleAdvertise(Advertise advertise) {
		if (this.closed.get()) {
			return;
		}
		if (this.awaitingAdvertise.compareAndSet(true, false)) {
			Thread deadline = this.advertiseDeadline;
			if (deadline != null) {
				deadline.interrupt();
			}
		}
		else if (!this.advertised) {
			// the deadline passed first: the stream is being failed
			return;
		}
		PeerBuild build = advertise.hasBuild() ? PeerBuilds.sanitize(advertise.getBuild()) : null;
		String described = build == null ? PeerBuilds.UNKNOWN : PeerBuilds.describe(build);
		if (advertise.getConnectionWindow() == 0) {
			log.atLevel(level(Level.WARN))
				.log("client {} runs {}; rejecting its tunnel stream: {}", this.clientId, described, WINDOW_REQUIRED);
			this.sender.fail(Status.FAILED_PRECONDITION.withDescription(WINDOW_REQUIRED).asRuntimeException());
			close();
			return;
		}
		// before the session is registered: every connection opened on it has the window
		applyWindow(advertise.getConnectionWindow());
		this.clientBuild = build;
		boolean first = !this.advertised;
		this.advertised = true;
		if (first) {
			log.atLevel(level(Level.INFO)).log("client {} runs {}", this.clientId, described);
		}
		try {
			this.advertiser.execute(() -> advertise(advertise.getUpstreamsList()));
		}
		catch (RejectedExecutionException e) {
			// closed: the routes of a closed session are not registered again
		}
	}

	/**
	 * Records the client's window (unsigned, non-zero) for the connections opened from
	 * now on.
	 */
	private void applyWindow(int window) {
		this.clientWindow = Integer.toUnsignedLong(window);
	}

	/**
	 * On the first advertise registers the session, replacing the client id's previous
	 * one, unless the registry refuses it: then the stream is failed with the refusal's
	 * status and the session closed. Then registers the advertised routes and binds their
	 * ports, and acknowledges. Runs on {@link #advertiser}. Skipped once the session is
	 * closed: the route removal of {@link #close()} runs after every advertise started
	 * before it.
	 */
	private void advertise(List<Upstream> advertised) {
		boolean first = false;
		TunnelSession replaced = null;
		SessionRegistry.Refusal refusal = null;
		synchronized (this.registration) {
			if (this.closed.get()) {
				return;
			}
			if (!this.inRegistry) {
				switch (this.registry.register(this)) {
					case SessionRegistry.Registration.Registered registration -> {
						this.inRegistry = true;
						first = true;
						replaced = registration.replaced().orElse(null);
					}
					case SessionRegistry.Registration.Refused refused -> refusal = refused.refusal();
				}
			}
		}
		if (refusal != null) {
			this.refused.accept(refusal);
			this.sender.fail(refusal.status().asRuntimeException());
			// on the advertiser: the advertises queued behind this one find the session
			// closed, and the route removal runs once this one returns
			close(false);
			return;
		}
		if (replaced != null) {
			// the routes are keyed by client id: the replaced session's removal spares
			// them, since this session is registered now
			replaced.close();
		}
		if (first) {
			this.registered.accept(this);
		}
		int registered = this.router.register(this.clientId, advertised);
		Set<Integer> rejected = Set.copyOf(this.tcpRoutes.reconcile(this.clientId, listenPorts(advertised)));
		List<Upstream> upstreams = List.copyOf(advertised);
		logAdvertise(first, upstreams, registered, rejected);
		this.upstreams = upstreams;
		this.rejectedPorts = rejected;
		// after the lines: once acknowledged, the advertise is logged
		this.sender.sendAdvertiseAck(AdvertiseAck.newBuilder()
			.addAllRejectedPorts(rejected)
			.setNodeId(this.nodeId)
			.setBuild(this.serverBuild)
			.build());
	}

	/**
	 * Logs an advertise: the first at the stream's level, a later one repeating the
	 * previous routes and outcome at debug, a later one changing them at info when it
	 * takes a permit of the stream log.
	 */
	private void logAdvertise(boolean first, List<Upstream> advertised, int registered, Set<Integer> rejected) {
		Level level;
		long unreported = 0;
		if (first) {
			level = level(Level.INFO);
		}
		else if (advertised.equals(this.upstreams) && rejected.equals(this.rejectedPorts)) {
			level = Level.DEBUG;
		}
		else {
			unreported = this.streamLog.tryAcquire();
			level = unreported >= 0 ? Level.INFO : Level.DEBUG;
		}
		log.atLevel(level)
			.log("client {} advertised {} upstream(s), {} listen port(s) rejected{}", this.clientId, registered,
					rejected.size(), first ? "" : demoted(unreported));
		for (Upstream upstream : advertised) {
			log.atLevel(level)
				.log("client {} upstream: host=[{}] target={} rewrite-host={} tls-passthrough={} listen-port={} verbatim-idle-timeout={}",
						this.clientId, upstream.getHost(), upstream.getTargetUrl(), upstream.getRewriteHost(),
						upstream.getTlsPassthrough(), upstream.getListenPort(),
						upstream.getVerbatimIdleTimeoutMillis() == 0 ? "server default"
								: Long.toUnsignedString(upstream.getVerbatimIdleTimeoutMillis()) + "ms");
		}
	}

	/**
	 * Opens a virtual connection to the given dial address and sends the CONNECT request
	 * to the client, announcing this side's initial window
	 * ({@link VirtualConnection#INITIAL_WINDOW}), which grows up to
	 * {@link VirtualConnection#DEFAULT_WINDOW} against the budget. Its send credit is the
	 * client's window, known once the client advertised: a session is found for routing
	 * only from then on. The connection carries bytes relayed without interpreting them.
	 * @throws IllegalStateException when the client has not advertised yet
	 */
	public VirtualConnection open(String address) {
		return this.open(address, "");
	}

	/**
	 * Opens a virtual connection as {@link #open(String)} does, on which this side speaks
	 * the given protocol (an ALPN id, {@code h2} or {@code http/1.1}): the client
	 * negotiates it with a TLS upstream.
	 * @throws IllegalStateException when the client has not advertised yet
	 */
	public VirtualConnection open(String address, String applicationProtocol) {
		long clientWindow = this.clientWindow;
		if (clientWindow == 0) {
			throw new IllegalStateException("client " + this.clientId + " has not advertised yet");
		}
		long connectionId = this.sequence.getAndIncrement();
		VirtualConnection connection = VirtualConnection.builder(connectionId, this.sender)
			.window(VirtualConnection.INITIAL_WINDOW, clientWindow)
			.growWindow(VirtualConnection.DEFAULT_WINDOW, this.budget)
			.creditWaited(this.creditWaited)
			.build();
		this.connections.put(connectionId, connection);
		this.sender.sendConnect(Connect.newBuilder()
			.setConnId(connectionId)
			.setAddress(address)
			.setConnectionWindow(VirtualConnection.INITIAL_WINDOW)
			.setApplicationProtocol(applicationProtocol)
			.build());
		return connection;
	}

	public void remove(long connectionId) {
		this.connections.remove(connectionId);
	}

	/**
	 * Number of in-flight virtual connections relayed by this session.
	 */
	public int connectionCount() {
		return this.connections.size();
	}

	/**
	 * Number of virtual connections opened over this session since it connected.
	 */
	public long connectionsOpened() {
		return this.sequence.get() - 1;
	}

	/**
	 * Accounts relayed bytes: {@link StreamRelay.Direction#TO_REMOTE} flows from the
	 * public peer into the tunnel (inbound), {@link StreamRelay.Direction#TO_LOCAL} back
	 * to the peer (outbound).
	 */
	public void recordRelayed(long count, StreamRelay.Direction direction) {
		switch (direction) {
			case TO_REMOTE -> this.bytesInbound.addAndGet(count);
			case TO_LOCAL -> this.bytesOutbound.addAndGet(count);
		}
	}

	public long bytesInbound() {
		return this.bytesInbound.get();
	}

	public long bytesOutbound() {
		return this.bytesOutbound.get();
	}

	/**
	 * Marks the session as drained and notifies the client that this node is going away.
	 */
	public void drain(String reason) {
		this.drained = true;
		try {
			this.sender.sendDrain(reason);
		}
		catch (RuntimeException e) {
			// the stream may already be dead
		}
	}

	public boolean drained() {
		return this.drained;
	}

	/**
	 * Pushes the current cluster membership to the client.
	 */
	public void pushMembership(List<Node> nodes, long membershipVersion) {
		this.sender.sendMembership(nodes, membershipVersion);
	}

	/**
	 * Tears the session down: unregisters it, removes its routes, stops the sender and
	 * fails its connections. The route removal runs on {@link #advertiser} after the
	 * advertises queued before it, so none of them registers the routes again; close
	 * waits for it at most {@code closeTimeout}, and an advertise stuck past that leaves
	 * the removal to run once it finishes. That advertise still registers its routes when
	 * it ends, after a newer session of the client may have registered its own.
	 */
	@Override
	public void close() {
		close(true);
	}

	/**
	 * {@link #close()}; on the {@link #advertiser} itself it must not wait for it
	 * ({@code awaitAdvertiser} false): the route removal then runs once the current
	 * advertise returns.
	 */
	private void close(boolean awaitAdvertiser) {
		synchronized (this.registration) {
			if (!this.closed.compareAndSet(false, true)) {
				return;
			}
			this.registry.remove(this);
		}
		Thread deadline = this.advertiseDeadline;
		if (deadline != null && deadline != Thread.currentThread()) {
			deadline.interrupt();
		}
		log.atLevel(level(Level.INFO)).log("closing session for client {}", this.clientId);
		this.advertiser.execute(this::removeRoutes);
		this.advertiser.shutdown();
		if (awaitAdvertiser) {
			try {
				if (!this.advertiser.awaitTermination(this.closeTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
					log.warn("client {}: advertise still running after {}; its routes are removed once it ends",
							this.clientId, this.closeTimeout);
				}
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
		// the sender first: a dead or stalled stream must not leave the connection
		// teardown below waiting on a full send queue (the worker stops and releases
		// every producer); from here on the frames of the closing connections are
		// dropped, the peer is gone anyway
		this.sender.close();
		for (VirtualConnection connection : List.copyOf(this.connections.values())) {
			connection.remoteFailed("tunnel session closed");
			connection.close();
		}
		this.connections.clear();
	}

	/**
	 * The level of one of the stream's own lines: {@code level} when the stream took a
	 * permit of the stream log at its start, debug otherwise.
	 */
	Level level(Level level) {
		return this.logged ? level : Level.DEBUG;
	}

	/**
	 * The suffix of a line that took a permit of the stream log, reporting the streams
	 * and route changes logged at debug since the previous one.
	 */
	private static String demoted(long unreported) {
		return unreported > 0 ? " (%d more tunnel streams or route changes logged at debug since the last report)"
			.formatted(unreported) : "";
	}

	/**
	 * Releases the client's routes and ports, unless a newer session of the same client
	 * id is live: the routes are keyed by client id, and that session owns them now (its
	 * advertise replaces them, its close removes them).
	 */
	private void removeRoutes() {
		if (this.registry.find(this.clientId).isPresent()) {
			return;
		}
		this.tcpRoutes.reconcile(this.clientId, Set.of());
		this.router.remove(this.clientId);
	}

}
