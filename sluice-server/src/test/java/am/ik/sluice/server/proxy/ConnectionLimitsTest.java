package am.ik.sluice.server.proxy;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import org.springframework.util.unit.DataSize;

import am.ik.sluice.server.config.SluiceServerProperties;
import am.ik.sluice.server.route.Router;
import am.ik.sluice.tunnel.ConnectionLimit;

import static org.assertj.core.api.Assertions.assertThat;

class ConnectionLimitsTest {

	private static final Duration STALL_TIMEOUT = Duration.ofMillis(400);

	private static final Duration VERBATIM_IDLE_TIMEOUT = Duration.ofMillis(300);

	private final SimpleMeterRegistry meters = new SimpleMeterRegistry();

	/** The meters of the limits with a short stall timeout. */
	private final SimpleMeterRegistry stallMeters = new SimpleMeterRegistry();

	private final SluiceServerProperties properties = SluiceServerProperties.builder()
		.limits(SluiceServerProperties.Limits.builder()
			.maxConnections(3)
			.maxConnectionsPerAddress(2)
			.headTimeout(Duration.ofMillis(200))
			.idleTimeout(Duration.ofMillis(300))
			.build())
		.build();

	private final ConnectionLimits limits = new ConnectionLimits(this.properties, this.meters);

	private final AccessLogger.Connection access = new AccessLogger(this.properties).accepted("test", new Socket());

	@AfterEach
	void destroy() {
		this.limits.destroy();
	}

	@Test
	void aReleasedAddressLeavesNoEntryBehind() throws Exception {
		InetAddress source = InetAddress.getByName("203.0.113.7");
		ConnectionLimits.Admission first = admit();
		ConnectionLimits.Admission second = admit();
		assertThat(first.claim(source)).isTrue();
		assertThat(second.claim(source)).isTrue();
		assertThat(admit().claim(source)).isFalse();
		assertThat(this.limits.admitted(source)).isEqualTo(2);
		first.release();
		first.release();
		second.release();
		assertThat(this.limits.admitted(source)).isZero();
		assertThat(this.limits.trackedAddresses()).isZero();
		assertThat(rejected("address-limit")).isEqualTo(1);
	}

	@Test
	void theGlobalLimitIsTakenAtAdmission() {
		admit();
		admit();
		admit();
		assertThat(this.limits.admit(this.access)).isNull();
		assertThat(this.limits.admitted()).isEqualTo(3);
		assertThat(rejected("limit")).isEqualTo(1);
	}

	@Test
	void aPerAddressLimitAboveTheGlobalOneIsCappedByIt() {
		SluiceServerProperties loose = SluiceServerProperties.builder()
			.limits(SluiceServerProperties.Limits.builder().maxConnections(3).maxConnectionsPerAddress(10).build())
			.build();
		ConnectionLimits capped = new ConnectionLimits(loose, new SimpleMeterRegistry());
		try {
			assertThat(capped.maxPerAddress()).isEqualTo(3);
		}
		finally {
			capped.destroy();
		}
	}

	@Test
	void aConnectionReleasedInItsHeadIsNotCutByTheDeadline() throws Exception {
		try (ServerSocket server = loopbackServer();
				Socket client = new Socket("127.0.0.1", server.getLocalPort());
				Socket accepted = server.accept()) {
			ConnectionLimits.Admission admission = admit();
			admission.startHead(accepted);
			admission.release();
			Thread.sleep(400);
			assertThat(accepted.isClosed()).isFalse();
			assertThat(admission.expired()).isFalse();
			assertThat(rejected("head-timeout")).isZero();
		}
	}

	@Test
	void theDeadlineClosesTheSocketAndEndHeadReportsIt() throws Exception {
		try (ServerSocket server = loopbackServer();
				Socket client = new Socket("127.0.0.1", server.getLocalPort());
				Socket accepted = server.accept()) {
			ConnectionLimits.Admission admission = admit();
			admission.startHead(accepted);
			Thread.sleep(400);
			assertThat(accepted.isClosed()).isTrue();
			assertThat(admission.endHead()).isFalse();
			assertThat(rejected("head-timeout")).isEqualTo(1);
			admission.release();
			assertThat(this.limits.admitted()).isZero();
		}
	}

	@Test
	void aKeepAlivePhaseEnteredAnewStartsItsTimeoutOver() throws Exception {
		try (ServerSocket server = loopbackServer();
				Socket client = new Socket("127.0.0.1", server.getLocalPort());
				Socket accepted = server.accept()) {
			ConnectionLimits.KeepAliveDeadline deadline = this.limits.keepAliveDeadline(accepted);
			CountDownLatch teardown = new CountDownLatch(1);
			deadline.onExpiry(teardown::countDown);
			assertThat(deadline.enter(ConnectionLimits.KeepAliveDeadline.Phase.IDLE)).isTrue();
			Thread.sleep(200);
			assertThat(deadline.enter(ConnectionLimits.KeepAliveDeadline.Phase.BUSY)).isTrue();
			assertThat(this.limits.scheduledDeadlines()).as("busy: nothing scheduled").isZero();
			assertThat(deadline.enter(ConnectionLimits.KeepAliveDeadline.Phase.IDLE)).isTrue();
			Thread.sleep(200);
			// the first idle timeout would have passed by now
			assertThat(deadline.expired()).isFalse();
			assertThat(teardown.await(1, TimeUnit.SECONDS)).isTrue();
			assertThat(deadline.expired()).isTrue();
			Awaitility.await().atMost(Duration.ofSeconds(1)).until(accepted::isClosed);
			assertThat(idleClosed()).isEqualTo(1);
			assertThat(rejected("head-timeout")).isZero();
			assertThat(deadline.enter(ConnectionLimits.KeepAliveDeadline.Phase.BUSY)).as("expired").isFalse();
		}
	}

	@Test
	void aKeepAlivePhaseEnteredAgainKeepsItsRunningTimeout() throws Exception {
		try (ServerSocket server = loopbackServer();
				Socket client = new Socket("127.0.0.1", server.getLocalPort());
				Socket accepted = server.accept()) {
			ConnectionLimits.KeepAliveDeadline deadline = this.limits.keepAliveDeadline(accepted);
			long start = System.nanoTime();
			assertThat(deadline.enter(ConnectionLimits.KeepAliveDeadline.Phase.HEAD)).isTrue();
			Thread.sleep(100);
			assertThat(deadline.enter(ConnectionLimits.KeepAliveDeadline.Phase.HEAD)).isTrue();
			Awaitility.await()
				.pollInterval(Duration.ofMillis(5))
				.atMost(Duration.ofSeconds(1))
				.until(accepted::isClosed);
			assertThat(Duration.ofNanos(System.nanoTime() - start)).as("the head timeout from the first entry")
				.isLessThan(Duration.ofMillis(290));
			assertThat(rejected("head-timeout")).isEqualTo(1);
			assertThat(idleClosed()).isZero();
		}
	}

	@Test
	void aDisarmedKeepAliveDeadlineNeverPasses() throws Exception {
		try (ServerSocket server = loopbackServer();
				Socket client = new Socket("127.0.0.1", server.getLocalPort());
				Socket accepted = server.accept()) {
			ConnectionLimits.KeepAliveDeadline deadline = this.limits.keepAliveDeadline(accepted);
			assertThat(deadline.enter(ConnectionLimits.KeepAliveDeadline.Phase.IDLE)).isTrue();
			assertThat(this.limits.scheduledDeadlines()).isEqualTo(1);
			deadline.disarm();
			assertThat(this.limits.scheduledDeadlines()).isZero();
			assertThat(deadline.enter(ConnectionLimits.KeepAliveDeadline.Phase.IDLE)).isTrue();
			assertThat(this.limits.scheduledDeadlines()).as("nothing armed once disarmed").isZero();
			Thread.sleep(500);
			assertThat(accepted.isClosed()).isFalse();
			assertThat(deadline.expired()).isFalse();
			assertThat(idleClosed()).isZero();
		}
	}

	@Test
	void aRouteVerbatimIdleTimeoutReplacesTheServerDefault() {
		ConnectionLimits defaulted = new ConnectionLimits(SluiceServerProperties.builder()
			.limits(SluiceServerProperties.Limits.builder().verbatimIdleTimeout(Duration.ofSeconds(30)).build())
			.build(), new SimpleMeterRegistry());
		try {
			assertThat(defaulted.verbatimIdleTimeout(route(Duration.ofSeconds(90)))).isEqualTo(Duration.ofSeconds(90));
			assertThat(defaulted.verbatimIdleTimeout(route(Duration.ofSeconds(5)))).isEqualTo(Duration.ofSeconds(5));
			assertThat(defaulted.verbatimIdleTimeout(route(Duration.ZERO))).isEqualTo(Duration.ofSeconds(30));
			// neither: off, the default of the server
			assertThat(this.properties.limits().verbatimIdleTimeout()).isZero();
			assertThat(this.limits.verbatimIdleTimeout(route(Duration.ZERO))).isZero();
			assertThat(this.limits.verbatimIdleTimeout(route(Duration.ofSeconds(5)))).isEqualTo(Duration.ofSeconds(5));
		}
		finally {
			defaulted.destroy();
		}
	}

	@Test
	void aVerbatimRelayReportingProgressIsKeptAndTornDownOnceItStops() throws Exception {
		try (ServerSocket server = loopbackServer();
				Socket client = new Socket("127.0.0.1", server.getLocalPort());
				Socket accepted = server.accept()) {
			ConnectionLimits.KeepAliveDeadline deadline = this.limits.keepAliveDeadline(accepted);
			CountDownLatch teardown = new CountDownLatch(1);
			deadline.onExpiry(teardown::countDown);
			assertThat(deadline.relayVerbatim(route(VERBATIM_IDLE_TIMEOUT))).isTrue();
			assertThat(this.limits.scheduledDeadlines()).isEqualTo(1);
			long end = System.nanoTime() + VERBATIM_IDLE_TIMEOUT.multipliedBy(3).toNanos();
			long stopped = System.nanoTime();
			while (System.nanoTime() < end) {
				stopped = System.nanoTime();
				deadline.progress(1);
				Thread.sleep(VERBATIM_IDLE_TIMEOUT.toMillis() / 6);
			}
			assertThat(deadline.expired()).as("kept by its progress").isFalse();
			assertThat(teardown.await(VERBATIM_IDLE_TIMEOUT.multipliedBy(4).toMillis(), TimeUnit.MILLISECONDS))
				.isTrue();
			assertThat(Duration.ofNanos(System.nanoTime() - stopped)).isGreaterThanOrEqualTo(VERBATIM_IDLE_TIMEOUT);
			Awaitility.await().atMost(Duration.ofSeconds(1)).until(accepted::isClosed);
			assertThat(verbatimIdleClosed()).isEqualTo(1);
			assertThat(idleClosed()).isZero();
			assertThat(deadline.relayVerbatim(route(VERBATIM_IDLE_TIMEOUT))).as("expired").isFalse();
		}
	}

	/**
	 * A teardown blocking -- a TLS socket's close waits for the write blocked on a client
	 * that does not read -- has the transport closed under it after the grace.
	 */
	@Test
	void aVerbatimRelayWhoseTeardownBlocksHasItsTransportClosedAfterTheGrace() throws Exception {
		try (ServerSocket server = loopbackServer();
				Socket client = new Socket("127.0.0.1", server.getLocalPort());
				Socket accepted = server.accept()) {
			ConnectionLimits.KeepAliveDeadline deadline = this.limits.keepAliveDeadline(accepted);
			CountDownLatch release = new CountDownLatch(1);
			CountDownLatch tearingDown = new CountDownLatch(1);
			deadline.onExpiry(() -> {
				tearingDown.countDown();
				try {
					release.await();
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			});
			try {
				deadline.relayVerbatim(route(VERBATIM_IDLE_TIMEOUT));
				assertThat(tearingDown.await(2, TimeUnit.SECONDS)).isTrue();
				long start = System.nanoTime();
				Awaitility.await()
					.pollInterval(Duration.ofMillis(10))
					.atMost(ConnectionLimits.KeepAliveDeadline.TEARDOWN_GRACE.plusSeconds(1))
					.until(accepted::isClosed);
				assertThat(Duration.ofNanos(System.nanoTime() - start))
					.isGreaterThan(ConnectionLimits.KeepAliveDeadline.TEARDOWN_GRACE.minusMillis(200));
			}
			finally {
				release.countDown();
			}
		}
	}

	@Test
	void theVerbatimPhaseIsEnteredForGood() throws Exception {
		try (ServerSocket server = loopbackServer();
				Socket client = new Socket("127.0.0.1", server.getLocalPort());
				Socket accepted = server.accept()) {
			ConnectionLimits.KeepAliveDeadline deadline = this.limits.keepAliveDeadline(accepted);
			long start = System.nanoTime();
			assertThat(deadline.relayVerbatim(route(VERBATIM_IDLE_TIMEOUT))).isTrue();
			Thread.sleep(100);
			// entered again: the running timeout stays; another phase arms nothing
			assertThat(deadline.relayVerbatim(route(Duration.ofSeconds(30)))).isTrue();
			assertThat(deadline.enter(ConnectionLimits.KeepAliveDeadline.Phase.IDLE)).isTrue();
			assertThat(this.limits.scheduledDeadlines()).isEqualTo(1);
			Awaitility.await()
				.pollInterval(Duration.ofMillis(5))
				.atMost(Duration.ofSeconds(2))
				.until(accepted::isClosed);
			assertThat(Duration.ofNanos(System.nanoTime() - start)).as("the timeout of the first entry")
				.isLessThan(Duration.ofSeconds(2));
			assertThat(verbatimIdleClosed()).isEqualTo(1);
			assertThat(idleClosed()).isZero();
		}
	}

	@Test
	void aVerbatimRelayWithoutATimeoutDisarmedOrWithTheLongestNeverPasses() throws Exception {
		try (ServerSocket server = loopbackServer();
				Socket client = new Socket("127.0.0.1", server.getLocalPort());
				Socket accepted = server.accept()) {
			ConnectionLimits.KeepAliveDeadline untimed = this.limits.keepAliveDeadline(accepted);
			assertThat(untimed.relayVerbatim(route(Duration.ZERO))).isTrue();
			assertThat(this.limits.scheduledDeadlines()).as("no timeout: nothing armed").isZero();
			ConnectionLimits.KeepAliveDeadline disarmed = this.limits.keepAliveDeadline(accepted);
			assertThat(disarmed.relayVerbatim(route(VERBATIM_IDLE_TIMEOUT))).isTrue();
			assertThat(this.limits.scheduledDeadlines()).isEqualTo(1);
			disarmed.disarm();
			assertThat(this.limits.scheduledDeadlines()).isZero();
			// past what nanoseconds hold: armed, saturated, never due
			ConnectionLimits.KeepAliveDeadline longest = this.limits.keepAliveDeadline(accepted);
			assertThat(longest.relayVerbatim(route(Duration.ofMillis(Long.MAX_VALUE)))).isTrue();
			assertThat(this.limits.scheduledDeadlines()).isEqualTo(1);
			Thread.sleep(VERBATIM_IDLE_TIMEOUT.multipliedBy(2).toMillis());
			assertThat(longest.expired()).isFalse();
			assertThat(accepted.isClosed()).isFalse();
			assertThat(verbatimIdleClosed()).isZero();
		}
	}

	@Test
	void anUnsetLimitFitsEverySlotsWorstCaseAndTheBudgetInHalfTheHeap() {
		long mib = 1024 * 1024;
		// a virtual connection's 2.16MiB plus an h2 relay's own queue and a leg's
		// response queue
		assertThat(ConnectionLimits.connectionBytes())
			.isEqualTo(2266112 + Http2DemuxRelay.UNBUDGETED_BYTES + Http2DemuxRelay.RESPONSE_QUEUE_BYTES);
		assertThat(ConnectionLimits.deriveBudget(400 * mib)).isEqualTo(25 * mib);
		// (200MiB - 25MiB) / 2.67MiB
		assertThat(ConnectionLimits.deriveMaxConnections(400 * mib, 25 * mib)).isEqualTo(65);
		// (2048MiB - 256MiB) / 2.67MiB
		assertThat(ConnectionLimits.deriveMaxConnections(4096 * mib, 256 * mib)).isEqualTo(670);
		assertThat(ConnectionLimits.deriveMaxConnections(400 * mib, 400 * mib)).isEqualTo(1);
	}

	@Test
	void aConfiguredBudgetIsTaken() {
		SluiceServerProperties configured = SluiceServerProperties.builder()
			.limits(SluiceServerProperties.Limits.builder()
				.maxConnections(3)
				.h2QueueBudget(org.springframework.util.unit.DataSize.ofKilobytes(512))
				.build())
			.build();
		ConnectionLimits limits = new ConnectionLimits(configured, new SimpleMeterRegistry());
		try {
			assertThat(limits.h2Budget().max()).isEqualTo(512 * 1024);
		}
		finally {
			limits.destroy();
		}
	}

	@Test
	void aLegTakesASlotOfTheGlobalLimit() {
		admit();
		admit();
		ConnectionLimit.Permit leg = Objects.requireNonNull(this.limits.tryAcquireLeg());
		assertThat(this.limits.tryAcquireLeg()).isNull();
		assertThat(this.limits.admit(this.access)).as("the leg holds the last slot").isNull();
		leg.release();
		assertThat(this.limits.admitted()).isEqualTo(2);
	}

	@Test
	void aWatchedRelayWaitingOnItsClientIsTornDownAtTheStallTimeout() throws Exception {
		try (ServerSocket server = loopbackServer();
				Socket client = new Socket("127.0.0.1", server.getLocalPort());
				Socket accepted = server.accept()) {
			ConnectionLimits limits = stallLimits(DataSize.ofKilobytes(16));
			try {
				ConnectionLimits.KeepAliveDeadline deadline = limits.keepAliveDeadline(accepted);
				CountDownLatch idle = new CountDownLatch(1);
				CountDownLatch stalled = new CountDownLatch(1);
				deadline.onExpiry(idle::countDown);
				long start = System.nanoTime();
				// busy from the start: watching arms the stall count at once
				deadline.watchStalls(() -> true, stalled::countDown);
				assertThat(limits.scheduledDeadlines()).isEqualTo(1);
				assertThat(stalled.await(2, TimeUnit.SECONDS)).isTrue();
				Duration elapsed = Duration.ofNanos(System.nanoTime() - start);
				assertThat(elapsed).as("between the stall timeout and two sample intervals past it")
					.isGreaterThanOrEqualTo(STALL_TIMEOUT)
					.isLessThan(STALL_TIMEOUT.plus(STALL_TIMEOUT.dividedBy(4)).plusMillis(100));
				assertThat(idle.getCount()).as("the idle teardown did not run").isEqualTo(1);
				Awaitility.await().atMost(Duration.ofSeconds(1)).until(accepted::isClosed);
				assertThat(deadline.expired()).isTrue();
				assertThat(deadline.enter(ConnectionLimits.KeepAliveDeadline.Phase.IDLE)).isFalse();
				assertThat(stallClosed()).isEqualTo(1);
			}
			finally {
				limits.destroy();
			}
		}
	}

	/**
	 * A relay waiting on its client but for a millisecond at every second sample -- the
	 * h1 relay caught forwarding a byte its client dripped -- is torn down at the stall
	 * timeout all the same.
	 */
	@Test
	void aRelayFoundBetweenTwoWaitsAtEverySecondSampleIsTornDownAtTheStallTimeout() throws Exception {
		try (ServerSocket server = loopbackServer();
				Socket client = new Socket("127.0.0.1", server.getLocalPort());
				Socket accepted = server.accept()) {
			ConnectionLimits limits = stallLimits(DataSize.ofKilobytes(16));
			try {
				ConnectionLimits.KeepAliveDeadline deadline = limits.keepAliveDeadline(accepted);
				CountDownLatch stalled = new CountDownLatch(1);
				AtomicLong readings = new AtomicLong();
				long start = System.nanoTime();
				long gap = TimeUnit.MILLISECONDS.toNanos(1);
				deadline.watchStallClock(() -> System.nanoTime() - start - readings.incrementAndGet() / 2 * gap,
						stalled::countDown);
				assertThat(stalled.await(2, TimeUnit.SECONDS)).isTrue();
				assertThat(Duration.ofNanos(System.nanoTime() - start))
					.as("between the stall timeout and two sample intervals past it")
					.isGreaterThanOrEqualTo(STALL_TIMEOUT)
					.isLessThan(STALL_TIMEOUT.plus(STALL_TIMEOUT.dividedBy(4)).plusMillis(100));
			}
			finally {
				limits.destroy();
			}
		}
	}

	@Test
	void aWatchedRelayWaitingOnItsUpstreamIsNeverTornDown() throws Exception {
		assertStallsAfter(DataSize.ofKilobytes(16), () -> false, 0, false);
	}

	@Test
	void progressOfTheStallMinimumPerSampleKeepsAWaitingRelay() throws Exception {
		assertStallsAfter(DataSize.ofKilobytes(16), () -> true, 16 * 1024, false);
	}

	@Test
	void progressBelowTheStallMinimumDoesNotKeepAWaitingRelay() throws Exception {
		assertStallsAfter(DataSize.ofKilobytes(16), () -> true, 1, true);
	}

	@Test
	void aStallMinimumOfOneByteCountsEveryByte() throws Exception {
		assertStallsAfter(DataSize.ofBytes(1), () -> true, 1, false);
	}

	@Test
	void anotherPhaseStopsTheStallCountAndBusyAgainStartsItOver() throws Exception {
		try (ServerSocket server = loopbackServer();
				Socket client = new Socket("127.0.0.1", server.getLocalPort());
				Socket accepted = server.accept()) {
			ConnectionLimits limits = stallLimits(DataSize.ofKilobytes(16));
			try {
				ConnectionLimits.KeepAliveDeadline deadline = limits.keepAliveDeadline(accepted);
				CountDownLatch stalled = new CountDownLatch(1);
				deadline.watchStalls(() -> true, stalled::countDown);
				Thread.sleep(STALL_TIMEOUT.multipliedBy(3).dividedBy(4).toMillis());
				assertThat(deadline.enter(ConnectionLimits.KeepAliveDeadline.Phase.HEAD)).isTrue();
				long busy = System.nanoTime();
				assertThat(deadline.enter(ConnectionLimits.KeepAliveDeadline.Phase.BUSY)).isTrue();
				assertThat(limits.scheduledDeadlines()).as("the head timeout gave way to the stall count").isEqualTo(1);
				assertThat(stalled.await(2, TimeUnit.SECONDS)).isTrue();
				assertThat(Duration.ofNanos(System.nanoTime() - busy)).as("counted from the busy phase entered anew")
					.isGreaterThanOrEqualTo(STALL_TIMEOUT);
				assertThat(this.stallMeters.get("sluice.data.connections.rejected")
					.tag("reason", "head-timeout")
					.counter()
					.count()).isZero();
			}
			finally {
				limits.destroy();
			}
		}
	}

	@Test
	void aDisarmedWatchedDeadlineNeverStalls() throws Exception {
		try (ServerSocket server = loopbackServer();
				Socket client = new Socket("127.0.0.1", server.getLocalPort());
				Socket accepted = server.accept()) {
			ConnectionLimits limits = stallLimits(DataSize.ofKilobytes(16));
			try {
				ConnectionLimits.KeepAliveDeadline deadline = limits.keepAliveDeadline(accepted);
				deadline.watchStalls(() -> true, () -> {
				});
				deadline.disarm();
				assertThat(limits.scheduledDeadlines()).isZero();
				Thread.sleep(STALL_TIMEOUT.multipliedBy(2).toMillis());
				assertThat(accepted.isClosed()).isFalse();
				assertThat(stallClosed()).isZero();
			}
			finally {
				limits.destroy();
			}
		}
	}

	/**
	 * Watches a busy deadline whose relay answers {@code waiting}, reporting
	 * {@code progress} bytes every quarter of a sample interval for three stall timeouts;
	 * asserts whether it stalled.
	 */
	private void assertStallsAfter(DataSize minBytes, BooleanSupplier waiting, int progress, boolean stalls)
			throws Exception {
		try (ServerSocket server = loopbackServer();
				Socket client = new Socket("127.0.0.1", server.getLocalPort());
				Socket accepted = server.accept()) {
			ConnectionLimits limits = stallLimits(minBytes);
			try {
				ConnectionLimits.KeepAliveDeadline deadline = limits.keepAliveDeadline(accepted);
				CountDownLatch stalled = new CountDownLatch(1);
				deadline.watchStalls(waiting, stalled::countDown);
				long end = System.nanoTime() + STALL_TIMEOUT.multipliedBy(3).toNanos();
				long step = STALL_TIMEOUT.dividedBy(ConnectionLimits.KeepAliveDeadline.STALL_SAMPLES * 4).toMillis();
				while (System.nanoTime() < end && stalled.getCount() > 0) {
					deadline.progress(progress);
					Thread.sleep(step);
				}
				assertThat(stalled.getCount() == 0).as("stalled").isEqualTo(stalls);
				assertThat(stallClosed()).isEqualTo(stalls ? 1 : 0);
			}
			finally {
				limits.destroy();
			}
		}
	}

	private ConnectionLimits stallLimits(DataSize minBytes) {
		return new ConnectionLimits(SluiceServerProperties.builder()
			.limits(SluiceServerProperties.Limits.builder()
				.maxConnections(3)
				.stallTimeout(STALL_TIMEOUT)
				.stallMinBytes(minBytes)
				.build())
			.build(), this.stallMeters);
	}

	private double stallClosed() {
		return this.stallMeters.get("sluice.data.connections.stall.closed").counter().count();
	}

	private static Router.Route route(Duration verbatimIdleTimeout) {
		return Router.Route.builder()
			.clientId("client")
			.domain("verbatim.test")
			.address("127.0.0.1:1")
			.verbatimIdleTimeout(verbatimIdleTimeout)
			.build();
	}

	private double verbatimIdleClosed() {
		return this.meters.get("sluice.data.connections.verbatim.idle.closed").counter().count();
	}

	private ConnectionLimits.Admission admit() {
		return Objects.requireNonNull(this.limits.admit(this.access));
	}

	private double idleClosed() {
		return this.meters.get("sluice.data.connections.idle.closed").counter().count();
	}

	private double rejected(String reason) {
		return this.meters.get("sluice.data.connections.rejected").tag("reason", reason).counter().count();
	}

	/**
	 * Listens on the loopback address the tests dial, so a listener of another process on
	 * the same port number cannot take the connection, and bounds {@code accept} so a
	 * stray connection fails the test instead of hanging it.
	 */
	private static ServerSocket loopbackServer() throws IOException {
		ServerSocket server = new ServerSocket(0, 50, InetAddress.getByName("127.0.0.1"));
		server.setSoTimeout(5_000);
		return server;
	}

}
