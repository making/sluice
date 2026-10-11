package am.ik.sluice.server.proxy;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import am.ik.sluice.server.config.SluiceServerProperties;
import am.ik.sluice.tunnel.ConnectionLimit;

import static org.assertj.core.api.Assertions.assertThat;

class ConnectionLimitsTest {

	private final SimpleMeterRegistry meters = new SimpleMeterRegistry();

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
		try (ServerSocket server = new ServerSocket(0);
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
		try (ServerSocket server = new ServerSocket(0);
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
		try (ServerSocket server = new ServerSocket(0);
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
		try (ServerSocket server = new ServerSocket(0);
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
		try (ServerSocket server = new ServerSocket(0);
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

	private ConnectionLimits.Admission admit() {
		return Objects.requireNonNull(this.limits.admit(this.access));
	}

	private double idleClosed() {
		return this.meters.get("sluice.data.connections.idle.closed").counter().count();
	}

	private double rejected(String reason) {
		return this.meters.get("sluice.data.connections.rejected").tag("reason", reason).counter().count();
	}

}
