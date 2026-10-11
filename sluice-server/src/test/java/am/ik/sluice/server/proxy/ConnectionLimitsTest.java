package am.ik.sluice.server.proxy;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.Objects;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
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
	void anUnsetLimitFitsEverySlotsWorstCaseAndTheBudgetInHalfTheHeap() {
		long mib = 1024 * 1024;
		// a virtual connection's 2.16MiB plus an h2 relay's own queue
		assertThat(ConnectionLimits.connectionBytes()).isEqualTo(2266112 + Http2DemuxRelay.UNBUDGETED_BYTES);
		assertThat(ConnectionLimits.deriveBudget(400 * mib)).isEqualTo(25 * mib);
		// (200MiB - 25MiB) / 2.45MiB
		assertThat(ConnectionLimits.deriveMaxConnections(400 * mib, 25 * mib)).isEqualTo(71);
		// (2048MiB - 256MiB) / 2.45MiB
		assertThat(ConnectionLimits.deriveMaxConnections(4096 * mib, 256 * mib)).isEqualTo(731);
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

	private double rejected(String reason) {
		return this.meters.get("sluice.data.connections.rejected").tag("reason", reason).counter().count();
	}

}
