package am.ik.sluice.it;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.client.config.Upstream;
import am.ik.sluice.client.tunnel.TunnelClient;
import am.ik.sluice.server.tunnel.TunnelSession;
import am.ik.sluice.v1.proto.Frame;
import am.ik.sluice.v1.proto.TunnelGrpc;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A client whose stream the server fails for a missing {@code Advertise} -- with the
 * status the server sends then -- logs the status and reconnects with its backoff, which
 * keeps growing: a stream that was never acknowledged does not reset it.
 */
@ExtendWith(OutputCaptureExtension.class)
class ClientAdvertiseTimeoutE2ETest {

	private static final Duration SERVER_TIMEOUT = Duration.ofMillis(200);

	private final MeterRegistry clientMeters = new SimpleMeterRegistry();

	private final FailingServer fake = new FailingServer();

	private @Nullable Server server;

	private @Nullable TunnelClient client;

	@AfterEach
	void tearDown() {
		if (this.client != null) {
			this.client.stop();
		}
		if (this.server != null) {
			this.server.shutdownNow();
		}
	}

	@Test
	void theClientLogsTheStatusAndReconnectsWithAGrowingBackoff(CapturedOutput output) throws Exception {
		int grpcPort = TestPorts.freePort();
		this.server = ServerBuilder.forPort(grpcPort).addService(this.fake).build().start();
		TunnelClient started = TunnelClient.builder()
			.properties(SluiceClientProperties.builder()
				.serverUrl("grpc://127.0.0.1:" + grpcPort)
				.upstream(Upstream.builder().host("late.local").target("http://127.0.0.1:1").build())
				.token("it-token")
				.build())
			.taskExecutor(task -> Thread.ofVirtual().name("e2e-advertise-timeout-worker").start(task))
			.meterRegistry(this.clientMeters)
			.build();
		started.start();
		this.client = started;

		Awaitility.await()
			.atMost(Duration.ofSeconds(15))
			.until(() -> this.fake.failedAt.size() >= 2 && this.fake.openedAt.size() >= 3);

		assertThat(this.fake.advertised).as("precondition: the client sent its advertise on every stream")
			.hasSizeGreaterThanOrEqualTo(2);
		long firstBackoff = this.fake.openedAt.get(1) - this.fake.failedAt.get(0);
		long secondBackoff = this.fake.openedAt.get(2) - this.fake.failedAt.get(1);
		assertThat(Duration.ofNanos(firstBackoff)).as("the first reconnect waits the initial backoff")
			.isBetween(Duration.ofMillis(900), Duration.ofMillis(2500));
		assertThat(Duration.ofNanos(secondBackoff)).as("the second waits twice as long")
			.isBetween(Duration.ofMillis(1900), Duration.ofMillis(4000));
		assertThat(started.isConnected()).isFalse();
		assertThat(this.clientMeters.get("sluice.reconnect.total").counter().count()).isGreaterThanOrEqualTo(2);
		String expected = "[grpc://127.0.0.1:" + grpcPort
				+ "] tunnel stream error: io.grpc.StatusRuntimeException: DEADLINE_EXCEEDED: "
				+ TunnelSession.advertiseTimeoutDescription(SERVER_TIMEOUT);
		assertThat(output.getAll().lines().filter(line -> line.endsWith(expected))).as("one line per failed stream")
			.hasSizeGreaterThanOrEqualTo(2);
	}

	/**
	 * Takes every stream's frames but never acknowledges the advertise; fails each stream
	 * {@link #SERVER_TIMEOUT} after it opened, as the server does at its advertise
	 * timeout.
	 */
	private static final class FailingServer extends TunnelGrpc.TunnelImplBase {

		final List<Long> openedAt = new CopyOnWriteArrayList<>();

		final List<Long> failedAt = new CopyOnWriteArrayList<>();

		final List<Frame> advertised = new CopyOnWriteArrayList<>();

		@Override
		public StreamObserver<Frame> connect(StreamObserver<Frame> responses) {
			this.openedAt.add(System.nanoTime());
			Thread.ofVirtual().start(() -> {
				try {
					TimeUnit.MILLISECONDS.sleep(SERVER_TIMEOUT.toMillis());
				}
				catch (InterruptedException e) {
					return;
				}
				this.failedAt.add(System.nanoTime());
				responses.onError(Status.DEADLINE_EXCEEDED
					.withDescription(TunnelSession.advertiseTimeoutDescription(SERVER_TIMEOUT))
					.asRuntimeException());
			});
			return new StreamObserver<>() {

				@Override
				public void onNext(Frame frame) {
					if (frame.hasAdvertise()) {
						FailingServer.this.advertised.add(frame);
					}
				}

				@Override
				public void onError(Throwable t) {
				}

				@Override
				public void onCompleted() {
				}

			};
		}

	}

}
