package am.ik.sluice.it;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.client.config.Upstream;
import am.ik.sluice.client.tunnel.TunnelClient;
import am.ik.sluice.server.SluiceServerApplication;
import am.ik.sluice.server.config.SluiceServerProperties;
import am.ik.sluice.server.tunnel.SessionRegistry;
import am.ik.sluice.v1.proto.Frame;
import am.ik.sluice.v1.proto.TunnelGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The bound per source address on the tunnel streams waiting for their client's first
 * {@code Advertise} ({@code sluice.limits.max-pending-tunnel-streams-per-address}), seen
 * by the Java client: refused with {@code RESOURCE_EXHAUSTED} while a silent stream from
 * the same address holds the place, it logs the status and retries with its backoff, and
 * connects once the place is free.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ExtendWith(OutputCaptureExtension.class)
class TunnelPendingPerAddressLimitE2ETest {

	private static final String CLIENT_ID = "per-address-client";

	private static int grpcPort;

	@Autowired
	private @Nullable SessionRegistry registry;

	@Autowired
	private @Nullable SluiceServerProperties properties;

	@Autowired
	private @Nullable MeterRegistry meterRegistry;

	private @Nullable ManagedChannel silentChannel;

	private @Nullable TunnelClient client;

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) {
		grpcPort = TestPorts.freePort();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(TestPorts.freePort()));
		registry.add("sluice.limits.max-pending-tunnel-streams-per-address", () -> "1");
		registry.add("sluice.token", () -> "it-token");
		registry.add("server.port", () -> 0);
	}

	@AfterEach
	void tearDown() {
		if (this.client != null) {
			this.client.stop();
		}
		if (this.silentChannel != null) {
			this.silentChannel.shutdownNow();
		}
	}

	@Test
	void theJavaClientRefusedForItsAddressRetriesAndConnectsOnceThePlaceIsFree(CapturedOutput output) throws Exception {
		assertThat(properties().limits().maxPendingTunnelStreamsPerAddress()).as("precondition: one per address")
			.isEqualTo(1);
		assertThat(properties().limits().advertiseTimeout())
			.as("precondition: the silent stream holds its place for the whole test")
			.isGreaterThanOrEqualTo(Duration.ofSeconds(10));
		CountDownLatch silentEnded = openSilentStream();
		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> registry().pending().size() == 1);
		double refusedBefore = refused();

		TunnelClient started = TunnelClient.builder()
			.properties(SluiceClientProperties.builder()
				.serverUrl("grpc://127.0.0.1:" + grpcPort)
				.clientId(CLIENT_ID)
				.upstream(Upstream.builder().host("per-address.local").target("http://127.0.0.1:1").build())
				.token("it-token")
				.build())
			.taskExecutor(task -> Thread.ofVirtual().name("e2e-per-address-worker").start(task))
			.meterRegistry(new SimpleMeterRegistry())
			.build();
		started.start();
		this.client = started;

		Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> refused() - refusedBefore >= 1);
		String expected = "[grpc://127.0.0.1:" + grpcPort
				+ "] tunnel stream error: io.grpc.StatusRuntimeException: RESOURCE_EXHAUSTED: "
				+ "too many tunnel streams from this address are waiting for their client's first Advertise";
		Awaitility.await()
			.atMost(Duration.ofSeconds(5))
			.alias("the client logs the status")
			.until(() -> output.getAll().lines().anyMatch(line -> line.endsWith(expected)));
		assertThat(started.isConnected()).isFalse();
		assertThat(registry().find(CLIENT_ID)).isEmpty();
		assertThat(silentEnded.getCount()).as("the silent stream keeps its place").isOne();

		Objects.requireNonNull(this.silentChannel).shutdownNow();
		Awaitility.await()
			.atMost(Duration.ofSeconds(10))
			.alias("the client connects at a retry once the place is free")
			.until(() -> registry().find(CLIENT_ID).isPresent() && started.isConnected());
		assertThat(registry().pending()).isEmpty();
	}

	private CountDownLatch openSilentStream() {
		ManagedChannel channel = ManagedChannelBuilder.forAddress("127.0.0.1", grpcPort).usePlaintext().build();
		this.silentChannel = channel;
		CountDownLatch ended = new CountDownLatch(1);
		Metadata metadata = new Metadata();
		metadata.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER), "Bearer it-token");
		metadata.put(Metadata.Key.of("x-sluice-id", Metadata.ASCII_STRING_MARSHALLER), "silent-client");
		TunnelGrpc.newStub(channel)
			.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata))
			.connect(new StreamObserver<>() {

				@Override
				public void onNext(Frame frame) {
				}

				@Override
				public void onError(Throwable t) {
					ended.countDown();
				}

				@Override
				public void onCompleted() {
					ended.countDown();
				}

			});
		return ended;
	}

	private double refused() {
		return Objects.requireNonNull(this.meterRegistry, "meterRegistry")
			.get("sluice.tunnel.streams.refused")
			.tag("reason", "pending-address-limit")
			.counter()
			.count();
	}

	private SessionRegistry registry() {
		return Objects.requireNonNull(this.registry, "registry");
	}

	private SluiceServerProperties properties() {
		return Objects.requireNonNull(this.properties, "properties");
	}

}
