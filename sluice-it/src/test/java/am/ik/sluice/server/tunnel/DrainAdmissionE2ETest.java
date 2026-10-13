package am.ik.sluice.server.tunnel;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;

import am.ik.sluice.it.TestPorts;
import am.ik.sluice.server.SluiceServerApplication;
import am.ik.sluice.server.config.SluiceServerProperties;
import am.ik.sluice.v1.proto.Frame;
import am.ik.sluice.v1.proto.TunnelGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A tunnel stream reaching its admission after the drain began -- held between the start
 * of its call and the registry's admission until the drain took the pending streams -- is
 * failed at once with {@code UNAVAILABLE}, and the context close stays within its budget
 * instead of waiting for the stream's advertise timeout or the gRPC shutdown grace.
 */
class DrainAdmissionE2ETest {

	private static final long CLOSE_BUDGET_SECONDS = 15;

	private static final Duration ADVERTISE_TIMEOUT = Duration.ofSeconds(60);

	private @Nullable ConfigurableApplicationContext serverContext;

	private @Nullable ManagedChannel channel;

	@AfterEach
	void tearDown() {
		if (this.channel != null) {
			this.channel.shutdownNow();
		}
		if (this.serverContext != null) {
			this.serverContext.close();
		}
	}

	@Test
	void aStreamAdmittedAfterTheDrainBeganIsFailedAtOnce() throws Exception {
		int grpcPort = TestPorts.freePort();
		ConfigurableApplicationContext context = new SpringApplicationBuilder(SluiceServerApplication.class,
				HookedRegistryConfiguration.class)
			.run("--server.port=0", "--spring.grpc.server.port=" + grpcPort,
					"--sluice.data-port=" + TestPorts.freePort(), "--sluice.token=it-token",
					"--sluice.limits.advertise-timeout=" + ADVERTISE_TIMEOUT.getSeconds() + "s",
					"--spring.main.allow-bean-definition-overriding=true");
		this.serverContext = context;
		HookedSessionRegistry registry = (HookedSessionRegistry) context.getBean(SessionRegistry.class);
		Counter refusedDraining = context.getBean(MeterRegistry.class)
			.get("sluice.tunnel.streams.refused")
			.tag("reason", "draining")
			.counter();
		assertThat(context.getBean(SluiceServerProperties.class).limits().advertiseTimeout())
			.as("precondition: the advertise timeout outlasts the close budget")
			.isEqualTo(ADVERTISE_TIMEOUT);
		AtomicReference<@Nullable Thread> closer = new AtomicReference<>();
		AtomicReference<@Nullable Long> closeTook = new AtomicReference<>();
		registry.beforeAdmit(() -> {
			long start = System.nanoTime();
			closer.set(Thread.ofVirtual().name("e2e-context-close").start(() -> {
				context.close();
				closeTook.set(System.nanoTime() - start);
			}));
			registry.awaitDrainBegun();
		});

		Stream stream = open(grpcPort);

		assertThat(stream.ended.await(10, TimeUnit.SECONDS)).as("the stream is failed at once").isTrue();
		assertThat(registry.admissionsAfterDrain()).as("precondition: the admission ran after the drain began").isOne();
		Status status = Objects.requireNonNull(stream.status.get());
		assertThat(status.getCode()).isEqualTo(Status.Code.UNAVAILABLE);
		assertThat(status.getDescription()).isEqualTo("node is draining");
		assertThat(registry.pending()).isEmpty();
		assertThat(refusedDraining.count()).as("counted").isEqualTo(1);
		Thread closing = Objects.requireNonNull(closer.get());
		assertThat(closing.join(Duration.ofSeconds(CLOSE_BUDGET_SECONDS))).as("the context closed within its budget")
			.isTrue();
		assertThat(Duration.ofNanos(Objects.requireNonNull(closeTook.get())))
			.isLessThan(Duration.ofSeconds(CLOSE_BUDGET_SECONDS));
	}

	private Stream open(int grpcPort) {
		ManagedChannel channel = ManagedChannelBuilder.forAddress("127.0.0.1", grpcPort).usePlaintext().build();
		this.channel = channel;
		Stream stream = new Stream();
		Metadata metadata = new Metadata();
		metadata.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER), "Bearer it-token");
		metadata.put(Metadata.Key.of("x-sluice-id", Metadata.ASCII_STRING_MARSHALLER), "late-client");
		TunnelGrpc.newStub(channel)
			.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata))
			.connect(stream);
		return stream;
	}

	/** A stream that never advertises. */
	private static final class Stream implements StreamObserver<Frame> {

		final CountDownLatch ended = new CountDownLatch(1);

		final AtomicReference<@Nullable Status> status = new AtomicReference<>();

		@Override
		public void onNext(Frame frame) {
		}

		@Override
		public void onError(Throwable t) {
			this.status.set(Status.fromThrowable(t));
			this.ended.countDown();
		}

		@Override
		public void onCompleted() {
			this.status.set(Status.OK);
			this.ended.countDown();
		}

	}

	/**
	 * Replaces the registry with {@link HookedSessionRegistry}; not a
	 * {@code @Configuration}, so the server's component scan of this package never picks
	 * it up in other tests.
	 */
	static class HookedRegistryConfiguration {

		@Bean
		SessionRegistry sessionRegistry(SluiceServerProperties properties) {
			return new HookedSessionRegistry(properties);
		}

	}

	/**
	 * A registry running a hook once before its first admission, and telling when the
	 * drain began.
	 */
	static final class HookedSessionRegistry extends SessionRegistry {

		private final CountDownLatch drainBegun = new CountDownLatch(1);

		private final AtomicBoolean hooked = new AtomicBoolean();

		private final AtomicReference<Runnable> beforeAdmit = new AtomicReference<>(() -> {
		});

		private volatile int admissionsAfterDrain;

		HookedSessionRegistry(SluiceServerProperties properties) {
			super(properties);
		}

		void beforeAdmit(Runnable hook) {
			this.beforeAdmit.set(hook);
		}

		void awaitDrainBegun() {
			try {
				assertThat(this.drainBegun.await(10, TimeUnit.SECONDS)).as("the drain began").isTrue();
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}

		int admissionsAfterDrain() {
			return this.admissionsAfterDrain;
		}

		@Override
		Optional<Refusal> admit(TunnelSession session) {
			if (this.hooked.compareAndSet(false, true)) {
				this.beforeAdmit.get().run();
			}
			if (this.drainBegun.getCount() == 0) {
				this.admissionsAfterDrain++;
			}
			return super.admit(session);
		}

		@Override
		List<TunnelSession> beginDrain() {
			try {
				return super.beginDrain();
			}
			finally {
				this.drainBegun.countDown();
			}
		}

	}

}
