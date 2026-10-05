package am.ik.sluice.it;

import java.net.ServerSocket;
import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import org.jspecify.annotations.Nullable;

import am.ik.sluice.server.SluiceServerApplication;
import am.ik.sluice.v1.proto.Frame;
import am.ik.sluice.v1.proto.TunnelGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.MetadataUtils;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import am.ik.sluice.server.tunnel.SessionRegistry;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The client pings the server periodically (gRPC keepalive); the server must permit
 * client pings, otherwise it GOAWAYs the stream with TOO_MANY_PINGS and the tunnel dies
 * while idle. Uses an aggressive 1s ping interval to keep the test fast.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(Lifecycle.PER_CLASS)
class GrpcKeepAliveTest {

	private static int grpcPort;

	private static @Nullable ManagedChannel channel;

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) {
		grpcPort = freePort();
		registry.add("spring.grpc.server.port", () -> String.valueOf(grpcPort));
		registry.add("sluice.data-port", () -> String.valueOf(freePort()));
		registry.add("sluice.token", () -> "it-token");
		registry.add("server.port", () -> String.valueOf(freePort()));
	}

	private static int freePort() {
		try (ServerSocket socket = new ServerSocket(0)) {
			return socket.getLocalPort();
		}
		catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	@BeforeAll
	void startChannel() {
		// ping interval (1s) far below the pre-tuning server permit (5m) on purpose
		this.channel = ManagedChannelBuilder.forAddress("127.0.0.1", grpcPort)
			.usePlaintext()
			.keepAliveTime(1, TimeUnit.SECONDS)
			.keepAliveWithoutCalls(true)
			.build();
	}

	@AfterAll
	void tearDown() {
		if (this.channel != null) {
			this.channel.shutdownNow();
		}
	}

	@Test
	void clientPingsArePermittedAndStreamSurvives() throws Exception {
		CountDownLatch connected = new CountDownLatch(1);
		AtomicReference<Throwable> error = new AtomicReference<>();
		ClientResponseObserver<Frame, Frame> observer = new ClientResponseObserver<>() {

			@Override
			public void beforeStart(ClientCallStreamObserver<Frame> call) {
				// no-op; the stream is driven manually below
			}

			@Override
			public void onNext(Frame frame) {
				if (frame.getBodyCase() == Frame.BodyCase.ADVERTISE_ACK) {
					connected.countDown();
				}
			}

			@Override
			public void onError(Throwable t) {
				error.set(t);
				connected.countDown();
			}

			@Override
			public void onCompleted() {
			}

		};
		Metadata metadata = new Metadata();
		metadata.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER), "Bearer it-token");
		metadata.put(Metadata.Key.of("x-sluice-id", Metadata.ASCII_STRING_MARSHALLER), UUID.randomUUID().toString());
		ClientCallStreamObserver<Frame> call = (ClientCallStreamObserver<Frame>) TunnelGrpc
			.newStub(Objects.requireNonNull(this.channel, "channel is required"))
			.withWaitForReady()
			.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata))
			.connect(observer);
		call.onNext(Frame.newBuilder().setAdvertise(am.ik.sluice.v1.proto.Advertise.getDefaultInstance()).build());
		assertThat(connected.await(5, TimeUnit.SECONDS)).isTrue();
		assertThat(error.get()).as("stream error: %s", error.get()).isNull();

		// hold the idle stream past several ping intervals; a TOO_MANY_PINGS GOAWAY
		// breaks streams opened on the channel afterwards
		Thread.sleep(Duration.ofSeconds(6).toMillis());
		assertThat(error.get()).as("stream error: %s", error.get()).isNull();
		assertThat(Objects.requireNonNull(this.channel, "channel is required").getState(true))
			.isEqualTo(io.grpc.ConnectivityState.READY);

		// a new stream on the ping-heavy channel must still be served
		CountDownLatch reconnect = new CountDownLatch(1);
		AtomicReference<Throwable> secondError = new AtomicReference<>();
		ClientResponseObserver<Frame, Frame> secondObserver = new ClientResponseObserver<>() {

			@Override
			public void beforeStart(ClientCallStreamObserver<Frame> firstCall) {
			}

			@Override
			public void onNext(Frame frame) {
				if (frame.getBodyCase() == Frame.BodyCase.ADVERTISE_ACK) {
					reconnect.countDown();
				}
			}

			@Override
			public void onError(Throwable t) {
				secondError.set(t);
				reconnect.countDown();
			}

			@Override
			public void onCompleted() {
			}

		};
		Metadata secondMetadata = new Metadata();
		secondMetadata.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER), "Bearer it-token");
		secondMetadata.put(Metadata.Key.of("x-sluice-id", Metadata.ASCII_STRING_MARSHALLER),
				UUID.randomUUID().toString());
		ClientCallStreamObserver<Frame> secondCall = (ClientCallStreamObserver<Frame>) TunnelGrpc
			.newStub(Objects.requireNonNull(this.channel, "channel is required"))
			.withWaitForReady()
			.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(secondMetadata))
			.connect(secondObserver);
		secondCall
			.onNext(Frame.newBuilder().setAdvertise(am.ik.sluice.v1.proto.Advertise.getDefaultInstance()).build());
		assertThat(reconnect.await(5, TimeUnit.SECONDS)).isTrue();
		assertThat(secondError.get()).as("second stream error: %s", secondError.get()).isNull();
	}

}
