package am.ik.sluice.server.tunnel;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

import am.ik.sluice.server.config.SluiceServerProperties;
import am.ik.sluice.server.route.Router;
import am.ik.sluice.tunnel.SessionSender;
import am.ik.sluice.tunnel.VirtualConnection;
import am.ik.sluice.v1.proto.Advertise;
import am.ik.sluice.v1.proto.Data;
import am.ik.sluice.v1.proto.Frame;
import am.ik.sluice.v1.proto.Upstream;
import am.ik.sluice.v1.proto.WindowUpdate;
import io.grpc.Status;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The session's receive path never waits on a slow ADVERTISE or a slow consumer of a
 * connection, the route removal of close() is not undone by an advertise that was
 * pending, and a client announcing no per-connection window is rejected.
 */
@Timeout(30)
class TunnelSessionReceivePathTest {

	private static final String CLIENT_ID = "client-a";

	private static final int LISTEN_PORT = 19123;

	private final SessionRegistry registry = new SessionRegistry(SluiceServerProperties.builder().build());

	private final Router router = new Router();

	private final BlockingTcpRoutes tcpRoutes = new BlockingTcpRoutes();

	private final RecordingStream stream = new RecordingStream();

	private final AtomicInteger windowUpdatesReceived = new AtomicInteger();

	@AfterEach
	void releaseReconcile() {
		this.tcpRoutes.release.countDown();
	}

	private TunnelSession session(Duration closeTimeout) {
		TunnelSession session = TunnelSession.builder()
			.clientId(CLIENT_ID)
			.router(this.router)
			.sender(new SessionSender(this.stream))
			.registry(this.registry)
			.tcpRoutes(this.tcpRoutes)
			.windowUpdateReceived(this.windowUpdatesReceived::incrementAndGet)
			.closeTimeout(closeTimeout)
			.build();
		session.start();
		return session;
	}

	private static Frame advertise(int connectionWindow) {
		Upstream upstream = Upstream.newBuilder()
			.setHost("tcp.local")
			.setTargetUrl("tcp://127.0.0.1:1")
			.setListenPort(LISTEN_PORT)
			.build();
		return Frame.newBuilder()
			.setAdvertise(Advertise.newBuilder().addUpstreams(upstream).setConnectionWindow(connectionWindow))
			.build();
	}

	private static Frame data(long connectionId, int length) {
		return Frame.newBuilder()
			.setData(Data.newBuilder().setConnId(connectionId).setPayload(ByteString.copyFrom(new byte[length])))
			.build();
	}

	/** Handles the frame on another thread; fails unless it returns within 100ms. */
	private static void handleWithin100ms(TunnelSession session, Frame frame) throws Exception {
		CompletableFuture.runAsync(() -> session.handle(frame), task -> Thread.ofVirtual().start(task))
			.get(100, TimeUnit.MILLISECONDS);
	}

	private void awaitReconcileEntered() throws InterruptedException {
		assertThat(this.tcpRoutes.entered.await(5, TimeUnit.SECONDS)).as("precondition: reconcile is running").isTrue();
	}

	@Test
	void advertiseStuckInReconcileDoesNotDelayData() throws Exception {
		TunnelSession session = session(Duration.ofSeconds(5));
		handleWithin100ms(session, advertise(VirtualConnection.DEFAULT_WINDOW));
		awaitReconcileEntered();
		VirtualConnection connection = session.open("127.0.0.1:1");
		handleWithin100ms(session, data(connection.connectionId(), 3));
		CompletableFuture<byte[]> read = CompletableFuture.supplyAsync(() -> readN(connection.source(), 3),
				task -> Thread.ofVirtual().start(task));
		assertThat(read.get(100, TimeUnit.MILLISECONDS)).hasSize(3);
	}

	@Test
	void closeWhileAnAdvertiseIsPendingLeavesNoRoute() throws Exception {
		TunnelSession session = session(Duration.ofSeconds(5));
		session.handle(advertise(VirtualConnection.DEFAULT_WINDOW));
		awaitReconcileEntered();
		assertThat(this.router.lookupByPort(LISTEN_PORT)).as("precondition: the advertise registered its route")
			.isPresent();
		CompletableFuture<Void> close = CompletableFuture.runAsync(session::close,
				task -> Thread.ofVirtual().start(task));
		TimeUnit.MILLISECONDS.sleep(200);
		assertThat(close).as("close waits for the pending advertise").isNotDone();
		this.tcpRoutes.release.countDown();
		close.get(5, TimeUnit.SECONDS);
		assertThat(this.router.clientCount()).isZero();
		assertThat(this.tcpRoutes.calls).containsExactly(Set.of(LISTEN_PORT), Set.of());
	}

	@Test
	void closeReturnsWithinItsBoundWhileAnAdvertiseIsStuck() throws Exception {
		TunnelSession session = session(Duration.ofMillis(300));
		session.handle(advertise(VirtualConnection.DEFAULT_WINDOW));
		awaitReconcileEntered();
		long start = System.nanoTime();
		session.close();
		long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
		assertThat(elapsedMillis).as("close with a stuck advertise").isBetween(300L, 1000L);
		assertThat(this.router.clientCount()).as("the removal waits behind the advertise").isOne();
		this.tcpRoutes.release.countDown();
		long deadline = System.currentTimeMillis() + 5000;
		while (this.router.clientCount() != 0 && System.currentTimeMillis() < deadline) {
			TimeUnit.MILLISECONDS.sleep(10);
		}
		assertThat(this.router.clientCount()).as("routes removed once the advertise ended").isZero();
		assertThat(this.tcpRoutes.calls).containsExactly(Set.of(LISTEN_PORT), Set.of());
	}

	/**
	 * The removal deferred behind a stuck advertise must not wipe the routes of the
	 * client's next session: routes are keyed by client id.
	 */
	@Test
	void deferredRemovalSparesTheRoutesOfANewerSession() throws Exception {
		TunnelSession first = session(Duration.ofMillis(300));
		first.handle(advertise(VirtualConnection.DEFAULT_WINDOW));
		awaitReconcileEntered();
		first.close();
		TunnelSession second = session(Duration.ofSeconds(5));
		second.handle(advertise(VirtualConnection.DEFAULT_WINDOW));
		awaitCalls(2);
		this.tcpRoutes.release.countDown();
		// the first session's advertise ends and its removal runs right after; a removal
		// that ran would show up as an empty reconcile
		TimeUnit.MILLISECONDS.sleep(500);
		assertThat(this.tcpRoutes.calls).as("the deferred removal releases nothing")
			.containsExactly(Set.of(LISTEN_PORT), Set.of(LISTEN_PORT));
		assertThat(this.router.lookupByPort(LISTEN_PORT)).isPresent();
		assertThat(this.registry.find(CLIENT_ID)).contains(second);
	}

	private void awaitCalls(int count) throws InterruptedException {
		long deadline = System.currentTimeMillis() + 5000;
		while (this.tcpRoutes.calls.size() < count && System.currentTimeMillis() < deadline) {
			TimeUnit.MILLISECONDS.sleep(10);
		}
		assertThat(this.tcpRoutes.calls).hasSizeGreaterThanOrEqualTo(count);
	}

	@Test
	void advertiseAfterCloseRegistersNothing() {
		TunnelSession session = session(Duration.ofSeconds(5));
		session.close();
		session.handle(advertise(VirtualConnection.DEFAULT_WINDOW));
		assertThat(this.router.clientCount()).isZero();
		assertThat(this.tcpRoutes.calls).containsExactly(Set.of());
	}

	@Test
	@ExtendWith(OutputCaptureExtension.class)
	void advertiseWithoutAWindowFailsTheStream(CapturedOutput output) throws Exception {
		TunnelSession session = session(Duration.ofSeconds(5));
		this.tcpRoutes.release.countDown();
		session.handle(advertise(0));
		Throwable failure = this.stream.awaitFailure();
		Status status = Status.fromThrowable(failure);
		assertThat(status.getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
		assertThat(status.getDescription()).isEqualTo(TunnelSession.WINDOW_REQUIRED);
		// a second windowless advertise on the same stream fails nothing more
		session.handle(advertise(0));
		TimeUnit.MILLISECONDS.sleep(200);
		assertThat(this.stream.failures).as("the stream failed once").hasSize(1);
		assertThat(this.stream.completions).as("failed, not completed").hasValue(0);
		assertThat(this.stream.frames).as("no ack, no route").isEmpty();
		assertThat(this.router.clientCount()).isZero();
		assertThat(this.tcpRoutes.calls).as("only the removal of close()").containsExactly(Set.of());
		assertThat(this.registry.find(CLIENT_ID)).as("no session left").isEmpty();
		assertThat(output.getAll().lines().filter(line -> line.contains(TunnelSession.WINDOW_REQUIRED)))
			.as("one line per rejected stream")
			.hasSize(1);
	}

	@Test
	void connectionsSendWithinTheClientsWindowAndAnnounceTheServers() throws Exception {
		TunnelSession session = session(Duration.ofSeconds(5));
		this.tcpRoutes.release.countDown();
		session.handle(advertise(10));
		VirtualConnection first = session.open("127.0.0.1:1");
		write(first, 10).get(1, TimeUnit.SECONDS);
		CompletableFuture<Void> beyond = write(first, 1);
		TimeUnit.MILLISECONDS.sleep(200);
		assertThat(beyond).as("the client's 10-byte window is used up").isNotDone();
		VirtualConnection second = session.open("127.0.0.1:1");
		write(second, 10).get(1, TimeUnit.SECONDS);
		List<Integer> windows = this.stream.awaitConnects(2)
			.stream()
			.map(frame -> frame.getConnect().getConnectionWindow())
			.toList();
		assertThat(windows).as("every CONNECT announces the server's initial window")
			.containsExactly(VirtualConnection.INITIAL_WINDOW, VirtualConnection.INITIAL_WINDOW);
	}

	private static CompletableFuture<Void> write(VirtualConnection connection, int length) {
		return CompletableFuture.runAsync(() -> {
			try {
				connection.sink().write(new byte[length]);
			}
			catch (IOException e) {
				throw new IllegalStateException(e);
			}
		}, task -> Thread.ofVirtual().start(task));
	}

	@Test
	void windowUpdateGrantsCreditToItsConnection() throws Exception {
		TunnelSession session = session(Duration.ofSeconds(5));
		this.tcpRoutes.release.countDown();
		session.handle(advertise(10));
		VirtualConnection connection = session.open("127.0.0.1:1");
		OutputStream sink = connection.sink();
		sink.write(new byte[10]);
		CompletableFuture<Void> parked = CompletableFuture.runAsync(() -> {
			try {
				sink.write(new byte[1]);
			}
			catch (IOException e) {
				throw new IllegalStateException(e);
			}
		}, task -> Thread.ofVirtual().start(task));
		TimeUnit.MILLISECONDS.sleep(200);
		assertThat(parked).as("the client's 10-byte window is used up").isNotDone();
		session.handle(Frame.newBuilder()
			.setWindowUpdate(WindowUpdate.newBuilder().setConnId(connection.connectionId()).setIncrement(1))
			.build());
		parked.get(1, TimeUnit.SECONDS);
		assertThat(this.windowUpdatesReceived).hasValue(1);
	}

	@Test
	void windowUpdateForAnUnknownConnectionIsIgnored() {
		TunnelSession session = session(Duration.ofSeconds(5));
		session.handle(
				Frame.newBuilder().setWindowUpdate(WindowUpdate.newBuilder().setConnId(4242).setIncrement(1)).build());
		assertThat(this.windowUpdatesReceived).hasValue(1);
		assertThat(session.connectionCount()).isZero();
	}

	@Test
	void dataBeyondTheWindowFailsOnlyItsConnection() throws Exception {
		TunnelSession session = session(Duration.ofSeconds(5));
		this.tcpRoutes.release.countDown();
		session.handle(advertise(VirtualConnection.DEFAULT_WINDOW));
		VirtualConnection violated = session.open("127.0.0.1:1");
		VirtualConnection other = session.open("127.0.0.1:1");
		handleWithin100ms(session, data(violated.connectionId(), VirtualConnection.DEFAULT_WINDOW + 1));
		assertThatThrownBy(() -> violated.source().read()).isInstanceOf(IOException.class)
			.hasMessage(VirtualConnection.WINDOW_EXCEEDED);
		Frame error = this.stream.await(Frame.BodyCase.ERROR, 1).getFirst();
		assertThat(error.getError().getConnId()).isEqualTo(violated.connectionId());
		assertThat(error.getError().getMessage()).isEqualTo(VirtualConnection.WINDOW_EXCEEDED);
		session.handle(data(other.connectionId(), 2));
		assertThat(readN(other.source(), 2)).hasSize(2);
	}

	private static byte[] readN(InputStream in, int count) {
		try {
			return in.readNBytes(count);
		}
		catch (IOException e) {
			throw new IllegalStateException(e);
		}
	}

	/**
	 * Records the listen port sets; blocks the first non-empty reconcile until released.
	 */
	private static final class BlockingTcpRoutes implements TcpRouteListener {

		final List<Set<Integer>> calls = new CopyOnWriteArrayList<>();

		final CountDownLatch entered = new CountDownLatch(1);

		final CountDownLatch release = new CountDownLatch(1);

		@Override
		public Set<Integer> reconcile(String clientId, Set<Integer> listenPorts) {
			this.calls.add(Set.copyOf(listenPorts));
			if (!listenPorts.isEmpty() && this.entered.getCount() > 0) {
				this.entered.countDown();
				try {
					this.release.await(10, TimeUnit.SECONDS);
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
			return Set.of();
		}

	}

}
