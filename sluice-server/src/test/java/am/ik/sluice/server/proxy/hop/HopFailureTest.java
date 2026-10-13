package am.ik.sluice.server.proxy.hop;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import am.ik.sluice.tunnel.DuplexPipe;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.Http2ConnectionAdapter;
import io.netty.handler.codec.http2.Http2Error;
import io.netty.handler.codec.http2.Http2FrameAdapter;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2Stream;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static am.ik.sluice.server.proxy.hop.Fixture.awaitTrue;
import static am.ik.sluice.server.proxy.hop.Fixture.settled;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * How a hop fails: every failure goes to the loop's exception sink and closes the hop,
 * the peer's EOF or reset ends it quietly, and in every case its threads end and no
 * ByteBuf leaks ({@link LeakCheck}).
 */
@Tag("netty-hop")
@ExtendWith(LeakCheck.class)
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class HopFailureTest {

	private static final int MIB = 1024 * 1024;

	@Test
	void aPipelineOperationWithoutTheLockIsDroppedReportedAndClosesTheHop() throws Exception {
		Socket[] sockets = Endpoint.socketPair();
		try (Endpoint a = Endpoint.over("a", sockets[0], builder -> builder.server(true));
				Endpoint peer = Endpoint.over("peer", sockets[1], builder -> builder)) {
			ChannelPromise promise = a.hop.ctx().newPromise();

			// no Loop#run around it
			a.hop.encoder().writePing(a.hop.ctx(), false, 42, promise);

			awaitTrue(() -> !a.hop.open(), () -> "failures=" + a.sink.failures());
			assertThat(a.sink.failures()).isNotEmpty()
				.allSatisfy(failure -> assertThat(failure.cause()).isInstanceOf(IllegalStateException.class)
					.hasMessage("a: write without the loop's lock"));
			assertThat(promise.cause()).hasMessage("a: write without the loop's lock");
			awaitTrue(() -> !peer.hop.open(), () -> "the peer is still open");
			assertThat(peer.sink.failures()).isEmpty();
		}
	}

	@Test
	void anExceptionOfTheFrameListenerIsReportedAndEndsTheConnectionWithInternalError() throws Exception {
		Socket[] sockets = Endpoint.socketPair();
		CompletableFuture<Long> goAway = new CompletableFuture<>();
		try (Endpoint a = Endpoint.over("a", sockets[0],
				builder -> builder.server(true).listener(hop -> new Http2FrameAdapter() {

					@Override
					public void onPingRead(ChannelHandlerContext ctx, long data) {
						throw new IllegalStateException("listener bug");
					}

				}));
				Endpoint peer = Endpoint.over("peer", sockets[1],
						builder -> builder.listener(hop -> new Http2FrameAdapter() {

							@Override
							public void onGoAwayRead(ChannelHandlerContext ctx, int lastStreamId, long errorCode,
									ByteBuf debugData) {
								goAway.complete(errorCode);
							}

						}))) {
			peer.loop.run(() -> peer.hop.encoder().writePing(peer.hop.ctx(), false, 7, peer.hop.ctx().newPromise()));

			assertThat(goAway.get(10, TimeUnit.SECONDS)).isEqualTo(Http2Error.INTERNAL_ERROR.code());
			awaitTrue(() -> !a.hop.open(), () -> "a is still open");
			assertThat(a.sink.failures()).extracting(failure -> failure.cause().getMessage())
				.containsExactly("listener bug");
			assertThat(peer.sink.failures()).isEmpty();
		}
	}

	@Test
	void anExceptionOfAConnectionListenerIsReportedAndClosesTheHop() throws Exception {
		Socket[] sockets = Endpoint.socketPair();
		try (Endpoint a = Endpoint.over("a", sockets[0], builder -> builder.server(true));
				Endpoint peer = Endpoint.over("peer", sockets[1], builder -> builder)) {
			a.loop.run(() -> a.hop.addConnectionListener(new Http2ConnectionAdapter() {

				@Override
				public void onStreamAdded(Http2Stream stream) {
					throw new IllegalStateException("connection listener bug");
				}

			}));

			peer.loop.run(() -> peer.hop.encoder()
				.writeHeaders(peer.hop.ctx(), 1, request(), 0, true, peer.hop.ctx().newPromise()));

			awaitTrue(() -> !a.hop.open(), () -> "a is still open");
			assertThat(a.sink.failures()).extracting(failure -> failure.cause().getMessage())
				.containsExactly("connection listener bug");
		}
	}

	@Test
	void anExceptionOfAPromiseListenerIsReportedAndClosesTheHop() throws Exception {
		Socket[] sockets = Endpoint.socketPair();
		try (Endpoint a = Endpoint.over("a", sockets[0], builder -> builder.server(true));
				Endpoint peer = Endpoint.over("peer", sockets[1], builder -> builder)) {
			a.loop.run(() -> a.hop.encoder().writePing(a.hop.ctx(), false, 1, a.hop.newPromise(future -> {
				throw new IllegalStateException("promise listener bug");
			})));

			awaitTrue(() -> !a.hop.open(), () -> "a is still open");
			assertThat(a.sink.failures()).extracting(failure -> failure.cause().getMessage())
				.containsExactly("promise listener bug");
		}
	}

	@ParameterizedTest
	@ValueSource(booleans = { false, true })
	void aTaskFailingInTheChannelIsReportedAndClosesTheHop(boolean error) throws Exception {
		Socket[] sockets = Endpoint.socketPair();
		// a silent peer: no read surfaces what the channel recorded
		try (Socket peer = sockets[1]; Endpoint a = Endpoint.over("a", sockets[0], builder -> builder.server(true))) {
			a.loop.run(() -> a.hop.ctx().executor().execute(() -> {
				// netty's EmbeddedChannel records an exception, but lets an error through
				if (error) {
					throw new AssertionError("task bug");
				}
				throw new IllegalStateException("task bug");
			}));

			awaitTrue(() -> !a.hop.open(), () -> "a is still open");
			assertThat(a.sink.failures()).extracting(failure -> failure.cause().getMessage())
				.containsExactly("task bug");
		}
	}

	@Test
	void aWriterFailingIsReportedAndClosesTheHop() throws Exception {
		Socket[] sockets = Endpoint.socketPair();
		try (Socket peer = sockets[1];
				Endpoint a = Endpoint.over("a", sockets[0],
						builder -> builder.server(true).pipe(new BrokenSinkPipe(DuplexPipe.of(sockets[0]))))) {

			awaitTrue(() -> !a.hop.open(), () -> "a is still open");
			assertThat(a.hop.awaitDone(Duration.ofSeconds(10))).isTrue();
			assertThat(a.sink.failures()).extracting(failure -> failure.cause().getMessage())
				.containsExactly("sink broke");
			assertThat(peer.getInputStream().read()).as("the peer sees the pipe closed").isEqualTo(-1);
		}
	}

	@Test
	void aWriterFailingBeforeTheReaderStartedIsTheOnlyFailure() throws Exception {
		Socket[] sockets = Endpoint.socketPair();
		// the reader asks for the source only once the writer failed and closed the pipe
		try (Socket peer = sockets[1];
				Endpoint a = Endpoint.over("a", sockets[0], builder -> builder.server(true)
					.pipe(new BrokenSinkPipe(new LateSourcePipe(DuplexPipe.of(sockets[0])))))) {

			awaitTrue(() -> !a.hop.open(), () -> "a is still open");
			assertThat(a.hop.awaitDone(Duration.ofSeconds(10))).isTrue();
			assertThat(a.sink.failures()).extracting(failure -> failure.cause().getMessage())
				.containsExactly("sink broke");
		}
	}

	@Test
	void whatTheChannelWroteBeforeClosingReachesThePeer() throws Exception {
		int pings = 10_000;
		Socket[] sockets = Endpoint.socketPair();
		GatedPipe peerPipe = new GatedPipe(DuplexPipe.of(sockets[1]));
		AtomicLong pingsRead = new AtomicLong();
		CompletableFuture<Long> goAway = new CompletableFuture<>();
		try (Endpoint a = Endpoint.over("a", sockets[0],
				builder -> builder.server(true).closeGrace(Duration.ofSeconds(5)));
				Endpoint peer = Endpoint.over("peer", sockets[1],
						builder -> builder.pipe(peerPipe).listener(hop -> new Http2FrameAdapter() {

							@Override
							public void onPingRead(ChannelHandlerContext ctx, long data) {
								pingsRead.incrementAndGet();
							}

							@Override
							public void onGoAwayRead(ChannelHandlerContext ctx, int lastStreamId, long errorCode,
									ByteBuf debugData) {
								goAway.complete(errorCode);
							}

						}))) {
			awaitTrue(() -> peer.hop.bytesRead() > 0, () -> "no SETTINGS yet");
			peerPipe.pause();
			a.loop.run(() -> {
				for (int i = 0; i < pings; i++) {
					a.hop.encoder().writePing(a.hop.ctx(), false, i, a.hop.ctx().newPromise());
				}
			});
			// the peer reads nothing: the writer blocks with PINGs still queued
			awaitTrue(() -> a.hop.queuedBytes() > 64 * 1024, () -> "queued=" + a.hop.queuedBytes());

			a.loop.run(a.hop::closeLocked);
			awaitTrue(() -> !a.hop.open(), () -> "a is still open");
			peerPipe.resume();

			assertThat(goAway.get(10, TimeUnit.SECONDS)).isEqualTo(Http2Error.NO_ERROR.code());
			assertThat(pingsRead).hasValue(pings);
			assertThat(a.sink.failures()).isEmpty();
		}
	}

	@Test
	void thePeersEofEndsTheHopQuietly() throws Exception {
		Socket[] sockets = Endpoint.socketPair();
		AtomicBoolean inactive = new AtomicBoolean();
		try (Endpoint a = Endpoint.over("a", sockets[0],
				builder -> builder.server(true).onInactive(() -> inactive.set(true)))) {
			Endpoint peer = Endpoint.over("peer", sockets[1], builder -> builder);
			awaitTrue(() -> a.hop.bytesRead() > 0, () -> "no preface yet");

			peer.close();

			awaitTrue(() -> !a.hop.open() && inactive.get(), () -> "a is still open");
			assertThat(a.hop.awaitDone(Duration.ofSeconds(10))).isTrue();
			assertThat(a.sink.failures()).isEmpty();
			assertThat(peer.sink.failures()).isEmpty();
		}
	}

	@Test
	void aPeerResettingTheConnectionMidTransferEndsTheHopQuietly() throws Exception {
		Socket[] sockets = Endpoint.socketPair();
		Socket raw = sockets[0];
		try (Endpoint a = Endpoint.over("a", sockets[1], builder -> builder)) {
			OutputStream out = raw.getOutputStream();
			out.write(RawFrames.initialWindowSize(Integer.MAX_VALUE));
			out.write(RawFrames.windowUpdate(0, Integer.MAX_VALUE - 65535));
			a.loop.run(() -> {
				a.hop.encoder().writeHeaders(a.hop.ctx(), 1, request(), 0, false, a.hop.ctx().newPromise());
				a.hop.encoder()
					.writeData(a.hop.ctx(), 1, Unpooled.wrappedBuffer(new byte[8 * MIB]), 0, true,
							a.hop.ctx().newPromise());
			});
			InputStream in = raw.getInputStream();
			long read = 0;
			byte[] buffer = new byte[16 * 1024];
			while (read < 256 * 1024) {
				read += in.read(buffer);
			}

			// RST instead of FIN
			raw.setSoLinger(true, 0);
			raw.close();

			awaitTrue(() -> !a.hop.open(), () -> "a is still open");
			assertThat(a.hop.awaitDone(Duration.ofSeconds(10))).isTrue();
			assertThat(a.hop.bytesWritten()).isLessThan(8L * MIB);
			assertThat(a.sink.failures()).isEmpty();
		}
	}

	@Test
	void aWriterBlockedOnAPeerNotReadingIsCutAtTheCloseGrace() throws Exception {
		Socket[] sockets = Endpoint.socketPair();
		try (Socket raw = sockets[0];
				Endpoint a = Endpoint.over("a", sockets[1], builder -> builder.closeGrace(Duration.ofMillis(300)))) {
			OutputStream out = raw.getOutputStream();
			out.write(RawFrames.initialWindowSize(Integer.MAX_VALUE));
			out.write(RawFrames.windowUpdate(0, Integer.MAX_VALUE - 65535));
			a.loop.run(() -> {
				a.hop.encoder().writeHeaders(a.hop.ctx(), 1, request(), 0, false, a.hop.ctx().newPromise());
				a.hop.encoder()
					.writeData(a.hop.ctx(), 1, Unpooled.wrappedBuffer(new byte[8 * MIB]), 0, true,
							a.hop.ctx().newPromise());
			});
			// the peer reads nothing: the writer blocks and the queue fills
			awaitTrue(() -> !a.hop.writable(), () -> "queued=" + a.hop.queuedBytes());
			long written = settled(a.hop::bytesWritten);

			long start = System.nanoTime();
			a.loop.run(a.hop::abortLocked);

			assertThat(a.hop.awaitDone(Duration.ofSeconds(10))).isTrue();
			assertThat(Duration.ofNanos(System.nanoTime() - start)).isGreaterThanOrEqualTo(Duration.ofMillis(250));
			assertThat(a.hop.bytesWritten()).isEqualTo(written);
			assertThat(a.sink.failures()).isEmpty();
		}
	}

	@Test
	void aPeerSendingWithoutReadingPausesTheReader() throws Exception {
		int readPause = 256 * 1024;
		Socket[] sockets = Endpoint.socketPair();
		AtomicLong flooded = new AtomicLong();
		try (Socket raw = sockets[1];
				Endpoint a = Endpoint.over("a", sockets[0],
						builder -> builder.server(true).readPauseBytes(readPause))) {
			// PINGs as fast as the socket takes them, reading nothing: every one makes
			// the hop queue an ack
			Thread flooder = Thread.ofVirtual().start(() -> {
				try {
					OutputStream out = raw.getOutputStream();
					out.write(RawFrames.clientPreface());
					byte[] pings = new byte[1000 * 17];
					for (int i = 0; i < 1000; i++) {
						System.arraycopy(RawFrames.ping(i), 0, pings, i * 17, 17);
					}
					while (flooded.get() < 64L * MIB) {
						out.write(pings);
						flooded.addAndGet(pings.length);
					}
				}
				catch (Exception e) {
					// closed at the end of the test
				}
			});

			long read = settled(a.hop::bytesRead, a.hop::queuedBytes, flooded::get);

			// paused past the read pause bytes, by at most one read's acks
			assertThat(a.hop.queuedBytes()).isGreaterThan(readPause).isLessThanOrEqualTo(readPause + 32 * 1024);
			assertThat(read).isLessThan(4L * MIB);
			assertThat(flooded.get()).isLessThan(64L * MIB);
			assertThat(a.hop.open()).isTrue();
			raw.close();
			flooder.join();
		}
	}

	@Test
	void aHopFailingToStartLeavesNoHopAndWritesNothing() throws Exception {
		Socket[] sockets = Endpoint.socketPair();
		RecordingSink sink = new RecordingSink();
		try (Socket peer = sockets[1]; Socket socket = sockets[0]) {
			Loop loop = new Loop("start", sink);
			try {
				Hop.Builder builder = Hop.builder()
					.name("a")
					.loop(loop)
					.pipe(DuplexPipe.of(socket))
					.listener(hop -> new Http2FrameAdapter())
					.codec(codec -> {
						throw new IllegalArgumentException("bad codec");
					});

				assertThatThrownBy(builder::build).isInstanceOf(IllegalStateException.class)
					.hasMessage("a hop is built inside Loop#run");
				assertThatThrownBy(() -> loop.run(builder::build)).isInstanceOf(IllegalArgumentException.class)
					.hasMessage("bad codec");

				assertThat(loop.call(loop::hopsLocked)).isZero();
				peer.setSoTimeout(300);
				assertThatThrownBy(() -> peer.getInputStream().read()).isInstanceOf(SocketTimeoutException.class);
				assertThat(socket.isClosed()).isFalse();
				assertThat(sink.failures()).isEmpty();
			}
			finally {
				loop.close();
			}
		}
	}

	/** A pipe whose source is handed out late: a reader thread starting late. */
	private record LateSourcePipe(DuplexPipe delegate) implements DuplexPipe {

		@Override
		public InputStream source() {
			try {
				Thread.sleep(200);
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			return this.delegate.source();
		}

		@Override
		public OutputStream sink() {
			return this.delegate.sink();
		}

		@Override
		public void shutdownOutput() throws Exception {
			this.delegate.shutdownOutput();
		}

		@Override
		public void close() throws Exception {
			this.delegate.close();
		}

	}

	/** A pipe whose sink throws what no I/O failure throws: a bug in the transport. */
	private record BrokenSinkPipe(DuplexPipe delegate) implements DuplexPipe {

		@Override
		public InputStream source() {
			return this.delegate.source();
		}

		@Override
		public OutputStream sink() {
			return new OutputStream() {

				@Override
				public void write(int b) {
					throw new IllegalStateException("sink broke");
				}

				@Override
				public void write(byte[] b, int off, int len) {
					throw new IllegalStateException("sink broke");
				}

			};
		}

		@Override
		public void shutdownOutput() throws Exception {
			this.delegate.shutdownOutput();
		}

		@Override
		public void close() throws Exception {
			this.delegate.close();
		}

	}

	private static Http2Headers request() {
		return new DefaultHttp2Headers().method("POST").scheme("http").authority("a.test").path("/");
	}

}
