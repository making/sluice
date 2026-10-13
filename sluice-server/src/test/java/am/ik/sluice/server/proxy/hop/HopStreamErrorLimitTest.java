package am.ik.sluice.server.proxy.hop;

import java.net.Socket;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntPredicate;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.Http2Error;
import io.netty.handler.codec.http2.Http2FrameAdapter;
import io.netty.handler.codec.http2.Http2FrameListener;
import io.netty.handler.codec.http2.Http2FrameLogger;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2Settings;
import io.netty.handler.logging.LogLevel;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;

import static am.ik.sluice.server.proxy.hop.Fixture.awaitTrue;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Which resets a hop's peer is cut off for: on a server hop only the stream errors
 * netty's codec answers the peer's own frames with count towards the limit, never the
 * resets the bridging code writes; a client hop limits neither the resets it sends nor
 * those it receives. A peer's frames are counted as they arrive ({@link Peer}), resets of
 * streams netty already forgot included.
 */
@Tag("netty-hop")
@ExtendWith(LeakCheck.class)
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class HopStreamErrorLimitTest {

	/** netty's limit on the resets a server endpoint sends, and on those it receives. */
	private static final int NETTY_RESET_LIMIT = 200;

	/** The streams opened at once. */
	private static final int BATCH = 50;

	private static final long[] BRIDGE_CODES = { Http2Error.REFUSED_STREAM.code(), Http2Error.INTERNAL_ERROR.code(),
			Http2Error.PROTOCOL_ERROR.code(), Http2Error.ENHANCE_YOUR_CALM.code() };

	/**
	 * A server hop's bridging code resetting past the default limit -- each stream after
	 * its response head, with DATA held for the peer's window of 0, so netty's codec also
	 * fails that DATA when the reset closes the stream -- keeps the connection.
	 */
	@Test
	void bridgeResetsPastTheDefaultLimitKeepTheConnection() throws Exception {
		assertThat(Hop.DEFAULT_MAX_STREAM_ERRORS).isEqualTo(NETTY_RESET_LIMIT);
		int streams = Hop.DEFAULT_MAX_STREAM_ERRORS + 10;
		Socket[] sockets = Endpoint.socketPair();
		Peer peerFrames = new Peer();
		try (Endpoint a = Endpoint.over("a", sockets[0],
				builder -> builder.server(true).listener(hop -> new ResettingBridge(hop, streamId -> true)));
				Endpoint peer = Endpoint.over("peer", sockets[1],
						builder -> peerFrames.attach(builder)
							.codec(codec -> codec.frameLogger(peerFrames.logger())
								.initialSettings(new Http2Settings().initialWindowSize(0))))) {
			openStreams(peer, peerFrames, streams);

			assertThat(peerFrames.resets).as("the bridge's resets").hasSize(streams);
			assertThat(peerFrames.resets).containsOnly(BRIDGE_CODES[0], BRIDGE_CODES[1], BRIDGE_CODES[2],
					BRIDGE_CODES[3]);
			assertThat(peerFrames.dataFrames).as("DATA after its reset").hasValue(0);
			peerFrames.assertPingAnswered(peer);
			assertThat(peerFrames.goAway.getNow(null)).as("GOAWAY").isNull();
			assertThat(a.hop.open()).isTrue();
			assertThat(a.sink.failures()).isEmpty();
			assertThat(peer.sink.failures()).isEmpty();
		}
	}

	/**
	 * The peer's stream errors count, the bridge's resets do not: past ten resets of the
	 * bridge and three stream errors of the peer -- a stream WINDOW_UPDATE of 0, on the
	 * closed stream too (RFC 9113 6.9) -- the connection goes on; the peer's fourth
	 * error, answered with its reset, ends it with GOAWAY {@code ENHANCE_YOUR_CALM}.
	 */
	@Test
	void onlyThePeersStreamErrorsCountTowardsTheLimit() throws Exception {
		int bridgeResets = 10;
		int errorStream = 2 * bridgeResets + 1;
		Socket[] sockets = Endpoint.socketPair();
		Peer peerFrames = new Peer();
		try (Endpoint a = Endpoint.over("a", sockets[0],
				builder -> builder.server(true)
					.streamErrorLimit(3, Duration.ofMinutes(1))
					.listener(hop -> new ResettingBridge(hop, streamId -> streamId < errorStream)));
				Endpoint peer = Endpoint.over("peer", sockets[1],
						builder -> peerFrames.attach(builder)
							.codec(codec -> codec.frameLogger(peerFrames.logger())
								.initialSettings(new Http2Settings().initialWindowSize(0))))) {
			peer.loop.run(() -> {
				for (int i = 0; i < bridgeResets; i++) {
					peer.hop.encoder()
						.writeHeaders(peer.hop.ctx(), 2 * i + 1, request(), 0, true, peer.hop.ctx().newPromise());
				}
				peer.hop.encoder()
					.writeHeaders(peer.hop.ctx(), errorStream, request(), 0, false, peer.hop.ctx().newPromise());
			});
			peerFrames.awaitResets(bridgeResets, peer);

			windowUpdatesOfZero(peer, errorStream, 3);

			peerFrames.awaitResets(bridgeResets + 3, peer);
			peerFrames.assertPingAnswered(peer);
			assertThat(peerFrames.resets.subList(bridgeResets, bridgeResets + 3))
				.containsOnly(Http2Error.PROTOCOL_ERROR.code());
			assertThat(a.hop.open()).isTrue();

			windowUpdatesOfZero(peer, errorStream, 1);

			assertThat(peerFrames.goAway.get(10, TimeUnit.SECONDS)).isEqualTo(Http2Error.ENHANCE_YOUR_CALM.code());
			awaitTrue(() -> !a.hop.open(), () -> "a is still open");
			assertThat(peerFrames.resets).as("the fourth error answered first").hasSize(bridgeResets + 4);
			assertThat(a.sink.failures()).isEmpty();
		}
	}

	/**
	 * A server hop still ends a connection whose peer resets streams past netty's limit
	 * on the resets a server receives ("Rapid Reset", 200 per 30s), with GOAWAY
	 * {@code ENHANCE_YOUR_CALM}.
	 */
	@Test
	void aServerHopEndsTheConnectionOfAPeerResettingPastNettysLimit() throws Exception {
		int streams = NETTY_RESET_LIMIT + 1;
		Socket[] sockets = Endpoint.socketPair();
		Peer peerFrames = new Peer();
		try (Endpoint a = Endpoint.over("a", sockets[0], builder -> builder.server(true));
				Endpoint peer = Endpoint.over("peer", sockets[1],
						builder -> peerFrames.attach(builder).codec(codec -> codec.frameLogger(peerFrames.logger())))) {
			for (int batch = 0; batch < streams; batch += BATCH) {
				int first = batch;
				// a batch's streams close once the loop flushed their resets
				peer.loop.run(() -> {
					for (int i = first; i < Math.min(first + BATCH, streams); i++) {
						int streamId = 2 * i + 1;
						peer.hop.encoder().writeHeaders(peer.hop.ctx(), streamId, request(), 0, false, written(peer));
						peer.hop.encoder()
							.writeRstStream(peer.hop.ctx(), streamId, Http2Error.CANCEL.code(), written(peer));
					}
				});
			}

			assertThat(peerFrames.goAway.get(10, TimeUnit.SECONDS)).isEqualTo(Http2Error.ENHANCE_YOUR_CALM.code());
			awaitTrue(() -> !a.hop.open(), () -> "a is still open");
		}
	}

	/**
	 * A client hop (a leg) answers its peer's stream errors past netty's limit on the
	 * resets a server sends, and keeps its connection.
	 */
	@Test
	void aClientHopAnswersStreamErrorsPastNettysLimit() throws Exception {
		int errors = NETTY_RESET_LIMIT + 10;
		Socket[] sockets = Endpoint.socketPair();
		Peer peerFrames = new Peer();
		try (Endpoint leg = Endpoint.over("leg", sockets[0], builder -> builder);
				Endpoint peer = Endpoint.over("peer", sockets[1], builder -> peerFrames.attach(builder.server(true))
					.codec(codec -> codec.frameLogger(peerFrames.logger())))) {
			leg.loop.run(() -> leg.hop.encoder()
				.writeHeaders(leg.hop.ctx(), 1, request(), 0, false, leg.hop.ctx().newPromise()));
			awaitTrue(() -> peer.loop.call(() -> peer.hop.connection().stream(1) != null), () -> "no stream 1 yet");

			windowUpdatesOfZero(peer, 1, errors);

			peerFrames.awaitResets(errors, peer);
			assertThat(peerFrames.resets).hasSize(errors).containsOnly(Http2Error.PROTOCOL_ERROR.code());
			peerFrames.assertPingAnswered(peer);
			assertThat(peerFrames.goAway.getNow(null)).as("GOAWAY").isNull();
			assertThat(leg.hop.open()).isTrue();
			assertThat(leg.sink.failures()).isEmpty();
		}
	}

	/**
	 * A client hop (a leg) whose peer resets its streams past netty's limit on the resets
	 * a server receives keeps its connection.
	 */
	@Test
	void aClientHopTakesResetsPastNettysLimit() throws Exception {
		int streams = NETTY_RESET_LIMIT + 10;
		Socket[] sockets = Endpoint.socketPair();
		Peer legFrames = new Peer();
		Peer peerFrames = new Peer();
		try (Endpoint leg = Endpoint.over("leg", sockets[0],
				builder -> legFrames.attach(builder)
					.codec(codec -> codec.frameLogger(legFrames.logger())
						.initialSettings(new Http2Settings().initialWindowSize(0))));
				Endpoint peer = Endpoint.over("peer", sockets[1],
						builder -> builder.server(true)
							.listener(hop -> new ResettingBridge(hop, streamId -> true))
							.codec(codec -> codec.frameLogger(peerFrames.logger())))) {
			openStreams(leg, legFrames, streams);

			assertThat(legFrames.resets).hasSize(streams);
			assertThat(legFrames.dataFrames).as("DATA after its reset").hasValue(0);
			legFrames.assertPingAnswered(leg);
			assertThat(peerFrames.goAway.getNow(null)).as("GOAWAY").isNull();
			assertThat(leg.hop.open()).isTrue();
			assertThat(leg.sink.failures()).isEmpty();
		}
	}

	/**
	 * Has the endpoint open the streams, a batch at a time -- within the concurrency
	 * netty allows before its peer's SETTINGS arrived --, each ended at once, and waits
	 * until each batch's streams were reset.
	 */
	private static void openStreams(Endpoint endpoint, Peer frames, int streams) throws InterruptedException {
		for (int batch = 0; batch < streams; batch += BATCH) {
			int first = batch;
			int end = Math.min(first + BATCH, streams);
			endpoint.loop.run(() -> {
				for (int i = first; i < end; i++) {
					endpoint.hop.encoder()
						.writeHeaders(endpoint.hop.ctx(), 2 * i + 1, request(), 0, true, written(endpoint));
				}
			});
			frames.awaitResets(end, endpoint);
		}
	}

	/** A promise whose failure goes to the endpoint's sink. */
	private static ChannelPromise written(Endpoint endpoint) {
		return endpoint.hop.newPromise(future -> {
			if (!future.isSuccess()) {
				throw new IllegalStateException("a write failed", future.cause());
			}
		});
	}

	/** Has the peer send stream WINDOW_UPDATEs of 0, past its codec's checks. */
	private static void windowUpdatesOfZero(Endpoint peer, int streamId, int count) {
		peer.loop.run(() -> {
			for (int i = 0; i < count; i++) {
				peer.hop.encoder()
					.frameWriter()
					.writeWindowUpdate(peer.hop.ctx(), streamId, 0, peer.hop.ctx().newPromise());
			}
		});
	}

	private static Http2Headers request() {
		return new DefaultHttp2Headers().method("GET").scheme("http").authority("a.test").path("/");
	}

	/**
	 * Bridging code answering each stream it is told to with a response head and one byte
	 * of DATA -- held for the peer's stream window of 0 --, then resetting it with one
	 * code after the other of {@link #BRIDGE_CODES}.
	 */
	private static final class ResettingBridge extends Http2FrameAdapter {

		private final Hop hop;

		private final IntPredicate resets;

		ResettingBridge(Hop hop, IntPredicate resets) {
			this.hop = hop;
			this.resets = resets;
		}

		@Override
		public void onHeadersRead(ChannelHandlerContext ctx, int streamId, Http2Headers headers, int streamDependency,
				short weight, boolean exclusive, int padding, boolean endStream) {
			if (!this.resets.test(streamId)) {
				return;
			}
			this.hop.encoder()
				.writeHeaders(this.hop.ctx(), streamId, new DefaultHttp2Headers().status("200"), 0, false,
						this.hop.ctx().newPromise());
			this.hop.encoder()
				.writeData(this.hop.ctx(), streamId, Unpooled.wrappedBuffer(new byte[1]), 0, true,
						this.hop.ctx().newPromise());
			this.hop.encoder()
				.writeRstStream(this.hop.ctx(), streamId, BRIDGE_CODES[(streamId / 2) % BRIDGE_CODES.length],
						this.hop.ctx().newPromise());
		}

	}

	/**
	 * What one endpoint read: every RST_STREAM and GOAWAY as it arrived ({@link #logger},
	 * which sees them whatever the stream's state), the DATA frames and its PING's ack
	 * (the listener {@link #attach} sets).
	 */
	private static final class Peer extends Http2FrameAdapter implements Http2FrameListener {

		final List<Long> resets = new CopyOnWriteArrayList<>();

		final CompletableFuture<Long> goAway = new CompletableFuture<>();

		final CompletableFuture<Long> pingAck = new CompletableFuture<>();

		final AtomicInteger dataFrames = new AtomicInteger();

		Hop.Builder attach(Hop.Builder builder) {
			return builder.listener(hop -> this);
		}

		Http2FrameLogger logger() {
			return new Http2FrameLogger(LogLevel.TRACE) {

				@Override
				public void logRstStream(Direction direction, ChannelHandlerContext ctx, int streamId, long errorCode) {
					if (direction == Direction.INBOUND) {
						Peer.this.resets.add(errorCode);
					}
				}

				@Override
				public void logGoAway(Direction direction, ChannelHandlerContext ctx, int lastStreamId, long errorCode,
						ByteBuf debugData) {
					if (direction == Direction.INBOUND) {
						Peer.this.goAway.complete(errorCode);
					}
				}

			};
		}

		/**
		 * Waits until {@code count} resets arrived at the endpoint, unless a GOAWAY came
		 * first or its connection closed.
		 */
		void awaitResets(int count, Endpoint endpoint) throws InterruptedException {
			awaitTrue(() -> this.resets.size() >= count || this.goAway.isDone() || !endpoint.hop.open(),
					() -> "resets=" + this.resets.size() + " goAway=" + this.goAway.getNow(null));
			assertThat(this.goAway.getNow(null)).as("GOAWAY after %d resets", this.resets.size()).isNull();
			assertThat(endpoint.hop.open()).as("open after %d resets", this.resets.size()).isTrue();
		}

		/** The endpoint's PING is answered: its peer read everything sent before it. */
		void assertPingAnswered(Endpoint endpoint) throws Exception {
			endpoint.loop.run(() -> endpoint.hop.encoder()
				.writePing(endpoint.hop.ctx(), false, 42, endpoint.hop.ctx().newPromise()));
			assertThat(this.pingAck.get(10, TimeUnit.SECONDS)).isEqualTo(42);
		}

		@Override
		public int onDataRead(ChannelHandlerContext ctx, int streamId, ByteBuf data, int padding, boolean endOfStream) {
			this.dataFrames.incrementAndGet();
			return data.readableBytes() + padding;
		}

		@Override
		public void onPingAckRead(ChannelHandlerContext ctx, long data) {
			this.pingAck.complete(data);
		}

	}

}
