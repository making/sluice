package am.ik.sluice.server.proxy.hop;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntPredicate;
import java.util.function.UnaryOperator;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.DefaultHttp2HeadersEncoder;
import io.netty.handler.codec.http2.Http2CodecUtil;
import io.netty.handler.codec.http2.Http2Error;
import io.netty.handler.codec.http2.Http2FrameAdapter;
import io.netty.handler.codec.http2.Http2Headers;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;

import static am.ik.sluice.server.proxy.hop.Fixture.awaitTrue;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Frames a peer had in flight when the bridging code reset its stream are ignored (RFC
 * 9113 5.1): neither answered with RST_STREAM {@code STREAM_CLOSED} nor counted towards
 * the limit on the peer's stream errors -- up to the stream window the stream had left at
 * the reset, one header list of header blocks, and until a frame ends the stream, for the
 * last streams reset. Frames past that, and frames on streams netty's codec reset, the
 * peer reset or the peer had ended, are answered and counted as before. The peer is
 * scripted and sends its frames once it read the reset: frames that were in flight, as
 * the hop sees them.
 */
@Tag("netty-hop")
@ExtendWith(LeakCheck.class)
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class HopFramesAfterResetTest {

	private static final int DATA = 0x0;

	private static final int HEADERS = 0x1;

	private static final int RST_STREAM = 0x3;

	private static final int PING = 0x6;

	private static final int GOAWAY = 0x7;

	private static final int WINDOW_UPDATE = 0x8;

	private static final int FLAG_END_STREAM = 0x1;

	private static final int FLAG_ACK = 0x1;

	private static final int FLAG_END_HEADERS = 0x4;

	/** The limit on the peer's stream errors of the server hops here. */
	private static final int MAX_ERRORS = 3;

	/** The most one read of the hop takes, so the most one chunk of the peer holds. */
	private static final int CHUNK = Hop.MAX_READ_BYTES;

	/**
	 * {@code :method GET}, {@code :scheme http}, {@code :path /} from the static table.
	 */
	private static final byte[] REQUEST_BLOCK = { (byte) 0x82, (byte) 0x86, (byte) 0x84 };

	/** {@code x-t: 1}, a literal without indexing. */
	private static final byte[] TRAILERS_BLOCK = { 0x00, 0x03, 'x', '-', 't', 0x01, '1' };

	/** {@code :status 200} from the static table. */
	private static final byte[] RESPONSE_BLOCK = { (byte) 0x88 };

	/**
	 * More streams than the limit allows errors, each reset by the bridge on its request
	 * head while the peer still sends its body: the body's DATA and the trailers the peer
	 * sends after the reset are ignored, and the connection goes on.
	 */
	@Test
	void framesInFlightOnStreamsTheBridgeResetAreIgnored() throws Exception {
		int streams = MAX_ERRORS + 2;
		try (Scripted scripted = Scripted.server(new ResettingBridge(streamId -> true, streamId -> false))) {
			ByteArrayOutputStream requests = new ByteArrayOutputStream();
			for (int i = 0; i < streams; i++) {
				requests.writeBytes(request(1 + 2 * i, false));
			}
			scripted.peer.send(requests.toByteArray());
			scripted.sync();
			for (int i = 0; i < streams; i++) {
				assertThat(scripted.resets(1 + 2 * i)).as("the bridge's reset of stream %d, before its body", 1 + 2 * i)
					.containsExactly(Http2Error.REFUSED_STREAM.code());
			}

			int dataFrames = 3;
			int body = 1000;
			for (int i = 0; i < streams; i++) {
				int streamId = 1 + 2 * i;
				ByteArrayOutputStream inFlight = new ByteArrayOutputStream();
				for (int frame = 0; frame < dataFrames; frame++) {
					inFlight.writeBytes(RawFrames.frame(DATA, 0, streamId, new byte[body]));
				}
				inFlight
					.writeBytes(RawFrames.frame(HEADERS, FLAG_END_HEADERS | FLAG_END_STREAM, streamId, TRAILERS_BLOCK));
				scripted.peer.send(inFlight.toByteArray());
			}
			scripted.sync();

			assertThat(streams * (dataFrames + 1)).as("the frames ignored, past the limit").isGreaterThan(MAX_ERRORS);
			for (int i = 0; i < streams; i++) {
				assertThat(scripted.peer.types(1 + 2 * i)).as("stream %d's frames", 1 + 2 * i)
					.containsExactly(RST_STREAM);
			}
			scripted.assertOpen();
		}
	}

	/**
	 * The bridge resets two streams on their first DATA, each stream window then 1000
	 * bytes short of the initial window: the peer's DATA up to the window left is
	 * ignored, and at a window left of 0 an empty DATA ending the stream still is. An
	 * empty one not ending it, and each byte past the window, is answered with RST_STREAM
	 * {@code STREAM_CLOSED} and counted; the frame past the limit ends the connection
	 * with GOAWAY {@code ENHANCE_YOUR_CALM}. The connection window refills for the DATA
	 * ignored: the peer may go on sending.
	 */
	@Test
	void dataPastTheStreamWindowLeftAtTheResetIsAnsweredAndCounted() throws Exception {
		int first = 1000;
		int windowLeft = Http2CodecUtil.DEFAULT_WINDOW_SIZE - first;
		try (Scripted scripted = Scripted.server(new ResettingBridge(streamId -> false, streamId -> true))) {
			scripted.peer.send(concat(request(1, false), RawFrames.frame(DATA, 0, 1, new byte[first]),
					request(3, false), RawFrames.frame(DATA, 0, 3, new byte[first])));
			scripted.sync();
			assertThat(scripted.resets(1)).as("the bridge's reset").containsExactly(Http2Error.REFUSED_STREAM.code());
			assertThat(scripted.resets(3)).as("the bridge's reset").containsExactly(Http2Error.REFUSED_STREAM.code());

			int piece = CHUNK - 9;
			for (int streamId : new int[] { 1, 3 }) {
				for (int left = windowLeft; left > 0; left -= piece) {
					scripted.peer.send(RawFrames.frame(DATA, 0, streamId, new byte[Math.min(piece, left)]));
				}
			}
			scripted.peer.send(RawFrames.frame(DATA, FLAG_END_STREAM, 3, new byte[0]));
			scripted.sync();
			assertThat(scripted.resets(1)).as("the stream window left ignored")
				.containsExactly(Http2Error.REFUSED_STREAM.code());
			assertThat(scripted.resets(3)).as("the stream window left and an empty end ignored")
				.containsExactly(Http2Error.REFUSED_STREAM.code());
			long sent = 2L * Http2CodecUtil.DEFAULT_WINDOW_SIZE;
			assertThat(scripted.connectionGranted()).as("the connection window granted back")
				.isGreaterThanOrEqualTo(sent - Http2CodecUtil.DEFAULT_WINDOW_SIZE / 2);

			scripted.peer.send(RawFrames.frame(DATA, 0, 1, new byte[0]));
			for (int i = 1; i < MAX_ERRORS; i++) {
				scripted.peer.send(RawFrames.frame(DATA, 0, 1, new byte[1]));
			}
			scripted.sync();
			assertThat(scripted.resets(1)).as("an empty frame and bytes past the window, %d frames", MAX_ERRORS)
				.containsExactly(Http2Error.REFUSED_STREAM.code(), Http2Error.STREAM_CLOSED.code(),
						Http2Error.STREAM_CLOSED.code(), Http2Error.STREAM_CLOSED.code());
			scripted.assertOpen();

			scripted.peer.send(RawFrames.frame(DATA, 0, 1, new byte[1]));
			scripted.assertGoAway(Http2Error.ENHANCE_YOUR_CALM);
		}
	}

	/**
	 * A frame ending the stream ends the grace: DATA after it is answered. And header
	 * blocks are ignored up to one header list (netty's default limit here): the second
	 * block of more than half of it is answered.
	 */
	@Test
	void framesPastTheEndOfTheStreamOrOneHeaderListAreAnswered() throws Exception {
		byte[] halfList = largeBlock((int) Http2CodecUtil.DEFAULT_HEADER_LIST_SIZE / 2 + 100);
		try (Scripted scripted = Scripted.server(new ResettingBridge(streamId -> true, streamId -> false))) {
			scripted.peer.send(concat(request(1, false), request(3, false)));
			scripted.sync();

			scripted.peer.send(RawFrames.frame(DATA, FLAG_END_STREAM, 1, new byte[10]));
			scripted.peer.send(RawFrames.frame(HEADERS, FLAG_END_HEADERS, 3, halfList));
			scripted.sync();
			assertThat(scripted.resets(1)).as("DATA ending the stream ignored")
				.containsExactly(Http2Error.REFUSED_STREAM.code());
			assertThat(scripted.resets(3)).as("a block within one header list ignored")
				.containsExactly(Http2Error.REFUSED_STREAM.code());

			scripted.peer.send(RawFrames.frame(DATA, 0, 1, new byte[10]));
			scripted.peer.send(RawFrames.frame(HEADERS, FLAG_END_HEADERS, 3, halfList));
			scripted.sync();
			assertThat(scripted.resets(1)).as("DATA after the end")
				.containsExactly(Http2Error.REFUSED_STREAM.code(), Http2Error.STREAM_CLOSED.code());
			assertThat(scripted.resets(3)).as("the block past one header list")
				.containsExactly(Http2Error.REFUSED_STREAM.code(), Http2Error.STREAM_CLOSED.code());
			scripted.assertOpen();
		}
	}

	/**
	 * Frames no stream reset of the bridge covers are answered and counted: on a stream
	 * netty's codec reset (a stream WINDOW_UPDATE of 0), on one the peer reset, on one
	 * the peer had ended before the bridge reset it. The fourth error ends the
	 * connection.
	 */
	@Test
	void framesAfterResetsNotOfTheBridgeAreAnsweredAndCounted() throws Exception {
		try (Scripted scripted = Scripted.server(new ResettingBridge(streamId -> streamId == 5, streamId -> false))) {
			scripted.peer.send(concat(request(1, false), request(3, false), request(5, true)));
			scripted.peer.send(RawFrames.windowUpdate(1, 0));
			scripted.peer.send(RawFrames.frame(RST_STREAM, 0, 3, ByteBuffer.allocate(4).putInt(0x8).array()));
			scripted.sync();
			assertThat(scripted.resets(1)).as("the codec's reset").containsExactly(Http2Error.PROTOCOL_ERROR.code());
			assertThat(scripted.resets(5)).as("the bridge's reset").containsExactly(Http2Error.REFUSED_STREAM.code());

			scripted.peer.send(RawFrames.frame(DATA, 0, 1, new byte[10]));
			scripted.peer.send(RawFrames.frame(DATA, 0, 3, new byte[10]));
			scripted.sync();
			assertThat(scripted.resets(1)).containsExactly(Http2Error.PROTOCOL_ERROR.code(),
					Http2Error.STREAM_CLOSED.code());
			assertThat(scripted.resets(3)).containsExactly(Http2Error.STREAM_CLOSED.code());
			scripted.assertOpen();

			scripted.peer.send(RawFrames.frame(DATA, 0, 5, new byte[10]));
			scripted.assertGoAway(Http2Error.ENHANCE_YOUR_CALM);
			assertThat(scripted.resets(5)).as("answered first")
				.containsExactly(Http2Error.REFUSED_STREAM.code(), Http2Error.STREAM_CLOSED.code());
		}
	}

	/**
	 * The hop remembers the last streams it reset, as many as it is told: of three
	 * streams reset with room for two, frames on the two last are ignored, those on the
	 * first answered.
	 */
	@Test
	void onlyTheLastStreamsResetAreRemembered() throws Exception {
		try (Scripted scripted = Scripted.server(new ResettingBridge(streamId -> true, streamId -> false),
				builder -> builder.rememberedResets(2))) {
			scripted.peer.send(concat(request(1, false), request(3, false), request(5, false)));
			scripted.sync();

			scripted.peer.send(concat(RawFrames.frame(DATA, 0, 5, new byte[10]),
					RawFrames.frame(DATA, 0, 3, new byte[10]), RawFrames.frame(DATA, 0, 1, new byte[10])));
			scripted.sync();

			assertThat(scripted.resets(5)).containsExactly(Http2Error.REFUSED_STREAM.code());
			assertThat(scripted.resets(3)).containsExactly(Http2Error.REFUSED_STREAM.code());
			assertThat(scripted.resets(1)).as("forgotten")
				.containsExactly(Http2Error.REFUSED_STREAM.code(), Http2Error.STREAM_CLOSED.code());
			scripted.assertOpen();
		}
	}

	/**
	 * A client hop (a leg) resetting its stream ignores the response frames its peer had
	 * in flight: a head, DATA and trailers draw no RST_STREAM {@code STREAM_CLOSED}.
	 */
	@Test
	void aClientHopIgnoresTheResponseInFlightOnAStreamItReset() throws Exception {
		try (Scripted scripted = Scripted.client()) {
			scripted.peer.send(RawFrames.frame(0x4, 0, 0, new byte[0]));
			scripted.loop.run(() -> scripted.hop.encoder()
				.writeHeaders(scripted.hop.ctx(), 1,
						new DefaultHttp2Headers().method("GET").scheme("http").authority("a.test").path("/"), 0, true,
						scripted.hop.ctx().newPromise()));
			scripted.sync();
			scripted.loop.run(() -> scripted.hop.encoder()
				.writeRstStream(scripted.hop.ctx(), 1, Http2Error.CANCEL.code(), scripted.hop.ctx().newPromise()));
			scripted.sync();
			assertThat(scripted.peer.types(1)).containsExactly(HEADERS, RST_STREAM);

			scripted.peer.send(concat(RawFrames.frame(HEADERS, FLAG_END_HEADERS, 1, RESPONSE_BLOCK),
					RawFrames.frame(DATA, 0, 1, new byte[1000]),
					RawFrames.frame(HEADERS, FLAG_END_HEADERS | FLAG_END_STREAM, 1, TRAILERS_BLOCK)));
			scripted.sync();

			assertThat(scripted.peer.types(1)).as("stream 1's frames").containsExactly(HEADERS, RST_STREAM);
			scripted.assertOpen();
		}
	}

	private static byte[] request(int streamId, boolean endStream) {
		return RawFrames.frame(HEADERS, FLAG_END_HEADERS | (endStream ? FLAG_END_STREAM : 0), streamId, REQUEST_BLOCK);
	}

	/** A header block of one never-indexed field whose HPACK size is {@code size}. */
	private static byte[] largeBlock(int size) throws Exception {
		String name = "x-large";
		Http2Headers headers = new DefaultHttp2Headers().add(name, "v".repeat(size - name.length() - 32));
		ByteBuf block = Unpooled.buffer();
		try {
			new DefaultHttp2HeadersEncoder((n, v) -> true).encodeHeaders(1, headers, block);
			return ByteBufUtil.getBytes(block);
		}
		finally {
			block.release();
		}
	}

	private static byte[] concat(byte[]... parts) {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		for (byte[] part : parts) {
			out.writeBytes(part);
		}
		return out.toByteArray();
	}

	/**
	 * Bridging code resetting ({@code REFUSED_STREAM}) the streams {@code onHeaders}
	 * names on their request head, and those {@code onData} names on their first DATA.
	 */
	private static final class ResettingBridge extends Http2FrameAdapter {

		private final IntPredicate onHeaders;

		private final IntPredicate onData;

		private @Nullable Hop hop;

		ResettingBridge(IntPredicate onHeaders, IntPredicate onData) {
			this.onHeaders = onHeaders;
			this.onData = onData;
		}

		ResettingBridge on(Hop hop) {
			this.hop = hop;
			return this;
		}

		@Override
		public void onHeadersRead(ChannelHandlerContext ctx, int streamId, Http2Headers headers, int streamDependency,
				short weight, boolean exclusive, int padding, boolean endStream) {
			onHeadersRead(ctx, streamId, headers, padding, endStream);
		}

		@Override
		public void onHeadersRead(ChannelHandlerContext ctx, int streamId, Http2Headers headers, int padding,
				boolean endStream) {
			if (this.onHeaders.test(streamId)) {
				reset(streamId);
			}
		}

		@Override
		public int onDataRead(ChannelHandlerContext ctx, int streamId, ByteBuf data, int padding, boolean endOfStream) {
			if (this.onData.test(streamId)) {
				reset(streamId);
			}
			return data.readableBytes() + padding;
		}

		private void reset(int streamId) {
			Hop hop = Objects.requireNonNull(this.hop);
			hop.encoder().writeRstStream(hop.ctx(), streamId, Http2Error.REFUSED_STREAM.code(), hop.ctx().newPromise());
		}

	}

	/** One hop on a loop of its own, driven by a scripted peer. */
	private static final class Scripted implements AutoCloseable {

		final RecordingSink sink = new RecordingSink();

		final Loop loop = new Loop("scripted", this.sink);

		final ScriptedPeer peer = new ScriptedPeer();

		final Hop hop;

		private final AtomicLong pings = new AtomicLong();

		private Scripted(UnaryOperator<Hop.Builder> customizer) {
			this.hop = this.loop.call(
					() -> customizer
						.apply(Hop.builder()
							.name("scripted")
							.loop(this.loop)
							.pipe(this.peer)
							.listener(hop -> new Http2FrameAdapter()))
						.build());
		}

		/**
		 * A server hop limiting its peer to {@link #MAX_ERRORS} stream errors, the peer's
		 * preface sent.
		 */
		static Scripted server(ResettingBridge bridge, UnaryOperator<Hop.Builder> customizer) {
			Scripted scripted = new Scripted(builder -> customizer
				.apply(builder.server(true).streamErrorLimit(MAX_ERRORS, Duration.ofMinutes(1)).listener(bridge::on)));
			scripted.peer.send(RawFrames.clientPreface());
			return scripted;
		}

		static Scripted server(ResettingBridge bridge) {
			return server(bridge, UnaryOperator.identity());
		}

		static Scripted client() {
			return new Scripted(UnaryOperator.identity());
		}

		/**
		 * Sends a PING as a read of its own and waits for its ack: the hop read
		 * everything sent before it, and what it wrote for that is out.
		 */
		void sync() throws InterruptedException {
			long data = this.pings.incrementAndGet();
			this.peer.send(RawFrames.ping(data));
			awaitTrue(() -> acked(data) || !this.hop.open(),
					() -> "no ack of PING " + data + ": " + this.peer.frames() + " " + this.sink.failures());
			assertThat(acked(data)).as("PING %d acked: %s", data, this.peer.frames()).isTrue();
		}

		private boolean acked(long data) {
			return this.peer.frames()
				.stream()
				.anyMatch(frame -> frame.type() == PING && (frame.flags() & FLAG_ACK) != 0
						&& ByteBuffer.wrap(frame.payload()).getLong() == data);
		}

		/** The error codes of the resets the hop wrote on the stream, in order. */
		List<Long> resets(int streamId) {
			return this.peer.frames()
				.stream()
				.filter(frame -> frame.type() == RST_STREAM && frame.streamId() == streamId)
				.map(frame -> ByteBuffer.wrap(frame.payload()).getInt() & 0xffffffffL)
				.toList();
		}

		/** The connection window increments the hop granted. */
		long connectionGranted() {
			return this.peer.frames()
				.stream()
				.filter(frame -> frame.type() == WINDOW_UPDATE && frame.streamId() == 0)
				.mapToLong(frame -> ByteBuffer.wrap(frame.payload()).getInt())
				.sum();
		}

		void assertOpen() {
			assertThat(this.peer.frames()).as("GOAWAY").noneMatch(frame -> frame.type() == GOAWAY);
			assertThat(this.hop.open()).isTrue();
			assertThat(this.sink.failures()).isEmpty();
		}

		void assertGoAway(Http2Error error) throws InterruptedException {
			awaitTrue(() -> this.peer.frames().stream().anyMatch(frame -> frame.type() == GOAWAY),
					() -> "no GOAWAY: " + this.peer.frames());
			ScriptedPeer.Frame goAway = this.peer.frames()
				.stream()
				.filter(frame -> frame.type() == GOAWAY)
				.findFirst()
				.orElseThrow();
			assertThat(ByteBuffer.wrap(goAway.payload(), 4, 4).getInt() & 0xffffffffL).isEqualTo(error.code());
			awaitTrue(() -> !this.hop.open(), () -> "still open");
		}

		@Override
		public void close() {
			this.loop.close();
			Fixture.awaitDone(List.of(this.hop));
		}

	}

}
