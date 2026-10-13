package am.ik.sluice.server.proxy.hop;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.IntPredicate;

import am.ik.sluice.tunnel.DuplexPipe;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.Http2Error;
import io.netty.handler.codec.http2.Http2FrameAdapter;
import io.netty.handler.codec.http2.Http2FrameListener;
import io.netty.handler.codec.http2.Http2Headers;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;

import static am.ik.sluice.server.proxy.hop.Fixture.awaitTrue;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A reset the bridging code writes on the hop being read, during that read, is followed
 * by no DATA of its stream (RFC 9113 5.1), whatever in the read makes DATA of the stream
 * writable: a WINDOW_UPDATE ahead of the frame the bridge resets on, a SETTINGS raising
 * the initial window behind it, or DATA the bridge wrote in the same call. The peer is
 * scripted: each chunk it sends is one read of the hop, so what one read holds is fixed.
 * A second stream, granted alike and not reset, shows the read did make DATA writable.
 */
@Tag("netty-hop")
@ExtendWith(LeakCheck.class)
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class HopResetOrderTest {

	private static final int DATA = 0x0;

	private static final int HEADERS = 0x1;

	private static final int RST_STREAM = 0x3;

	private static final int SETTINGS = 0x4;

	private static final int PING = 0x6;

	private static final int FLAG_END_STREAM = 0x1;

	private static final int FLAG_ACK = 0x1;

	private static final int FLAG_END_HEADERS = 0x4;

	private static final int BODY = 100;

	/**
	 * {@code :method GET}, {@code :scheme http}, {@code :path /} from the static table.
	 */
	private static final byte[] REQUEST_BLOCK = { (byte) 0x82, (byte) 0x86, (byte) 0x84 };

	/** {@code x-t: 1}, a literal without indexing. */
	private static final byte[] TRAILERS_BLOCK = { 0x00, 0x03, 'x', '-', 't', 0x01, '1' };

	/** {@code :status 103}, a literal without indexing of an indexed name. */
	private static final byte[] INTERIM_BLOCK = { 0x08, 0x03, '1', '0', '3' };

	/**
	 * Server role: the peer's grant of both streams' windows and stream 1's trailers in
	 * one read, the bridge resetting stream 1 on its trailers.
	 */
	@Test
	void aGrantReadWithTheResetReleasesNoDataOfItsStreamBehindIt() throws Exception {
		try (Scripted scripted = Scripted.server(new AnsweringBridge(streamId -> false))) {
			scripted.peer.send(concat(RawFrames.clientPreface(), RawFrames.initialWindowSize(0), request(1, false),
					request(3, false)));
			scripted.sync();
			assertThat(scripted.peer.types(1)).as("stream 1 before the read: its DATA held").containsExactly(HEADERS);

			scripted.peer.send(concat(RawFrames.windowUpdate(1, BODY), RawFrames.windowUpdate(3, BODY),
					RawFrames.frame(HEADERS, FLAG_END_HEADERS | FLAG_END_STREAM, 1, TRAILERS_BLOCK)));
			scripted.sync();

			scripted.assertResetWithoutDataBehind();
		}
	}

	/**
	 * Server role: stream 1's trailers, then a SETTINGS raising the initial window, in
	 * one read -- netty's flow controller writes the DATA the raise releases at once,
	 * inside the read, not at the read's flush.
	 */
	@Test
	void aSettingsRaiseReadAfterTheResetReleasesNoDataOfItsStreamBehindIt() throws Exception {
		try (Scripted scripted = Scripted.server(new AnsweringBridge(streamId -> false))) {
			scripted.peer.send(concat(RawFrames.clientPreface(), RawFrames.initialWindowSize(0), request(1, false),
					request(3, false)));
			scripted.sync();
			assertThat(scripted.peer.types(1)).as("stream 1 before the read: its DATA held").containsExactly(HEADERS);

			scripted.peer.send(concat(RawFrames.frame(HEADERS, FLAG_END_HEADERS | FLAG_END_STREAM, 1, TRAILERS_BLOCK),
					RawFrames.initialWindowSize(65535)));
			scripted.sync();

			scripted.assertResetWithoutDataBehind();
		}
	}

	/**
	 * Server role, the peer's window open: the bridge answers stream 1 with a head and
	 * DATA and resets it in the same call -- the DATA waits in netty's flow controller
	 * until the read's flush.
	 */
	@Test
	void dataWrittenInTheCallThatResetsItsStreamDoesNotFollowTheReset() throws Exception {
		try (Scripted scripted = Scripted.server(new AnsweringBridge(streamId -> streamId == 1))) {
			scripted.peer.send(concat(RawFrames.clientPreface(), request(1, false), request(3, false)));
			scripted.sync();

			scripted.assertResetWithoutDataBehind();
		}
	}

	/**
	 * Client role (a leg): the peer's grant of both streams' windows and an interim head
	 * on stream 1 in one read, the bridge resetting stream 1 on the interim head.
	 */
	@Test
	void aClientHopsGrantReadWithTheResetReleasesNoDataOfItsStreamBehindIt() throws Exception {
		try (Scripted scripted = Scripted.client(new ResettingOnInterim())) {
			scripted.peer.send(concat(RawFrames.frame(SETTINGS, 0, 0, new byte[0]), RawFrames.initialWindowSize(0)));
			scripted.sync();
			scripted.loop.run(() -> {
				for (int streamId = 1; streamId <= 3; streamId += 2) {
					Hop hop = scripted.hop;
					hop.encoder()
						.writeHeaders(hop.ctx(), streamId,
								new DefaultHttp2Headers().method("POST").scheme("http").path("/"), 0, false,
								hop.ctx().newPromise());
					hop.encoder()
						.writeData(hop.ctx(), streamId, Unpooled.wrappedBuffer(new byte[BODY]), 0, false,
								hop.ctx().newPromise());
				}
			});
			scripted.sync();
			assertThat(scripted.peer.types(1)).as("stream 1 before the read: its DATA held").containsExactly(HEADERS);

			scripted.peer.send(concat(RawFrames.windowUpdate(1, BODY), RawFrames.windowUpdate(3, BODY),
					RawFrames.frame(HEADERS, FLAG_END_HEADERS, 1, INTERIM_BLOCK)));
			scripted.sync();

			scripted.assertResetWithoutDataBehind();
		}
	}

	/**
	 * Server role: a reset written by the listener of a DATA write the running flush
	 * completes -- stream 3's, once a grant let it out -- is written by that flush and
	 * closes its stream there: a later SETTINGS raising the initial window releases no
	 * DATA of stream 1 behind its reset.
	 */
	@Test
	void aResetWrittenWhileTheChannelFlushesReleasesNoDataOfItsStreamBehindIt() throws Exception {
		AnsweringBridge bridge = new AnsweringBridge(streamId -> false).resettingWhenWritten(3, 1);
		try (Scripted scripted = Scripted.server(bridge)) {
			scripted.peer.send(concat(RawFrames.clientPreface(), RawFrames.initialWindowSize(0), request(1, false),
					request(3, false)));
			scripted.sync();
			assertThat(scripted.peer.types(1)).as("stream 1 before the grant: its DATA held").containsExactly(HEADERS);

			scripted.peer.send(RawFrames.windowUpdate(3, BODY));
			scripted.sync();
			assertThat(bridge.resetsDoneAtReturn).as("the reset written inside the running flush")
				.containsExactly(false);
			scripted.peer.send(RawFrames.initialWindowSize(65535));
			scripted.sync();

			scripted.assertResetWithoutDataBehind();
		}
	}

	private static byte[] request(int streamId, boolean endStream) {
		return RawFrames.frame(HEADERS, FLAG_END_HEADERS | (endStream ? FLAG_END_STREAM : 0), streamId, REQUEST_BLOCK);
	}

	private static byte[] concat(byte[]... parts) {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		for (byte[] part : parts) {
			out.writeBytes(part);
		}
		return out.toByteArray();
	}

	/**
	 * Bridging code answering each request with a head and {@link #BODY} bytes of DATA,
	 * and resetting a stream ({@code ENHANCE_YOUR_CALM}) on its trailers -- or in the
	 * same call as the answer, for the streams {@code resetsAtOnce} names.
	 */
	private static final class AnsweringBridge extends Http2FrameAdapter {

		private final IntPredicate resetsAtOnce;

		private @Nullable Hop hop;

		/** The stream whose DATA written resets {@link #resetWhenWritten}; 0 = none. */
		private int writtenResets;

		private int resetWhenWritten;

		/** Whether each such reset's write was complete when the reset call returned. */
		final List<Boolean> resetsDoneAtReturn = new CopyOnWriteArrayList<>();

		AnsweringBridge(IntPredicate resetsAtOnce) {
			this.resetsAtOnce = resetsAtOnce;
		}

		/**
		 * Resets stream {@code reset} from the listener of stream {@code written}'s DATA.
		 */
		AnsweringBridge resettingWhenWritten(int written, int reset) {
			this.writtenResets = written;
			this.resetWhenWritten = reset;
			return this;
		}

		AnsweringBridge on(Hop hop) {
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
			Hop hop = Objects.requireNonNull(this.hop);
			if (headers.method() == null) {
				reset(hop, streamId);
				return;
			}
			hop.encoder()
				.writeHeaders(hop.ctx(), streamId, new DefaultHttp2Headers().status("200"), 0, false,
						hop.ctx().newPromise());
			ChannelPromise written = (streamId != this.writtenResets) ? hop.ctx().newPromise()
					: hop.newPromise(future -> this.resetsDoneAtReturn.add(reset(hop, this.resetWhenWritten).isDone()));
			hop.encoder().writeData(hop.ctx(), streamId, Unpooled.wrappedBuffer(new byte[BODY]), 0, false, written);
			if (this.resetsAtOnce.test(streamId)) {
				reset(hop, streamId);
			}
		}

	}

	/**
	 * Bridging code resetting a stream ({@code ENHANCE_YOUR_CALM}) on an interim head.
	 */
	private static final class ResettingOnInterim extends Http2FrameAdapter {

		private @Nullable Hop hop;

		ResettingOnInterim on(Hop hop) {
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
			CharSequence status = headers.status();
			if (status != null && status.charAt(0) == '1') {
				reset(Objects.requireNonNull(this.hop), streamId);
			}
		}

	}

	private static ChannelFuture reset(Hop hop, int streamId) {
		return hop.encoder()
			.writeRstStream(hop.ctx(), streamId, Http2Error.ENHANCE_YOUR_CALM.code(), hop.ctx().newPromise());
	}

	/** One hop on a loop of its own, driven by a scripted peer. */
	private static final class Scripted implements AutoCloseable {

		final RecordingSink sink = new RecordingSink();

		final Loop loop = new Loop("scripted", this.sink);

		final ScriptedPeer peer = new ScriptedPeer();

		final Hop hop;

		private final AtomicLong pings = new AtomicLong();

		private Scripted(boolean server, Function<Hop, Http2FrameListener> listener) {
			this.hop = this.loop.call(() -> Hop.builder()
				.name(server ? "server" : "client")
				.loop(this.loop)
				.pipe(this.peer)
				.server(server)
				.listener(listener)
				.build());
		}

		static Scripted server(AnsweringBridge bridge) {
			return new Scripted(true, bridge::on);
		}

		static Scripted client(ResettingOnInterim bridge) {
			return new Scripted(false, bridge::on);
		}

		/**
		 * Sends a PING as a read of its own and waits for its ack: what the hop wrote for
		 * every read before it is out.
		 */
		void sync() throws InterruptedException {
			long data = this.pings.incrementAndGet();
			this.peer.send(RawFrames.ping(data));
			awaitTrue(
					() -> this.peer.frames()
						.stream()
						.anyMatch(frame -> frame.type() == PING && (frame.flags() & FLAG_ACK) != 0
								&& ByteBuffer.wrap(frame.payload()).getLong() == data),
					() -> "no ack of PING " + data + ": " + this.peer.frames() + " " + this.sink.failures());
		}

		/**
		 * Stream 1 got its reset and no DATA, before it or after; stream 3, granted
		 * alike, its DATA.
		 */
		void assertResetWithoutDataBehind() {
			assertThat(this.peer.types(1)).as("stream 1's frames").containsExactly(HEADERS, RST_STREAM);
			assertThat(this.peer.types(3)).as("stream 3's frames: the read made DATA writable")
				.containsExactly(HEADERS, DATA);
			assertThat(this.hop.open()).isTrue();
			assertThat(this.sink.failures()).isEmpty();
		}

		@Override
		public void close() {
			this.loop.close();
			Fixture.awaitDone(List.of(this.hop));
		}

	}

	record Frame(int type, int flags, int streamId, byte[] payload) {

		@Override
		public String toString() {
			return "type=" + this.type + " flags=" + this.flags + " stream=" + this.streamId;
		}

	}

	/**
	 * The peer's end of a hop's pipe: each chunk sent is what one read of the hop returns
	 * (the hop reads at most {@link Hop#MAX_READ_BYTES} at once, more than any chunk
	 * here); what the hop writes is kept and parsed into frames. The hop's half-close
	 * ends the peer's sending too.
	 */
	private static final class ScriptedPeer implements DuplexPipe {

		private static final byte[] EOF = new byte[0];

		private static final byte[] CLIENT_PREFACE = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
			.getBytes(StandardCharsets.US_ASCII);

		private final BlockingQueue<byte[]> chunks = new LinkedBlockingQueue<>();

		private final ByteArrayOutputStream written = new ByteArrayOutputStream();

		private final InputStream source = new InputStream() {

			private boolean ended;

			@Override
			public int read() throws IOException {
				throw new UnsupportedOperationException("reads come in chunks");
			}

			@Override
			public int read(byte[] b, int off, int len) throws IOException {
				if (this.ended) {
					return -1;
				}
				byte[] chunk;
				try {
					chunk = ScriptedPeer.this.chunks.take();
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					throw new InterruptedIOException();
				}
				if (chunk == EOF) {
					this.ended = true;
					return -1;
				}
				if (chunk.length > len) {
					throw new IllegalStateException("a chunk larger than one read: " + chunk.length);
				}
				System.arraycopy(chunk, 0, b, off, chunk.length);
				return chunk.length;
			}

		};

		private final OutputStream sink = new OutputStream() {

			@Override
			public void write(int b) {
				write(new byte[] { (byte) b }, 0, 1);
			}

			@Override
			public void write(byte[] b, int off, int len) {
				synchronized (ScriptedPeer.this.written) {
					ScriptedPeer.this.written.write(b, off, len);
				}
			}

		};

		void send(byte[] chunk) {
			this.chunks.add(chunk);
		}

		/** The frames the hop wrote so far, a client's preface skipped. */
		List<Frame> frames() {
			byte[] bytes;
			synchronized (this.written) {
				bytes = this.written.toByteArray();
			}
			int offset = 0;
			if (bytes.length >= CLIENT_PREFACE.length
					&& Arrays.equals(bytes, 0, CLIENT_PREFACE.length, CLIENT_PREFACE, 0, CLIENT_PREFACE.length)) {
				offset = CLIENT_PREFACE.length;
			}
			List<Frame> frames = new ArrayList<>();
			while (bytes.length - offset >= 9) {
				int length = (bytes[offset] & 0xff) << 16 | (bytes[offset + 1] & 0xff) << 8
						| (bytes[offset + 2] & 0xff);
				if (bytes.length - offset - 9 < length) {
					break;
				}
				int streamId = ByteBuffer.wrap(bytes, offset + 5, 4).getInt() & 0x7fffffff;
				frames.add(new Frame(bytes[offset + 3] & 0xff, bytes[offset + 4] & 0xff, streamId,
						Arrays.copyOfRange(bytes, offset + 9, offset + 9 + length)));
				offset += 9 + length;
			}
			return frames;
		}

		/** The types of the frames the hop wrote on the stream, in order. */
		List<Integer> types(int streamId) {
			return frames().stream().filter(frame -> frame.streamId() == streamId).map(Frame::type).toList();
		}

		@Override
		public InputStream source() {
			return this.source;
		}

		@Override
		public OutputStream sink() {
			return this.sink;
		}

		@Override
		public void shutdownOutput() {
			this.chunks.add(EOF);
		}

		@Override
		public void close() {
			this.chunks.add(EOF);
		}

	}

}
