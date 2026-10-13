package am.ik.sluice.server.proxy.hop;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.Http2ConnectionAdapter;
import io.netty.handler.codec.http2.Http2Error;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2Stream;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;

import static am.ik.sluice.server.proxy.hop.Fixture.awaitTrue;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Once a hop's channel closed, what fails writing is no error of the peer: netty fails
 * the DATA it held of a stream as the stream closes, and a write of the bridging code
 * fails on the closed channel, yet the hop resets no stream and sends no GOAWAY -- none
 * could be written -- so neither looks like a protocol error to the bridging code. On an
 * open channel an outbound error is answered as netty does.
 */
@Tag("netty-hop")
@ExtendWith(LeakCheck.class)
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class HopClosedChannelTest {

	private static final int GOAWAY = 0x7;

	private static final int SETTINGS_MAX_HEADER_LIST_SIZE = 0x6;

	@Test
	void dataHeldWhenThePeerGoesAwayClosesItsStreamUnreset() throws Exception {
		Socket[] sockets = Endpoint.socketPair();
		Recorder recorder = new Recorder();
		try (Socket peer = sockets[1]; Endpoint a = Endpoint.over("a", sockets[0], builder -> builder)) {
			a.loop.run(() -> a.hop.addConnectionListener(recorder));
			peer.getOutputStream().write(RawFrames.initialWindowSize(0));
			awaitTrue(() -> a.loop.call(() -> a.hop.connection().remote().flowController().initialWindowSize() == 0),
					() -> "the peer's window is not applied");
			a.loop.run(() -> {
				a.hop.encoder().writeHeaders(a.hop.ctx(), 1, request(), 0, false, a.hop.ctx().newPromise());
				a.hop.encoder().writeData(a.hop.ctx(), 1, data(1024), 0, false, a.hop.ctx().newPromise());
			});
			assertThat(
					a.loop.call(() -> a.hop.encoder().flowController().hasFlowControlled(a.hop.connection().stream(1))))
				.as("netty holds the DATA")
				.isTrue();

			peer.close();

			awaitTrue(() -> !a.hop.open() && recorder.closed.size() == 1, () -> "closed: " + recorder.closed);
			assertThat(recorder.closed).as("stream 1, reset sent").containsExactly(new Closed(1, false));
			assertThat(recorder.goAwaysSent).as("GOAWAY codes sent").isEmpty();
			assertThat(a.sink.failures()).isEmpty();
		}
	}

	@Test
	void aWriteOnTheClosedChannelSendsNoGoAway() throws Exception {
		Socket[] sockets = Endpoint.socketPair();
		Recorder recorder = new Recorder();
		AtomicReference<@Nullable Hop> self = new AtomicReference<>();
		AtomicReference<@Nullable ChannelPromise> written = new AtomicReference<>();
		try (Socket peer = sockets[1]; Endpoint a = Endpoint.over("a", sockets[0], builder -> builder.onInactive(() -> {
			// before netty closes the streams: stream 1 is still open
			Hop hop = self.get();
			if (hop != null) {
				ChannelPromise promise = hop.ctx().newPromise();
				written.set(promise);
				hop.encoder().writeHeaders(hop.ctx(), 1, new DefaultHttp2Headers(), 0, true, promise);
			}
		}))) {
			self.set(a.hop);
			a.loop.run(() -> {
				a.hop.addConnectionListener(recorder);
				a.hop.encoder().writeHeaders(a.hop.ctx(), 1, request(), 0, false, a.hop.ctx().newPromise());
			});

			peer.close();

			awaitTrue(() -> !a.hop.open() && recorder.closed.size() == 1, () -> "closed: " + recorder.closed);
			ChannelPromise promise = written.get();
			assertThat(promise).as("the write on the closed channel").isNotNull();
			assertThat(promise.isDone() && !promise.isSuccess()).as("failed").isTrue();
			assertThat(recorder.goAwaysSent).as("GOAWAY codes sent").isEmpty();
			assertThat(recorder.closed).as("stream 1, reset sent").containsExactly(new Closed(1, false));
			assertThat(a.sink.failures()).isEmpty();
		}
	}

	/**
	 * On an open channel netty answers an outbound stream error as ever: a head past the
	 * peer's header list limit resets its stream.
	 */
	@Test
	void anOutboundStreamErrorOnAnOpenChannelResetsItsStream() throws Exception {
		Socket[] sockets = Endpoint.socketPair();
		Recorder recorder = new Recorder();
		try (Socket peer = sockets[1]; Endpoint a = Endpoint.over("a", sockets[0], builder -> builder)) {
			a.loop.run(() -> a.hop.addConnectionListener(recorder));
			peer.getOutputStream().write(RawFrames.frame(0x4, 0, 0, setting(SETTINGS_MAX_HEADER_LIST_SIZE, 16)));
			awaitTrue(() -> a.loop.call(() -> peerLimitApplied(a.hop)), () -> "the peer's limit is not applied");
			a.loop
				.run(() -> a.hop.encoder().writeHeaders(a.hop.ctx(), 1, request(), 0, false, a.hop.ctx().newPromise()));

			awaitTrue(() -> recorder.closed.size() == 1, () -> "closed: " + recorder.closed);
			assertThat(recorder.closed).as("stream 1, reset sent").containsExactly(new Closed(1, true));
			assertThat(a.hop.open()).isTrue();
		}
	}

	/**
	 * On an open channel netty answers an outbound connection error as ever: a head on a
	 * stream id the endpoint may not open sends the peer a GOAWAY {@code PROTOCOL_ERROR}.
	 */
	@Test
	void anOutboundConnectionErrorOnAnOpenChannelSendsAGoAway() throws Exception {
		Socket[] sockets = Endpoint.socketPair();
		Recorder recorder = new Recorder();
		try (Socket peer = sockets[1]; Endpoint a = Endpoint.over("a", sockets[0], builder -> builder)) {
			a.loop.run(() -> {
				a.hop.addConnectionListener(recorder);
				// even: a server's stream id
				a.hop.encoder().writeHeaders(a.hop.ctx(), 2, request(), 0, false, a.hop.ctx().newPromise());
			});

			peer.setSoTimeout(10_000);
			// past the client connection preface's magic
			assertThat(peer.getInputStream().readNBytes(24)).hasSize(24);
			assertThat(awaitGoAway(peer.getInputStream())).as("the GOAWAY's code")
				.isEqualTo(Http2Error.PROTOCOL_ERROR.code());
			assertThat(recorder.goAwaysSent).containsExactly(Http2Error.PROTOCOL_ERROR.code());
		}
	}

	/** Whether the hop applied the peer's header list limit; with the loop's lock. */
	private static boolean peerLimitApplied(Hop hop) {
		return hop.encoder().configuration().headersConfiguration().maxHeaderListSize() == 16;
	}

	private static byte[] setting(int id, int value) {
		return ByteBuffer.allocate(6).putShort((short) id).putInt(value).array();
	}

	/** Reads frames until a GOAWAY; its error code. */
	private static long awaitGoAway(InputStream in) throws IOException {
		while (true) {
			byte[] header = in.readNBytes(9);
			if (header.length < 9) {
				throw new IOException("EOF before a GOAWAY");
			}
			int length = (header[0] & 0xff) << 16 | (header[1] & 0xff) << 8 | (header[2] & 0xff);
			byte[] payload = in.readNBytes(length);
			if (header[3] == GOAWAY) {
				return ByteBuffer.wrap(payload).getInt(4) & 0xffffffffL;
			}
		}
	}

	private static Http2Headers request() {
		return new DefaultHttp2Headers().method("POST").scheme("http").path("/").authority("example.test");
	}

	private static ByteBuf data(int bytes) {
		return Unpooled.wrappedBuffer(new byte[bytes]);
	}

	/** A stream closed, and whether a reset of it was sent. */
	private record Closed(int streamId, boolean resetSent) {
	}

	/** Records the streams closed and the GOAWAYs sent; with the loop's lock. */
	private static final class Recorder extends Http2ConnectionAdapter {

		final List<Closed> closed = new CopyOnWriteArrayList<>();

		final List<Long> goAwaysSent = new CopyOnWriteArrayList<>();

		@Override
		public void onStreamClosed(Http2Stream stream) {
			this.closed.add(new Closed(stream.id(), stream.isResetSent()));
		}

		@Override
		public void onGoAwaySent(int lastStreamId, long errorCode, ByteBuf debugData) {
			this.goAwaysSent.add(errorCode);
		}

	}

}
