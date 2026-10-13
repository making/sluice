package am.ik.sluice.server.proxy.hop;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;

import static am.ik.sluice.server.proxy.hop.Fixture.awaitTrue;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The writer writes in pieces of at most the write piece bytes, a large write sliced and
 * the small writes queued meanwhile coalesced, each piece reported as progress once the
 * pipe took it, the bytes intact and in order.
 */
@Tag("netty-hop")
@ExtendWith(LeakCheck.class)
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class HopWritePieceTest {

	private static final int PIECE = 1024;

	private static final int PINGS = 1000;

	/** Past what the socket buffers on the way take: the writer blocks on it. */
	private static final int GOAWAY_DEBUG = 1024 * 1024;

	private static final int PING = 0x6;

	private static final int GOAWAY = 0x7;

	@Test
	void aLargeWriteIsSlicedAndTheSmallWritesQueuedMeanwhileCoalescedIntoReportedPieces() throws Exception {
		Socket[] sockets = Endpoint.socketPair();
		List<Long> pieces = new CopyOnWriteArrayList<>();
		try (Socket peer = sockets[1];
				Endpoint a = Endpoint.over("a", sockets[0],
						builder -> builder.server(true).writePiece(PIECE).progress(pieces::add))) {
			peer.getOutputStream().write(RawFrames.clientPreface());
			byte[] debug = new byte[GOAWAY_DEBUG];
			for (int i = 0; i < debug.length; i++) {
				debug[i] = (byte) i;
			}
			a.loop.run(() -> a.hop.encoder()
				.frameWriter()
				.writeGoAway(a.hop.ctx(), 0, 0, Unpooled.wrappedBuffer(debug), a.hop.ctx().newPromise()));
			// the peer reads nothing yet: the writer blocks inside the GOAWAY
			Thread.sleep(Duration.ofMillis(200));
			assertThat(a.hop.queuedBytes()).as("the GOAWAY not written whole").isPositive();
			a.loop.run(() -> {
				for (int i = 0; i < PINGS; i++) {
					a.hop.encoder().writePing(a.hop.ctx(), false, i, a.hop.ctx().newPromise());
				}
			});

			InputStream in = peer.getInputStream();
			assertThat(readGoAway(in)).as("the GOAWAY's debug data intact").isEqualTo(debug);
			assertThat(readPings(in)).as("the PINGs in order").hasSize(PINGS).isSorted();

			awaitTrue(() -> sum(pieces) == a.hop.bytesWritten(),
					() -> "pieces " + sum(pieces) + " written " + a.hop.bytesWritten());
			assertThat(pieces).as("pieces of at most the write piece").allMatch(piece -> piece > 0 && piece <= PIECE);
			assertThat(pieces).as("the GOAWAY sliced, the PINGs queued behind it coalesced")
				.hasSizeLessThanOrEqualTo((GOAWAY_DEBUG + PINGS * 17) / PIECE + 10);
		}
	}

	private static long sum(List<Long> pieces) {
		return pieces.stream().mapToLong(Long::longValue).sum();
	}

	/** Reads frames until a GOAWAY; its debug data. */
	private static byte[] readGoAway(InputStream in) throws IOException {
		while (true) {
			byte[][] frame = readFrame(in);
			if (frame[0][3] == GOAWAY) {
				byte[] debug = new byte[frame[1].length - 8];
				System.arraycopy(frame[1], 8, debug, 0, debug.length);
				return debug;
			}
		}
	}

	/** Reads {@link #PINGS} PINGs; their data. */
	private static List<Long> readPings(InputStream in) throws IOException {
		List<Long> pings = new ArrayList<>();
		while (pings.size() < PINGS) {
			byte[][] frame = readFrame(in);
			if (frame[0][3] == PING && (frame[0][4] & 0x1) == 0) {
				pings.add(ByteBuffer.wrap(frame[1]).getLong());
			}
		}
		return pings;
	}

	/** One frame: its header and its payload. */
	private static byte[][] readFrame(InputStream in) throws IOException {
		byte[] header = in.readNBytes(9);
		if (header.length < 9) {
			throw new IOException("EOF");
		}
		int length = (header[0] & 0xff) << 16 | (header[1] & 0xff) << 8 | (header[2] & 0xff);
		return new byte[][] { header, in.readNBytes(length) };
	}

}
