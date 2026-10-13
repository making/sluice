package am.ik.sluice.server.proxy.hop;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;

import static am.ik.sluice.server.proxy.hop.Fixture.awaitTrue;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The peer's EOF: without an EOF action it closes the channel; with one, the action runs
 * and the channel stays open for writing (a peer that half-closed still reads), until the
 * action or a failure closes it.
 */
@Tag("netty-hop")
@ExtendWith(LeakCheck.class)
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class HopEofTest {

	private static final int PING = 0x6;

	@Test
	void withoutAnEofActionThePeersEofClosesTheChannel() throws Exception {
		Socket[] sockets = Endpoint.socketPair();
		try (Socket peer = sockets[1]; Endpoint a = Endpoint.over("a", sockets[0], builder -> builder.server(true))) {
			peer.getOutputStream().write(RawFrames.clientPreface());
			peer.shutdownOutput();

			awaitTrue(() -> !a.hop.open(), () -> "a is still open");
			assertThat(a.sink.failures()).isEmpty();
		}
	}

	@Test
	void thePeersEofRunsTheEofActionAndLeavesTheChannelOpenForWriting() throws Exception {
		Socket[] sockets = Endpoint.socketPair();
		AtomicInteger eofs = new AtomicInteger();
		try (Socket peer = sockets[1];
				Endpoint a = Endpoint.over("a", sockets[0],
						builder -> builder.server(true).onEof(eofs::incrementAndGet))) {
			peer.getOutputStream().write(RawFrames.clientPreface());
			peer.shutdownOutput();
			awaitTrue(() -> eofs.get() == 1, () -> "no EOF action");

			a.loop.run(() -> a.hop.encoder().writePing(a.hop.ctx(), false, 42, a.hop.ctx().newPromise()));

			assertThat(awaitPing(peer.getInputStream())).as("the PING written after the EOF").isEqualTo(42);
			assertThat(a.hop.open()).isTrue();
			a.loop.run(a.hop::closeLocked);
			awaitTrue(() -> !a.hop.open(), () -> "a is still open");
			assertThat(eofs.get()).isEqualTo(1);
			assertThat(a.sink.failures()).isEmpty();
		}
	}

	@Test
	void anEofActionThrowingIsReportedAndClosesTheChannel() throws Exception {
		Socket[] sockets = Endpoint.socketPair();
		try (Socket peer = sockets[1];
				Endpoint a = Endpoint.over("a", sockets[0], builder -> builder.server(true).onEof(() -> {
					throw new IllegalStateException("EOF action bug");
				}))) {
			peer.getOutputStream().write(RawFrames.clientPreface());
			peer.shutdownOutput();

			awaitTrue(() -> !a.hop.open(), () -> "a is still open");
			assertThat(a.sink.failures()).extracting(failure -> failure.cause().getMessage())
				.containsExactly("EOF action bug");
		}
	}

	/** Reads frames until a PING; its data. */
	private static long awaitPing(InputStream in) throws IOException {
		while (true) {
			byte[] header = in.readNBytes(9);
			if (header.length < 9) {
				throw new IOException("EOF before a PING");
			}
			int length = (header[0] & 0xff) << 16 | (header[1] & 0xff) << 8 | (header[2] & 0xff);
			byte[] payload = in.readNBytes(length);
			if (header[3] == PING) {
				return ByteBuffer.wrap(payload).getLong();
			}
		}
	}

}
