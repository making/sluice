package am.ik.sluice.server.proxy.hop;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.handler.codec.http2.Http2Error;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;

import static am.ik.sluice.server.proxy.hop.Fixture.awaitTrue;
import static am.ik.sluice.server.proxy.hop.Fixture.settled;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * A hop's connection window gate: while withheld, the connection-level WINDOW_UPDATEs of
 * netty's auto refill are held back -- the peer can spend no more than the connection
 * window it holds, though its stream windows are granted back -- and let go they reach
 * the peer as one WINDOW_UPDATE; a hop built with a larger connection window grants it
 * behind its preface, refills to it, and withheld holds the peer to it; a hop watching
 * its writes runs its action after the writer's batches; the loop's after-settle hook
 * runs after every action, again while it acts.
 */
@Tag("netty-hop")
@ExtendWith(LeakCheck.class)
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class HopConnectionWindowTest {

	private static final int WINDOW = 65535;

	private static final int LARGE_WINDOW = 1024 * 1024;

	private static final int FRAME = 16 * 1024;

	/** POST / of a.test, http: static table entries and a literal authority. */
	private static final byte[] POST_HEAD = { (byte) 0x83, (byte) 0x84, (byte) 0x86, 0x41, 0x06, 'a', '.', 't', 'e',
			's', 't' };

	@Test
	void aWithheldConnectionWindowHoldsThePeerToTheWindowItHasUntilLetGo() throws Exception {
		Socket[] sockets = Endpoint.socketPair();
		List<int[]> updates = new CopyOnWriteArrayList<>();
		try (Socket raw = sockets[1]; Endpoint a = Endpoint.over("a", sockets[0], builder -> builder.server(true))) {
			OutputStream out = raw.getOutputStream();
			Thread reader = Thread.ofVirtual().start(() -> readWindowUpdates(raw, updates));
			out.write(RawFrames.clientPreface());
			out.write(RawFrames.frame(0x1, 0x4, 1, POST_HEAD));
			out.flush();
			awaitTrue(() -> a.loop.call(() -> a.hop.connection().numActiveStreams()) == 1, () -> "no stream");
			assertThat(a.loop.call(() -> a.hop.withholdConnectionWindowLocked(true))).isFalse();

			// three frames: the auto refill grants the connection window back past half
			// of it, and the stream window too, as the listener consumes on arrival
			for (int i = 0; i < 3; i++) {
				out.write(RawFrames.frame(0x0, 0, 1, new byte[FRAME]));
			}
			out.flush();
			settled(a.hop::bytesRead, () -> updates.size());

			assertThat(a.loop.call(a.hop::owedConnectionWindowLocked)).isEqualTo(2 * FRAME);
			assertThat(increments(updates, 0)).as("no connection window granted").isZero();
			assertThat(increments(updates, 1)).as("the stream window granted back").isEqualTo(2 * FRAME);
			// the peer may still spend the rest of the window it holds
			out.write(RawFrames.frame(0x0, 0, 1, new byte[WINDOW - 3 * FRAME]));
			out.flush();
			settled(a.hop::bytesRead, () -> updates.size());
			assertThat(a.hop.open()).as("within the window the peer holds").isTrue();

			assertThat(a.loop.call(() -> a.hop.withholdConnectionWindowLocked(false))).isTrue();

			awaitTrue(() -> increments(updates, 0) > 0, () -> "updates=" + describe(updates));
			assertThat(updates.stream().filter(update -> update[0] == 0).toList()).as("one connection WINDOW_UPDATE")
				.hasSize(1);
			assertThat(increments(updates, 0)).isEqualTo(2 * FRAME);
			assertThat(a.loop.call(a.hop::owedConnectionWindowLocked)).isZero();
			// let go, the gate passes the auto refill's next grant
			out.write(RawFrames.frame(0x0, 0, 1, new byte[FRAME]));
			out.write(RawFrames.frame(0x0, 0, 1, new byte[FRAME]));
			out.flush();
			awaitTrue(() -> increments(updates, 0) > 2 * FRAME, () -> "updates=" + describe(updates));
			assertThat(a.sink.failures()).isEmpty();
			raw.close();
			reader.join();
		}
	}

	@Test
	void aPeerPassingTheWindowItHoldsWhileWithheldIsAFlowControlError() throws Exception {
		Socket[] sockets = Endpoint.socketPair();
		List<int[]> updates = new CopyOnWriteArrayList<>();
		try (Socket raw = sockets[1]; Endpoint a = Endpoint.over("a", sockets[0], builder -> builder.server(true))) {
			OutputStream out = raw.getOutputStream();
			Thread reader = Thread.ofVirtual().start(() -> readWindowUpdates(raw, updates));
			out.write(RawFrames.clientPreface());
			out.write(RawFrames.frame(0x1, 0x4, 1, POST_HEAD));
			out.write(RawFrames.frame(0x1, 0x4, 3, POST_HEAD));
			out.flush();
			awaitTrue(() -> a.loop.call(() -> a.hop.connection().numActiveStreams()) == 2, () -> "no streams");
			a.loop.run(() -> a.hop.withholdConnectionWindowLocked(true));

			// the window the peer holds, then past it on another stream with window left
			for (int i = 0; i < 3; i++) {
				out.write(RawFrames.frame(0x0, 0, 1, new byte[FRAME]));
			}
			out.write(RawFrames.frame(0x0, 0, 3, new byte[WINDOW - 3 * FRAME]));
			out.write(RawFrames.frame(0x0, 0, 3, new byte[FRAME]));
			out.flush();

			awaitTrue(() -> goAwayCode(updates) >= 0, () -> "updates=" + describe(updates) + " open=" + a.hop.open());
			assertThat(goAwayCode(updates)).isEqualTo(Http2Error.FLOW_CONTROL_ERROR.code());
			raw.close();
			reader.join();
		}
	}

	@Test
	void aLargerConnectionWindowIsGrantedBehindThePrefaceAndRefilledToItsSize() throws Exception {
		Socket[] sockets = Endpoint.socketPair();
		List<int[]> updates = new CopyOnWriteArrayList<>();
		try (Socket raw = sockets[1];
				Endpoint a = Endpoint.over("a", sockets[0],
						builder -> builder.server(true).connectionWindow(LARGE_WINDOW))) {
			OutputStream out = raw.getOutputStream();
			Thread reader = Thread.ofVirtual().start(() -> readWindowUpdates(raw, updates));
			out.write(RawFrames.clientPreface());
			out.write(RawFrames.frame(0x1, 0x4, 1, POST_HEAD));
			out.flush();
			awaitTrue(() -> increments(updates, 0) > 0, () -> "updates=" + describe(updates));
			assertThat(connectionUpdates(updates)).as("the grant behind the preface")
				.containsExactly(LARGE_WINDOW - WINDOW);

			// half the window, consumed on arrival: the auto refill grants it back
			for (int i = 0; i < LARGE_WINDOW / 2 / FRAME; i++) {
				out.write(RawFrames.frame(0x0, 0, 1, new byte[FRAME]));
			}
			out.flush();
			awaitTrue(() -> connectionUpdates(updates).size() == 2, () -> "updates=" + describe(updates));
			assertThat(connectionUpdates(updates)).as("refilled to its size, not to RFC 9113's")
				.containsExactly(LARGE_WINDOW - WINDOW, LARGE_WINDOW / 2);
			assertThat(a.sink.failures()).isEmpty();
			raw.close();
			reader.join();
		}
	}

	@Test
	void aWithheldLargerConnectionWindowHoldsThePeerToItsSize() throws Exception {
		Socket[] sockets = Endpoint.socketPair();
		List<int[]> updates = new CopyOnWriteArrayList<>();
		try (Socket raw = sockets[1];
				Endpoint a = Endpoint.over("a", sockets[0],
						builder -> builder.server(true).connectionWindow(LARGE_WINDOW))) {
			OutputStream out = raw.getOutputStream();
			Thread reader = Thread.ofVirtual().start(() -> readWindowUpdates(raw, updates));
			out.write(RawFrames.clientPreface());
			out.write(RawFrames.frame(0x1, 0x4, 1, POST_HEAD));
			out.flush();
			awaitTrue(() -> increments(updates, 0) > 0, () -> "updates=" + describe(updates));
			a.loop.run(() -> a.hop.withholdConnectionWindowLocked(true));

			// the whole window the peer holds, the stream's refilled on arrival
			for (int i = 0; i < LARGE_WINDOW / FRAME; i++) {
				out.write(RawFrames.frame(0x0, 0, 1, new byte[FRAME]));
			}
			out.flush();
			settled(a.hop::bytesRead, () -> updates.size());
			assertThat(a.hop.open()).as("within the window the peer holds").isTrue();
			assertThat(connectionUpdates(updates)).as("nothing granted while withheld")
				.containsExactly(LARGE_WINDOW - WINDOW);
			assertThat(a.loop.call(a.hop::owedConnectionWindowLocked)).isEqualTo(LARGE_WINDOW);

			out.write(RawFrames.frame(0x0, 0, 1, new byte[1]));
			out.flush();
			awaitTrue(() -> goAwayCode(updates) >= 0, () -> "updates=" + describe(updates) + " open=" + a.hop.open());
			assertThat(goAwayCode(updates)).isEqualTo(Http2Error.FLOW_CONTROL_ERROR.code());
			raw.close();
			reader.join();
		}
	}

	@Test
	void aConnectionWindowBelowTheSpecsIsRefused() {
		assertThatIllegalArgumentException().isThrownBy(() -> Hop.builder().connectionWindow(WINDOW - 1));
	}

	@Test
	void aHopWatchingItsWritesRunsItsActionAfterTheWritersBatches() throws Exception {
		Socket[] sockets = Endpoint.socketPair();
		AtomicInteger runs = new AtomicInteger();
		try (Socket raw = sockets[1];
				Endpoint a = Endpoint.over("a", sockets[0],
						builder -> builder.server(true).onWritten(runs::incrementAndGet))) {
			raw.getOutputStream().write(RawFrames.clientPreface());
			settled(a.hop::bytesWritten);
			assertThat(runs.get()).as("not watching").isZero();

			a.hop.watchWrites(true);
			a.loop.run(() -> a.hop.encoder().writePing(a.hop.ctx(), false, 1, a.hop.ctx().newPromise()));
			awaitTrue(() -> runs.get() > 0, () -> "never ran");

			a.hop.watchWrites(false);
			int before = runs.get();
			a.loop.run(() -> a.hop.encoder().writePing(a.hop.ctx(), false, 2, a.hop.ctx().newPromise()));
			settled(a.hop::bytesWritten);
			assertThat(runs.get()).as("watching no more").isEqualTo(before);
			assertThat(a.sink.failures()).isEmpty();
		}
	}

	@Test
	void theAfterSettleHookRunsAfterEveryActionAndAgainWhileItActs() throws Exception {
		RecordingSink sink = new RecordingSink();
		Loop loop = new Loop("hooked", sink);
		AtomicInteger runs = new AtomicInteger();
		AtomicInteger acting = new AtomicInteger(2);
		try {
			loop.afterSettle(() -> {
				runs.incrementAndGet();
				return acting.getAndDecrement() > 0;
			});
			loop.run(() -> {
			});
			// acted twice: two more passes, the last one doing nothing
			assertThat(runs.get()).isEqualTo(3);
			loop.run(() -> loop.run(() -> {
			}));
			assertThat(runs.get()).as("the outermost call settles").isEqualTo(4);

			loop.afterSettle(() -> {
				throw new IllegalStateException("hook bug");
			});
			loop.run(() -> {
			});
			assertThat(sink.failures()).extracting(failure -> failure.cause().getMessage()).containsExactly("hook bug");
		}
		finally {
			loop.close();
		}
	}

	private static List<Integer> connectionUpdates(List<int[]> updates) {
		return updates.stream().filter(update -> update[0] == 0).map(update -> update[1]).toList();
	}

	private static long increments(List<int[]> updates, int streamId) {
		return updates.stream().filter(update -> update[0] == streamId).mapToLong(update -> update[1]).sum();
	}

	private static long goAwayCode(List<int[]> frames) {
		return frames.stream().filter(frame -> frame[0] == -1).mapToLong(frame -> frame[1]).findFirst().orElse(-1);
	}

	private static String describe(List<int[]> updates) {
		return updates.stream().map(update -> update[0] + ":" + update[1]).toList().toString();
	}

	/**
	 * Records each WINDOW_UPDATE as {stream, increment}, and a GOAWAY as {-1, code},
	 * until the socket ends.
	 */
	private static void readWindowUpdates(Socket socket, List<int[]> updates) {
		try {
			InputStream in = socket.getInputStream();
			DataInputStream frames = new DataInputStream(in);
			while (true) {
				int length = (frames.readUnsignedByte() << 16) | (frames.readUnsignedByte() << 8)
						| frames.readUnsignedByte();
				int type = frames.readUnsignedByte();
				frames.readUnsignedByte();
				int streamId = frames.readInt() & 0x7fffffff;
				byte[] payload = frames.readNBytes(length);
				if (type == 0x8) {
					updates.add(new int[] { streamId, java.nio.ByteBuffer.wrap(payload).getInt() & 0x7fffffff });
				}
				else if (type == 0x7) {
					updates.add(new int[] { -1, java.nio.ByteBuffer.wrap(payload, 4, 4).getInt() });
				}
			}
		}
		catch (IOException e) {
			// the socket ended
		}
	}

}
