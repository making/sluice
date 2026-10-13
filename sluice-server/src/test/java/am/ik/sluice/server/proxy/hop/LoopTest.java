package am.ik.sluice.server.proxy.hop;

import java.net.Socket;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import am.ik.sluice.tunnel.DuplexPipe;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.Http2FrameAdapter;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;

import static am.ik.sluice.server.proxy.hop.Fixture.awaitTrue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The lock and the ticker of a {@link Loop}.
 */
@Tag("netty-hop")
@ExtendWith(LeakCheck.class)
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class LoopTest {

	@Test
	void aCallInsideAnActionRunsAtOnceAndTheOutermostCallSettles() throws Exception {
		Socket[] sockets = Endpoint.socketPair();
		try (Socket peer = sockets[1]; Endpoint a = Endpoint.over("a", sockets[0], builder -> builder.server(true))) {
			awaitTrue(() -> a.hop.bytesWritten() > 0, () -> "no SETTINGS written");
			long before = a.hop.bytesWritten() + a.hop.queuedBytes();

			long inside = a.loop.call(() -> {
				a.hop.encoder().writePing(a.hop.ctx(), false, 1, a.hop.ctx().newPromise());
				int nested = a.loop.call(() -> 41);
				assertThat(nested).isEqualTo(41);
				// the nested call did not settle: the PING is not flushed yet
				return a.hop.bytesWritten() + a.hop.queuedBytes();
			});

			assertThat(inside).isEqualTo(before);
			assertThat(a.hop.bytesWritten() + a.hop.queuedBytes()).isEqualTo(before + 17);
			assertThat(a.loop.heldByCurrentThread()).isFalse();
		}
	}

	@Test
	void anActionsExceptionReachesTheCallerAndReleasesTheLock() throws Exception {
		Loop loop = new Loop("throwing", new RecordingSink());
		try {
			assertThatThrownBy(() -> loop.run(() -> {
				throw new IllegalStateException("action failed");
			})).hasMessage("action failed");

			CompletableFuture<Integer> other = CompletableFuture.supplyAsync(() -> loop.call(() -> 1));
			assertThat(other.get(5, TimeUnit.SECONDS)).isEqualTo(1);
		}
		finally {
			loop.close();
		}
	}

	@Test
	void theTickerRunsNettysGracefulCloseTimerWithoutAnyIo() throws Exception {
		Socket[] sockets = Endpoint.socketPair();
		try (Endpoint a = Endpoint.over("a", sockets[0],
				builder -> builder.server(true).codec(codec -> codec.gracefulShutdownTimeoutMillis(300)));
				Endpoint peer = Endpoint.over("peer", sockets[1], builder -> builder)) {
			// a stream the peer never ends holds the graceful close open
			peer.loop.run(() -> peer.hop.encoder()
				.writeHeaders(peer.hop.ctx(), 1,
						new DefaultHttp2Headers().method("GET").scheme("http").authority("a.test").path("/"), 0, false,
						peer.hop.ctx().newPromise()));
			awaitTrue(() -> a.loop.call(() -> a.hop.connection().numActiveStreams()) == 1, () -> "no stream");

			long start = System.nanoTime();
			a.loop.run(a.hop::closeLocked);

			assertThat(a.hop.open()).as("waiting for the stream").isTrue();
			awaitTrue(() -> !a.hop.open(), () -> "a is still open");
			assertThat(Duration.ofNanos(System.nanoTime() - start)).isGreaterThanOrEqualTo(Duration.ofMillis(250));
			assertThat(a.sink.failures()).isEmpty();
		}
	}

	@Test
	void aTaskFailingInATickClosesItsHopAndTheTickerGoesOn() throws Exception {
		Socket[] first = Endpoint.socketPair();
		Socket[] second = Endpoint.socketPair();
		RecordingSink sink = new RecordingSink();
		Loop loop = new Loop("ticking", sink);
		Hop failing = loop.call(() -> server("failing", loop, first[0]));
		Hop other = loop.call(() -> server("other", loop, second[0]));
		CompletableFuture<Void> later = new CompletableFuture<>();
		try (Socket firstPeer = first[1]; Socket secondPeer = second[1]) {
			loop.run(() -> {
				failing.ctx().executor().schedule(() -> failing.ctx().executor().execute(() -> {
					throw new AssertionError("task bug");
				}), 50, TimeUnit.MILLISECONDS);
				other.ctx().executor().schedule(() -> later.complete(null), 300, TimeUnit.MILLISECONDS);
			});

			later.get(5, TimeUnit.SECONDS);
			assertThat(failing.open()).isFalse();
			assertThat(other.open()).isTrue();
			assertThat(sink.failures()).extracting(failure -> failure.source() + ": " + failure.cause().getMessage())
				.containsExactly("failing: task bug");
			assertThat(loop.tickerAlive()).isTrue();
		}
		finally {
			loop.close();
			Fixture.awaitDone(List.of(failing, other));
		}
		awaitTrue(() -> !loop.tickerAlive(), () -> "the ticker still runs");
	}

	@Test
	void channelsThatDoNotSettleAreReportedAndClosed() throws Exception {
		Socket[] sockets = Endpoint.socketPair();
		try (Socket peer = sockets[1]; Endpoint a = Endpoint.over("a", sockets[0], builder -> builder.server(true))) {
			// every completed PING writes the next one
			a.loop.run(() -> pingForever(a.hop));

			assertThat(a.hop.open()).isFalse();
			assertThat(a.sink.failures()).extracting(failure -> failure.cause().getMessage())
				.containsExactly("the channels did not settle within 10000 rounds; closing them");
		}
	}

	private static void pingForever(Hop hop) {
		hop.encoder().writePing(hop.ctx(), false, 1, hop.newPromise(future -> {
			if (future.isSuccess()) {
				pingForever(hop);
			}
		}));
	}

	private static Hop server(String name, Loop loop, Socket socket) {
		return Hop.builder()
			.name(name)
			.loop(loop)
			.pipe(DuplexPipe.of(socket))
			.server(true)
			.listener(hop -> new Http2FrameAdapter())
			.build();
	}

}
