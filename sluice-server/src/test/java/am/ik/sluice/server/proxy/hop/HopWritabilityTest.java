package am.ik.sluice.server.proxy.hop;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import io.netty.handler.codec.http2.Http2Settings;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;

import static am.ik.sluice.server.proxy.hop.Fixture.awaitTrue;
import static am.ik.sluice.server.proxy.hop.Fixture.settled;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A client that stops reading its socket stops its upstream through the client hop's
 * writability, although its HTTP/2 windows allow far more; the other leg meanwhile goes
 * on, and no thread holds the relay's lock while the client hop's writer blocks.
 */
@Tag("netty-hop")
@ExtendWith(LeakCheck.class)
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class HopWritabilityTest {

	private static final int MIB = 1024 * 1024;

	private static final int DOWNLOAD = 16 * MIB;

	/** Below the default initial window the relay announces to the client. */
	private static final int UPLOAD = 60 * 1024;

	@Test
	void aClientNotReadingItsSocketStopsTheUpstream() throws Exception {
		try (Fixture fixture = Fixture.twoRoutes(new Http2Settings().initialWindowSize(64 * MIB))) {
			// the client's windows allow the whole download: only writability can stop it
			fixture.client.growConnectionWindow(64 * MIB);
			awaitTrue(() -> fixture.relay.clientConnectionWindow() > 64 * MIB,
					() -> "window=" + fixture.relay.clientConnectionWindow());
			fixture.client.pauseReading();

			TestClient.Sent download = fixture.client.send("a.test", "/download/" + DOWNLOAD + "/7", new byte[0]);

			Hop clientHop = fixture.relay.client();
			awaitTrue(() -> !clientHop.writable(), fixture::describe);
			long relayed = settled(() -> fixture.relay.fromUpstream(download.streamId()),
					() -> fixture.client.received(download.streamId()));
			// socket buffers, the writer queue past its high water mark, a leg window:
			// far below the 16MiB the windows allow
			assertThat(relayed).as(fixture.describe()).isLessThan(2L * MIB);
			assertThat(clientHop.writable()).isFalse();
			assertThat(clientHop.queuedBytes()).as(fixture.describe()).isLessThan(256 * 1024);

			// the client hop's writer blocks on the socket, holding no lock: the relay
			// goes on, and the other leg takes a whole upload meanwhile (within the
			// windows granted already: the client reads no WINDOW_UPDATE now)
			CompletableFuture<Integer> entered = CompletableFuture
				.supplyAsync(() -> fixture.relay.loop().call(() -> 1));
			assertThat(entered.get(5, TimeUnit.SECONDS)).isEqualTo(1);
			byte[] upload = Payloads.of(8, UPLOAD);
			TestClient.Sent other = fixture.client.send("b.test", "/echo", upload);
			awaitTrue(() -> fixture.upstream("b.test").received(other.streamId()) == UPLOAD, fixture::describe);

			fixture.client.resumeReading();
			TestClient.Response response = download.response().get(30, TimeUnit.SECONDS);
			assertThat(response.body()).hasSize(DOWNLOAD);
			assertThat(Payloads.crc(response.body())).isEqualTo(Payloads.crc(Payloads.of(7, DOWNLOAD)));
			assertThat(other.response().get(10, TimeUnit.SECONDS).text()).isEqualTo(
					"upstream=b stream=%d bytes=%d crc=%s".formatted(other.streamId(), UPLOAD, Payloads.crc(upload)));
			assertThat(fixture.failures()).isEmpty();
		}
	}

}
