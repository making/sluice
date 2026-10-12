package am.ik.sluice.server.proxy;

import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.jspecify.annotations.Nullable;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Holds the first write an h2 demux relay decides at a site on a stream, between its
 * decision and its write with the client stream's lock held, until {@link #proceed()}; a
 * hold left over by a failed test lets go after 10s.
 */
final class HeldWrite {

	private final AtomicReference<@Nullable Http2DemuxRelay> relay = new AtomicReference<>();

	private final CountDownLatch decided = new CountDownLatch(1);

	private final CountDownLatch proceed = new CountDownLatch(1);

	private HeldWrite() {
	}

	/**
	 * Installs the hold on the relays the server starts from now on, replacing any write
	 * seam set before.
	 */
	static HeldWrite install(DataProxyServer server, Http2DemuxRelay.WriteSeam.Decision target) {
		HeldWrite held = new HeldWrite();
		AtomicBoolean once = new AtomicBoolean();
		server.h2WriteSeam((relay, decision) -> {
			if (!decision.equals(target) || !once.compareAndSet(false, true)) {
				return;
			}
			held.relay.set(relay);
			held.decided.countDown();
			try {
				held.proceed.await(10, TimeUnit.SECONDS);
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		});
		return held;
	}

	/** Waits until the write is decided and held; returns its relay. */
	Http2DemuxRelay awaitDecided() throws InterruptedException {
		assertThat(this.decided.await(5, TimeUnit.SECONDS)).as("the write was decided and held").isTrue();
		return Objects.requireNonNull(this.relay.get());
	}

	/** Lets the held write go on. */
	void proceed() {
		this.proceed.countDown();
	}

}
