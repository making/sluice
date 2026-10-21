package am.ik.sluice.tunnel;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The receive window grows against the budget while it bounds a transfer, shrinks and
 * releases its charge as its reader falls behind or the remote leaves it unused, gives
 * the remote's credit back once idle, and releases everything on every exit.
 */
@Timeout(60)
class VirtualConnectionWindowTest {

	private static final int KIB = 1024;

	private static final int MIB = 1024 * KIB;

	private static final int INITIAL = VirtualConnection.INITIAL_WINDOW;

	private static final int MAX = VirtualConnection.DEFAULT_WINDOW;

	/** Each way: a round trip far above the time to deliver a window in memory. */
	private static final Duration DELAY = Duration.ofMillis(5);

	private static TunnelLink link(TunnelBudget budget, Duration reclaimAfter) {
		return new TunnelLink(DELAY, writer -> VirtualConnection.builder(1, writer).window(INITIAL, INITIAL).build(),
				writer -> VirtualConnection.builder(1, writer)
					.window(INITIAL, INITIAL)
					.growWindow(MAX, budget)
					.reclaimAfter(reclaimAfter)
					.build());
	}

	/** Writes {@code bytes} through the connection's sink on a thread of its own. */
	private static CompletableFuture<Void> write(VirtualConnection connection, long bytes) {
		return CompletableFuture.runAsync(() -> {
			try {
				OutputStream sink = connection.sink();
				byte[] chunk = new byte[64 * KIB];
				long left = bytes;
				while (left > 0) {
					int n = (int) Math.min(left, chunk.length);
					sink.write(chunk, 0, n);
					left -= n;
				}
			}
			catch (IOException e) {
				throw new IllegalStateException(e);
			}
		}, command -> Thread.ofVirtual().start(command));
	}

	/**
	 * Doubles the window once, whatever the timing: the remote sends its whole credit and
	 * says it is blocked, then the reader takes it all at once. The remote is granted
	 * twice the announced window and sends the byte it has left, which is read too. Where
	 * a transfer leaves the window depends on the timing, down to the announced window.
	 */
	private static void doubleTheWindow(TunnelLink link) throws Exception {
		CompletableFuture<Void> written = write(link.a, INITIAL + 1);
		await(() -> link.toB.blocked.get() == 1 && link.toB.idle(), "the remote said it is blocked");
		read(link.b, INITIAL);
		assertThat(link.b.receiveWindow()).isEqualTo(2L * INITIAL);
		written.get(20, TimeUnit.SECONDS);
		read(link.b, 1);
	}

	/** Reads {@code bytes} as fast as they arrive. */
	private static void read(VirtualConnection connection, long bytes) throws IOException {
		read(connection, bytes, 64 * KIB, Duration.ZERO);
	}

	/** Reads {@code bytes}, at most {@code piece} per read and pausing between reads. */
	private static void read(VirtualConnection connection, long bytes, int piece, Duration pause) throws IOException {
		InputStream source = connection.source();
		byte[] buffer = new byte[piece];
		long left = bytes;
		while (left > 0) {
			int n = source.read(buffer, 0, (int) Math.min(left, buffer.length));
			assertThat(n).as("payload before the end").isPositive();
			left -= n;
			if (!pause.isZero()) {
				sleep(pause);
			}
		}
	}

	private static void sleep(Duration duration) {
		try {
			TimeUnit.NANOSECONDS.sleep(duration.toNanos());
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException(e);
		}
	}

	private static void await(BooleanSupplier condition, String description) {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
		while (!condition.getAsBoolean()) {
			if (System.nanoTime() - deadline > 0) {
				throw new AssertionError("timed out waiting until " + description);
			}
			sleep(Duration.ofMillis(5));
		}
	}

	/**
	 * A fast reader behind a delay: the window bounds the transfer, so it doubles from
	 * the announced window to the maximum, charging the budget for the part past the
	 * announced one. The writer waits about once per round trip of the window, so the
	 * transfer takes a few waits per maximum window and the doublings, where the
	 * announced window alone would take one per half of it.
	 */
	@Test
	void aWindowBoundTransferGrowsTheWindowToTheMaximumAgainstTheBudget() throws Exception {
		TunnelBudget budget = new TunnelBudget(16L * MIB);
		long bytes = 16L * MIB;
		try (TunnelLink link = link(budget, Duration.ofMinutes(1))) {
			CompletableFuture<Void> written = write(link.a, bytes);
			read(link.b, bytes);
			written.get(20, TimeUnit.SECONDS);
			assertThat(link.b.peakWindow()).isEqualTo(MAX);
			assertThat(link.toB.blocked.get()).as("the writer said it was blocked").isPositive();
			assertThat(link.b.charged()).isEqualTo(link.b.receiveWindow() - INITIAL)
				.isEqualTo(budget.used())
				.isLessThanOrEqualTo(MAX - INITIAL);
			long waits = link.a.creditWaits();
			System.out.printf("window growth: %dMiB in %d credit waits, %d grants%n", bytes / MIB, waits,
					link.toA.grants.get());
			assertThat(waits).as("credit waits of a %dMiB transfer", bytes / MIB)
				.isLessThanOrEqualTo(4 * bytes / MAX + 2 * Integer.numberOfTrailingZeros(MAX / INITIAL))
				.isLessThan(bytes / (INITIAL / 2) / 4);
		}
		assertThat(budget.used()).as("closed: released").isZero();
		assertThat(budget.holders()).isZero();
	}

	/**
	 * A reader that falls behind -- it never finds the buffer empty -- halves the window
	 * at each decision, granting back less than it read, down to the announced window:
	 * the charge goes back while the transfer goes on.
	 */
	@Test
	void aReaderThatFallsBehindShrinksTheWindowAndReleasesItsCharge() throws Exception {
		TunnelBudget budget = new TunnelBudget(16L * MIB);
		try (TunnelLink link = link(budget, Duration.ofMinutes(1))) {
			CompletableFuture<Void> written = write(link.a, 12L * MIB);
			read(link.b, 8L * MIB);
			assertThat(link.b.peakWindow()).isEqualTo(MAX);
			// the reader pauses: the remote fills the window
			await(() -> link.a.creditWaits() > 0 && link.b.receiveCredit() == 0, "the window is full");
			assertThat(link.b.charged()).isPositive();
			// about 2MB/s: a window takes the reader several round trips, so it never
			// finds the buffer empty; each halving takes two decisions (the remote, not
			// granted more, says it is blocked only once it was granted again), 1.4MiB
			// read from 1MiB down
			read(link.b, 5L * MIB / 2, 2 * KIB, Duration.ofMillis(1));
			assertThat(link.b.receiveWindow()).isEqualTo(INITIAL);
			assertThat(link.b.charged()).isZero();
			assertThat(budget.used()).isZero();
			read(link.b, 3L * MIB / 2);
			written.get(20, TimeUnit.SECONDS);
		}
	}

	/**
	 * Connections whose readers stopped hold the budget; another one, its window bounding
	 * its transfer, stays at the announced window. Once one of them drained and idled,
	 * giving its remote's credit back, the starved one grows into what it released.
	 */
	@Test
	void aStarvedConnectionGetsCreditOnceAnotherDrains() throws Exception {
		TunnelBudget budget = new TunnelBudget(2L * MIB);
		List<TunnelLink> holders = new ArrayList<>();
		List<CompletableFuture<Void>> writes = new ArrayList<>();
		try (TunnelLink starved = link(budget, Duration.ofMinutes(1))) {
			// each grows while its reader keeps up, then the reader stops; until the
			// budget is taken (each takes at most its share of what is left)
			while (budget.used() <= budget.max() - 64 && holders.size() < 8) {
				TunnelLink holder = link(budget, Duration.ofMillis(100));
				holders.add(holder);
				writes.add(write(holder.a, 8L * MIB));
				read(holder.b, 4L * MIB);
				await(() -> holder.b.receiveCredit() == 0 && holder.a.creditWaits() > 0, "the window is full");
			}
			long held = budget.used();
			System.out.printf("held by the stopped readers: %d of %d%n", held, budget.max());
			assertThat(held).as("the stopped readers hold all but rounding").isGreaterThan(budget.max() - 64);
			CompletableFuture<Void> written = write(starved.a, 4L * MIB);
			read(starved.b, 2L * MIB);
			assertThat(starved.toB.blocked.get()).as("its window bounds it").isPositive();
			assertThat(starved.b.peakWindow()).as("starved").isEqualTo(INITIAL);
			assertThat(starved.b.charged()).isZero();
			// the first holder drains its buffer, its writer done, and idles
			TunnelLink drained = holders.getFirst();
			read(drained.b, 4L * MIB);
			writes.getFirst().get(20, TimeUnit.SECONDS);
			CompletableFuture.runAsync(() -> {
				try {
					drained.b.source().read(new byte[1]);
				}
				catch (IOException e) {
					// closed
				}
			}, command -> Thread.ofVirtual().start(command));
			await(() -> drained.b.charged() == 0, "the drained one released its charge");
			read(starved.b, 2L * MIB);
			written.get(20, TimeUnit.SECONDS);
			assertThat(starved.b.peakWindow()).as("grown into what the drained one released").isGreaterThan(INITIAL);
		}
		finally {
			holders.forEach(TunnelLink::close);
		}
		assertThat(budget.used()).isZero();
	}

	/**
	 * A reader idle for the reclaim delay with the window past the announced one asks the
	 * remote for its credit back; the remote keeps the announced window and releases the
	 * rest, the window and the charge drop, and the transfer goes on afterwards.
	 */
	@Test
	void anIdleConnectionTakesTheRemotesCreditBack() throws Exception {
		TunnelBudget budget = new TunnelBudget(16L * MIB);
		try (TunnelLink link = link(budget, Duration.ofMillis(100))) {
			doubleTheWindow(link);
			assertThat(link.b.charged()).isEqualTo(INITIAL);
			CompletableFuture<Void> idle = CompletableFuture.runAsync(() -> {
				try {
					read(link.b, 3L * MIB);
				}
				catch (IOException e) {
					throw new IllegalStateException(e);
				}
			}, command -> Thread.ofVirtual().start(command));
			await(() -> link.b.charged() == 0, "the credit was given back");
			assertThat(link.b.receiveWindow()).isEqualTo(INITIAL);
			assertThat(link.b.reclaims()).isEqualTo(1);
			assertThat(link.toB.released.get()).isPositive();
			assertThat(link.a.sendCredit()).isLessThanOrEqualTo(INITIAL);
			assertThat(budget.used()).isZero();
			write(link.a, 3L * MIB).get(20, TimeUnit.SECONDS);
			idle.get(20, TimeUnit.SECONDS);
		}
	}

	/**
	 * A remote that does not answer the reclaim (it predates it) keeps its credit, and
	 * the window its charge; the reader asks once, not at every reclaim delay.
	 */
	@Test
	void aRemoteThatDoesNotAnswerIsAskedOnce() throws Exception {
		TunnelBudget budget = new TunnelBudget(16L * MIB);
		try (TunnelLink link = link(budget, Duration.ofMillis(50))) {
			link.toA.reclaims = false;
			doubleTheWindow(link);
			CompletableFuture.runAsync(() -> {
				try {
					link.b.source().read(new byte[1]);
				}
				catch (IOException e) {
					// closed
				}
			}, command -> Thread.ofVirtual().start(command));
			await(() -> link.b.reclaims() == 1, "a reclaim was asked");
			sleep(Duration.ofMillis(300));
			assertThat(link.b.reclaims()).isEqualTo(1);
			assertThat(link.toA.reclaimsSent.get()).isEqualTo(1);
			assertThat(link.b.charged()).isPositive().isEqualTo(budget.used());
		}
		assertThat(budget.used()).isZero();
	}

	@Test
	void aReleasePastTheCreditFailsTheConnection() {
		AtomicLong failures = new AtomicLong();
		TunnelBudget budget = new TunnelBudget(MIB);
		VirtualConnection connection = VirtualConnection.builder(1, new TunnelLink.Direction(DELAY, "x") {

			@Override
			public void sendError(long connectionId, String message) {
				failures.incrementAndGet();
			}

		}).window(INITIAL, INITIAL).growWindow(MAX, budget).build();
		connection.released(INITIAL + 1);
		assertThat(failures.get()).isEqualTo(1);
		assertThatThrownBy(() -> connection.source().read(new byte[1])).isInstanceOf(IOException.class)
			.hasMessage(VirtualConnection.WINDOW_EXCEEDED);
	}

	/** Grows a window to the maximum, then leaves its remote's last window unread. */
	private static TunnelLink grown(TunnelBudget budget) throws Exception {
		TunnelLink link = link(budget, Duration.ofMinutes(1));
		write(link.a, 9L * MIB);
		read(link.b, 8L * MIB);
		await(() -> link.b.receiveCredit() == 0 && link.a.creditWaits() > 0, "the window is full");
		assertThat(link.b.charged()).isPositive();
		return link;
	}

	@Test
	void closeReleasesTheCharge() throws Exception {
		TunnelBudget budget = new TunnelBudget(16L * MIB);
		try (TunnelLink link = grown(budget)) {
			assertThat(budget.buffered()).isPositive();
			link.b.close();
			assertThat(link.b.charged()).isZero();
			assertThat(budget.used()).isZero();
			assertThat(budget.buffered()).isZero();
		}
	}

	@Test
	void aWindowViolationReleasesTheCharge() throws Exception {
		TunnelBudget budget = new TunnelBudget(16L * MIB);
		try (TunnelLink link = grown(budget)) {
			link.b.acceptData(new byte[1]);
			assertThat(link.b.charged()).isZero();
			assertThat(budget.used()).isZero();
			assertThat(budget.buffered()).isZero();
		}
	}

	/**
	 * Once the remote closed its direction, the window holds only what is buffered: the
	 * charge past it goes at once, the rest as it is read.
	 */
	@Test
	void theRemotesEndReleasesTheChargeAsTheBufferDrains() throws Exception {
		TunnelBudget budget = new TunnelBudget(16L * MIB);
		try (TunnelLink link = grown(budget)) {
			long buffered = budget.buffered();
			link.b.remoteClosed();
			assertThat(link.b.charged()).isEqualTo(Math.max(0, buffered - INITIAL));
			read(link.b, buffered);
			assertThat(link.b.charged()).isZero();
			assertThat(budget.used()).isZero();
			assertThat(budget.buffered()).isZero();
		}
	}

	@Test
	void theRemotesFailureReleasesTheChargeAsTheBufferDrains() throws Exception {
		TunnelBudget budget = new TunnelBudget(16L * MIB);
		try (TunnelLink link = grown(budget)) {
			link.b.remoteFailed("gone");
			assertThat(link.b.charged()).isLessThanOrEqualTo(Math.max(0, budget.buffered() - INITIAL));
			link.b.close();
			assertThat(budget.used()).isZero();
			assertThat(budget.buffered()).isZero();
		}
	}

}
