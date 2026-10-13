package am.ik.sluice.server.proxy.hop;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The lock and the clock of a group of {@link Hop}s -- the netty channels of one relay,
 * or of one test peer.
 * <p>
 * A hop's channel is an {@code EmbeddedChannel}: it has no thread of its own,
 * {@code EmbeddedEventLoop#inEventLoop} answers {@code true} on every thread, and its
 * task queue is a plain {@code ArrayDeque}, so netty's own event-loop checks never catch
 * a second thread. This lock is the guard instead: readers, writers, the ticker and the
 * code bridging the hops enter the channels only through {@link #run} or {@link #call}
 * (each hop reports a pipeline operation without the lock, see {@link Hop}).
 * <p>
 * After each action the channels are settled: pending tasks run, every channel is flushed
 * and what it wrote moves to its hop's writer queue -- until no channel has anything more
 * to do, since a write on one hop can complete a promise whose listener writes on
 * another. Then the after-settle hook runs ({@link #afterSettle}), and again after a
 * further settle for as long as it acts.
 * <p>
 * Netty schedules timers (the close after a GOAWAY, say) on the channel's executor, which
 * an {@code EmbeddedChannel} runs only when told to. A ticker thread sleeps until the
 * earliest scheduled task of the group is due, then settles. It runs until the loop is
 * closed.
 */
public final class Loop implements AutoCloseable {

	private static final Logger log = LoggerFactory.getLogger(Loop.class);

	/** Settle rounds before the group counts as livelocked. */
	private static final int MAX_SETTLE_ROUNDS = 10_000;

	private final String name;

	private final ExceptionSink sink;

	private final ReentrantLock lock = new ReentrantLock();

	/** The open hops; with the lock. */
	private final List<Hop> hops = new ArrayList<>();

	private final ReentrantLock tickLock = new ReentrantLock();

	private final Condition tickChanged = this.tickLock.newCondition();

	/** When the ticker settles next, in {@link System#nanoTime()}; with tickLock. */
	private long tickAt = Long.MAX_VALUE;

	private final Thread ticker;

	private volatile boolean closed;

	/** Run once the channels settled, until it reports nothing done; with the lock. */
	private volatile @Nullable BooleanSupplier afterSettle;

	/** Failures reported without the lock, for the next settle. */
	private final Queue<Failure> pendingFailures = new ConcurrentLinkedQueue<>();

	/**
	 * A group named {@code name} whose failures -- exceptions in a channel, a pipeline
	 * operation without the lock, a failed tick -- go to the sink.
	 */
	public Loop(String name, ExceptionSink sink) {
		this.name = Objects.requireNonNull(name, "name is required");
		this.sink = Objects.requireNonNull(sink, "sink is required");
		this.ticker = Thread.ofVirtual().name(name + "-tick").start(this::tick);
	}

	/**
	 * Where a group reports failures. Called with the loop's lock held: it must not block
	 * or enter another loop.
	 */
	@FunctionalInterface
	public interface ExceptionSink {

		/** {@code source} names the hop, or the ticker. */
		void exceptionCaught(String source, Throwable cause);

	}

	/**
	 * Runs the action with the lock, then settles the channels. Called from inside an
	 * action (a promise listener, say), the action runs at once and the outermost call
	 * settles. An exception of the action reaches the caller once the channels settled.
	 */
	public void run(Runnable action) {
		call(() -> {
			action.run();
			return null;
		});
	}

	/** {@link #run}, returning the action's result. */
	public <T extends @Nullable Object> T call(Supplier<T> action) {
		if (this.lock.isHeldByCurrentThread()) {
			return action.get();
		}
		this.lock.lock();
		try {
			T result;
			try {
				result = action.get();
			}
			catch (RuntimeException | Error e) {
				try {
					settleLocked();
				}
				catch (RuntimeException | Error suppressed) {
					e.addSuppressed(suppressed);
				}
				throw e;
			}
			settleLocked();
			return result;
		}
		finally {
			this.lock.unlock();
		}
	}

	/**
	 * Whether the current thread holds the lock: inside {@link #run} or {@link #call}.
	 */
	public boolean heldByCurrentThread() {
		return this.lock.isHeldByCurrentThread();
	}

	/** The group's name. */
	public String name() {
		return this.name;
	}

	/**
	 * Closes every open channel of the group at once ({@link Hop#abortLocked()}) and
	 * stops the ticker. The hops finish writing what their channels wrote within their
	 * close grace; a hop built later runs without the ticker.
	 */
	@Override
	public void close() {
		run(() -> List.copyOf(this.hops).forEach(Hop::abortLocked));
		this.closed = true;
		this.tickLock.lock();
		try {
			this.tickChanged.signalAll();
		}
		finally {
			this.tickLock.unlock();
		}
	}

	/**
	 * Has {@code hook} run with the lock each time the channels settled: what bridges the
	 * hops reviews their state there. It returns whether it did anything to the channels
	 * (a write, say); if so, the channels settle and the hook runs again.
	 */
	public void afterSettle(BooleanSupplier hook) {
		this.afterSettle = hook;
	}

	/** Whether the ticker thread is alive; for tests. */
	boolean tickerAlive() {
		return this.ticker.isAlive();
	}

	/** The open hops; with the lock. */
	int hopsLocked() {
		return this.hops.size();
	}

	void registerLocked(Hop hop) {
		this.hops.add(hop);
	}

	/** Reports a failure; with the lock. What the sink throws is logged. */
	void report(String source, Throwable cause) {
		try {
			this.sink.exceptionCaught(source, cause);
		}
		catch (Throwable e) {
			log.warn("{}: the exception sink failed on {}", source, cause, e);
		}
	}

	private void settleLocked() {
		for (Failure failure = this.pendingFailures.poll(); failure != null; failure = this.pendingFailures.poll()) {
			report(failure.source(), failure.cause());
		}
		settleChannelsLocked();
		BooleanSupplier hook = this.afterSettle;
		for (int pass = 0; hook != null && runHook(hook); pass++) {
			settleChannelsLocked();
			if (pass == MAX_SETTLE_ROUNDS) {
				report(this.name, new IllegalStateException("the after-settle hook kept acting for " + MAX_SETTLE_ROUNDS
						+ " passes; closing the channels"));
				List.copyOf(this.hops).forEach(hop -> {
					hop.abortLocked();
					hop.settleLocked();
				});
				break;
			}
		}
		long next = Long.MAX_VALUE;
		for (Hop hop : List.copyOf(this.hops)) {
			if (hop.finishedLocked()) {
				this.hops.remove(hop);
			}
			else {
				next = Math.min(next, hop.nextScheduledLocked());
			}
		}
		if (next != Long.MAX_VALUE) {
			wakeTickerAt(next);
		}
	}

	/** Runs the hook; what it throws is reported, and counts as nothing done. */
	private boolean runHook(BooleanSupplier hook) {
		try {
			return hook.getAsBoolean();
		}
		catch (RuntimeException | Error e) {
			report(this.name, e);
			return false;
		}
	}

	private void settleChannelsLocked() {
		for (int round = 0;; round++) {
			boolean moved = false;
			for (Hop hop : List.copyOf(this.hops)) {
				moved |= hop.settleLocked();
			}
			if (!moved) {
				break;
			}
			if (round == MAX_SETTLE_ROUNDS) {
				// the channels keep writing to each other: a bug in the bridging code
				report(this.name, new IllegalStateException(
						"the channels did not settle within " + MAX_SETTLE_ROUNDS + " rounds; closing them"));
				List.copyOf(this.hops).forEach(hop -> {
					hop.abortLocked();
					hop.settleLocked();
				});
				break;
			}
		}
	}

	/**
	 * Reports a failure at the next settle, which the ticker runs now: for failures
	 * without the lock.
	 */
	void reportLater(String source, Throwable cause) {
		this.pendingFailures.add(new Failure(source, cause));
		wakeTickerAt(System.nanoTime());
	}

	private void wakeTickerAt(long at) {
		this.tickLock.lock();
		try {
			if (this.tickAt == Long.MAX_VALUE || at - this.tickAt < 0) {
				this.tickAt = at;
				this.tickChanged.signalAll();
			}
		}
		finally {
			this.tickLock.unlock();
		}
	}

	private void tick() {
		while (awaitTick()) {
			try {
				// settling runs the due tasks and schedules the next tick
				run(() -> {
				});
			}
			catch (Throwable e) {
				// the ticker never dies of a tick: the hops' timers depend on it
				this.lock.lock();
				try {
					report(this.name + "-tick", e);
				}
				finally {
					this.lock.unlock();
				}
			}
		}
	}

	private record Failure(String source, Throwable cause) {
	}

	/** Waits until a tick is due; {@code false} once the loop closed. */
	private boolean awaitTick() {
		this.tickLock.lock();
		try {
			while (!this.closed) {
				if (this.tickAt == Long.MAX_VALUE) {
					this.tickChanged.await();
					continue;
				}
				long wait = this.tickAt - System.nanoTime();
				if (wait <= 0) {
					this.tickAt = Long.MAX_VALUE;
					return true;
				}
				this.tickChanged.awaitNanos(Math.min(wait, TimeUnit.SECONDS.toNanos(60)));
			}
			return false;
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return false;
		}
		finally {
			this.tickLock.unlock();
		}
	}

}
