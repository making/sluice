package am.ik.sluice.tunnel;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.jspecify.annotations.Nullable;

import am.ik.sluice.v1.proto.Connect;

/**
 * Two virtual connections joined like the two ends of one tunnel connection: what one
 * writes reaches the other after a fixed delay, each direction in order. Counts the flow
 * control frames each end sent.
 */
final class TunnelLink implements AutoCloseable {

	final Direction toB;

	final Direction toA;

	final VirtualConnection a;

	final VirtualConnection b;

	TunnelLink(Duration delay, Function<FrameWriter, VirtualConnection> aFactory,
			Function<FrameWriter, VirtualConnection> bFactory) {
		this.toB = new Direction(delay, "to-b");
		this.toA = new Direction(delay, "to-a");
		this.a = aFactory.apply(this.toB);
		this.b = bFactory.apply(this.toA);
		this.toB.peer = this.b;
		this.toA.peer = this.a;
	}

	@Override
	public void close() {
		this.toB.thread.interrupt();
		this.toA.thread.interrupt();
		this.a.close();
		this.b.close();
	}

	/** One direction: the frames one end sends, delivered to the other in order. */
	static class Direction implements FrameWriter {

		private record Delivery(long due, Runnable action) {
		}

		private final long delayNanos;

		private final LinkedBlockingQueue<Delivery> queue = new LinkedBlockingQueue<>();

		private final Thread thread;

		private volatile @Nullable VirtualConnection peer;

		/** Whether reclaims are delivered; a peer predating them drops them. */
		volatile boolean reclaims = true;

		final AtomicLong grants = new AtomicLong();

		final AtomicLong reclaimsSent = new AtomicLong();

		final AtomicLong released = new AtomicLong();

		final AtomicLong blocked = new AtomicLong();

		final AtomicLong errors = new AtomicLong();

		private final AtomicLong sent = new AtomicLong();

		private final AtomicLong delivered = new AtomicLong();

		Direction(Duration delay, String name) {
			this.delayNanos = delay.toNanos();
			this.thread = Thread.ofVirtual().name("link-" + name).start(this::deliver);
		}

		private void deliver() {
			try {
				while (true) {
					Delivery delivery = this.queue.take();
					long wait = delivery.due() - System.nanoTime();
					if (wait > 0) {
						TimeUnit.NANOSECONDS.sleep(wait);
					}
					delivery.action().run();
					this.delivered.incrementAndGet();
				}
			}
			catch (InterruptedException e) {
				// closed
			}
		}

		private void send(Runnable action) {
			this.sent.incrementAndGet();
			this.queue.add(new Delivery(System.nanoTime() + this.delayNanos, action));
		}

		/** Whether every frame sent was delivered. */
		boolean idle() {
			return this.delivered.get() == this.sent.get();
		}

		private VirtualConnection peer() {
			return Objects.requireNonNull(this.peer, "peer");
		}

		@Override
		public void sendConnect(Connect connect) {
		}

		@Override
		public boolean sendData(long connectionId, byte[] payload) {
			byte[] copy = payload.clone();
			send(() -> peer().acceptData(copy));
			return true;
		}

		@Override
		public void sendClose(long connectionId) {
			send(() -> peer().remoteClosed());
		}

		@Override
		public void sendError(long connectionId, String message) {
			this.errors.incrementAndGet();
			send(() -> peer().remoteFailed(message));
		}

		@Override
		public void sendWindowUpdate(long connectionId, int increment) {
			this.grants.incrementAndGet();
			send(() -> peer().grant(increment));
		}

		@Override
		public void sendWindowReclaim(long connectionId, int keep) {
			this.reclaimsSent.incrementAndGet();
			if (this.reclaims) {
				send(() -> peer().reclaim(keep));
			}
		}

		@Override
		public void sendWindowReleased(long connectionId, int released) {
			this.released.addAndGet(released);
			send(() -> peer().released(released));
		}

		@Override
		public void sendWindowBlocked(long connectionId) {
			this.blocked.incrementAndGet();
			send(() -> peer().remoteBlocked());
		}

	}

}
