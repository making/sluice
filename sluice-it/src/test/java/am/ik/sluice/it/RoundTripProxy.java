package am.ik.sluice.it;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A loopback TCP proxy turning one direction's round trips into discrete rounds: it
 * relays the sending direction at once, and once {@link #arm() armed} holds the other
 * (the receiver's answers: its flow control credit among them) until the sending
 * direction was quiet for the quiet time, then releases everything held at once. A sender
 * bound by credit sends what it holds and stops, so the bytes that passed between two
 * releases are what it could send on one round trip's credit, however fast the machine
 * is. Relays the first accepted connection only.
 */
public final class RoundTripProxy implements AutoCloseable {

	private final ServerSocket server;

	private final int targetPort;

	private final boolean targetSends;

	private final long quietNanos;

	private final List<Socket> sockets = new CopyOnWriteArrayList<>();

	private final ReentrantLock lock = new ReentrantLock();

	private final Condition changed = this.lock.newCondition();

	/** The answers held back; guarded by {@link #lock}. */
	private final ByteArrayOutputStream held = new ByteArrayOutputStream();

	/** The bytes sent in each completed round; guarded by {@link #lock}. */
	private final List<Long> rounds = new ArrayList<>();

	/** Guarded by {@link #lock}. */
	private boolean armed;

	/** The bytes sent since the previous release; guarded by {@link #lock}. */
	private long roundBytes;

	/** The bytes sent since {@link #arm()}; guarded by {@link #lock}. */
	private long sent;

	/** When the sending direction last relayed bytes; guarded by {@link #lock}. */
	private long lastSent = System.nanoTime();

	/** Guarded by {@link #lock}. */
	private boolean closed;

	private RoundTripProxy(int targetPort, boolean targetSends, Duration quiet) throws IOException {
		this.targetPort = targetPort;
		this.targetSends = targetSends;
		this.quietNanos = quiet.toNanos();
		this.server = new ServerSocket();
		this.server.bind(new InetSocketAddress("127.0.0.1", 0), 16);
		Thread.ofVirtual().name("e2e-round-trip-proxy-accept").start(this::accept);
	}

	/**
	 * A proxy to {@code 127.0.0.1:targetPort} whose connecting side sends: what the
	 * target answers is held.
	 */
	public static RoundTripProxy clientSends(int targetPort, Duration quiet) throws IOException {
		return new RoundTripProxy(targetPort, false, quiet);
	}

	/**
	 * A proxy to {@code 127.0.0.1:targetPort} whose target sends: what the connecting
	 * side answers is held.
	 */
	public static RoundTripProxy targetSends(int targetPort, Duration quiet) throws IOException {
		return new RoundTripProxy(targetPort, true, quiet);
	}

	/** The port the proxy listens on. */
	public int port() {
		return this.server.getLocalPort();
	}

	/** Starts holding the answers; until then both directions pass at once. */
	public void arm() {
		this.lock.lock();
		try {
			this.armed = true;
			this.roundBytes = 0;
			this.sent = 0;
			this.rounds.clear();
			this.changed.signalAll();
		}
		finally {
			this.lock.unlock();
		}
	}

	/** The bytes sent in each round completed since {@link #arm()}, in order. */
	public List<Long> rounds() {
		this.lock.lock();
		try {
			return List.copyOf(this.rounds);
		}
		finally {
			this.lock.unlock();
		}
	}

	/**
	 * The bytes sent since {@link #arm()}: the completed rounds' and the round still
	 * open.
	 */
	public long sent() {
		this.lock.lock();
		try {
			return this.sent;
		}
		finally {
			this.lock.unlock();
		}
	}

	private void accept() {
		try {
			Socket from = this.server.accept();
			Socket to = new Socket("127.0.0.1", this.targetPort);
			from.setTcpNoDelay(true);
			to.setTcpNoDelay(true);
			this.sockets.add(from);
			this.sockets.add(to);
			Socket sender = this.targetSends ? to : from;
			Socket receiver = this.targetSends ? from : to;
			Thread.ofVirtual().name("e2e-round-trip-proxy-send").start(() -> send(sender, receiver));
			Thread.ofVirtual().name("e2e-round-trip-proxy-hold").start(() -> hold(receiver));
			Thread.ofVirtual().name("e2e-round-trip-proxy-release").start(() -> release(sender));
		}
		catch (IOException e) {
			// closed
		}
	}

	/** Relays the sending direction at once, counting it. */
	private void send(Socket sender, Socket receiver) {
		try (InputStream in = sender.getInputStream(); OutputStream out = receiver.getOutputStream()) {
			byte[] buffer = new byte[64 * 1024];
			int n;
			while ((n = in.read(buffer)) >= 0) {
				out.write(buffer, 0, n);
				this.lock.lock();
				try {
					this.roundBytes += n;
					this.sent += n;
					this.lastSent = System.nanoTime();
				}
				finally {
					this.lock.unlock();
				}
			}
		}
		catch (IOException e) {
			// closed
		}
		close();
	}

	/** Collects the answers for {@link #release}. */
	private void hold(Socket receiver) {
		try (InputStream in = receiver.getInputStream()) {
			byte[] buffer = new byte[64 * 1024];
			int n;
			while ((n = in.read(buffer)) >= 0) {
				this.lock.lock();
				try {
					this.held.write(buffer, 0, n);
					this.changed.signalAll();
				}
				finally {
					this.lock.unlock();
				}
			}
		}
		catch (IOException e) {
			// closed
		}
		close();
	}

	/**
	 * Writes the held answers to the sender: at once while not armed, else once the
	 * sending direction was quiet for the quiet time, closing a round.
	 */
	private void release(Socket sender) {
		try (OutputStream out = sender.getOutputStream()) {
			while (true) {
				byte[] answers;
				this.lock.lock();
				try {
					while (true) {
						if (this.closed) {
							return;
						}
						if (this.held.size() > 0) {
							if (!this.armed) {
								break;
							}
							long quietFor = System.nanoTime() - this.lastSent;
							if (quietFor >= this.quietNanos) {
								this.rounds.add(this.roundBytes);
								this.roundBytes = 0;
								break;
							}
							this.changed.awaitNanos(this.quietNanos - quietFor);
						}
						else {
							this.changed.await(100, TimeUnit.MILLISECONDS);
						}
					}
					answers = this.held.toByteArray();
					this.held.reset();
				}
				finally {
					this.lock.unlock();
				}
				out.write(answers);
				out.flush();
			}
		}
		catch (IOException e) {
			// closed
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		finally {
			close();
		}
	}

	@Override
	public void close() {
		this.lock.lock();
		try {
			this.closed = true;
			this.changed.signalAll();
		}
		finally {
			this.lock.unlock();
		}
		closeQuietly(this.server);
		this.sockets.forEach(RoundTripProxy::closeQuietly);
	}

	private static void closeQuietly(AutoCloseable closeable) {
		try {
			closeable.close();
		}
		catch (Exception e) {
			// closed already
		}
	}

}
