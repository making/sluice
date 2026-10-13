package am.ik.sluice.it;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import org.jspecify.annotations.Nullable;

/**
 * A loopback TCP proxy delaying every chunk it relays by a fixed time, in both
 * directions, so a connection through it has a round trip of twice the delay; bandwidth
 * is not limited. Each accepted connection is relayed to the target over a connection of
 * its own; an EOF passes on as a half-close once its delay passed.
 */
public final class DelayingProxy implements AutoCloseable {

	private final ServerSocket server;

	private final int targetPort;

	private final long delayNanos;

	private final AtomicLong relayed = new AtomicLong();

	private final List<Socket> sockets = new CopyOnWriteArrayList<>();

	private DelayingProxy(int targetPort, Duration delay) throws IOException {
		this.targetPort = targetPort;
		this.delayNanos = delay.toNanos();
		this.server = new ServerSocket();
		this.server.bind(new InetSocketAddress("127.0.0.1", 0), 16);
		Thread.ofVirtual().name("e2e-delaying-proxy-accept").start(this::accept);
	}

	/**
	 * Starts a proxy to {@code 127.0.0.1:targetPort} delaying each way by {@code delay}.
	 */
	public static DelayingProxy start(int targetPort, Duration delay) throws IOException {
		return new DelayingProxy(targetPort, delay);
	}

	/** The port the proxy listens on. */
	public int port() {
		return this.server.getLocalPort();
	}

	/** The bytes relayed so far, both ways. */
	public long relayed() {
		return this.relayed.get();
	}

	private void accept() {
		while (!this.server.isClosed()) {
			try {
				Socket from = this.server.accept();
				Socket to = new Socket("127.0.0.1", this.targetPort);
				from.setTcpNoDelay(true);
				to.setTcpNoDelay(true);
				this.sockets.add(from);
				this.sockets.add(to);
				relay(from, to);
				relay(to, from);
			}
			catch (IOException e) {
				return;
			}
		}
	}

	private void relay(Socket from, Socket to) throws IOException {
		LinkedBlockingQueue<Chunk> queue = new LinkedBlockingQueue<>();
		InputStream in = from.getInputStream();
		OutputStream out = to.getOutputStream();
		Thread.ofVirtual().name("e2e-delaying-proxy-read").start(() -> {
			byte[] buffer = new byte[64 * 1024];
			try {
				int n;
				while ((n = in.read(buffer)) >= 0) {
					queue.add(new Chunk(System.nanoTime() + this.delayNanos, Arrays.copyOf(buffer, n)));
				}
			}
			catch (IOException e) {
				// closed
			}
			queue.add(new Chunk(System.nanoTime() + this.delayNanos, null));
		});
		Thread.ofVirtual().name("e2e-delaying-proxy-write").start(() -> {
			try {
				while (true) {
					Chunk chunk = queue.take();
					long wait;
					while ((wait = chunk.due() - System.nanoTime()) > 0) {
						LockSupport.parkNanos(wait);
					}
					byte[] bytes = chunk.bytes();
					if (bytes == null) {
						to.shutdownOutput();
						return;
					}
					out.write(bytes);
					out.flush();
					this.relayed.addAndGet(bytes.length);
				}
			}
			catch (IOException | InterruptedException e) {
				closeQuietly(from);
				closeQuietly(to);
			}
		});
	}

	@Override
	public void close() {
		closeQuietly(this.server);
		this.sockets.forEach(DelayingProxy::closeQuietly);
	}

	private static void closeQuietly(AutoCloseable closeable) {
		try {
			closeable.close();
		}
		catch (Exception e) {
			// closed already
		}
	}

	/** Bytes to write once due; null bytes = the EOF. */
	private record Chunk(long due, byte @Nullable [] bytes) {
	}

}
