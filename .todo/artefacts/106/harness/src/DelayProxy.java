import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.LockSupport;

/**
 * TCP proxy adding a fixed one-way delay to every chunk in both directions (RTT = 2 x
 * delay), bandwidth unbounded: emulates the WAN between sluice-client and sluice-server.
 * Usage: DelayProxy listenPort targetHost targetPort delayMillis
 */
public final class DelayProxy {

	record Chunk(long due, byte[] bytes) {
	}

	public static void main(String[] args) throws Exception {
		int listen = Integer.parseInt(args[0]);
		String host = args[1];
		int port = Integer.parseInt(args[2]);
		long delay = Long.parseLong(args[3]) * 1_000_000L;
		try (ServerSocket server = new ServerSocket()) {
			server.bind(new InetSocketAddress("127.0.0.1", listen));
			System.out.println("delay proxy ready " + listen + " -> " + host + ":" + port + " delay=" + args[3] + "ms");
			while (true) {
				Socket a = server.accept();
				Socket b = new Socket(host, port);
				a.setTcpNoDelay(true);
				b.setTcpNoDelay(true);
				pipe(a, b, delay);
				pipe(b, a, delay);
			}
		}
	}

	static void pipe(Socket from, Socket to, long delay) throws Exception {
		LinkedBlockingQueue<Chunk> queue = new LinkedBlockingQueue<>();
		InputStream in = from.getInputStream();
		OutputStream out = to.getOutputStream();
		Thread.ofVirtual().start(() -> {
			byte[] buf = new byte[65536];
			try {
				int n;
				while ((n = in.read(buf)) >= 0) {
					queue.put(new Chunk(System.nanoTime() + delay, Arrays.copyOf(buf, n)));
				}
			}
			catch (Exception e) {
				// closed
			}
			try {
				queue.put(new Chunk(System.nanoTime() + delay, null));
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		});
		Thread.ofVirtual().start(() -> {
			try {
				while (true) {
					Chunk c = queue.take();
					long wait = c.due() - System.nanoTime();
					if (wait > 0) {
						LockSupport.parkNanos(wait);
						while ((wait = c.due() - System.nanoTime()) > 0) {
							LockSupport.parkNanos(wait);
						}
					}
					if (c.bytes() == null) {
						to.shutdownOutput();
						break;
					}
					out.write(c.bytes());
					out.flush();
				}
			}
			catch (Exception e) {
				try {
					from.close();
					to.close();
				}
				catch (Exception ignored) {
				}
			}
		});
	}

}
