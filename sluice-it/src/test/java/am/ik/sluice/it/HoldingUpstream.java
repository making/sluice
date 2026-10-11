package am.ik.sluice.it;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

import org.jspecify.annotations.Nullable;

/**
 * A test upstream that holds its connections open. Each HTTP/1.1 request head is answered
 * with {@code 200 ok} on the kept-alive connection; a request carrying
 * {@code Upgrade: websocket} is answered with {@code 101} and the connection echoes bytes
 * from then on; anything else (an h2 preface, say) is read and never answered.
 */
public final class HoldingUpstream implements AutoCloseable {

	/** The response to every plain request head. */
	public static final String OK = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";

	/** The response to an upgrade request head. */
	public static final String SWITCHING = "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n"
			+ "Connection: Upgrade\r\n\r\n";

	private final ServerSocket serverSocket;

	private final AtomicInteger accepted = new AtomicInteger();

	private HoldingUpstream(ServerSocket serverSocket) {
		this.serverSocket = serverSocket;
	}

	public static HoldingUpstream start() throws IOException {
		ServerSocket serverSocket = new ServerSocket();
		serverSocket.setReuseAddress(true);
		serverSocket.bind(new InetSocketAddress("127.0.0.1", 0), 128);
		HoldingUpstream upstream = new HoldingUpstream(serverSocket);
		Thread.ofVirtual().name("holding-upstream-accept").start(upstream::acceptLoop);
		return upstream;
	}

	public int port() {
		return this.serverSocket.getLocalPort();
	}

	/** Connections accepted so far. */
	public int accepted() {
		return this.accepted.get();
	}

	private void acceptLoop() {
		while (!this.serverSocket.isClosed()) {
			try {
				Socket socket = this.serverSocket.accept();
				this.accepted.incrementAndGet();
				Thread.ofVirtual().name("holding-upstream-conn").start(() -> serve(socket));
			}
			catch (IOException e) {
				return;
			}
		}
	}

	private static void serve(Socket socket) {
		try (socket; InputStream in = socket.getInputStream(); OutputStream out = socket.getOutputStream()) {
			while (true) {
				String head = readHead(in);
				if (head == null) {
					return;
				}
				if (!head.startsWith("GET ")) {
					in.transferTo(OutputStream.nullOutputStream());
					return;
				}
				if (head.toLowerCase(java.util.Locale.ROOT).contains("upgrade: websocket")) {
					out.write(SWITCHING.getBytes(StandardCharsets.US_ASCII));
					out.flush();
					byte[] buffer = new byte[1024];
					int n;
					while ((n = in.read(buffer)) > 0) {
						out.write(buffer, 0, n);
						out.flush();
					}
					return;
				}
				out.write(OK.getBytes(StandardCharsets.US_ASCII));
				out.flush();
			}
		}
		catch (IOException e) {
			// the peer went away
		}
	}

	/** The bytes up to and including the blank line; null at EOF. */
	private static @Nullable String readHead(InputStream in) throws IOException {
		ByteArrayOutputStream head = new ByteArrayOutputStream();
		int matched = 0;
		int b;
		while ((b = in.read()) >= 0) {
			head.write(b);
			matched = switch (matched) {
				case 0, 2 -> b == '\r' ? matched + 1 : 0;
				case 1, 3 -> b == '\n' ? matched + 1 : (b == '\r' ? 1 : 0);
				default -> 0;
			};
			if (matched == 4) {
				return head.toString(StandardCharsets.US_ASCII);
			}
			if (head.size() == 4 && head.toString(StandardCharsets.US_ASCII).equals("PRI ")) {
				return head.toString(StandardCharsets.US_ASCII);
			}
		}
		return null;
	}

	@Override
	public void close() throws IOException {
		this.serverSocket.close();
	}

}
