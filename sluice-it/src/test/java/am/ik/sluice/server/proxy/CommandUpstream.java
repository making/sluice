package am.ik.sluice.server.proxy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PushbackInputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.jspecify.annotations.Nullable;

/**
 * A raw TCP test upstream driven by its client. A connection opening with a TLS record (a
 * ClientHello relayed in passthrough) has it skipped; then CRLF-ended lines are read: an
 * HTTP/1.1 {@code GET} head is answered with {@link #SWITCHING} -- with {@link #UNFRAMED}
 * when its path is {@code /unframed} --, a command line runs the command for the rest of
 * the connection, and any other line (the framing lines of a RESP array, say) is skipped.
 * Commands: {@code echo} echoes every byte, {@code sink} reads every byte and never
 * writes, {@code push} writes a {@code .} every {@link #PUSH_EVERY}, {@code flood} writes
 * as fast as its peer takes, {@code quit} closes the connection.
 */
final class CommandUpstream implements AutoCloseable {

	static final String SWITCHING = "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n"
			+ "Connection: Upgrade\r\n\r\n";

	/** A response head without a status code: the relay cannot frame it. */
	static final String UNFRAMED = "HTTP/1.1 unframed\r\n\r\n";

	static final Duration PUSH_EVERY = Duration.ofMillis(500);

	private static final int TLS_HANDSHAKE = 0x16;

	private final ServerSocket serverSocket;

	private final List<Socket> sockets = new CopyOnWriteArrayList<>();

	private final AtomicInteger helloes = new AtomicInteger();

	private final AtomicInteger ended = new AtomicInteger();

	private final AtomicLong received = new AtomicLong();

	private final AtomicLong flooded = new AtomicLong();

	private CommandUpstream(ServerSocket serverSocket) {
		this.serverSocket = serverSocket;
	}

	static CommandUpstream start() {
		try {
			ServerSocket serverSocket = new ServerSocket();
			serverSocket.bind(new InetSocketAddress("127.0.0.1", 0), 128);
			CommandUpstream upstream = new CommandUpstream(serverSocket);
			Thread.ofVirtual().name("command-upstream-accept").start(upstream::acceptLoop);
			return upstream;
		}
		catch (IOException e) {
			throw new IllegalStateException(e);
		}
	}

	int port() {
		return this.serverSocket.getLocalPort();
	}

	/** TLS records skipped at the start of a connection so far. */
	int helloes() {
		return this.helloes.get();
	}

	/** Connections whose handling ended: the peer closed, or a write failed. */
	int ended() {
		return this.ended.get();
	}

	/** Bytes {@code sink} read so far. */
	long received() {
		return this.received.get();
	}

	/** Bytes {@code flood} wrote so far. */
	long flooded() {
		return this.flooded.get();
	}

	private void acceptLoop() {
		while (!this.serverSocket.isClosed()) {
			try {
				Socket socket = this.serverSocket.accept();
				this.sockets.add(socket);
				Thread.ofVirtual().name("command-upstream-conn").start(() -> serve(socket));
			}
			catch (IOException e) {
				return;
			}
		}
	}

	private void serve(Socket socket) {
		try (socket) {
			PushbackInputStream in = new PushbackInputStream(socket.getInputStream());
			OutputStream out = socket.getOutputStream();
			int first = in.read();
			if (first < 0) {
				return;
			}
			if (first == TLS_HANDSHAKE) {
				byte[] header = in.readNBytes(4);
				in.skipNBytes(((header[2] & 0xff) << 8) | (header[3] & 0xff));
				this.helloes.incrementAndGet();
			}
			else {
				in.unread(first);
			}
			String line;
			while ((line = readLine(in)) != null) {
				if (line.startsWith("GET ")) {
					String response = line.startsWith("GET /unframed ") ? UNFRAMED : SWITCHING;
					while (!line.isEmpty()) {
						line = readLine(in);
						if (line == null) {
							return;
						}
					}
					write(out, response.getBytes(StandardCharsets.US_ASCII));
					continue;
				}
				switch (line) {
					case "echo" -> in.transferTo(out);
					case "sink" -> sink(in);
					case "push" -> push(out);
					case "flood" -> flood(out);
					case "quit" -> {
					}
					default -> {
						continue;
					}
				}
				return;
			}
		}
		catch (IOException e) {
			// the peer went away
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		finally {
			this.ended.incrementAndGet();
		}
	}

	private void sink(InputStream in) throws IOException {
		byte[] buffer = new byte[8192];
		int n;
		while ((n = in.read(buffer)) > 0) {
			this.received.addAndGet(n);
		}
	}

	private static void push(OutputStream out) throws IOException, InterruptedException {
		while (true) {
			write(out, new byte[] { '.' });
			Thread.sleep(PUSH_EVERY.toMillis());
		}
	}

	private void flood(OutputStream out) throws IOException {
		byte[] chunk = new byte[64 * 1024];
		while (true) {
			out.write(chunk);
			this.flooded.addAndGet(chunk.length);
		}
	}

	private static void write(OutputStream out, byte[] bytes) throws IOException {
		out.write(bytes);
		out.flush();
	}

	/** A line without its CRLF; null at EOF. */
	private static @Nullable String readLine(InputStream in) throws IOException {
		ByteArrayOutputStream line = new ByteArrayOutputStream();
		int b;
		while ((b = in.read()) >= 0) {
			if (b == '\n') {
				byte[] bytes = line.toByteArray();
				int length = bytes.length > 0 && bytes[bytes.length - 1] == '\r' ? bytes.length - 1 : bytes.length;
				return new String(bytes, 0, length, StandardCharsets.US_ASCII);
			}
			line.write(b);
		}
		return null;
	}

	@Override
	public void close() {
		try {
			this.serverSocket.close();
		}
		catch (IOException e) {
			// ignore
		}
		for (Socket socket : this.sockets) {
			try {
				socket.close();
			}
			catch (IOException e) {
				// ignore
			}
		}
	}

}
