package am.ik.sluice.it;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

/** Raw socket helpers for the data plane tests. */
public final class TestSockets {

	private TestSockets() {
	}

	/** A plain GET head for {@code host}, kept alive. */
	public static byte[] get(String host) {
		return ("GET / HTTP/1.1\r\nHost: " + host + "\r\n\r\n").getBytes(StandardCharsets.US_ASCII);
	}

	/**
	 * Opens a connection to {@code port}, writes {@code prefix} then a GET for
	 * {@code host}, and reads the {@link HoldingUpstream#OK} response: the returned
	 * socket is an established relay held open by the upstream.
	 */
	public static Socket establish(int port, byte[] prefix, String host) throws IOException {
		Socket socket = new Socket("127.0.0.1", port);
		try {
			socket.setSoTimeout(5000);
			socket.getOutputStream().write(prefix);
			request(socket, host);
			return socket;
		}
		catch (IOException | RuntimeException e) {
			socket.close();
			throw e;
		}
	}

	/** Sends a GET for {@code host} on an established relay and reads the response. */
	public static void request(Socket socket, String host) throws IOException {
		socket.getOutputStream().write(get(host));
		socket.getOutputStream().flush();
		String response = readUntil(socket.getInputStream(), HoldingUpstream.OK.length());
		if (!response.equals(HoldingUpstream.OK)) {
			throw new IOException("unexpected response: " + response);
		}
	}

	/** Reads exactly {@code length} bytes as ASCII, or fails at EOF. */
	public static String readUntil(InputStream in, int length) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		while (out.size() < length) {
			int b = in.read();
			if (b < 0) {
				throw new IOException("EOF after " + out.toString(StandardCharsets.US_ASCII));
			}
			out.write(b);
		}
		return out.toString(StandardCharsets.US_ASCII);
	}

	/**
	 * Waits for the peer to close {@code socket} -- EOF or a reset -- and returns how
	 * long it took; fails when the socket is still open after {@code within} or delivers
	 * bytes.
	 */
	public static Duration awaitClosed(Socket socket, Duration within) throws IOException {
		long start = System.nanoTime();
		socket.setSoTimeout((int) within.toMillis());
		try {
			int b = socket.getInputStream().read();
			if (b >= 0) {
				throw new AssertionError("expected the connection to be closed, but read a byte");
			}
		}
		catch (SocketTimeoutException e) {
			throw new AssertionError("the connection is still open after " + within, e);
		}
		catch (IOException e) {
			// reset (closed with unread bytes), or a TLS socket closed without
			// close_notify
		}
		return Duration.ofNanos(System.nanoTime() - start);
	}

}
