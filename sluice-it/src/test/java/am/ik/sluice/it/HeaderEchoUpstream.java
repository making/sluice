package am.ik.sluice.it;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PushbackInputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http2.DefaultHttp2HeadersDecoder;
import io.netty.handler.codec.http2.Http2Headers;
import org.jspecify.annotations.Nullable;

/**
 * An upstream speaking HTTP/1.1 (keep-alive) and h2c with prior knowledge on one port:
 * every request is answered {@code 200} with {@link #body} naming the echoed headers it
 * carried (the credential headers by default), so a test sees what reached the upstream.
 */
public final class HeaderEchoUpstream implements AutoCloseable {

	public static final String GREETING = "hello-from-upstream";

	/** The headers the body names when the request carried them. */
	private final List<String> echoed;

	private final ServerSocket server;

	HeaderEchoUpstream() throws IOException {
		this("authorization", "x-api-token");
	}

	/** An upstream echoing the headers named, lowercase. */
	public HeaderEchoUpstream(String... echoed) throws IOException {
		this.echoed = List.of(echoed);
		this.server = new ServerSocket();
		this.server.bind(new InetSocketAddress("127.0.0.1", 0));
		Thread.ofVirtual().name("e2e-header-echo-accept").start(this::acceptLoop);
	}

	public int port() {
		return this.server.getLocalPort();
	}

	/**
	 * The body answering a request with the headers: {@link #GREETING}, then
	 * {@code  <name>=<value>} for each echoed header present.
	 */
	private String body(Function<String, @Nullable String> header) {
		StringBuilder body = new StringBuilder(GREETING);
		for (String name : this.echoed) {
			String value = header.apply(name);
			if (value != null) {
				body.append(' ').append(name).append('=').append(value);
			}
		}
		return body.toString();
	}

	private void acceptLoop() {
		while (!this.server.isClosed()) {
			try {
				Socket socket = this.server.accept();
				Thread.ofVirtual().name("e2e-header-echo-conn").start(() -> this.serve(socket));
			}
			catch (IOException e) {
				return;
			}
		}
	}

	private void serve(Socket socket) {
		try (socket) {
			PushbackInputStream in = new PushbackInputStream(socket.getInputStream(), 3);
			byte[] start = in.readNBytes(3);
			if (start.length == 0) {
				return;
			}
			in.unread(start);
			if ("PRI".equals(new String(start, StandardCharsets.US_ASCII))) {
				this.serveH2(in, socket.getOutputStream());
			}
			else {
				this.serveH1(in, socket.getOutputStream());
			}
		}
		catch (Exception e) {
			// the connection ended
		}
	}

	private void serveH1(InputStream in, OutputStream out) throws IOException {
		String head;
		while ((head = readHead(in)) != null) {
			Map<String, String> headers = new HashMap<>();
			for (String line : head.split("\r\n")) {
				int colon = line.indexOf(':');
				if (colon > 0) {
					headers.putIfAbsent(line.substring(0, colon).trim().toLowerCase(Locale.ROOT),
							line.substring(colon + 1).trim());
				}
			}
			in.skipNBytes(Long.parseLong(headers.getOrDefault("content-length", "0")));
			byte[] body = this.body(headers::get).getBytes(StandardCharsets.UTF_8);
			out.write(("HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: " + body.length + "\r\n\r\n")
				.getBytes(StandardCharsets.US_ASCII));
			out.write(body);
			out.flush();
			if ("close".equalsIgnoreCase(headers.get("connection"))) {
				return;
			}
		}
	}

	/** The next request head without its blank line; null at EOF. */
	private static @Nullable String readHead(InputStream in) throws IOException {
		ByteArrayOutputStream head = new ByteArrayOutputStream();
		int b;
		while ((b = in.read()) >= 0) {
			head.write(b);
			byte[] bytes = head.toByteArray();
			int n = bytes.length;
			if (n >= 4 && bytes[n - 4] == '\r' && bytes[n - 3] == '\n' && bytes[n - 2] == '\r'
					&& bytes[n - 1] == '\n') {
				return new String(bytes, 0, n - 4, StandardCharsets.ISO_8859_1);
			}
		}
		return null;
	}

	private void serveH2(InputStream in, OutputStream out) throws Exception {
		if (in.readNBytes(TestH2.PREFACE.length).length < TestH2.PREFACE.length) {
			return;
		}
		TestH2.writeFrame(out, TestH2.SETTINGS, 0, 0, new byte[0]);
		DefaultHttp2HeadersDecoder decoder = new DefaultHttp2HeadersDecoder(true);
		Map<Integer, Http2Headers> requests = new HashMap<>();
		TestH2.@Nullable Frame frame;
		while ((frame = TestH2.readFrame(in)) != null) {
			if (frame.type() == TestH2.SETTINGS && (frame.flags() & 0x1) == 0) {
				TestH2.writeFrame(out, TestH2.SETTINGS, 0x1, 0, new byte[0]);
			}
			else if (frame.type() == TestH2.HEADERS) {
				Http2Headers headers = decoder.decodeHeaders(frame.streamId(), Unpooled.wrappedBuffer(frame.payload()));
				requests.put(frame.streamId(), headers);
				if ((frame.flags() & TestH2.FLAG_END_STREAM) != 0) {
					this.respond(out, frame.streamId(), headers);
				}
			}
			else if (frame.type() == TestH2.DATA && (frame.flags() & TestH2.FLAG_END_STREAM) != 0) {
				Http2Headers headers = requests.get(frame.streamId());
				if (headers != null) {
					this.respond(out, frame.streamId(), headers);
				}
			}
		}
	}

	private void respond(OutputStream out, int streamId, Http2Headers headers) throws IOException {
		String body = this.body(name -> {
			CharSequence value = headers.get(name);
			return value == null ? null : value.toString();
		});
		// :status 200 is HPACK static-table index 8
		TestH2.writeFrame(out, TestH2.HEADERS, TestH2.FLAG_END_HEADERS, streamId, new byte[] { (byte) 0x88 });
		TestH2.writeFrame(out, TestH2.DATA, TestH2.FLAG_END_STREAM, streamId, body.getBytes(StandardCharsets.UTF_8));
	}

	@Override
	public void close() throws IOException {
		this.server.close();
	}

}
