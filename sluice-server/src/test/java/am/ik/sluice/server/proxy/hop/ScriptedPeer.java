package am.ik.sluice.server.proxy.hop;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import am.ik.sluice.tunnel.DuplexPipe;

/**
 * The peer's end of a hop's pipe: each chunk sent is what one read of the hop returns
 * (the hop reads at most {@link Hop#MAX_READ_BYTES} at once, more than any chunk sent may
 * be); what the hop writes is kept and parsed into frames. The hop's half-close ends the
 * peer's sending too.
 */
final class ScriptedPeer implements DuplexPipe {

	private static final byte[] EOF = new byte[0];

	private static final byte[] CLIENT_PREFACE = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".getBytes(StandardCharsets.US_ASCII);

	private final BlockingQueue<byte[]> chunks = new LinkedBlockingQueue<>();

	private final ByteArrayOutputStream written = new ByteArrayOutputStream();

	private final InputStream source = new InputStream() {

		private boolean ended;

		@Override
		public int read() throws IOException {
			throw new UnsupportedOperationException("reads come in chunks");
		}

		@Override
		public int read(byte[] b, int off, int len) throws IOException {
			if (this.ended) {
				return -1;
			}
			byte[] chunk;
			try {
				chunk = ScriptedPeer.this.chunks.take();
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new InterruptedIOException();
			}
			if (chunk == EOF) {
				this.ended = true;
				return -1;
			}
			if (chunk.length > len) {
				throw new IllegalStateException("a chunk larger than one read: " + chunk.length);
			}
			System.arraycopy(chunk, 0, b, off, chunk.length);
			return chunk.length;
		}

	};

	private final OutputStream sink = new OutputStream() {

		@Override
		public void write(int b) {
			write(new byte[] { (byte) b }, 0, 1);
		}

		@Override
		public void write(byte[] b, int off, int len) {
			synchronized (ScriptedPeer.this.written) {
				ScriptedPeer.this.written.write(b, off, len);
			}
		}

	};

	void send(byte[] chunk) {
		this.chunks.add(chunk);
	}

	/** The frames the hop wrote so far, a client's preface skipped. */
	List<Frame> frames() {
		byte[] bytes;
		synchronized (this.written) {
			bytes = this.written.toByteArray();
		}
		int offset = 0;
		if (bytes.length >= CLIENT_PREFACE.length
				&& Arrays.equals(bytes, 0, CLIENT_PREFACE.length, CLIENT_PREFACE, 0, CLIENT_PREFACE.length)) {
			offset = CLIENT_PREFACE.length;
		}
		List<Frame> frames = new ArrayList<>();
		while (bytes.length - offset >= 9) {
			int length = (bytes[offset] & 0xff) << 16 | (bytes[offset + 1] & 0xff) << 8 | (bytes[offset + 2] & 0xff);
			if (bytes.length - offset - 9 < length) {
				break;
			}
			int streamId = ByteBuffer.wrap(bytes, offset + 5, 4).getInt() & 0x7fffffff;
			frames.add(new Frame(bytes[offset + 3] & 0xff, bytes[offset + 4] & 0xff, streamId,
					Arrays.copyOfRange(bytes, offset + 9, offset + 9 + length)));
			offset += 9 + length;
		}
		return frames;
	}

	/** The types of the frames the hop wrote on the stream, in order. */
	List<Integer> types(int streamId) {
		return frames().stream().filter(frame -> frame.streamId() == streamId).map(Frame::type).toList();
	}

	@Override
	public InputStream source() {
		return this.source;
	}

	@Override
	public OutputStream sink() {
		return this.sink;
	}

	@Override
	public void shutdownOutput() {
		this.chunks.add(EOF);
	}

	@Override
	public void close() {
		this.chunks.add(EOF);
	}

	/** A frame the hop wrote. */
	record Frame(int type, int flags, int streamId, byte[] payload) {

		@Override
		public String toString() {
			return "type=" + this.type + " flags=" + this.flags + " stream=" + this.streamId;
		}

	}

}
