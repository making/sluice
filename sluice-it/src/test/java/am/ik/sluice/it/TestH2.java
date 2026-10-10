package am.ik.sluice.it;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * Minimal handcrafted HTTP/2 framing for the E2E tests: enough to speak prior-knowledge
 * h2 with the data plane and a stub upstream (frame encode/decode, no HPACK table
 * dynamics -- literal no-index encoding only).
 */
final class TestH2 {

	static final byte[] PREFACE = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".getBytes(StandardCharsets.US_ASCII);

	static final int DATA = 0x0;

	static final int HEADERS = 0x1;

	static final int SETTINGS = 0x4;

	static final int RST_STREAM = 0x3;

	static final int CONTINUATION = 0x9;

	static final int FLAG_END_STREAM = 0x1;

	static final int FLAG_END_HEADERS = 0x4;

	private TestH2() {
	}

	record Frame(int type, int flags, int streamId, byte[] payload) {

		boolean endStream() {
			return (this.flags & FLAG_END_STREAM) != 0 && this.type == DATA;
		}

	}

	static void writeFrame(OutputStream out, int type, int flags, int streamId, byte[] payload) throws IOException {
		ByteArrayOutputStream frame = new ByteArrayOutputStream(9 + payload.length);
		frame.write((payload.length >>> 16) & 0xff);
		frame.write((payload.length >>> 8) & 0xff);
		frame.write(payload.length & 0xff);
		frame.write(type);
		frame.write(flags);
		frame.write((streamId >>> 24) & 0x7f);
		frame.write((streamId >>> 16) & 0xff);
		frame.write((streamId >>> 8) & 0xff);
		frame.write(streamId & 0xff);
		frame.writeBytes(payload);
		out.write(frame.toByteArray());
		out.flush();
	}

	/**
	 * Emits the client preface (magic + empty SETTINGS) followed by a HEADERS frame.
	 * Header block: literal-no-index entries as {@code name=value} pairs.
	 */
	static void writeClientHead(OutputStream out, int streamId, String... headers) throws IOException {
		out.write(PREFACE);
		writeFrame(out, SETTINGS, 0x0, 0, new byte[0]);
		writeFrame(out, HEADERS, FLAG_END_HEADERS | FLAG_END_STREAM, streamId, headerBlock(headers));
	}

	static byte[] headerBlock(String... headers) {
		ByteArrayOutputStream block = new ByteArrayOutputStream();
		for (String header : headers) {
			int eq = header.indexOf('=');
			writeLiteral(block, header.substring(0, eq), header.substring(eq + 1));
		}
		return block.toByteArray();
	}

	/** HPACK literal without indexing, no huffman. */
	private static void writeLiteral(ByteArrayOutputStream out, String name, String value) {
		byte[] nameBytes = name.getBytes(StandardCharsets.US_ASCII);
		byte[] valueBytes = value.getBytes(StandardCharsets.US_ASCII);
		out.write(0x00);
		out.write(nameBytes.length);
		out.writeBytes(nameBytes);
		out.write(valueBytes.length);
		out.writeBytes(valueBytes);
	}

	/** Reads frames until the next frame; returns it, or null on EOF. */
	static @Nullable Frame readFrame(InputStream in) throws IOException {
		byte[] header = in.readNBytes(9);
		if (header.length < 9) {
			return null;
		}
		int length = (header[0] & 0xff) << 16 | (header[1] & 0xff) << 8 | (header[2] & 0xff);
		int type = header[3] & 0xff;
		int flags = header[4] & 0xff;
		int streamId = (header[5] & 0x7f) << 24 | (header[6] & 0xff) << 16 | (header[7] & 0xff) << 8
				| (header[8] & 0xff);
		byte[] payload = in.readNBytes(length);
		return new Frame(type, flags, streamId, payload);
	}

	/** Reads frames, skipping SETTINGS, until a DATA or HEADERS frame arrives. */
	static List<Frame> readResponseFrames(InputStream in) throws IOException {
		List<Frame> frames = new ArrayList<>();
		Frame frame;
		while ((frame = readFrame(in)) != null) {
			if (frame.type == SETTINGS) {
				continue;
			}
			frames.add(frame);
			if (frame.type == DATA && frame.endStream()) {
				break;
			}
		}
		return frames;
	}

	static String bodyOf(List<Frame> frames) {
		ByteArrayOutputStream body = new ByteArrayOutputStream();
		frames.stream().filter(frame -> frame.type == DATA).forEach(frame -> body.writeBytes(frame.payload()));
		return body.toString(StandardCharsets.UTF_8);
	}

	/**
	 * The stub upstream's canned response: SETTINGS, HEADERS with {@code :status 200}
	 * (HPACK static-table index 8) and a DATA frame with END_STREAM.
	 */
	static byte[] cannedResponse(int streamId, String body) {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		try {
			writeFrame(out, SETTINGS, 0x0, 0, new byte[0]);
			writeFrame(out, HEADERS, FLAG_END_HEADERS, streamId, new byte[] { (byte) 0x88 }); // :status
																								// 200
			writeFrame(out, DATA, FLAG_END_STREAM, streamId, body.getBytes(StandardCharsets.UTF_8));
		}
		catch (IOException e) {
			throw new UncheckedIOException(e);
		}
		return out.toByteArray();
	}

	static final class UncheckedIOException extends RuntimeException {

		UncheckedIOException(java.io.IOException cause) {
			super(cause);
		}

	}

}
