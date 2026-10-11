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
public final class TestH2 {

	public static final byte[] PREFACE = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".getBytes(StandardCharsets.US_ASCII);

	public static final int DATA = 0x0;

	public static final int HEADERS = 0x1;

	public static final int SETTINGS = 0x4;

	public static final int RST_STREAM = 0x3;

	public static final int CONTINUATION = 0x9;

	public static final int PING = 0x6;

	public static final int GOAWAY = 0x7;

	public static final int WINDOW_UPDATE = 0x8;

	public static final int FLAG_PADDED = 0x8;

	public static final int SETTINGS_MAX_CONCURRENT_STREAMS = 0x3;

	public static final int REFUSED_STREAM = 0x7;

	public static final int FLOW_CONTROL_ERROR = 0x3;

	public static final int FLAG_END_STREAM = 0x1;

	public static final int FLAG_ACK = 0x1;

	public static final int FLAG_END_HEADERS = 0x4;

	private TestH2() {
	}

	public record Frame(int type, int flags, int streamId, byte[] payload) {

		boolean endStream() {
			return (this.flags & FLAG_END_STREAM) != 0 && this.type == DATA;
		}

	}

	public static void writeFrame(OutputStream out, int type, int flags, int streamId, byte[] payload)
			throws IOException {
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
	public static void writeClientHead(OutputStream out, int streamId, String... headers) throws IOException {
		out.write(PREFACE);
		writeFrame(out, SETTINGS, 0x0, 0, new byte[0]);
		writeFrame(out, HEADERS, FLAG_END_HEADERS | FLAG_END_STREAM, streamId, headerBlock(headers));
	}

	public static byte[] headerBlock(String... headers) {
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
	public static @Nullable Frame readFrame(InputStream in) throws IOException {
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

	/**
	 * Appends a literal-with-incremental-indexing header: the entry becomes dynamic index
	 * 62+order.
	 */
	public static byte[] appendLiteralIncremental(byte[] block, String name, String value) {
		ByteArrayOutputStream out = new ByteArrayOutputStream(block.length + 64);
		out.write(block, 0, block.length);
		byte[] nameBytes = name.getBytes(StandardCharsets.US_ASCII);
		byte[] valueBytes = value.getBytes(StandardCharsets.US_ASCII);
		out.write(0x40);
		out.write(nameBytes.length);
		out.writeBytes(nameBytes);
		out.write(valueBytes.length);
		out.writeBytes(valueBytes);
		return out.toByteArray();
	}

	/**
	 * Appends a reference to a dynamic-table entry (index >= 62) set by an earlier block.
	 */
	public static byte[] appendIndexedDynamic(byte[] block, int index) {
		ByteArrayOutputStream out = new ByteArrayOutputStream(block.length + 1);
		out.write(block, 0, block.length);
		out.write(0x80 | index);
		return out.toByteArray();
	}

	public static String bodyOf(List<Frame> frames) {
		return new String(contentOf(frames), StandardCharsets.UTF_8);
	}

	/** The DATA content of the frames, padding stripped. */
	public static byte[] contentOf(List<Frame> frames) {
		ByteArrayOutputStream body = new ByteArrayOutputStream();
		for (Frame frame : frames) {
			if (frame.type != DATA) {
				continue;
			}
			byte[] payload = frame.payload();
			if ((frame.flags & FLAG_PADDED) != 0 && payload.length > 0) {
				int padLength = payload[0] & 0xff;
				body.write(payload, 1, payload.length - 1 - padLength);
			}
			else {
				body.writeBytes(payload);
			}
		}
		return body.toByteArray();
	}

	/**
	 * The stub upstream's canned response: SETTINGS, HEADERS with {@code :status 200}
	 * (HPACK static-table index 8) and a DATA frame with END_STREAM.
	 */
	public static byte[] cannedResponse(int streamId, String body) {
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

	/**
	 * Reads and discards frames up to the completed client request head, contents unused;
	 * tolerates any framing the relay produced (settings, continuations).
	 */
	public static void readClientHead(java.io.InputStream in) throws IOException {
		if (in.readNBytes(PREFACE.length).length < PREFACE.length) {
			return;
		}
		Frame frame;
		while ((frame = readFrame(in)) != null) {
			boolean headFragment = frame.type == HEADERS || frame.type == CONTINUATION;
			if (headFragment && (frame.flags & FLAG_END_HEADERS) != 0) {
				return;
			}
		}
	}

	/** The 31-bit value at the start of the payload: a WINDOW_UPDATE increment. */
	public static int increment(Frame frame) {
		return int31(frame.payload(), 0);
	}

	/** The error code of a RST_STREAM (offset 0) or GOAWAY (offset 4) payload. */
	public static int errorCode(Frame frame) {
		int offset = frame.type() == GOAWAY ? 4 : 0;
		byte[] p = frame.payload();
		return (p[offset] & 0xff) << 24 | (p[offset + 1] & 0xff) << 16 | (p[offset + 2] & 0xff) << 8
				| (p[offset + 3] & 0xff);
	}

	public static byte[] int32(int value) {
		return new byte[] { (byte) (value >>> 24), (byte) (value >>> 16), (byte) (value >>> 8), (byte) value };
	}

	private static int int31(byte[] bytes, int offset) {
		return (bytes[offset] & 0x7f) << 24 | (bytes[offset + 1] & 0xff) << 16 | (bytes[offset + 2] & 0xff) << 8
				| (bytes[offset + 3] & 0xff);
	}

	static final class UncheckedIOException extends RuntimeException {

		UncheckedIOException(java.io.IOException cause) {
			super(cause);
		}

	}

}
