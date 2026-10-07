package am.ik.sluice.server.proxy;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jspecify.annotations.Nullable;

import org.junit.jupiter.api.Test;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http2.DefaultHttp2HeadersDecoder;
import io.netty.handler.codec.http2.Http2Exception;
import io.netty.handler.codec.http2.Http2Headers;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link Http2Rewriter}: HEADERS blocks are decoded, re-encoded with the
 * route authority and split under the max frame size; the re-encoded stream must decode
 * with a single stateful decoder (the upstream's view) and non-header frames pass through
 * verbatim.
 */
class Http2RewriterTest {

	private static final String AUTHORITY = "127.0.0.1:3128";

	@Test
	void rewritesAuthorityOfTwoConsecutiveRequests() {
		byte[] first = head(1, ":authority=demo.local", ":method=GET", ":path=/one");
		byte[] second = head(3, ":authority=demo.local", ":method=GET", ":path=/two");
		byte[] output = rewriteAll(preface(), first, second);
		Decoded decoded = decodeAll(output);
		assertThat(decoded.authorityOf(1)).isEqualTo(AUTHORITY);
		assertThat(decoded.pathOf(1)).isEqualTo("/one");
		assertThat(decoded.authorityOf(3)).isEqualTo(AUTHORITY);
		assertThat(decoded.pathOf(3)).isEqualTo("/two");
	}

	@Test
	void nonHeaderFramesPassThroughVerbatim() {
		byte[] ping = frame(0x6, 0x0, 0, new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 });
		byte[] head = head(1, ":authority=demo.local", ":method=GET", ":path=/");
		byte[] output = rewriteAll(preface(), ping, head);
		Frame pingFrame = framesOf(output).stream().filter(f -> f.type() == 0x6).findFirst().orElseThrow();
		assertThat(pingFrame.payload()).isEqualTo(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 });
	}

	@Test
	void endStreamFlagIsPreserved() {
		byte[] head = head(1, 0x1 | 0x4, headerBlock(":authority=demo.local", ":method=GET", ":path=/"));
		byte[] output = rewriteAll(preface(), head);
		Frame headers = framesOf(output).stream().filter(f -> f.type() == 0x1).findFirst().orElseThrow();
		assertThat(headers.flags() & 0x1).isEqualTo(0x1);
		assertThat(headers.streamId()).isEqualTo(1);
	}

	@Test
	void oversizedBlockIsSplitIntoContinuations() {
		String longPath = "/x".repeat(30000);
		byte[] head = head(1, ":authority=demo.local", ":method=GET", ":path=" + longPath);
		byte[] output = rewriteAll(preface(), head);
		List<Frame> headerFrames = framesOf(output).stream().filter(f -> f.type() == 0x1 || f.type() == 0x9).toList();
		assertThat(headerFrames.size()).as(framesOf(output).toString()).isGreaterThan(1);
		assertThat(headerFrames.getFirst().type()).isEqualTo(0x1);
		assertThat(headerFrames.getLast().flags() & 0x4).isEqualTo(0x4);
		for (Frame frame : headerFrames) {
			assertThat(frame.payload().length).isLessThanOrEqualTo(16 * 1024);
		}
		Decoded decoded = decodeAll(output);
		assertThat(decoded.pathOf(1)).as(decoded.errorOf(1)).isEqualTo(longPath);
		assertThat(decoded.authorityOf(1)).isEqualTo(AUTHORITY);
	}

	@Test
	void malformedBlockDegradesToPassthrough() {
		byte[] head = head(1, ":authority=demo.local", ":method=GET", ":path=/");
		// an indexed representation referencing header entry 0, which is illegal
		byte[] broken = head(5, 0x5, new byte[] { (byte) 0x80 });
		byte[] after = head(7, ":authority=demo.local", ":method=GET", ":path=/after");
		byte[] output = rewriteAll(preface(), head, broken, after);
		Decoded decoded = decodeAll(output);
		assertThat(decoded.authorityOf(1)).isEqualTo(AUTHORITY);
		// after the degradation the original bytes are relayed verbatim
		assertThat(framesOf(output).stream().filter(f -> f.streamId() == 5).count()).isEqualTo(1);
	}

	private static byte[] rewriteAll(byte[]... chunks) {
		Http2Rewriter rewriter = new Http2Rewriter(AUTHORITY);
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		HostRewritingPipe.Sink sink = (bytes, offset, length) -> out.write(bytes, offset, length);
		for (byte[] chunk : chunks) {
			rewriter.rewrite(chunk, 0, chunk.length, sink);
		}
		rewriter.finish(sink);
		return out.toByteArray();
	}

	record Frame(int type, int flags, int streamId, byte[] payload) {
	}

	private static List<Frame> framesOf(byte[] bytes) {
		List<Frame> frames = new ArrayList<>();
		int pos = ConnectionHeadParser.H2_MAGIC.length();
		while (pos + 9 <= bytes.length) {
			int length = (bytes[pos] & 0xff) << 16 | (bytes[pos + 1] & 0xff) << 8 | (bytes[pos + 2] & 0xff);
			int type = bytes[pos + 3] & 0xff;
			int flags = bytes[pos + 4] & 0xff;
			int streamId = (bytes[pos + 5] & 0x7f) << 24 | (bytes[pos + 6] & 0xff) << 16 | (bytes[pos + 7] & 0xff) << 8
					| (bytes[pos + 8] & 0xff);
			byte[] payload = new byte[Math.min(length, bytes.length - pos - 9)];
			System.arraycopy(bytes, pos + 9, payload, 0, payload.length);
			frames.add(new Frame(type, flags, streamId, payload));
			pos += 9 + length;
		}
		return frames;
	}

	/** Decodes the HEADERS blocks of the stream the way an upstream would: in order. */
	private static Decoded decodeAll(byte[] output) {
		DefaultHttp2HeadersDecoder decoder = new DefaultHttp2HeadersDecoder(true);
		try {
			// mirror the rewriter's own limits; the upstream's are unknown to the proxy
			decoder.configuration().maxHeaderListSize(64 * 1024, 64 * 1024);
		}
		catch (Http2Exception e) {
			throw new IllegalStateException(e);
		}
		Decoded decoded = new Decoded();
		byte[] block = null;
		int blockStreamId = -1;
		for (Frame frame : framesOf(output)) {
			if (frame.type() == 0x1) {
				block = frame.payload();
				blockStreamId = frame.streamId();
			}
			else if (frame.type() == 0x9 && block != null) {
				block = concat(block, frame.payload());
			}
			else {
				continue;
			}
			if ((frame.flags() & 0x4) != 0 && block != null) {
				try {
					Http2Headers headers = decoder.decodeHeaders(blockStreamId, Unpooled.wrappedBuffer(block));
					decoded.add(blockStreamId, textOf(headers.authority()), textOf(headers.path()));
				}
				catch (Http2Exception e) {
					// a degraded verbatim block may not decode; keep mirroring the stream
					decoded.addError(blockStreamId, e.toString());
				}
				block = null;
			}
		}
		return decoded;
	}

	private static @Nullable String textOf(CharSequence value) {
		return value == null ? null : value.toString();
	}

	private static byte[] concat(byte[] a, byte[] b) {
		byte[] out = new byte[a.length + b.length];
		System.arraycopy(a, 0, out, 0, a.length);
		System.arraycopy(b, 0, out, a.length, b.length);
		return out;
	}

	/** The headers seen per stream, mirroring the decoder state of an upstream. */
	private static final class Decoded {

		private final Map<Integer, String> authorities = new HashMap<>();

		private final Map<Integer, String> paths = new HashMap<>();

		private final Map<Integer, String> errors = new HashMap<>();

		void addError(int streamId, String error) {
			this.errors.put(streamId, error);
		}

		@Nullable String errorOf(int streamId) {
			return this.errors.get(streamId);
		}

		void add(int streamId, @Nullable String authority, @Nullable String path) {
			if (authority != null) {
				this.authorities.put(streamId, authority);
			}
			if (path != null) {
				this.paths.put(streamId, path);
			}
		}

		@Nullable String authorityOf(int streamId) {
			return this.authorities.get(streamId);
		}

		@Nullable String pathOf(int streamId) {
			return this.paths.get(streamId);
		}

	}

	private static byte[] head(int streamId, String... headers) {
		return head(streamId, 0x1 | 0x4, headerBlock(headers)); // END_STREAM |
																// END_HEADERS
	}

	private static byte[] preface() {
		byte[] magic = ConnectionHeadParser.H2_MAGIC.getBytes(StandardCharsets.US_ASCII);
		return concat(magic, frame(0x4, 0x0, 0, new byte[0]));
	}

	private static byte[] head(int streamId, int flags, byte[] block) {
		return frame(0x1, flags, streamId, block);
	}

	private static byte[] frame(int type, int flags, int streamId, byte[] payload) {
		return ByteBuffer.allocate(9 + payload.length)
			.put((byte) ((payload.length >>> 16) & 0xff))
			.put((byte) ((payload.length >>> 8) & 0xff))
			.put((byte) (payload.length & 0xff))
			.put((byte) type)
			.put((byte) flags)
			.put((byte) ((streamId >>> 24) & 0x7f))
			.put((byte) ((streamId >>> 16) & 0xff))
			.put((byte) ((streamId >>> 8) & 0xff))
			.put((byte) (streamId & 0xff))
			.put(payload)
			.array();
	}

	/** HPACK integer with the 7-bit continuation encoding. */
	private static void writeInt(ByteArrayOutputStream out, int value) {
		if (value < 127) {
			out.write(value);
			return;
		}
		out.write(127);
		int rest = value - 127;
		while (rest >= 128) {
			out.write(rest % 128 | 0x80);
			rest /= 128;
		}
		out.write(rest);
	}

	private static byte[] headerBlock(String... headers) {
		ByteArrayOutputStream block = new ByteArrayOutputStream();
		for (String header : headers) {
			int eq = header.indexOf('=');
			byte[] name = header.substring(0, eq).getBytes(StandardCharsets.US_ASCII);
			byte[] value = header.substring(eq + 1).getBytes(StandardCharsets.US_ASCII);
			block.write(0x00);
			writeInt(block, name.length);
			block.writeBytes(name);
			writeInt(block, value.length);
			block.writeBytes(value);
		}
		return block.toByteArray();
	}

}
