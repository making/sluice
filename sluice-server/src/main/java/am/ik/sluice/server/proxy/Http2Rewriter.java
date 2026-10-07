package am.ik.sluice.server.proxy;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http2.DefaultHttp2HeadersDecoder;
import io.netty.handler.codec.http2.DefaultHttp2HeadersEncoder;
import io.netty.handler.codec.http2.Http2Exception;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.util.AsciiString;

/**
 * Per-request h2 {@code :authority} rewriter: a frame level filter that decodes every
 * HEADERS block (assembled across CONTINUATION frames) with one stateful HPACK decoder,
 * sets {@code :authority}, and re-encodes it with one stateful HPACK encoder, splitting
 * blocks that exceed the default max frame size into HEADERS plus CONTINUATIONs. Because
 * every HEADERS block is re-encoded, the dynamic table the upstream decoder sees always
 * mirrors this encoder. Any decode/encode failure degrades the connection to verbatim
 * passthrough (matching the best-effort contract of the connection-head rewrite).
 */
final class Http2Rewriter implements HostRewritingPipe.Rewriter {

	private static final int FRAME_HEADER_LENGTH = 9;

	private static final int FRAME_HEADERS = 0x1;

	private static final int FRAME_CONTINUATION = 0x9;

	private static final int FLAG_END_STREAM = 0x1;

	private static final int FLAG_END_HEADERS = 0x4;

	private static final int FLAG_PADDED = 0x8;

	private static final int FLAG_PRIORITY = 0x20;

	/** Spec default; frames are split to this size unless the upstream says otherwise. */
	private static final int MAX_FRAME_SIZE = 16 * 1024;

	private final String authority;

	private final DefaultHttp2HeadersDecoder decoder = new DefaultHttp2HeadersDecoder(true);

	private final DefaultHttp2HeadersEncoder encoder = new DefaultHttp2HeadersEncoder();

	private final ByteArrayOutputStream pending = new ByteArrayOutputStream(1024);

	private final ByteArrayOutputStream block = new ByteArrayOutputStream(256);

	private final ByteArrayOutputStream raw = new ByteArrayOutputStream(256);

	private boolean magicDone;

	private boolean assembling;

	private boolean degraded;

	private int streamId;

	private boolean endStream;

	private int scanned;

	Http2Rewriter(String authority) {
		this.authority = authority;
		try {
			// mirror the connection head limit: headers beyond it are degraded to
			// passthrough anyway
			long maxHeaderListSize = 64 * 1024;
			this.encoder.maxHeaderListSize(maxHeaderListSize);
			this.decoder.configuration().maxHeaderListSize(maxHeaderListSize, maxHeaderListSize);
			// encode without the dynamic table (static indexing and literals only) so the
			// blocks we emit never depend on the encoder table state the upstream saw
			this.encoder.maxHeaderTableSize(0);
		}
		catch (Http2Exception e) {
			throw new IllegalStateException(e);
		}
	}

	@Override
	public void rewrite(byte[] bytes, int offset, int length, HostRewritingPipe.Sink sink) {
		this.pending.write(bytes, offset, length);
		this.drain(sink);
	}

	private void drain(HostRewritingPipe.Sink sink) {
		byte[] bytes = this.pending.toByteArray();
		while (true) {
			int available = bytes.length - this.scanned;
			if (!this.magicDone) {
				if (available < ConnectionHeadParser.H2_MAGIC.length()) {
					break;
				}
				sink.accept(bytes, this.scanned, ConnectionHeadParser.H2_MAGIC.length());
				this.scanned += ConnectionHeadParser.H2_MAGIC.length();
				this.magicDone = true;
				continue;
			}
			if (available < FRAME_HEADER_LENGTH) {
				break;
			}
			ByteBuffer header = ByteBuffer.wrap(bytes, this.scanned, FRAME_HEADER_LENGTH);
			int frameLength = unsigned24(header);
			if (available < FRAME_HEADER_LENGTH + frameLength) {
				break; // frame not fully buffered yet
			}
			int type = header.get() & 0xff;
			int flags = header.get() & 0xff;
			int frameStreamId = header.getInt() & 0x7fffffff;
			int payload = this.scanned + FRAME_HEADER_LENGTH;
			int frameEnd = payload + frameLength;
			if (this.degraded || type != FRAME_HEADERS && type != FRAME_CONTINUATION) {
				sink.accept(bytes, this.scanned, FRAME_HEADER_LENGTH + frameLength);
			}
			else if (type == FRAME_HEADERS && !this.assembling) {
				this.assembling = true;
				this.streamId = frameStreamId;
				this.endStream = (flags & FLAG_END_STREAM) != 0;
				this.block.reset();
				this.raw.reset();
				this.raw.write(bytes, this.scanned, FRAME_HEADER_LENGTH + frameLength);
				this.appendFragment(bytes, payload, frameLength, flags, true);
				if ((flags & FLAG_END_HEADERS) != 0) {
					this.emit(sink);
					this.assembling = false;
				}
			}
			else if (type == FRAME_CONTINUATION && this.assembling) {
				this.raw.write(bytes, this.scanned, FRAME_HEADER_LENGTH + frameLength);
				this.block.write(bytes, payload, frameLength);
				if ((flags & FLAG_END_HEADERS) != 0) {
					this.emit(sink);
					this.assembling = false;
				}
			}
			else {
				// unexpected framing: relay verbatim
				sink.accept(bytes, this.scanned, FRAME_HEADER_LENGTH + frameLength);
			}
			this.scanned = frameEnd;
		}
		if (this.scanned > 0) {
			byte[] remainder = new byte[bytes.length - this.scanned];
			System.arraycopy(bytes, this.scanned, remainder, 0, remainder.length);
			this.pending.reset();
			this.pending.write(remainder, 0, remainder.length);
			this.scanned = 0;
		}
	}

	private void appendFragment(byte[] bytes, int payload, int length, int flags, boolean headers) {
		int start = payload;
		int fragmentLength = length;
		if ((flags & FLAG_PADDED) != 0 && length > 0) {
			int padLength = bytes[start] & 0xff;
			start += 1;
			fragmentLength -= 1 + padLength;
		}
		if ((flags & FLAG_PRIORITY) != 0 && headers && fragmentLength > 0) {
			start += 5;
			fragmentLength -= 5;
		}
		if (fragmentLength > 0) {
			this.block.write(bytes, start, fragmentLength);
		}
	}

	private void emit(HostRewritingPipe.Sink sink) {
		byte[] blockBytes = this.block.toByteArray();
		try {
			Http2Headers headers = this.decoder.decodeHeaders(this.streamId, Unpooled.wrappedBuffer(blockBytes));
			headers.authority(AsciiString.of(this.authority));
			ByteBuf encoded = Unpooled.buffer(256);
			this.encoder.encodeHeaders(this.streamId, headers, encoded);
			byte[] encodedBlock = new byte[encoded.readableBytes()];
			encoded.readBytes(encodedBlock);
			encoded.release();
			int offset = 0;
			boolean first = true;
			do {
				int length = Math.min(MAX_FRAME_SIZE, encodedBlock.length - offset);
				boolean last = offset + length >= encodedBlock.length;
				int type = first ? FRAME_HEADERS : FRAME_CONTINUATION;
				int flags = first && this.endStream ? FLAG_END_STREAM : 0;
				if (last) {
					flags |= FLAG_END_HEADERS;
				}
				this.emitFrame(sink, type, flags, this.streamId, encodedBlock, offset, length);
				first = false;
				offset += length;
			}
			while (offset < encodedBlock.length);
		}
		catch (Exception e) {
			// degrade: relay this and all following frames verbatim
			byte[] rawBytes = this.raw.toByteArray();
			sink.accept(rawBytes, 0, rawBytes.length);
			this.degraded = true;
		}
	}

	private void emitFrame(HostRewritingPipe.Sink sink, int type, int flags, int streamId, byte[] block, int offset,
			int length) {
		byte[] frame = new byte[FRAME_HEADER_LENGTH + length];
		frame[0] = (byte) (length >>> 16 & 0xff);
		frame[1] = (byte) (length >>> 8 & 0xff);
		frame[2] = (byte) (length & 0xff);
		frame[3] = (byte) type;
		frame[4] = (byte) flags;
		frame[5] = (byte) (streamId >>> 24 & 0x7f);
		frame[6] = (byte) (streamId >>> 16 & 0xff);
		frame[7] = (byte) (streamId >>> 8 & 0xff);
		frame[8] = (byte) (streamId & 0xff);
		System.arraycopy(block, offset, frame, FRAME_HEADER_LENGTH, length);
		sink.accept(frame, 0, frame.length);
	}

	private static int unsigned24(ByteBuffer buf) {
		return (buf.get() & 0xff) << 16 | (buf.get() & 0xff) << 8 | (buf.get() & 0xff);
	}

	@Override
	public void finish(HostRewritingPipe.Sink sink) {
		// a truncated frame is flushed verbatim
		byte[] bytes = this.pending.toByteArray();
		if (bytes.length - this.scanned > 0) {
			sink.accept(bytes, this.scanned, bytes.length - this.scanned);
		}
	}

}
