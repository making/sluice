import java.util.concurrent.atomic.AtomicLongArray;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

/**
 * Counts the raw bytes of a connection per direction and, for HTTP/2, per frame type
 * (frame header included). Sits first in the pipeline, so it sees the bytes on the
 * socket. Shared tallies: one instance of {@link Totals} per process side.
 */
@ChannelHandler.Sharable
final class FrameTally extends ChannelDuplexHandler {

	static final String[] TYPES = { "DATA", "HEADERS", "PRIORITY", "RST_STREAM", "SETTINGS", "PUSH_PROMISE", "PING",
			"GOAWAY", "WINDOW_UPDATE", "CONTINUATION", "OTHER" };

	/** Byte totals: [0..10] per frame type, [11] raw total, for in and out. */
	static final class Totals {

		final AtomicLongArray in = new AtomicLongArray(12);

		final AtomicLongArray out = new AtomicLongArray(12);

		String json() {
			StringBuilder sb = new StringBuilder("{\"in\":{");
			append(sb, this.in);
			sb.append("},\"out\":{");
			append(sb, this.out);
			return sb.append("}}").toString();
		}

		private static void append(StringBuilder sb, AtomicLongArray a) {
			for (int i = 0; i < TYPES.length; i++) {
				sb.append('"').append(TYPES[i]).append("\":").append(a.get(i)).append(',');
			}
			sb.append("\"raw\":").append(a.get(11));
		}

	}

	/** Per-connection parser state (a parser per direction), attached as a handler. */
	static final class Parser {

		private final AtomicLongArray tally;

		private final boolean h2;

		private int preface;

		private final byte[] header = new byte[9];

		private int headerFilled;

		private long frameRemaining;

		private int type;

		Parser(AtomicLongArray tally, boolean h2, int prefaceBytes) {
			this.tally = tally;
			this.h2 = h2;
			this.preface = prefaceBytes;
		}

		void feed(ByteBuf buf) {
			int n = buf.readableBytes();
			this.tally.addAndGet(11, n);
			if (!this.h2) {
				return;
			}
			int i = buf.readerIndex();
			int end = i + n;
			while (i < end) {
				if (this.preface > 0) {
					int k = Math.min(this.preface, end - i);
					this.preface -= k;
					i += k;
					continue;
				}
				if (this.frameRemaining > 0) {
					int k = (int) Math.min(this.frameRemaining, end - i);
					this.frameRemaining -= k;
					this.tally.addAndGet(this.type, k);
					i += k;
					continue;
				}
				this.header[this.headerFilled++] = buf.getByte(i++);
				if (this.headerFilled == 9) {
					this.headerFilled = 0;
					int length = ((this.header[0] & 0xff) << 16) | ((this.header[1] & 0xff) << 8)
							| (this.header[2] & 0xff);
					int t = this.header[3] & 0xff;
					this.type = t < 10 ? t : 10;
					this.tally.addAndGet(this.type, 9);
					this.frameRemaining = length;
				}
			}
		}

	}

	private final Totals totals;

	private final boolean h2;

	private final boolean server;

	FrameTally(Totals totals, boolean h2, boolean server) {
		this.totals = totals;
		this.h2 = h2;
		this.server = server;
	}

	/** A per-connection handler: parser state is per channel. */
	ChannelDuplexHandler forConnection() {
		Parser in = new Parser(this.totals.in, this.h2, this.server ? 24 : 0);
		Parser out = new Parser(this.totals.out, this.h2, this.server ? 0 : 24);
		return new ChannelDuplexHandler() {
			@Override
			public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
				if (msg instanceof ByteBuf buf) {
					in.feed(buf);
				}
				ctx.fireChannelRead(msg);
			}

			@Override
			public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
				if (msg instanceof ByteBuf buf) {
					out.feed(buf);
				}
				ctx.write(msg, promise);
			}
		};
	}

}
