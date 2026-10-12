package am.ik.sluice.spike.h2demux;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.DefaultHttp2HeadersEncoder;
import io.netty.handler.codec.http2.Http2ConnectionHandler;
import io.netty.handler.codec.http2.Http2FrameAdapter;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2Settings;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Single-threaded: one netty HTTP/2 server endpoint fed a client's bytes in socket-sized
 * chunks, its listener keeping every DATA payload as a retained slice of the decoder's
 * cumulation -- what the relay does while a leg's flow controller holds the frame. Checks
 * every payload as it is parsed.
 */
class RetainedSliceProbeTest {

	@Test
	void payloadsParsedWhileEarlierSlicesAreRetained() throws Exception {
		int total = 1024 * 1024;
		byte[] wire = clientBytes(total);
		List<ByteBuf> retained = new ArrayList<>();
		long[] offset = { 0 };
		int[] mismatches = { 0 };
		Http2ConnectionHandler handler = Http2Handlers.spec()
			.server(true)
			.settings(new Http2Settings().initialWindowSize(Integer.MAX_VALUE))
			.autoRefillConnectionWindow(true)
			.listener(new Http2FrameAdapter() {

				@Override
				public int onDataRead(ChannelHandlerContext ctx, int streamId, ByteBuf data, int padding,
						boolean endOfStream) {
					for (int i = 0; i < data.readableBytes(); i++) {
						if (data.getByte(data.readerIndex() + i) != (byte) ((offset[0] + i) % 251)) {
							mismatches[0]++;
							break;
						}
					}
					offset[0] += data.readableBytes();
					retained.add(data.retainedSlice());
					if (retained.size() > 8) {
						retained.remove(0).release();
					}
					return 0;
				}

			})
			.build();
		EmbeddedChannel channel = new EmbeddedChannel(handler);
		Random random = new Random(1);
		for (int position = 0; position < wire.length;) {
			int n = Math.min(wire.length - position, 1 + random.nextInt(16 * 1024));
			channel.writeInbound(Unpooled.copiedBuffer(wire, position, n));
			position += n;
			for (Object out = channel.readOutbound(); out != null; out = channel.readOutbound()) {
				((ByteBuf) out).release();
			}
		}
		retained.forEach(ByteBuf::release);
		channel.finishAndReleaseAll();
		assertThat(offset[0]).isEqualTo(total);
		assertThat(mismatches[0]).isZero();
	}

	private static byte[] clientBytes(int total) throws Exception {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		out.writeBytes("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".getBytes());
		out.writeBytes(frame(0x4, 0, 0, new byte[0]));
		// the ack of the server's SETTINGS: its large initial window applies from here
		out.writeBytes(frame(0x4, 0x1, 0, new byte[0]));
		Http2Headers headers = new DefaultHttp2Headers().method("POST").scheme("http").authority("a.test").path("/");
		ByteBuf block = Unpooled.buffer();
		new DefaultHttp2HeadersEncoder().encodeHeaders(1, headers, block);
		byte[] blockBytes = new byte[block.readableBytes()];
		block.readBytes(blockBytes);
		out.writeBytes(frame(0x1, 0x4, 1, blockBytes));
		byte[] body = TestUpstream.pattern(total);
		for (int at = 0; at < total; at += 16 * 1024) {
			int length = Math.min(16 * 1024, total - at);
			byte[] payload = new byte[length];
			System.arraycopy(body, at, payload, 0, length);
			out.writeBytes(frame(0x0, at + length == total ? 0x1 : 0, 1, payload));
		}
		return out.toByteArray();
	}

	private static byte[] frame(int type, int flags, int stream, byte[] payload) {
		byte[] frame = new byte[9 + payload.length];
		frame[0] = (byte) (payload.length >>> 16);
		frame[1] = (byte) (payload.length >>> 8);
		frame[2] = (byte) payload.length;
		frame[3] = (byte) type;
		frame[4] = (byte) flags;
		frame[8] = (byte) stream;
		System.arraycopy(payload, 0, frame, 9, payload.length);
		return frame;
	}

}
