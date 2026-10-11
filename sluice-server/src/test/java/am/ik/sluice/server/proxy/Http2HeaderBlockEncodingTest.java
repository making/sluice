package am.ik.sluice.server.proxy;

import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.DefaultHttp2HeadersDecoder;
import io.netty.handler.codec.http2.Http2Headers;

import io.netty.buffer.Unpooled;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The demux re-encodes request heads so a leg decoder can start mid-connection (a leg
 * recreated on tunnel recovery, or a second upstream of a coalesced connection): every
 * emitted block must be decodable on its own, referencing no dynamic-table entry.
 */
class Http2HeaderBlockEncodingTest {

	@Test
	void reencodedBlocksAreSelfContained() throws Exception {
		var encoder = Http2DemuxRelay.newHeadersEncoder();
		var seen = new DefaultHttp2HeadersDecoder(true);
		var fresh = new DefaultHttp2HeadersDecoder(true);
		var first = new DefaultHttp2Headers().method("GET").scheme("https").authority("a.local");
		first.add("user-agent", "repeat");
		var second = new DefaultHttp2Headers().method("GET").scheme("https").authority("b.local");
		second.add("user-agent", "repeat");
		var firstBlock = encode(encoder, 1, first);
		var secondBlock = encode(encoder, 3, second);
		seen.decodeHeaders(1, firstBlock.duplicate());
		// a leg that joins now (decoder state empty) must decode the block alone
		Http2Headers decoded = fresh.decodeHeaders(3, secondBlock.duplicate());
		assertThat(decoded.method()).isNotEmpty();
		assertThat(decoded.authority().toString()).isEqualTo("b.local");
	}

	private static io.netty.buffer.ByteBuf encode(io.netty.handler.codec.http2.DefaultHttp2HeadersEncoder encoder,
			int streamId, io.netty.handler.codec.http2.Http2Headers headers) throws Exception {
		io.netty.buffer.ByteBuf encoded = io.netty.buffer.Unpooled.buffer(256);
		encoder.encodeHeaders(streamId, headers, encoded);
		return encoded;
	}

}
