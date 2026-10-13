import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.DefaultHttp2HeadersEncoder;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2HeadersEncoder;

/**
 * Encoded size and encode time of the harness's browser-like request head and the
 * upstream's response head: never-indexed literals (the relay's choice on both faces)
 * against netty's default (dynamic table), one stateful encoder per connection.
 */
public final class HpackBench {

	public static void main(String[] args) throws Exception {
		Http2Headers request = new DefaultHttp2Headers().method("GET")
			.scheme("http")
			.authority("h2.local")
			.path("/small");
		Load.browserHeaders((n, v) -> request.add(n, v));
		Http2Headers response = new DefaultHttp2Headers().status("200")
			.set("content-type", "text/plain;charset=utf-8")
			.set("date", "Tue, 13 Oct 2026 12:00:00 GMT")
			.set("server", "sluice-harness/1.0")
			.set("cache-control", "no-cache, no-store, must-revalidate")
			.set("vary", "Accept-Encoding")
			.set("x-request-id", "0b8f0d4e-3c1a-4c63-9a57-2f9b1f3e9d11")
			.set("content-length", "2");
		for (String which : new String[] { "request", "response" }) {
			Http2Headers h = which.equals("request") ? request : response;
			for (boolean never : new boolean[] { true, false }) {
				Http2HeadersEncoder.SensitivityDetector detector = never ? (n, v) -> true
						: Http2HeadersEncoder.NEVER_SENSITIVE;
				DefaultHttp2HeadersEncoder enc = new DefaultHttp2HeadersEncoder(detector);
				ByteBuf out = Unpooled.buffer(4096);
				// warm up, then measure the steady state (dynamic table filled)
				long size = 0;
				for (int i = 0; i < 2_000_000; i++) {
					out.clear();
					enc.encodeHeaders(1, h, out);
				}
				size = out.readableBytes();
				int n = 5_000_000;
				long t0 = System.nanoTime();
				for (int i = 0; i < n; i++) {
					out.clear();
					enc.encodeHeaders(1, h, out);
				}
				long ns = (System.nanoTime() - t0) / n;
				System.out.printf("%-8s %-13s %4d bytes %5d ns/encode%n", which, never ? "never-indexed" : "indexed",
						size, ns);
			}
		}
	}

}
