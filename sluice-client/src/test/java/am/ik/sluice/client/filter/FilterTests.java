package am.ik.sluice.client.filter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;

import am.ik.sluice.tunnel.DuplexPipe;
import am.ik.sluice.tunnel.TunnelBudget;

/**
 * Filters behind the pipe with no network, on the virtual thread a connection has: the
 * same on a JVM and, with {@code -PnativeTest}, on a native image.
 */
final class FilterTests {

	private FilterTests() {
	}

	/** What the relay would read and what it wrote. */
	static final class ScriptedPipe implements DuplexPipe {

		private final InputStream in;

		private final ByteArrayOutputStream requests = new ByteArrayOutputStream();

		ScriptedPipe(byte[] response, int segment) {
			this.in = new ByteArrayInputStream(response) {

				@Override
				public synchronized int read(byte[] bytes, int offset, int length) {
					return super.read(bytes, offset, Math.min(length, segment));
				}

			};
		}

		@Override
		public InputStream source() {
			return this.in;
		}

		@Override
		public OutputStream sink() {
			return this.requests;
		}

		@Override
		public void shutdownOutput() {
		}

		@Override
		public void close() {
		}

	}

	/** An upstream answering when told, on a read that blocks as a socket's does. */
	static final class BlockingUpstream implements DuplexPipe {

		private static final byte[] END = new byte[0];

		private final LinkedBlockingQueue<byte[]> responses = new LinkedBlockingQueue<>();

		private final ByteArrayOutputStream requests = new ByteArrayOutputStream();

		private final InputStream in = new InputStream() {

			private byte[] current = new byte[0];

			private int position;

			@Override
			public int read() throws IOException {
				byte[] one = new byte[1];
				return read(one, 0, 1) < 0 ? -1 : one[0] & 0xff;
			}

			@Override
			public int read(byte[] bytes, int offset, int length) throws IOException {
				while (this.position == this.current.length) {
					if (this.current == END) {
						return -1;
					}
					try {
						this.current = BlockingUpstream.this.responses.take();
						this.position = 0;
					}
					catch (InterruptedException e) {
						throw new IOException(e);
					}
				}
				int n = Math.min(length, this.current.length - this.position);
				System.arraycopy(this.current, this.position, bytes, offset, n);
				this.position += n;
				return n;
			}

		};

		void answer(String response) {
			this.responses.add(response.getBytes(StandardCharsets.ISO_8859_1));
		}

		void end() {
			this.responses.add(END);
		}

		String received() {
			synchronized (this.requests) {
				return this.requests.toString(StandardCharsets.ISO_8859_1);
			}
		}

		@Override
		public InputStream source() {
			return this.in;
		}

		@Override
		public OutputStream sink() {
			return new OutputStream() {

				@Override
				public void write(int b) {
					write(new byte[] { (byte) b }, 0, 1);
				}

				@Override
				public void write(byte[] bytes, int offset, int length) {
					synchronized (BlockingUpstream.this.requests) {
						BlockingUpstream.this.requests.write(bytes, offset, length);
					}
				}

			};
		}

		@Override
		public void shutdownOutput() {
		}

		@Override
		public void close() {
			end();
		}

	}

	static UpstreamFilterPipe pipe(DuplexPipe upstream, UpstreamFilter filter) {
		return UpstreamFilterPipe.builder()
			.upstream(upstream)
			.filter(filter)
			.route("route.test")
			.address("upstream:1")
			.budget(TunnelBudget.unlimited())
			.build();
	}

	/** What the upstream read of the requests, and what the relay read of responses. */
	record Exchanged(String upstream, String relayed) {
	}

	static <T> T onAVirtualThread(Callable<T> action) throws Exception {
		AtomicReference<T> result = new AtomicReference<>();
		AtomicReference<Exception> failure = new AtomicReference<>();
		Thread.ofVirtual().start(() -> {
			try {
				result.set(action.call());
				if (Thread.currentThread().isInterrupted()) {
					failure.set(new IllegalStateException("a plugin's interrupt reached the connection"));
				}
			}
			catch (Exception e) {
				failure.set(e);
			}
		}).join();
		if (failure.get() != null) {
			throw failure.get();
		}
		return result.get();
	}

	static Exchanged exchange(UpstreamFilter filter, String requests, byte[] response, int segment) throws Exception {
		return exchange(filter, TunnelBudget.unlimited(), requests, response, segment);
	}

	static Exchanged exchange(UpstreamFilter filter, TunnelBudget budget, String requests, byte[] response, int segment)
			throws Exception {
		return onAVirtualThread(() -> {
			ScriptedPipe upstream = new ScriptedPipe(response, segment);
			try (UpstreamFilterPipe pipe = UpstreamFilterPipe.builder()
				.upstream(upstream)
				.filter(filter)
				.route("route.test")
				.address("upstream:1")
				.budget(budget)
				.build()) {
				byte[] bytes = requests.getBytes(StandardCharsets.ISO_8859_1);
				// in pieces, as the relay's pump may have them
				for (int at = 0; at < bytes.length; at += segment) {
					pipe.sink().write(bytes, at, Math.min(segment, bytes.length - at));
				}
				String relayed = new String(pipe.source().readAllBytes(), StandardCharsets.ISO_8859_1);
				return new Exchanged(upstream.requests.toString(StandardCharsets.ISO_8859_1), relayed);
			}
		});
	}

	static String relayed(UpstreamFilter filter, String method, String target, String response, int segment)
			throws Exception {
		return exchange(filter, head(method, target), response.getBytes(StandardCharsets.UTF_8), segment).relayed();
	}

	static String head(String method, String target) {
		return method + " " + target + " HTTP/1.1\r\nHost: api.test\r\n\r\n";
	}

	static byte[] gzip(byte[] bytes) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		try (GZIPOutputStream gzip = new GZIPOutputStream(out)) {
			gzip.write(bytes);
		}
		return out.toByteArray();
	}

	static byte[] gunzip(byte[] bytes) throws IOException {
		try (InputStream in = new GZIPInputStream(new ByteArrayInputStream(bytes))) {
			return in.readAllBytes();
		}
	}

	/** A chunked response as relayed: its head, and the payload of its chunks. */
	record Chunked(String head, byte[] body) {

		static Chunked of(String relayed) {
			int end = relayed.indexOf("\r\n\r\n") + 4;
			byte[] bytes = relayed.substring(end).getBytes(StandardCharsets.ISO_8859_1);
			ByteArrayOutputStream payload = new ByteArrayOutputStream();
			int at = 0;
			while (at < bytes.length) {
				int eol = at;
				while (bytes[eol] != '\r') {
					eol++;
				}
				int size = Integer.parseInt(new String(bytes, at, eol - at, StandardCharsets.US_ASCII), 16);
				payload.write(bytes, eol + 2, size);
				at = eol + 2 + size + 2;
			}
			return new Chunked(relayed.substring(0, end), payload.toByteArray());
		}

	}

	/** What a response's bytes read as, as {@link #exchange} gives them. */
	static String latin1(String text) {
		return new String(text.getBytes(StandardCharsets.UTF_8), StandardCharsets.ISO_8859_1);
	}

	/** The counters above zero: their names and tags but the route's. */
	static Map<String, Long> counters(MeterRegistry registry) {
		Map<String, Long> values = new TreeMap<>();
		for (Meter meter : registry.getMeters()) {
			if (meter instanceof Counter counter && counter.count() > 0) {
				List<String> tags = new ArrayList<>();
				meter.getId().getTags().forEach(tag -> {
					if (!tag.getKey().equals("route")) {
						tags.add(tag.getValue());
					}
				});
				String name = meter.getId().getName().replace("sluice.client.filter.", "");
				values.put(name + " " + String.join(" ", tags), (long) counter.count());
			}
		}
		return values;
	}

}
