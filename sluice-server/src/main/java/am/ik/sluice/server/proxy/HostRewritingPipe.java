package am.ik.sluice.server.proxy;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Objects;

import org.jspecify.annotations.Nullable;

import am.ik.sluice.tunnel.DuplexPipe;

/**
 * Wraps the local {@link DuplexPipe} of a relayed HTTP/2 connection so that every
 * request's {@code :authority} carries the route authority -- h2 multiplexes requests, so
 * every HEADERS block is re-encoded, keeping connections correct where the one-shot
 * connection-head rewrite only ever saw the first stream. (The HTTP/1.1 counterpart is
 * the rerouting relay, which resolves the route of every request head.) The
 * upstream-facing direction is relayed untouched.
 * <p>
 * Rewriting is best-effort: any structure the rewriter cannot reproduce (upgrades, HPACK
 * failures) degrades that connection to verbatim passthrough.
 */
final class HostRewritingPipe implements DuplexPipe {

	private final DuplexPipe delegate;

	private final byte[] seed;

	private final Rewriter rewriter;

	private HostRewritingPipe(DuplexPipe delegate, byte[] seed, Rewriter rewriter) {
		this.delegate = delegate;
		this.seed = seed;
		this.rewriter = rewriter;
	}

	/**
	 * Wraps the pipe with a per-request authority rewriter; {@code null} when the
	 * connection is not plain HTTP/2 (TLS passthrough, hostless protocols, h1 relayed by
	 * the rerouting relay) or the authority is blank.
	 */
	static @Nullable HostRewritingPipe of(DuplexPipe delegate, ConnectionHeadParser.Head head, String authority) {
		boolean h2 = head.h2() != null || head.request() != null && "2".equals(head.request().version());
		if (!h2 || head.encrypted() || head.host() == null || authority == null || authority.isBlank()) {
			return null;
		}
		return new HostRewritingPipe(delegate, head.bytes(), new Http2Rewriter(authority));
	}

	/** Consumes bytes of the local stream and emits the rewritten equivalents. */
	interface Rewriter {

		void rewrite(byte[] bytes, int offset, int length, Sink sink);

		/** Flushes partially consumed state at end of input into the sink. */
		void finish(Sink sink);

	}

	/** Receives the bytes to relay after rewriting. */
	interface Sink {

		void accept(byte[] bytes, int offset, int length);

		default void accept(byte value) {
			this.accept(new byte[] { value }, 0, 1);
		}

	}

	@Override
	public InputStream source() {
		return new RewritingInputStream(this.delegate.source(), this.seed, this.rewriter);
	}

	@Override
	public OutputStream sink() {
		return this.delegate.sink();
	}

	@Override
	public void shutdownOutput() throws Exception {
		this.delegate.shutdownOutput();
	}

	@Override
	public void close() throws Exception {
		this.delegate.close();
	}

	/**
	 * Streams the rewriter output: the already consumed head bytes are seeded in front of
	 * the underlying stream, rewritten chunks are buffered in a compacting queue.
	 */
	private static final class RewritingInputStream extends InputStream implements Sink {

		private static final int CHUNK = 16 * 1024;

		private final InputStream source;

		private final Rewriter rewriter;

		private final byte[] chunk = new byte[CHUNK];

		private byte[] buffer = new byte[2048];

		private int length;

		private int position;

		private boolean eof;

		RewritingInputStream(InputStream source, byte[] seed, Rewriter rewriter) {
			this.source = source;
			this.rewriter = rewriter;
			// the head bytes were consumed from the socket during routing: replay them
			// through the rewriter so the first request is rewritten like any other
			this.rewriter.rewrite(seed, 0, seed.length, this);
		}

		private void fill() throws IOException {
			while (this.position >= this.length && !this.eof) {
				int n = this.source.read(this.chunk);
				if (n < 0) {
					this.rewriter.finish(this);
					this.eof = true;
				}
				else {
					this.rewriter.rewrite(this.chunk, 0, n, this);
				}
			}
		}

		@Override
		public int read() throws IOException {
			this.fill();
			return this.position < this.length ? this.buffer[this.position++] & 0xff : -1;
		}

		@Override
		public int read(byte[] out, int off, int len) throws IOException {
			Objects.checkFromIndexSize(off, len, out.length);
			this.fill();
			int n = Math.min(len, this.length - this.position);
			if (len > 0 && n <= 0) {
				return -1;
			}
			System.arraycopy(this.buffer, this.position, out, off, n);
			this.position += n;
			return n;
		}

		@Override
		public int available() {
			return this.length - this.position;
		}

		@Override
		public void accept(byte[] bytes, int offset, int added) {
			if (this.position > 0) {
				// compact the consumed prefix
				System.arraycopy(this.buffer, this.position, this.buffer, 0, this.length - this.position);
				this.length -= this.position;
				this.position = 0;
			}
			int needed = this.length + added;
			if (needed > this.buffer.length) {
				this.buffer = Arrays.copyOf(this.buffer, Math.max(this.buffer.length * 2, needed));
			}
			System.arraycopy(bytes, offset, this.buffer, this.length, added);
			this.length = needed;
		}

	}

}
