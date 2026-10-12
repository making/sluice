package am.ik.sluice.server.proxy;

import java.io.FilterInputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.function.LongConsumer;

import am.ik.sluice.tunnel.DuplexPipe;

/**
 * A {@link DuplexPipe} reporting the bytes moved through it: every read returning bytes,
 * and every write as each of its pieces of
 * {@link ConnectionLimits.KeepAliveDeadline#WRITE_PIECE} completes -- a blocking write
 * tells nothing until it returns, so a large one is made in pieces.
 */
final class ProgressPipe implements DuplexPipe {

	private final DuplexPipe pipe;

	private final LongConsumer progress;

	private ProgressPipe(DuplexPipe pipe, LongConsumer progress) {
		this.pipe = pipe;
		this.progress = progress;
	}

	/** {@code pipe}, reporting the bytes moved through it to {@code progress}. */
	static DuplexPipe of(DuplexPipe pipe, LongConsumer progress) {
		return new ProgressPipe(pipe, progress);
	}

	@Override
	public InputStream source() {
		return new FilterInputStream(this.pipe.source()) {

			@Override
			public int read() throws IOException {
				int b = super.read();
				if (b >= 0) {
					ProgressPipe.this.progress.accept(1);
				}
				return b;
			}

			@Override
			public int read(byte[] buffer, int offset, int length) throws IOException {
				int n = super.read(buffer, offset, length);
				if (n > 0) {
					ProgressPipe.this.progress.accept(n);
				}
				return n;
			}

		};
	}

	@Override
	public OutputStream sink() {
		return new FilterOutputStream(this.pipe.sink()) {

			@Override
			public void write(int b) throws IOException {
				this.out.write(b);
				ProgressPipe.this.progress.accept(1);
			}

			@Override
			public void write(byte[] bytes, int offset, int length) throws IOException {
				for (int written = 0; written < length;) {
					int piece = Math.min(ConnectionLimits.KeepAliveDeadline.WRITE_PIECE, length - written);
					this.out.write(bytes, offset + written, piece);
					written += piece;
					ProgressPipe.this.progress.accept(piece);
				}
			}

		};
	}

	@Override
	public void shutdownOutput() throws Exception {
		this.pipe.shutdownOutput();
	}

	@Override
	public void close() throws Exception {
		this.pipe.close();
	}

}
