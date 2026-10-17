package am.ik.sluice.server.proxy;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import am.ik.sluice.tunnel.DuplexPipe;

/**
 * A client pipe whose writes take as long as the bytes they carry at {@code rate} bytes
 * per second before they pass on: a client taking the bytes at that rate. Counts the
 * bytes that passed and the largest write.
 */
final class RateLimitedPipe implements DuplexPipe {

	private final DuplexPipe pipe;

	private final OutputStream sink;

	private final AtomicLong written = new AtomicLong();

	private final AtomicLong largestWrite = new AtomicLong();

	RateLimitedPipe(DuplexPipe pipe, int rate) {
		this.pipe = pipe;
		this.sink = new FilterOutputStream(pipe.sink()) {

			@Override
			public void write(int b) throws IOException {
				write(new byte[] { (byte) b }, 0, 1);
			}

			@Override
			public void write(byte[] bytes, int offset, int length) throws IOException {
				RateLimitedPipe.this.largestWrite.accumulateAndGet(length, Math::max);
				try {
					Thread.sleep(Duration.ofNanos(length * 1_000_000_000L / rate));
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					throw new InterruptedIOException();
				}
				this.out.write(bytes, offset, length);
				RateLimitedPipe.this.written.addAndGet(length);
			}

		};
	}

	/** The bytes that passed on. */
	long written() {
		return this.written.get();
	}

	/** The most bytes one write carried. */
	long largestWrite() {
		return this.largestWrite.get();
	}

	@Override
	public InputStream source() {
		return this.pipe.source();
	}

	@Override
	public OutputStream sink() {
		return this.sink;
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
