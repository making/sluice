package am.ik.sluice.server.proxy;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import am.ik.sluice.tunnel.DuplexPipe;

/**
 * A client pipe recording what a relay does with it, in order: every write, with its
 * bytes and when it began, failed ones included, and every half-close and close.
 */
final class RecordingPipe implements DuplexPipe {

	/** What the relay called. */
	enum Call {

		WRITE, SHUTDOWN_OUTPUT, CLOSE

	}

	/** A write the relay began at {@code nanoTime}. */
	record Write(long nanoTime, byte[] bytes) {
	}

	private final DuplexPipe pipe;

	private final OutputStream sink;

	private final List<Write> writes = new CopyOnWriteArrayList<>();

	private final List<Call> calls = new CopyOnWriteArrayList<>();

	RecordingPipe(DuplexPipe pipe) {
		this.pipe = pipe;
		this.sink = new FilterOutputStream(pipe.sink()) {

			@Override
			public void write(int b) throws IOException {
				write(new byte[] { (byte) b }, 0, 1);
			}

			@Override
			public void write(byte[] bytes, int offset, int length) throws IOException {
				byte[] copy = new byte[length];
				System.arraycopy(bytes, offset, copy, 0, length);
				RecordingPipe.this.writes.add(new Write(System.nanoTime(), copy));
				RecordingPipe.this.calls.add(Call.WRITE);
				this.out.write(bytes, offset, length);
			}

		};
	}

	/** The writes begun so far. */
	List<Write> writes() {
		return List.copyOf(this.writes);
	}

	/** The calls so far. */
	List<Call> calls() {
		return List.copyOf(this.calls);
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
		this.calls.add(Call.SHUTDOWN_OUTPUT);
		this.pipe.shutdownOutput();
	}

	@Override
	public void close() throws Exception {
		this.calls.add(Call.CLOSE);
		this.pipe.close();
	}

}
