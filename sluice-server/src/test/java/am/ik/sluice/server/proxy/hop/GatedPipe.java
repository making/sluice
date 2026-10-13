package am.ik.sluice.server.proxy.hop;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import am.ik.sluice.tunnel.DuplexPipe;

/** A pipe whose reads can be held: a peer that stops reading its socket. */
final class GatedPipe implements DuplexPipe {

	private final DuplexPipe delegate;

	private final ReentrantLock lock = new ReentrantLock();

	private final Condition changed = this.lock.newCondition();

	private boolean paused;

	private final InputStream source;

	GatedPipe(DuplexPipe delegate) {
		this.delegate = delegate;
		this.source = new FilterInputStream(delegate.source()) {

			@Override
			public int read(byte[] b, int off, int len) throws IOException {
				awaitResumed();
				return super.read(b, off, len);
			}

			@Override
			public int read() throws IOException {
				awaitResumed();
				return super.read();
			}

		};
	}

	void pause() {
		this.lock.lock();
		try {
			this.paused = true;
		}
		finally {
			this.lock.unlock();
		}
	}

	void resume() {
		this.lock.lock();
		try {
			this.paused = false;
			this.changed.signalAll();
		}
		finally {
			this.lock.unlock();
		}
	}

	@Override
	public InputStream source() {
		return this.source;
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
		resume();
		this.delegate.close();
	}

	private void awaitResumed() throws InterruptedIOException {
		this.lock.lock();
		try {
			while (this.paused) {
				this.changed.await();
			}
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new InterruptedIOException();
		}
		finally {
			this.lock.unlock();
		}
	}

}
