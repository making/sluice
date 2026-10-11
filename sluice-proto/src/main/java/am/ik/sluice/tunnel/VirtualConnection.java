package am.ik.sluice.tunnel;

import org.jspecify.annotations.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.Objects;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.ReentrantLock;

/**
 * One multiplexed virtual TCP connection over the tunnel stream (the port of
 * remotedialer's connection.go).
 * <p>
 * The inbound direction is an unbounded-number-of-writers / single-reader queue; DATA is
 * bounded (a producer blocks in {@link #acceptData(byte[])} to propagate backpressure to
 * the gRPC stream) while close/failure markers are never delayed by it. The outbound
 * direction forwards to the session's {@link FrameWriter}. CLOSE is honored as a
 * half-close so the opposite direction may keep flowing (required for HTTP keep-alive and
 * half-duplex protocols).
 */
public final class VirtualConnection implements AutoCloseable {

	private static final int QUEUE_CAPACITY = 128; // x 64KiB chunks

	/** Zero-length marker meaning end of the inbound stream. */
	private static final byte[] EOF = {};

	private sealed interface Inbound permits Chunk, Eof, Failure {

	}

	private record Chunk(byte[] data) implements Inbound {
	}

	private record Eof() implements Inbound {
	}

	private record Failure(String message) implements Inbound {
	}

	private final long connectionId;

	private final FrameWriter frameWriter;

	private final LinkedBlockingDeque<Inbound> inbound = new LinkedBlockingDeque<>();

	/**
	 * Bounds the buffered DATA chunks; released by {@link Source#read}. Markers are not
	 * counted, so remote close/failure can never be delayed by the data bound.
	 */
	private final Semaphore inboundPermits = new Semaphore(QUEUE_CAPACITY);

	private final Source source = new Source();

	private final Sink sink = new Sink();

	private final ReentrantLock writeLock = new ReentrantLock();

	private volatile boolean closed;

	private volatile boolean readClosed;

	private volatile boolean writeClosed;

	private volatile @Nullable String failure;

	public VirtualConnection(long connectionId, FrameWriter frameWriter) {
		this.connectionId = connectionId;
		this.frameWriter = frameWriter;
	}

	public long connectionId() {
		return this.connectionId;
	}

	/** Blocking end for the locally consumed direction. */
	public InputStream source() {
		return this.source;
	}

	/** Origin end for the locally produced direction. */
	public OutputStream sink() {
		return this.sink;
	}

	/**
	 * Feeds a chunk received from the remote side. Blocks while the DATA bound (128
	 * chunks) is reached, which applies backpressure to the gRPC stream; markers queued
	 * by {@link #remoteClosed()} / {@link #remoteFailed(String)} are never delayed by it.
	 */
	public void acceptData(byte[] data) {
		if (this.readClosed || this.closed) {
			return;
		}
		Chunk chunk = new Chunk(Arrays.copyOf(data, data.length));
		try {
			this.inboundPermits.acquire();
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return;
		}
		if (this.readClosed || this.closed) {
			this.inboundPermits.release();
			return;
		}
		this.inbound.offerLast(chunk);
	}

	/**
	 * The remote half-closed its -> local direction; the local side may continue writing.
	 */
	public void remoteClosed() {
		this.inbound.offerLast(new Eof());
	}

	/** The remote terminated the connection abnormally. */
	public void remoteFailed(String message) {
		this.failure = message;
		this.inbound.offerLast(new Failure(message == null ? "remote failure" : message));
	}

	@Override
	public void close() {
		this.closeSink();
		this.closed = true;
		this.readClosed = true;
		this.inbound.clear();
		this.inbound.offer(new Eof()); // wake a blocked reader; queue was cleared
	}

	private void closeSink() {
		this.writeLock.lock();
		boolean notify;
		try {
			notify = !this.writeClosed;
			this.writeClosed = true;
		}
		finally {
			this.writeLock.unlock();
		}
		if (notify) {
			this.frameWriter.sendClose(this.connectionId);
		}
	}

	private final class Source extends InputStream {

		private byte @Nullable [] pending;

		private int pendingPos;

		private int pendingLength;

		private boolean eofSeen;

		@Override
		public int read() throws IOException {
			byte[] one = new byte[1];
			int n = read(one, 0, 1);
			return n == 1 ? one[0] & 0xff : -1;
		}

		@Override
		public int read(byte[] b, int off, int len) throws IOException {
			Objects.requireNonNull(b, "b is required");
			if (off < 0 || len < 0 || off + len > b.length) {
				throw new IndexOutOfBoundsException("off=%d len=%d b.length=%d".formatted(off, len, b.length));
			}
			if (len == 0) {
				return 0;
			}
			while (true) {
				if (this.pending != null) {
					int n = Math.min(len, this.pendingLength - this.pendingPos);
					System.arraycopy(this.pending, this.pendingPos, b, off, n);
					this.pendingPos += n;
					if (this.pendingPos == this.pendingLength) {
						this.pending = null;
					}
					return n;
				}
				if (this.eofSeen) {
					return -1;
				}
				Inbound inbound;
				try {
					inbound = VirtualConnection.this.inbound.take();
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					throw new IOException("interrupted while waiting for inbound data", e);
				}
				switch (inbound) {
					case Chunk chunk -> {
						VirtualConnection.this.inboundPermits.release();
						if (chunk.data().length == 0) {
							this.eofSeen = true;
						}
						else {
							this.pending = chunk.data();
							this.pendingPos = 0;
							this.pendingLength = chunk.data().length;
						}
					}
					case Eof eof -> this.eofSeen = true;
					case Failure failure -> {
						this.eofSeen = true;
						throw new IOException(failure.message());
					}
				}
			}
		}

	}

	private final class Sink extends OutputStream {

		@Override
		public void write(int b) throws IOException {
			write(new byte[] { (byte) b }, 0, 1);
		}

		@Override
		public void write(byte[] b, int off, int len) throws IOException {
			Objects.requireNonNull(b, "b is required");
			if (off < 0 || len < 0 || off + len > b.length) {
				throw new IndexOutOfBoundsException("off=%d len=%d b.length=%d".formatted(off, len, b.length));
			}
			VirtualConnection.this.writeLock.lock();
			try {
				if (VirtualConnection.this.writeClosed || VirtualConnection.this.closed) {
					throw new IOException("connection %d is closed".formatted(VirtualConnection.this.connectionId));
				}
			}
			finally {
				VirtualConnection.this.writeLock.unlock();
			}
			byte[] payload = Arrays.copyOfRange(b, off, off + len);
			if (!VirtualConnection.this.frameWriter.sendData(VirtualConnection.this.connectionId, payload)) {
				throw new IOException("tunnel stream closed");
			}
		}

		/**
		 * Half-closes the local -> remote direction; the remote may keep sending.
		 */
		@Override
		public void close() {
			closeSink();
		}

	}

}
