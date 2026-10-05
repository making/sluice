package am.ik.sluice.tunnel;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import org.jspecify.annotations.Nullable;

/**
 * Pumps bytes between a local {@link DuplexPipe} and a {@link VirtualConnection} in both
 * directions, one virtual thread per direction (the direct analogue of the two goroutines
 * per connection in the original implementation).
 * <p>
 * Half-close semantics: local EOF emits CLOSE to the remote while the local side keeps
 * reading; remote CLOSE shuts down the pipe output only.
 */
public final class StreamRelay {

	private static final int BUFFER_SIZE = 64 * 1024;

	private final DuplexPipe pipe;

	private final VirtualConnection connection;

	private final FrameWriter frameWriter;

	private final byte @Nullable [] prefix;

	private final @Nullable Listener listener;

	private final @Nullable Runnable onComplete;

	private StreamRelay(Builder builder) {
		this.pipe = builder.pipe;
		this.connection = builder.connection;
		this.frameWriter = builder.frameWriter;
		this.prefix = builder.prefix;
		this.listener = builder.listener;
		this.onComplete = builder.onComplete;
	}

	public static Builder builder(DuplexPipe pipe, VirtualConnection connection, FrameWriter frameWriter) {
		return new Builder(pipe, connection, frameWriter);
	}

	/** Direction of a relayed chunk. */
	public enum Direction {

		/** Local pipe towards the remote (peer to upstream). */
		TO_REMOTE,

		/** Remote towards the local pipe (upstream to peer). */
		TO_LOCAL

	}

	/** Observer of relay activity. */
	public interface Listener {

		/** Called for each chunk relayed towards the remote. */
		void onBytesRelayed(long count);

		/**
		 * Called for each relayed chunk when its direction is known; the default
		 * delegates to the direction-less variant.
		 */
		default void onBytesRelayed(long count, Direction direction) {
			this.onBytesRelayed(count);
		}

	}

	public static final class Builder {

		private final DuplexPipe pipe;

		private final VirtualConnection connection;

		private final FrameWriter frameWriter;

		private byte @Nullable [] prefix;

		private @Nullable Listener listener;

		private @Nullable Runnable onComplete;

		private Builder(DuplexPipe pipe, VirtualConnection connection, FrameWriter frameWriter) {
			this.pipe = Objects.requireNonNull(pipe, "pipe is required");
			this.connection = Objects.requireNonNull(connection, "connection is required");
			this.frameWriter = Objects.requireNonNull(frameWriter, "frameWriter is required");
		}

		/** Raw bytes injected before anything read from the pipe. */
		public Builder prefix(byte @Nullable [] prefix) {
			this.prefix = prefix;
			return this;
		}

		public Builder listener(@Nullable Listener listener) {
			this.listener = listener;
			return this;
		}

		public Builder onComplete(@Nullable Runnable onComplete) {
			this.onComplete = onComplete;
			return this;
		}

		public StreamRelay build() {
			return new StreamRelay(this);
		}

	}

	private final class Completion {

		private final AtomicInteger pending = new AtomicInteger(2);

		void done() {
			if (this.pending.decrementAndGet() == 0) {
				try {
					StreamRelay.this.pipe.close();
				}
				catch (Exception e) {
					// ignore
				}
				if (StreamRelay.this.onComplete != null) {
					StreamRelay.this.onComplete.run();
				}
			}
		}

	}

	/**
	 * Starts both relay directions on virtual threads.
	 */
	public void start() {
		Completion completion = new Completion();
		long connectionId = this.connection.connectionId();
		Thread.ofVirtual().name("sluice-relay-out-" + connectionId).start(() -> this.relayToRemote(completion));
		Thread.ofVirtual().name("sluice-relay-in-" + connectionId).start(() -> this.relayToLocal(completion));
	}

	private void relayToRemote(Completion completion) {
		byte[] buffer = new byte[BUFFER_SIZE];
		try {
			this.doRelayToRemote(buffer);
		}
		catch (Exception e) {
			this.frameWriter.sendError(this.connection.connectionId(), describe(e));
		}
		finally {
			completion.done();
		}
	}

	private void relayToLocal(Completion completion) {
		try {
			this.doRelayToLocal();
		}
		catch (Exception e) {
			this.abandon();
		}
		finally {
			completion.done();
		}
	}

	private void doRelayToRemote(byte[] buffer) throws Exception {
		if (this.prefix != null && this.prefix.length > 0) {
			if (this.listener != null) {
				this.listener.onBytesRelayed(this.prefix.length, Direction.TO_REMOTE);
			}
			this.frameWriter.sendData(this.connection.connectionId(), Arrays.copyOf(this.prefix, this.prefix.length));
		}
		InputStream in = this.pipe.source();
		int n;
		while ((n = in.read(buffer)) > 0) {
			if (this.listener != null) {
				this.listener.onBytesRelayed(n, Direction.TO_REMOTE);
			}
			this.frameWriter.sendData(this.connection.connectionId(), Arrays.copyOf(buffer, n));
		}
		this.frameWriter.sendClose(this.connection.connectionId());
	}

	private void doRelayToLocal() throws Exception {
		byte[] buffer = new byte[BUFFER_SIZE];
		InputStream source = this.connection.source();
		OutputStream out = this.pipe.sink();
		int n;
		while ((n = source.read(buffer)) > 0) {
			if (this.listener != null) {
				this.listener.onBytesRelayed(n, Direction.TO_LOCAL);
			}
			out.write(buffer, 0, n);
			out.flush();
		}
		// remote half-closed: propagate half-close to the local pipe
		this.pipe.shutdownOutput();
	}

	/**
	 * Tears the relay down after a local failure: closes the pipe and the virtual
	 * connection.
	 */
	public void abandon() {
		try {
			this.pipe.close();
		}
		catch (Exception e) {
			// ignore
		}
		this.connection.close();
	}

	private static String describe(Exception e) {
		String message = e.getMessage();
		return e.getClass().getSimpleName() + (message == null ? "" : ": " + message);
	}

}
