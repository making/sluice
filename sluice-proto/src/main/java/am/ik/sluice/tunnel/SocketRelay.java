package am.ik.sluice.tunnel;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Arrays;
import java.util.Objects;

import org.jspecify.annotations.Nullable;

/**
 * Pumps bytes between a local {@link Socket} and a {@link VirtualConnection} in both
 * directions, one virtual thread per direction (the direct analogue of the two goroutines
 * per connection in the original implementation).
 * <p>
 * Half-close semantics: local socket EOF emits CLOSE to the remote while the local side
 * keeps reading; remote CLOSE shuts down the socket output only.
 */
public final class SocketRelay {

	private static final int BUFFER_SIZE = 64 * 1024;

	/** Observer of relay activity. */
	public interface Listener {

		/** Called for each chunk relayed towards the remote. */
		void onBytesRelayed(long count);

	}

	private final Socket socket;

	private final VirtualConnection connection;

	private final FrameWriter frameWriter;

	private final byte @Nullable [] prefix;

	private final @Nullable Listener listener;

	private final @Nullable Runnable onComplete;

	private SocketRelay(Builder builder) {
		this.socket = builder.socket;
		this.connection = builder.connection;
		this.frameWriter = builder.frameWriter;
		this.prefix = builder.prefix;
		this.listener = builder.listener;
		this.onComplete = builder.onComplete;
	}

	public static Builder builder(Socket socket, VirtualConnection connection, FrameWriter frameWriter) {
		return new Builder(socket, connection, frameWriter);
	}

	public static final class Builder {

		private final Socket socket;

		private final VirtualConnection connection;

		private final FrameWriter frameWriter;

		private byte @Nullable [] prefix;

		private @Nullable Listener listener;

		private @Nullable Runnable onComplete;

		private Builder(Socket socket, VirtualConnection connection, FrameWriter frameWriter) {
			this.socket = Objects.requireNonNull(socket, "socket is required");
			this.connection = Objects.requireNonNull(connection, "connection is required");
			this.frameWriter = Objects.requireNonNull(frameWriter, "frameWriter is required");
		}

		/** Raw bytes injected before anything read from the socket. */
		public Builder prefix(byte[] prefix) {
			this.prefix = prefix;
			return this;
		}

		public Builder listener(Listener listener) {
			this.listener = listener;
			return this;
		}

		public Builder onComplete(Runnable onComplete) {
			this.onComplete = onComplete;
			return this;
		}

		public SocketRelay build() {
			return new SocketRelay(this);
		}

	}

	private final class Completion {

		private final java.util.concurrent.atomic.AtomicInteger pending = new java.util.concurrent.atomic.AtomicInteger(
				2);

		void done() {
			if (this.pending.decrementAndGet() == 0) {
				try {
					SocketRelay.this.socket.close();
				}
				catch (Exception e) {
					// ignore
				}
				if (SocketRelay.this.onComplete != null) {
					SocketRelay.this.onComplete.run();
				}
			}
		}

	}

	/**
	 * Starts both relay directions on virtual threads.
	 */
	public void start() {
		Completion completion = new Completion();
		Thread.ofVirtual()
			.name("sluice-relay-out-" + this.connection.connectionId())
			.start(() -> this.relayToRemote(completion));
		Thread.ofVirtual()
			.name("sluice-relay-in-" + this.connection.connectionId())
			.start(() -> this.relayToLocal(completion));
	}

	private void relayToRemote(Completion completion) {
		byte[] buffer = new byte[BUFFER_SIZE];
		try {
			if (this.prefix != null && this.prefix.length > 0) {
				if (this.listener != null) {
					this.listener.onBytesRelayed(this.prefix.length);
				}
				this.frameWriter.sendData(this.connection.connectionId(),
						Arrays.copyOf(this.prefix, this.prefix.length));
			}
			InputStream in = this.socket.getInputStream();
			int n;
			while ((n = in.read(buffer)) > 0) {
				if (this.listener != null) {
					this.listener.onBytesRelayed(n);
				}
				this.frameWriter.sendData(this.connection.connectionId(), Arrays.copyOf(buffer, n));
			}
			this.frameWriter.sendClose(this.connection.connectionId());
		}
		catch (Exception e) {
			this.frameWriter.sendError(this.connection.connectionId(), describe(e));
		}
		finally {
			completion.done();
		}
	}

	private void relayToLocal(Completion completion) {
		byte[] buffer = new byte[BUFFER_SIZE];
		try {
			InputStream source = this.connection.source();
			OutputStream out = this.socket.getOutputStream();
			int n;
			while ((n = source.read(buffer)) > 0) {
				out.write(buffer, 0, n);
				out.flush();
			}
			// remote half-closed: propagate half-close to the local socket
			this.socket.shutdownOutput();
		}
		catch (Exception e) {
			this.abandon();
		}
		finally {
			completion.done();
		}
	}

	/**
	 * Tears the relay down after a local failure: closes the socket and the virtual
	 * connection.
	 */
	public void abandon() {
		try {
			this.socket.close();
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
