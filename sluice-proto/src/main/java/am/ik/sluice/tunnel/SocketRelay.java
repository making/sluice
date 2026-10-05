package am.ik.sluice.tunnel;

import java.net.Socket;
import java.util.Objects;

import org.jspecify.annotations.Nullable;

/**
 * Relays a {@link Socket} over a {@link VirtualConnection} by adapting it to a
 * {@link DuplexPipe} and delegating to {@link StreamRelay}.
 *
 * @see StreamRelay
 */
public final class SocketRelay {

	private final StreamRelay delegate;

	private SocketRelay(Builder builder) {
		this.delegate = StreamRelay.builder(DuplexPipe.of(builder.socket), builder.connection, builder.frameWriter)
			.prefix(builder.prefix)
			.listener(builder.listener)
			.onComplete(builder.onComplete)
			.build();
	}

	public static Builder builder(Socket socket, VirtualConnection connection, FrameWriter frameWriter) {
		return new Builder(socket, connection, frameWriter);
	}

	public static final class Builder {

		private final Socket socket;

		private final VirtualConnection connection;

		private final FrameWriter frameWriter;

		private byte @Nullable [] prefix;

		private StreamRelay.@Nullable Listener listener;

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

		public Builder listener(StreamRelay.Listener listener) {
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

	/**
	 * Starts both relay directions on virtual threads.
	 */
	public void start() {
		this.delegate.start();
	}

	/**
	 * Tears the relay down after a local failure: closes the socket and the virtual
	 * connection.
	 */
	public void abandon() {
		this.delegate.abandon();
	}

}
