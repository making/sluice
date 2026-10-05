package am.ik.sluice.tunnel;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Objects;

/**
 * A bidirectional byte transport with explicit half-close support, decoupling the relay
 * from {@link Socket} so transports other than a plain socket can be relayed.
 */
public interface DuplexPipe extends AutoCloseable {

	/**
	 * Blocking input from the remote; returns -1 on EOF (remote half-closed or closed).
	 */
	InputStream source();

	/** Blocking output to the remote; each write should be flushed. */
	OutputStream sink();

	/** Half-close: no more data will be written to the remote; reads remain valid. */
	void shutdownOutput() throws Exception;

	@Override
	void close() throws Exception;

	/** Adapts a {@link Socket} into a {@link DuplexPipe}. */
	static DuplexPipe of(Socket socket) {
		Objects.requireNonNull(socket, "socket is required");
		return new DuplexPipe() {

			@Override
			public InputStream source() {
				try {
					return socket.getInputStream();
				}
				catch (Exception e) {
					throw new IllegalStateException("failed to get input stream", e);
				}
			}

			@Override
			public OutputStream sink() {
				try {
					return socket.getOutputStream();
				}
				catch (Exception e) {
					throw new IllegalStateException("failed to get output stream", e);
				}
			}

			@Override
			public void shutdownOutput() throws Exception {
				socket.shutdownOutput();
			}

			@Override
			public void close() throws Exception {
				socket.close();
			}

		};
	}

}
