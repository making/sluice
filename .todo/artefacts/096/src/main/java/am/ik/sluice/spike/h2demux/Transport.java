package am.ik.sluice.spike.h2demux;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import am.ik.sluice.tunnel.VirtualConnection;

/**
 * A blocking byte duplex a {@link Hop} drives: the client's socket, or a leg's
 * {@link VirtualConnection}.
 */
public interface Transport {

	InputStream in();

	OutputStream out();

	/** Ends the outbound direction once written; the inbound one keeps reading. */
	void shutdownOutput();

	/** Closes both directions; idempotent. */
	void close();

	static Transport of(Socket socket) {
		return new Transport() {

			@Override
			public InputStream in() {
				try {
					return socket.getInputStream();
				}
				catch (IOException e) {
					throw new IllegalStateException(e);
				}
			}

			@Override
			public OutputStream out() {
				try {
					return socket.getOutputStream();
				}
				catch (IOException e) {
					throw new IllegalStateException(e);
				}
			}

			@Override
			public void shutdownOutput() {
				try {
					socket.shutdownOutput();
				}
				catch (IOException e) {
					// closed already
				}
			}

			@Override
			public void close() {
				try {
					socket.close();
				}
				catch (IOException e) {
					// closed already
				}
			}

		};
	}

	static Transport of(VirtualConnection connection) {
		return new Transport() {

			@Override
			public InputStream in() {
				return connection.source();
			}

			@Override
			public OutputStream out() {
				return connection.sink();
			}

			@Override
			public void shutdownOutput() {
				try {
					connection.sink().close();
				}
				catch (IOException e) {
					// closed already
				}
			}

			@Override
			public void close() {
				connection.close();
			}

		};
	}

}
