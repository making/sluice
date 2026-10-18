package am.ik.sluice.server.proxy;

import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.jspecify.annotations.Nullable;

import am.ik.sluice.server.config.SluiceServerProperties;

/**
 * The server's access logger, handing a test the server's end of every data plane
 * connection as it is accepted, before the accept loop takes it up.
 */
public final class HookedAccessLogger extends AccessLogger {

	private final AtomicReference<@Nullable Socket> last = new AtomicReference<>();

	private final AtomicBoolean closeNext = new AtomicBoolean();

	public HookedAccessLogger(SluiceServerProperties properties) {
		super(properties);
	}

	@Override
	public Connection accepted(String listener, Socket socket) {
		this.last.set(socket);
		if (this.closeNext.compareAndSet(true, false)) {
			try {
				socket.close();
			}
			catch (IOException e) {
				throw new IllegalStateException(e);
			}
		}
		return super.accepted(listener, socket);
	}

	/** The server's end of the connection accepted last. */
	public Socket last() {
		Socket socket = this.last.get();
		if (socket == null) {
			throw new IllegalStateException("no connection accepted yet");
		}
		return socket;
	}

	/** Closes the server's end of the next connection the moment it is accepted. */
	public void closeNext() {
		this.closeNext.set(true);
	}

}
