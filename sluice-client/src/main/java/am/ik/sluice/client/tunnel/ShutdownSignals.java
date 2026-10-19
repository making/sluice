package am.ik.sluice.client.tunnel;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/**
 * Lets a second {@code SIGTERM} / {@code SIGINT} end the client's drain at once: the
 * first signal goes to the handler installed before (the JVM's, which starts the shutdown
 * and with it the drain), every later one aborts the drain
 * ({@link TunnelClient#abortDrain()}). The JVM alone ignores a signal arriving while its
 * shutdown runs. A signal without a Java handler (the JVM started with {@code -Xrs}, or a
 * native image without exit handlers) or ignored is left as it is.
 */
@Component
public class ShutdownSignals implements SmartLifecycle {

	private static final Logger log = LoggerFactory.getLogger(ShutdownSignals.class);

	static final List<String> SIGNALS = List.of("TERM", "INT");

	/** Installs a handler for a signal and returns the one it replaced. */
	@FunctionalInterface
	interface Installer {

		SignalHandler install(Signal signal, SignalHandler handler);

	}

	private final Runnable abortDrain;

	private final Installer installer;

	private final AtomicInteger received = new AtomicInteger();

	private volatile boolean running;

	@Autowired
	ShutdownSignals(TunnelClient tunnelClient) {
		this(tunnelClient::abortDrain, Signal::handle);
	}

	ShutdownSignals(Runnable abortDrain, Installer installer) {
		this.abortDrain = abortDrain;
		this.installer = installer;
	}

	@Override
	public void start() {
		this.running = true;
		for (String name : SIGNALS) {
			try {
				Signal signal = new Signal(name);
				Handler handler = new Handler();
				SignalHandler previous = this.installer.install(signal, handler);
				if (previous == SignalHandler.SIG_DFL || previous == SignalHandler.SIG_IGN) {
					// no Java handler starts the shutdown on it, or it is ignored: as it
					// was
					this.installer.install(signal, previous);
					continue;
				}
				handler.previous = previous;
			}
			catch (IllegalArgumentException | UnsupportedOperationException e) {
				log.debug("SIG{} is not handled: {}", name, e.toString());
			}
		}
	}

	/** Keeps the handlers: the shutdown they serve runs after this stop. */
	@Override
	public void stop() {
		this.running = false;
	}

	@Override
	public boolean isRunning() {
		return this.running;
	}

	private final class Handler implements SignalHandler {

		private volatile SignalHandler previous = SignalHandler.SIG_DFL;

		@Override
		public void handle(Signal signal) {
			if (ShutdownSignals.this.received.getAndIncrement() == 0) {
				SignalHandler previous = this.previous;
				if (previous == SignalHandler.SIG_DFL) {
					// arrived before the installation completed: what the JVM's does
					Runtime.getRuntime().exit(128 + signal.getNumber());
				}
				else {
					// the JVM's handler runs the shutdown hooks on this thread
					previous.handle(signal);
				}
				return;
			}
			log.warn("SIG{} during the shutdown: ending the drain at once", signal.getName());
			ShutdownSignals.this.abortDrain.run();
		}

	}

}
