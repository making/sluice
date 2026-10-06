package am.ik.sluice.server.console;

import java.util.OptionalInt;

import org.springframework.context.event.EventListener;
import org.springframework.grpc.server.lifecycle.GrpcServerStartedEvent;
import org.springframework.stereotype.Component;

/**
 * Remembers the port the gRPC control plane actually listens on (the configured port may
 * be {@code 0}).
 */
@Component
public class ControlPlanePort {

	private volatile int port = -1;

	@EventListener
	void onStarted(GrpcServerStartedEvent event) {
		this.port = event.getPort();
	}

	public OptionalInt port() {
		int current = this.port;
		return current < 0 ? OptionalInt.empty() : OptionalInt.of(current);
	}

}
