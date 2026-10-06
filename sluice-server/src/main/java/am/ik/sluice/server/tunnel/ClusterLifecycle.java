package am.ik.sluice.server.tunnel;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.jspecify.annotations.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.boot.availability.AvailabilityChangeEvent;
import org.springframework.boot.availability.ReadinessState;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import am.ik.sluice.server.config.SluiceServerProperties;

/**
 * Cluster lifecycle: keeps readiness down for a warm-up period after start (so clients
 * connect before the node enters load balancer rotation) and drains gracefully on stop:
 * readiness down, a {@code Drain} frame to every client, then wait for the in-flight
 * virtual connections to finish within the grace period before the rest of the context
 * shuts down.
 */
@Component
public class ClusterLifecycle implements SmartLifecycle, ApplicationListener<ContextClosedEvent> {

	private static final Logger log = LoggerFactory.getLogger(ClusterLifecycle.class);

	private final SessionRegistry sessions;

	private final SluiceServerProperties properties;

	private final ApplicationContext context;

	private volatile boolean running;

	private volatile boolean draining;

	private volatile @Nullable Thread warmup;

	public ClusterLifecycle(SessionRegistry sessions, SluiceServerProperties properties, ApplicationContext context) {
		this.sessions = sessions;
		this.properties = properties;
		this.context = context;
	}

	@Override
	public void start() {
		this.running = true;
		if (!this.properties.clusterEnabled()) {
			return;
		}
		AvailabilityChangeEvent.publish(this.context, ReadinessState.REFUSING_TRAFFIC);
		this.warmup = Thread.ofVirtual().name("sluice-warmup").start(() -> {
			try {
				TimeUnit.MILLISECONDS.sleep(this.properties.cluster().warmup().toMillis());
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return;
			}
			if (this.running) {
				log.info("warm-up of {} finished; accepting traffic", this.properties.cluster().warmup());
				AvailabilityChangeEvent.publish(this.context, ReadinessState.ACCEPTING_TRAFFIC);
			}
		});
	}

	@Override
	public void onApplicationEvent(ContextClosedEvent event) {
		// the context closed event fires before the lifecycle stop, so the drain wait
		// precedes the gRPC server / data plane shutdown
		this.drain();
	}

	@Override
	public void stop() {
		this.drain();
	}

	private void drain() {
		if (this.draining) {
			return;
		}
		this.running = false;
		Thread warmup = this.warmup;
		if (warmup != null) {
			warmup.interrupt();
		}
		this.draining = true;
		AvailabilityChangeEvent.publish(this.context, ReadinessState.REFUSING_TRAFFIC);
		List<TunnelSession> live = this.sessions.all();
		if (live.isEmpty()) {
			return;
		}
		log.info("draining: notifying {} client session(s)", live.size());
		for (TunnelSession session : live) {
			session.drain("server stopping");
		}
		long deadline = System.nanoTime() + this.properties.cluster().drainGrace().toNanos();
		while (System.nanoTime() < deadline) {
			if (this.sessions.all().stream().allMatch(session -> session.connectionCount() == 0)) {
				break;
			}
			try {
				TimeUnit.MILLISECONDS.sleep(100);
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				break;
			}
		}
	}

	public boolean isDraining() {
		return this.draining;
	}

	@Override
	public boolean isRunning() {
		return this.running;
	}

}
