package am.ik.sluice.server.tunnel;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.jspecify.annotations.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import am.ik.sluice.server.cluster.NodeDirectory;
import am.ik.sluice.server.config.SluiceServerProperties;
import am.ik.sluice.v1.proto.Node;

/**
 * Pushes the {@link NodeDirectory} membership to every connected client: once when a
 * session opens and whenever the directory version changes (polled on
 * {@code sluice.cluster.membership-poll}).
 */
@Component
public class MembershipBroadcaster implements SmartLifecycle {

	private static final Logger log = LoggerFactory.getLogger(MembershipBroadcaster.class);

	private final NodeDirectory directory;

	private final SessionRegistry sessions;

	private final SluiceServerProperties properties;

	private final AtomicLong lastVersion = new AtomicLong(Long.MIN_VALUE);

	private volatile boolean running;

	private volatile @Nullable Thread poller;

	public MembershipBroadcaster(NodeDirectory directory, SessionRegistry sessions, SluiceServerProperties properties) {
		this.directory = directory;
		this.sessions = sessions;
		this.properties = properties;
	}

	@Override
	public void start() {
		this.running = true;
		this.poller = Thread.ofVirtual().name("sluice-membership-poller").start(this::pollLoop);
	}

	@Override
	public void stop() {
		this.running = false;
		Thread poller = this.poller;
		if (poller != null) {
			poller.interrupt();
		}
	}

	@Override
	public boolean isRunning() {
		return this.running;
	}

	/**
	 * Pushes the current membership to a newly connected session.
	 */
	public void onSessionCreated(TunnelSession session) {
		this.push(session);
	}

	private void pollLoop() {
		while (this.running) {
			try {
				Thread.sleep(this.properties.cluster().membershipPoll().toMillis());
				long version = this.directory.version();
				long seen = this.lastVersion.get();
				if (version != seen && this.lastVersion.compareAndSet(seen, version)) {
					this.broadcast("membership changed to version " + version);
				}
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return;
			}
			catch (RuntimeException e) {
				log.warn("membership poll failed: {}", e.toString());
			}
		}
	}

	private void broadcast(String cause) {
		log.info("broadcasting membership ({})", cause);
		for (TunnelSession session : this.sessions.all()) {
			this.push(session);
		}
	}

	private void push(TunnelSession session) {
		try {
			session.pushMembership(this.toProto(this.directory.nodes()), this.directory.version());
		}
		catch (RuntimeException e) {
			// the session may have closed concurrently; the next poll retries
			log.debug("membership push to client {} skipped: {}", session.clientId(), e.toString());
		}
	}

	private List<Node> toProto(List<NodeDirectory.NodeMember> members) {
		return members.stream()
			.map(member -> Node.newBuilder().setNodeId(member.nodeId()).setPublicUrl(member.publicUrl()).build())
			.toList();
	}

}
