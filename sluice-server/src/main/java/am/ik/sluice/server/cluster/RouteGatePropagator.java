package am.ik.sluice.server.cluster;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.stereotype.Component;

import am.ik.sluice.server.auth.TokenValidator;
import am.ik.sluice.server.cluster.NodeDirectory.NodeMember;
import am.ik.sluice.server.config.GrpcChannels;
import am.ik.sluice.server.config.SluiceServerProperties;
import am.ik.sluice.v1.proto.SetRouteGateRequest;
import am.ik.sluice.v1.proto.TunnelGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;

/**
 * Carries a route gate toggle made on this node's console over to the other cluster
 * nodes, through their control plane ({@code SetRouteGate}). Best effort: a node down at
 * the time misses the toggle and keeps its gates until the next one; the gates are in
 * memory anyway, back at their defaults after a restart.
 * <p>
 * Only this node propagates: the receivers apply the toggle without forwarding it, so
 * there are no loops.
 */
@Component
public class RouteGatePropagator implements DisposableBean {

	private static final Logger log = LoggerFactory.getLogger(RouteGatePropagator.class);

	/** Deadline of one peer call. */
	static final java.time.Duration CALL_TIMEOUT = java.time.Duration.ofSeconds(2);

	private final NodeDirectory nodeDirectory;

	private final SluiceServerProperties properties;

	private final TokenValidator tokenValidator;

	private final SslBundles sslBundles;

	/** One channel per peer address, built on first use. */
	private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();

	public RouteGatePropagator(NodeDirectory nodeDirectory, SluiceServerProperties properties,
			TokenValidator tokenValidator, SslBundles sslBundles) {
		this.nodeDirectory = nodeDirectory;
		this.properties = properties;
		this.tokenValidator = tokenValidator;
		this.sslBundles = sslBundles;
	}

	/**
	 * Applies the toggle to every peer with a reachable control plane. Failures are
	 * logged and otherwise ignored.
	 * @param key route key as shown in the console
	 * @param open {@code true} to open the gate, {@code false} to shut it
	 */
	public void propagate(String key, boolean open) {
		String self = this.properties.node().id();
		for (NodeMember member : this.nodeDirectory.nodes()) {
			if (member.nodeId().equals(self) || member.publicUrl().isEmpty()) {
				continue;
			}
			try {
				stub(member)
					.setRouteGate(SetRouteGateRequest.newBuilder().setKey(key).setOpen(open).setOrigin(self).build())
					.get(CALL_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
			}
			catch (Exception e) {
				log.warn("node {} did not take the gate toggle of route [{}]: {}", member.nodeId(), key, e.toString());
			}
		}
	}

	private TunnelGrpc.TunnelFutureStub stub(NodeMember member) {
		ManagedChannel channel = this.channels.computeIfAbsent(member.publicUrl(), this::channel);
		Metadata headers = new Metadata();
		headers.put(TunnelAuthHeaders.AUTHORIZATION, this.tokenValidator.authorizationHeader());
		return TunnelGrpc.newFutureStub(channel)
			.withDeadlineAfter(CALL_TIMEOUT.toSeconds(), TimeUnit.SECONDS)
			.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
	}

	private ManagedChannel channel(String url) {
		return GrpcChannels
			.builder(url, this.properties.cluster().tlsBundle(), this.properties.cluster().insecure(), this.sslBundles)
			.build();
	}

	@Override
	public void destroy() {
		this.channels.values().forEach(ManagedChannel::shutdownNow);
	}

	/** The metadata keys of the control plane calls, shared with the server side. */
	interface TunnelAuthHeaders {

		Metadata.Key<String> AUTHORIZATION = Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

	}

}
