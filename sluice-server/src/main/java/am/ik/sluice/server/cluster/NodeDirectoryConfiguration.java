package am.ik.sluice.server.cluster;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import am.ik.sluice.server.config.SluiceServerProperties;

import am.ik.sluice.server.cluster.NodeDirectory.NodeMember;

@Configuration(proxyBeanMethods = false)
public class NodeDirectoryConfiguration {

	/**
	 * The provider is decided in this bean body instead of {@code @ConditionalOnProperty}
	 * beans: bean conditions are evaluated at build time in the native image, which would
	 * pin the membership source to the properties of the build machine (the static
	 * provider vanished whenever the image was built without
	 * {@code sluice.cluster.nodes}, so runtime configuration was silently ignored).
	 * Future providers (e.g. DNS SRV) must be selected here for the same reason.
	 */
	@Bean
	NodeDirectory nodeDirectory(SluiceServerProperties properties) {
		if (properties.cluster().nodes().isEmpty()) {
			return singleNodeDirectory(properties);
		}
		return staticNodeDirectory(properties);
	}

	/**
	 * {@link NodeDirectory} backed by the static {@code sluice.cluster.nodes} list
	 * ({@code nodeId=publicUrl} entries). The local node is always part of the
	 * membership.
	 * <p>
	 * A static list never changes at runtime, so the version is constant; the version
	 * hook exists for future dynamic providers (e.g. DNS SRV).
	 */
	static NodeDirectory staticNodeDirectory(SluiceServerProperties properties) {
		Map<String, String> byId = new LinkedHashMap<>();
		for (String entry : properties.cluster().nodes()) {
			int sep = entry.indexOf('=');
			String nodeId = (sep < 0 ? entry : entry.substring(0, sep)).strip();
			String publicUrl = sep < 0 ? "" : entry.substring(sep + 1).strip();
			if (!nodeId.isEmpty()) {
				byId.putIfAbsent(nodeId, publicUrl);
			}
		}
		// the local node is always a member; its configured entry wins
		byId.putIfAbsent(properties.node().id(), properties.node().publicUrl());
		List<NodeMember> members = new ArrayList<>();
		byId.forEach((nodeId, publicUrl) -> members.add(new NodeMember(nodeId, publicUrl)));
		return new NodeDirectory() {

			private final AtomicLong version = new AtomicLong(1);

			@Override
			public long version() {
				return this.version.get();
			}

			@Override
			public List<NodeMember> nodes() {
				return List.copyOf(members);
			}

		};
	}

	/**
	 * Single-node fallback membership: only this node, with no public URL, so clients
	 * keep their bootstrap address. Keeps {@code ListNodes} meaningful without cluster
	 * configuration.
	 */
	static NodeDirectory singleNodeDirectory(SluiceServerProperties properties) {
		NodeMember self = new NodeMember(properties.node().id(), properties.node().publicUrl());
		return new NodeDirectory() {

			@Override
			public long version() {
				return 0;
			}

			@Override
			public List<NodeMember> nodes() {
				return List.of(self);
			}

		};
	}

}
