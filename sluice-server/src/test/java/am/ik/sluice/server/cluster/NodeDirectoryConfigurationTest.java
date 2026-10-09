package am.ik.sluice.server.cluster;

import java.util.List;

import org.junit.jupiter.api.Test;

import am.ik.sluice.server.config.SluiceServerProperties;

import static org.assertj.core.api.Assertions.assertThat;

class NodeDirectoryConfigurationTest {

	private final NodeDirectoryConfiguration configuration = new NodeDirectoryConfiguration();

	@Test
	void staticDirectoryParsesNodeIdUrlPairsAndIncludesSelf() {
		SluiceServerProperties properties = SluiceServerProperties.builder()
			.node(SluiceServerProperties.Node.builder().id("self").publicUrl("grpcs://self:8001").build())
			.cluster(SluiceServerProperties.Cluster.builder()
				.nodes(List.of("alpha=grpcs://alpha:8001", "beta", "self=grpcs://self:8001",
						"  gamma = grpc://g:9000 "))
				.build())
			.build();
		NodeDirectory directory = this.configuration.nodeDirectory(properties);
		assertThat(directory.version()).isEqualTo(1);
		assertThat(directory.nodes()).containsExactly(new NodeDirectory.NodeMember("alpha", "grpcs://alpha:8001"),
				new NodeDirectory.NodeMember("beta", ""), new NodeDirectory.NodeMember("self", "grpcs://self:8001"),
				new NodeDirectory.NodeMember("gamma", "grpc://g:9000"));
	}

	@Test
	void singleNodeDirectoryReturnsOnlySelfWithoutPublicUrl() {
		SluiceServerProperties properties = SluiceServerProperties.builder()
			.node(SluiceServerProperties.Node.builder().id("alone").build())
			.build();
		NodeDirectory directory = this.configuration.nodeDirectory(properties);
		assertThat(directory.version()).isZero();
		assertThat(directory.nodes()).containsExactly(new NodeDirectory.NodeMember("alone", ""));
	}

	@Test
	void nodeIdFallsBackToHostnameWhenBlank() {
		String host = SluiceServerProperties.Node.defaultNodeId();
		assertThat(host).isNotBlank();
		SluiceServerProperties.Node node = SluiceServerProperties.Node.builder().id(" ").build();
		assertThat(node.id()).isEqualTo(host);
	}

}
