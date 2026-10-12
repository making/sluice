package am.ik.sluice.client.tunnel;

import java.time.Instant;
import java.util.Properties;

import org.junit.jupiter.api.Test;

import am.ik.sluice.v1.proto.PeerBuild;

import org.springframework.boot.info.BuildProperties;
import org.springframework.boot.info.GitProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The client's build comes from the packaged build-info / git.properties, and a client
 * packaged without them (e.g. a test classpath) still names its implementation.
 */
class ClientBuildConfigurationTest {

	private final ApplicationContextRunner runner = new ApplicationContextRunner()
		.withUserConfiguration(ClientBuildConfiguration.class);

	@Test
	void buildCarriesVersionCommitDirtyAndTime() {
		Properties build = new Properties();
		build.setProperty("version", "1.2.3");
		build.setProperty("time", Instant.parse("2026-10-12T01:04:33.456Z").toEpochMilli() + "");
		Properties git = new Properties();
		git.setProperty("commit.id", "73124728ab1e627fd8e02011f057709c0dc1fe59");
		git.setProperty("dirty", "true");
		this.runner.withBean(BuildProperties.class, () -> new BuildProperties(build))
			.withBean(GitProperties.class, () -> new GitProperties(git))
			.run(context -> assertThat(context.getBean(PeerBuild.class)).isEqualTo(PeerBuild.newBuilder()
				.setImplementation("sluice-client")
				.setVersion("1.2.3")
				.setCommit("73124728ab1e627fd8e02011f057709c0dc1fe59")
				.setDirty(true)
				.setBuildTime("2026-10-12T01:04:33Z")
				.build()));
	}

	@Test
	void withoutBuildMetadataOnlyTheImplementationIsKnown() {
		this.runner.run(context -> assertThat(context.getBean(PeerBuild.class))
			.isEqualTo(PeerBuild.newBuilder().setImplementation("sluice-client").build()));
	}

}
