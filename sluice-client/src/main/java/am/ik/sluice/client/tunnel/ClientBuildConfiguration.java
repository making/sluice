package am.ik.sluice.client.tunnel;

import java.time.temporal.ChronoUnit;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.info.BuildProperties;
import org.springframework.boot.info.GitProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import am.ik.sluice.v1.proto.PeerBuild;

/**
 * The client's own build, announced in every {@code Advertise}. Taken from the packaged
 * {@code META-INF/build-info.properties} and {@code git.properties}; a field whose source
 * is missing stays empty.
 */
@Configuration(proxyBeanMethods = false)
class ClientBuildConfiguration {

	static final String IMPLEMENTATION = "sluice-client";

	@Bean
	PeerBuild clientBuild(ObjectProvider<BuildProperties> buildProperties,
			ObjectProvider<GitProperties> gitProperties) {
		PeerBuild.Builder build = PeerBuild.newBuilder().setImplementation(IMPLEMENTATION);
		BuildProperties buildInfo = buildProperties.getIfAvailable();
		if (buildInfo != null) {
			if (buildInfo.getVersion() != null) {
				build.setVersion(buildInfo.getVersion());
			}
			if (buildInfo.getTime() != null) {
				build.setBuildTime(buildInfo.getTime().truncatedTo(ChronoUnit.SECONDS).toString());
			}
		}
		GitProperties git = gitProperties.getIfAvailable();
		if (git != null) {
			if (git.getCommitId() != null) {
				build.setCommit(git.getCommitId());
			}
			build.setDirty(Boolean.parseBoolean(git.get("dirty")));
		}
		return build.build();
	}

}
