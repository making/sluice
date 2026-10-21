package am.ik.sluice.server.config;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class GrpcChannelsTest {

	@Test
	void addressIsParsedHostAndPort() {
		assertThat(GrpcChannels.HostPort.parse("grpc://node-1:8001"))
			.isEqualTo(new GrpcChannels.HostPort("node-1", 8001));
		assertThat(GrpcChannels.HostPort.parse("grpcs://node-2")).isEqualTo(new GrpcChannels.HostPort("node-2", 443));
		assertThat(GrpcChannels.HostPort.parse("grpc://node-3")).isEqualTo(new GrpcChannels.HostPort("node-3", 80));
		assertThatIllegalArgumentException().isThrownBy(() -> GrpcChannels.HostPort.parse("flume:8002"));
	}

}
