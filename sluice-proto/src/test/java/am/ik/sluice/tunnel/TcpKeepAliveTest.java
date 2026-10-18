package am.ik.sluice.tunnel;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.StandardSocketOptions;
import java.time.Duration;

import jdk.net.ExtendedSocketOptions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;

class TcpKeepAliveTest {

	@ParameterizedTest
	@CsvSource({ "PT45S, 45", "PT1.5S, 2", "PT0.001S, 1", "PT1000000H, 32767", "PT0S, 0" })
	void theIdleTimeIsSetInWholeSecondsAndZeroLeavesKeepAliveOff(Duration idle, int seconds) throws Exception {
		try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
				Socket socket = new Socket(server.getInetAddress(), server.getLocalPort());
				Socket accepted = server.accept()) {
			assertThat(accepted.getOption(StandardSocketOptions.SO_KEEPALIVE)).isFalse();
			TcpKeepAlive.enable(accepted, idle);
			assertThat(accepted.getOption(StandardSocketOptions.SO_KEEPALIVE)).isEqualTo(seconds > 0);
			if (seconds > 0) {
				assertThat(accepted.getOption(ExtendedSocketOptions.TCP_KEEPIDLE)).isEqualTo(seconds);
				assertThat(accepted.getOption(ExtendedSocketOptions.TCP_KEEPINTERVAL)).isEqualTo(10);
				assertThat(accepted.getOption(ExtendedSocketOptions.TCP_KEEPCOUNT)).isEqualTo(3);
			}
		}
	}

}
