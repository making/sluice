package am.ik.sluice.client.tunnel;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LocalConnectorTest {

	@Test
	void allowsAllPermitsAnyHost() {
		assertThat(LocalConnector.allowsAll("example.com")).isTrue();
	}

	@Test
	void permitsRegisteredHost() {
		LocalConnector connector = new LocalConnector(Map.of("tcp", "http://example.com"), true, false);
		assertThat(connector.permits("example.com")).isTrue();
		assertThat(connector.permits("example.com:80")).isTrue();
	}

	@Test
	void rejectsUnregisteredHost() {
		LocalConnector connector = new LocalConnector(Map.of("tcp", "example.com"), true, false);
		assertThat(connector.permits("test.com")).isFalse();
	}

	@Test
	void nonStrictPermitsAnyHost() {
		LocalConnector connector = new LocalConnector(Map.of(), false, false);
		assertThat(connector.permits("test.com")).isTrue();
	}

	@Test
	void httpsDefaultPortMatches() {
		LocalConnector connector = new LocalConnector(Map.of("s", "https://example.com"), true, false);
		assertThat(connector.permits("example.com:443")).isTrue();
	}

	@Test
	@EnabledOnOs({ OS.LINUX, OS.MAC })
	void dialsRegisteredUpstream() throws Exception {
		int port;
		try (ServerSocket server = new ServerSocket(0)) {
			port = server.getLocalPort();
			Thread acceptor = Thread.ofVirtual().start(() -> {
				try (Socket ignored = server.accept()) {
					// accept and close
				}
				catch (Exception e) {
					// ignore
				}
			});
			LocalConnector connector = new LocalConnector(Map.of("local", "http://127.0.0.1:" + port), true, false);
			try (Socket socket = connector.dial("127.0.0.1:" + port)) {
				assertThat(socket.isConnected()).isTrue();
			}
			acceptor.join(1000);
		}
	}

	@Test
	void dialRejectsUnregisteredHostInStrictMode() {
		LocalConnector connector = new LocalConnector(Map.of("local", "http://127.0.0.1:1"), true, false);
		// unreachable port keeps the test hermetic; the filter rejects before dialing
		assertThatThrownBy(() -> connector.dial("10.255.255.1:1")).isInstanceOf(Exception.class);
	}

}
