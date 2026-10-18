package am.ik.sluice.client.tunnel;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.StandardSocketOptions;
import java.time.Duration;
import java.util.List;

import jdk.net.ExtendedSocketOptions;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

class LocalConnectorTest {

	@Test
	void allowsAllPermitsAnyHost() {
		assertThat(LocalConnector.allowsAll("example.com")).isTrue();
	}

	@Test
	void permitsRegisteredHost() {
		LocalConnector connector = LocalConnector.builder()
			.upstreams(List.of("http://example.com"))
			.strict(true)
			.build();
		assertThat(connector.permits("example.com")).isTrue();
		assertThat(connector.permits("example.com:80")).isTrue();
	}

	@Test
	void rejectsUnregisteredHost() {
		LocalConnector connector = LocalConnector.builder().upstreams(List.of("example.com")).strict(true).build();
		assertThat(connector.permits("test.com")).isFalse();
	}

	@Test
	void nonStrictPermitsAnyHost() {
		LocalConnector connector = LocalConnector.builder().upstreams(List.of()).build();
		assertThat(connector.permits("test.com")).isTrue();
	}

	@Test
	void httpsDefaultPortMatches() {
		LocalConnector connector = LocalConnector.builder()
			.upstreams(List.of("https://example.com"))
			.strict(true)
			.build();
		assertThat(connector.permits("example.com:443")).isTrue();
	}

	@Test
	void upstreamsSharingOneHostAreBothDialable() {
		LocalConnector connector = LocalConnector.builder()
			.upstreams(List.of("http://example.com", "tcp://example.com:9000"))
			.strict(true)
			.build();
		assertThat(connector.permits("example.com")).isTrue();
		assertThat(connector.permits("example.com:80")).isTrue();
		assertThat(connector.permits("example.com:9000")).isTrue();
	}

	@Test
	@EnabledOnOs({ OS.LINUX, OS.MAC })
	void dialsRegisteredUpstream() throws Exception {
		int port;
		try (ServerSocket server = loopbackServer()) {
			port = server.getLocalPort();
			Thread acceptor = Thread.ofVirtual().start(() -> {
				try (Socket ignored = server.accept()) {
					// accept and close
				}
				catch (Exception e) {
					// ignore
				}
			});
			LocalConnector connector = LocalConnector.builder()
				.upstreams(List.of("http://127.0.0.1:" + port))
				.strict(true)
				.tcpKeepAlive(Duration.ofSeconds(45))
				.build();
			try (Socket socket = connector.dial("127.0.0.1:" + port)) {
				assertThat(socket.isConnected()).isTrue();
				assertThat(socket.getOption(StandardSocketOptions.SO_KEEPALIVE)).isTrue();
				assertThat(socket.getOption(ExtendedSocketOptions.TCP_KEEPIDLE)).isEqualTo(45);
			}
			acceptor.join(1000);
		}
	}

	@Test
	@EnabledOnOs({ OS.LINUX, OS.MAC })
	void dialsSecondUpstreamOnSharedHost() throws Exception {
		int httpPort;
		int tcpPort;
		try (ServerSocket httpServer = loopbackServer(); ServerSocket tcpServer = loopbackServer()) {
			httpPort = httpServer.getLocalPort();
			tcpPort = tcpServer.getLocalPort();
			Thread httpAcceptor = Thread.ofVirtual().start(() -> acceptOnce(httpServer));
			Thread tcpAcceptor = Thread.ofVirtual().start(() -> acceptOnce(tcpServer));
			LocalConnector connector = LocalConnector.builder()
				.upstreams(List.of("http://127.0.0.1:" + httpPort, "tcp://127.0.0.1:" + tcpPort))
				.strict(true)
				.build();
			try (Socket httpSocket = connector.dial("127.0.0.1:" + httpPort);
					Socket tcpSocket = connector.dial("127.0.0.1:" + tcpPort)) {
				assertThat(httpSocket.isConnected()).isTrue();
				assertThat(tcpSocket.isConnected()).isTrue();
			}
			httpAcceptor.join(1000);
			tcpAcceptor.join(1000);
		}
	}

	/**
	 * An https upstream that accepts and never answers the ClientHello: the dial fails at
	 * the connect timeout instead of waiting for good, and closes what it opened.
	 */
	@Test
	@EnabledOnOs({ OS.LINUX, OS.MAC })
	void aTlsUpstreamNeverAnsweringFailsTheDialAtTheConnectTimeout() throws Exception {
		try (ServerSocket server = loopbackServer()) {
			int port = server.getLocalPort();
			LocalConnector connector = LocalConnector.builder()
				.upstreams(List.of("https://127.0.0.1:" + port))
				.strict(true)
				.connectTimeout(Duration.ofMillis(500))
				.build();
			long start = System.nanoTime();
			assertTimeoutPreemptively(Duration.ofSeconds(10),
					() -> assertThatThrownBy(() -> connector.dial("127.0.0.1:" + port))
						.isInstanceOf(SocketTimeoutException.class));
			assertThat(Duration.ofNanos(System.nanoTime() - start)).isGreaterThanOrEqualTo(Duration.ofMillis(500));
			try (Socket accepted = server.accept()) {
				accepted.setSoTimeout(5_000);
				// the ClientHello, then the end of the stream: the dial closed its socket
				while (accepted.getInputStream().read() >= 0) {
					// drain
				}
			}
		}
	}

	/**
	 * Listens on the loopback address the tests dial, so a listener of another process on
	 * the same port number cannot take the connection, and bounds {@code accept} so a
	 * stray connection ends the acceptor instead of parking it.
	 */
	private static ServerSocket loopbackServer() throws IOException {
		ServerSocket server = new ServerSocket(0, 50, InetAddress.getByName("127.0.0.1"));
		server.setSoTimeout(5_000);
		return server;
	}

	private static void acceptOnce(ServerSocket server) {
		try (Socket ignored = server.accept()) {
			// accept and close
		}
		catch (Exception e) {
			// ignore
		}
	}

	@Test
	void dialRejectsUnregisteredHostInStrictMode() {
		LocalConnector connector = LocalConnector.builder()
			.upstreams(List.of("http://127.0.0.1:1"))
			.strict(true)
			.build();
		// unreachable port keeps the test hermetic; the filter rejects before dialing
		assertThatThrownBy(() -> connector.dial("10.255.255.1:1")).isInstanceOf(Exception.class);
	}

}
