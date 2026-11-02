package am.ik.sluice.it;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;

import javax.net.ServerSocketFactory;

import com.microsoft.playwright.Browser;
import com.microsoft.playwright.BrowserContext;
import com.microsoft.playwright.Page;
import com.microsoft.playwright.Playwright;
import com.microsoft.playwright.Response;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.testcontainers.containers.Network;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ChromiumTest {

	/**
	 * A Docker network added and removed is an address coming and going on the host, with
	 * the navigation in its TLS handshake, the state Chromium drops: the bare browser
	 * shows the change reached it.
	 */
	@Test
	@EnabledOnOs(OS.LINUX)
	void aNavigationOutlivesAnInterfaceChange() throws Exception {
		Path keystore = TestKeystore.create();
		ServerSocketFactory tls = TestKeystore.serverContext(keystore).getServerSocketFactory();
		try (Playwright playwright = Playwright.create()) {
			try (Browser bare = playwright.chromium().launch()) {
				assertThatThrownBy(() -> navigateAcrossAnInterfaceChange(bare, tls))
					.hasMessageContaining("net::ERR_NETWORK_CHANGED");
			}
			try (Browser browser = Chromium.launch(playwright)) {
				assertThat(navigateAcrossAnInterfaceChange(browser, tls).status()).isEqualTo(200);
			}
		}
	}

	private static Response navigateAcrossAnInterfaceChange(Browser browser, ServerSocketFactory tls)
			throws IOException {
		CountDownLatch handshaking = new CountDownLatch(1);
		CountDownLatch changed = new CountDownLatch(1);
		try (ServerSocket server = tls.createServerSocket(0);
				BrowserContext context = browser.newContext(new Browser.NewContextOptions().setIgnoreHTTPSErrors(true));
				Page page = context.newPage()) {
			Thread.ofVirtual().start(() -> answerOnceChanged(server, handshaking, changed));
			Thread.ofVirtual().start(() -> {
				try {
					handshaking.await();
					try (Network network = Network.newNetwork()) {
						network.getId();
					}
				}
				catch (InterruptedException ex) {
					Thread.currentThread().interrupt();
				}
				finally {
					changed.countDown();
				}
			});
			return Objects.requireNonNull(page.navigate("https://127.0.0.1:" + server.getLocalPort() + "/"));
		}
	}

	/**
	 * Each connection waits for the change in its TLS handshake, which starts with the
	 * first read. Chromium's first handshake is one it rejects the certificate in: it
	 * ignores that error on a connection of its own.
	 */
	private static void answerOnceChanged(ServerSocket server, CountDownLatch handshaking, CountDownLatch changed) {
		try {
			while (true) {
				Socket socket = server.accept();
				handshaking.countDown();
				Thread.ofVirtual().start(() -> answer(socket, changed));
			}
		}
		catch (IOException ex) {
			// the server is closed
		}
	}

	private static void answer(Socket accepted, CountDownLatch changed) {
		try (Socket socket = accepted) {
			changed.await();
			BufferedReader in = new BufferedReader(
					new InputStreamReader(socket.getInputStream(), StandardCharsets.US_ASCII));
			for (String line; (line = in.readLine()) != null && !line.isEmpty();) {
				// the request head
			}
			socket.getOutputStream()
				.write("HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
					.getBytes(StandardCharsets.US_ASCII));
		}
		catch (IOException ex) {
			// the browser hung up: the rejected certificate, the dropped navigation
		}
		catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
		}
	}

}
