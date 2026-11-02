package am.ik.sluice.it;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.microsoft.playwright.Browser;
import com.microsoft.playwright.BrowserType;
import com.microsoft.playwright.Playwright;

/**
 * Launches the browser the tests drive, deaf to the host's interfaces. On any address
 * change Chromium fails what is connecting or queued for a connection with
 * {@code net::ERR_NETWORK_CHANGED} (requests on the wire go on), and anything may change
 * one: a container of another fork, a Firecracker tap. It has no switch for that, so on
 * Linux it runs with {@code src/test/c/no-netlink.c} preloaded, which the sluice-it build
 * compiles with {@code cc}. Elsewhere it is launched as it is.
 */
final class Chromium {

	private static final Path NO_NETLINK = Path.of("target", "no-netlink.so");

	private Chromium() {
	}

	static Browser launch(Playwright playwright, String... args) {
		BrowserType.LaunchOptions options = new BrowserType.LaunchOptions().setArgs(List.of(args));
		if ("Linux".equals(System.getProperty("os.name"))) {
			if (!Files.exists(NO_NETLINK)) {
				throw new IllegalStateException("no " + NO_NETLINK.toAbsolutePath() + ": the sluice-it build makes it");
			}
			// the option replaces the browser's whole environment
			Map<String, String> env = new HashMap<>(System.getenv());
			env.put("LD_PRELOAD", NO_NETLINK.toAbsolutePath().toString());
			options.setEnv(env);
		}
		return playwright.chromium().launch(options);
	}

}
