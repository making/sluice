package am.ik.sluice.client.filter;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;

import org.springframework.core.io.DefaultResourceLoader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Where a plugin may come from: anything a Spring resource location names, what comes
 * from elsewhere than a file or the class path by its checksum only.
 */
class PluginModuleTest {

	private static final Path PLUGIN = Path.of("src/test/resources/wasm/http_body.wasm");

	private static String sha256() throws Exception {
		return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(PLUGIN)));
	}

	private static void load(String location, String sha256) {
		PluginModule.load("name", location, sha256, new DefaultResourceLoader());
	}

	@Test
	void aPluginFromAFileOrTheClassPathNeedsNoChecksumAndIsCheckedAgainstOne() throws Exception {
		String wrong = "0".repeat(64);
		for (String location : new String[] { PLUGIN.toString(), PLUGIN.toAbsolutePath().toUri().toString(),
				"classpath:wasm/http_body.wasm" }) {
			load(location, "");
			load(location, sha256());
			assertThatThrownBy(() -> load(location, wrong)).as(location)
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessage("plugin of filter name has sha256 " + sha256() + ", not the one set: " + wrong);
		}
	}

	@Test
	void aPluginFromElsewhereGoesByItsChecksum() throws Exception {
		byte[] bytes = Files.readAllBytes(PLUGIN);
		HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		server.createContext("/plugin.wasm", exchange -> {
			exchange.sendResponseHeaders(200, bytes.length);
			exchange.getResponseBody().write(bytes);
			exchange.close();
		});
		server.start();
		try {
			String base = "http://127.0.0.1:" + server.getAddress().getPort();
			String location = base + "/plugin.wasm";

			load(location, sha256());

			assertThatThrownBy(() -> load(location, "")).isInstanceOf(IllegalArgumentException.class)
				.hasMessage("plugin of filter name comes from http: it needs its sha256 "
						+ "(sluice.client.filters.name.sha256)");
			assertThatThrownBy(() -> load(location, "1".repeat(64))).isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("has sha256 " + sha256());
			assertThatThrownBy(() -> load(base + "/missing.wasm", sha256()))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("not read");
		}
		finally {
			server.stop(0);
		}
		assertThat(bytes).isNotEmpty();
	}

	@Test
	void whatIsNoModuleIsRefused(@org.junit.jupiter.api.io.TempDir Path directory) throws Exception {
		Path text = directory.resolve("text.wasm");
		Files.writeString(text, "no module");

		assertThatThrownBy(() -> load(text.toString(), "")).isInstanceOf(IllegalArgumentException.class)
			.hasMessageContaining("is not a WebAssembly module");
	}

}
