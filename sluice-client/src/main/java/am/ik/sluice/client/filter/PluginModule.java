package am.ik.sluice.client.filter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HexFormat;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import run.endive.wasm.Parser;
import run.endive.wasm.WasmModule;

import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;

/**
 * Reads a proxy-wasm plugin from a Spring resource location, once, at the client's start:
 * {@code file:}, {@code classpath:}, {@code https:} or whatever the context resolves; a
 * location without a scheme is a file path. The bytes read are the bytes checked and the
 * bytes parsed. A plugin is code the client runs, so one from elsewhere than a file or
 * the class path needs its {@code sha256}: a location does not vouch for what it serves.
 */
final class PluginModule {

	/** The most read of a plugin. */
	static final int MAX_SIZE = 64 * 1024 * 1024;

	private static final Duration READ_TIMEOUT = Duration.ofSeconds(60);

	private static final Logger log = LoggerFactory.getLogger(PluginModule.class);

	/** A scheme of two letters or more: {@code C:\} stays a path. */
	private static final Pattern SCHEME = Pattern.compile("^[a-zA-Z][a-zA-Z0-9+.-]+:.*");

	private PluginModule() {
	}

	/**
	 * Reads and checks the plugin of filter {@code name}.
	 * @param sha256 its expected SHA-256 in lower case hex; empty when none is set
	 * @throws IllegalArgumentException when it cannot be read, does not match, or is not
	 * a WebAssembly module: the client does not start
	 */
	static WasmModule load(String name, String location, String sha256, ResourceLoader loader) {
		boolean hasScheme = SCHEME.matcher(location).matches();
		String scheme = hasScheme ? location.substring(0, location.indexOf(':')).toLowerCase(Locale.ROOT) : "file";
		boolean local = scheme.equals("file") || scheme.equals("classpath");
		if (!local && sha256.isEmpty()) {
			throw new IllegalArgumentException("plugin of filter " + name + " comes from " + scheme
					+ ": it needs its sha256 (sluice.client.filters." + name + ".sha256)");
		}
		Resource resource = hasScheme ? loader.getResource(location) : new FileSystemResource(location);
		byte[] bytes = read(name, resource);
		String actual = HexFormat.of().formatHex(sha256Of(bytes));
		if (!sha256.isEmpty() && !sha256.equals(actual)) {
			throw new IllegalArgumentException(
					"plugin of filter " + name + " has sha256 " + actual + ", not the one set: " + sha256);
		}
		WasmModule module;
		try {
			module = Parser.parse(bytes);
		}
		catch (RuntimeException e) {
			throw new IllegalArgumentException("plugin of filter " + name + " is not a WebAssembly module: " + e, e);
		}
		log.info("filter {}: plugin of {} bytes from {}, sha256 {}", name, bytes.length, scheme, actual);
		return module;
	}

	/**
	 * Under a deadline, whatever the resource is: most know no timeout of their own, and
	 * a location that does not answer must not leave the client starting for good.
	 */
	private static byte[] read(String name, Resource resource) {
		try (ExecutorService reader = Executors.newVirtualThreadPerTaskExecutor()) {
			CompletableFuture<byte[]> read = CompletableFuture.supplyAsync(() -> readAll(name, resource), reader);
			try {
				return read.get(READ_TIMEOUT.toNanos(), TimeUnit.NANOSECONDS);
			}
			catch (TimeoutException e) {
				reader.shutdownNow();
				throw new IllegalArgumentException("plugin of filter " + name + " not read in " + READ_TIMEOUT, e);
			}
			catch (ExecutionException e) {
				throw new IllegalArgumentException("plugin of filter " + name + " not read: " + e.getCause(),
						e.getCause());
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				reader.shutdownNow();
				throw new IllegalArgumentException("plugin of filter " + name + " not read: interrupted", e);
			}
		}
	}

	private static byte[] readAll(String name, Resource resource) {
		try (InputStream in = resource.getInputStream()) {
			ByteArrayOutputStream bytes = new ByteArrayOutputStream();
			byte[] buffer = new byte[64 * 1024];
			int n;
			while ((n = in.read(buffer)) >= 0) {
				bytes.write(buffer, 0, n);
				if (bytes.size() > MAX_SIZE) {
					throw new IOException("past " + MAX_SIZE + " bytes");
				}
			}
			return bytes.toByteArray();
		}
		catch (IOException e) {
			throw new IllegalStateException("plugin of filter " + name + ": " + e, e);
		}
	}

	private static byte[] sha256Of(byte[] bytes) {
		try {
			return MessageDigest.getInstance("SHA-256").digest(bytes);
		}
		catch (NoSuchAlgorithmException e) {
			throw new IllegalStateException(e);
		}
	}

}
