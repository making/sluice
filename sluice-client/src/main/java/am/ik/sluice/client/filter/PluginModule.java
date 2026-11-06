package am.ik.sluice.client.filter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;

/**
 * A proxy-wasm plugin, read from a Spring resource location once, at the client's start:
 * {@code file:}, {@code classpath:}, {@code https:} or whatever the context resolves; a
 * location without a scheme is a file path. The bytes read are the bytes checked and the
 * bytes compiled by wasmtime, once in the process for the same bytes. A plugin is code
 * the client runs, so one from elsewhere than a file or the class path needs its
 * {@code sha256}: a location does not vouch for what it serves.
 */
final class PluginModule {

	/** The most read of a plugin. */
	static final int MAX_SIZE = 64 * 1024 * 1024;

	private static final int FUNCTION = 0;

	private static final Duration READ_TIMEOUT = Duration.ofSeconds(60);

	private static final Logger log = LoggerFactory.getLogger(PluginModule.class);

	/** A scheme of two letters or more: {@code C:\} stays a path. */
	private static final Pattern SCHEME = Pattern.compile("^[a-zA-Z][a-zA-Z0-9+.-]+:.*");

	/**
	 * The modules compiled, by their SHA-256: never freed, as plugins are defined for
	 * good.
	 */
	private static final Map<String, PluginModule> COMPILED = new ConcurrentHashMap<>();

	private final long handle;

	private final int memories;

	private final long initialBytes;

	private final List<Import> imports;

	private final Map<String, Integer> functions;

	/** Of the library's description of a module ({@code sw_module_describe}). */
	private PluginModule(long handle, byte[] description) {
		this.handle = handle;
		ByteBuffer in = ByteBuffer.wrap(description).order(ByteOrder.LITTLE_ENDIAN);
		int version = in.getInt();
		if (version != 1) {
			throw new IllegalStateException("a module's description of version " + version);
		}
		this.memories = in.getInt();
		this.initialBytes = in.getLong();
		List<Import> imports = new ArrayList<>();
		for (int i = in.getInt(); i > 0; i--) {
			int kind = in.get();
			boolean plain = in.get() != 0;
			imports.add(new Import(name(in), name(in), kind == FUNCTION, plain));
		}
		this.imports = List.copyOf(imports);
		Map<String, Integer> functions = new HashMap<>();
		int exports = in.getInt();
		for (int i = 0; i < exports; i++) {
			int kind = in.get();
			String export = name(in);
			if (kind == FUNCTION) {
				functions.put(export, i);
			}
		}
		this.functions = Map.copyOf(functions);
	}

	private static String name(ByteBuffer in) {
		byte[] bytes = new byte[in.getInt()];
		in.get(bytes);
		return new String(bytes, StandardCharsets.UTF_8);
	}

	/**
	 * Reads, checks and compiles the plugin of filter {@code name}.
	 * @param sha256 its expected SHA-256 in lower case hex; empty when none is set
	 * @throws IllegalArgumentException when it cannot be read, does not match, or is not
	 * a WebAssembly module: the client does not start
	 */
	static PluginModule load(String name, String location, String sha256, ResourceLoader loader) {
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
		PluginModule module = COMPILED.computeIfAbsent(actual, key -> compile(name, bytes));
		log.info("filter {}: plugin of {} bytes from {}, sha256 {}", name, bytes.length, scheme, actual);
		return module;
	}

	private static PluginModule compile(String name, byte[] bytes) {
		long handle;
		try {
			handle = Wasmtime.compile(bytes);
		}
		catch (IllegalArgumentException ex) {
			throw new IllegalArgumentException(
					"plugin of filter " + name + " is not a WebAssembly module this client runs: " + ex.getMessage(),
					ex);
		}
		return new PluginModule(handle, Wasmtime.describe(handle));
	}

	/** The library's handle of the compiled module. */
	long handle() {
		return this.handle;
	}

	/** The memories it defines. */
	int memories() {
		return this.memories;
	}

	/** What its defined memories and tables take at its instantiation, as charged. */
	long initialBytes() {
		return this.initialBytes;
	}

	/** Its imports, in their order. */
	List<Import> imports() {
		return this.imports;
	}

	/** Whether it exports a function of that name. */
	boolean exports(String export) {
		return this.functions.containsKey(export);
	}

	/**
	 * The index of its export of a function of that name among its exports; -1 for none.
	 */
	int function(String export) {
		return this.functions.getOrDefault(export, -1);
	}

	/**
	 * An import of the module.
	 *
	 * @param function whether it is a function
	 * @param plain of i32s and i64s, one result at most: what the host passes
	 */
	record Import(String module, String name, boolean function, boolean plain) {
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
