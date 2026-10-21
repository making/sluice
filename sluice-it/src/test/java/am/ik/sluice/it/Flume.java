package am.ik.sluice.it;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * A sluice-flume running as a child process of the test JVM, on a port of
 * {@link TestPorts}. The binary is the debug build the sluice-it build makes (system
 * property {@code sluice.flume.binary}). Its stdin stays open: the flume exits once it
 * closes, so it dies with the JVM however that ends.
 */
final class Flume implements AutoCloseable {

	static final String TOKEN = "it-flume-token";

	private static final Duration STARTUP = Duration.ofSeconds(30);

	private final Process process;

	private final int port;

	private Flume(Process process, int port) {
		this.process = process;
		this.port = port;
	}

	/** Starts a flume and waits for its {@code listening} line. */
	static Flume start() {
		return start(TestPorts.freePort());
	}

	/**
	 * Starts a flume on the port (one of {@link TestPorts}, for a server configured with
	 * its {@link #url(int)} before) and waits for its {@code listening} line.
	 */
	static Flume start(int port) {
		Path binary = Path.of(System.getProperty("sluice.flume.binary", "../sluice-flume/target/debug/sluice-flume"));
		if (!Files.isExecutable(binary)) {
			throw new IllegalStateException("no sluice-flume build at " + binary.toAbsolutePath()
					+ ": run cargo build --manifest-path sluice-flume/Cargo.toml (the sluice-it build does)");
		}
		Process process;
		try {
			process = new ProcessBuilder(binary.toString(), "--host", "127.0.0.1", "--port", String.valueOf(port),
					"--token", TOKEN, "--exit-on-stdin-close")
				.redirectErrorStream(true)
				.start();
		}
		catch (IOException e) {
			throw new UncheckedIOException(e);
		}
		CountDownLatch listening = new CountDownLatch(1);
		Thread.ofVirtual().name("flume-" + port).start(() -> {
			try (BufferedReader out = new BufferedReader(
					new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
				for (String line; (line = out.readLine()) != null;) {
					System.out.println("[flume:" + port + "] " + line);
					if (line.contains("listening")) {
						listening.countDown();
					}
				}
			}
			catch (IOException e) {
				// the process is gone
			}
		});
		try {
			if (!listening.await(STARTUP.toMillis(), TimeUnit.MILLISECONDS)) {
				process.destroyForcibly();
				throw new IllegalStateException("sluice-flume did not listen within " + STARTUP);
			}
		}
		catch (InterruptedException e) {
			process.destroyForcibly();
			Thread.currentThread().interrupt();
			throw new IllegalStateException(e);
		}
		return new Flume(process, port);
	}

	String url() {
		return url(this.port);
	}

	static String url(int port) {
		return "grpc://127.0.0.1:" + port;
	}

	@Override
	public void close() {
		this.process.destroy();
		try {
			if (!this.process.waitFor(10, TimeUnit.SECONDS)) {
				this.process.destroyForcibly().waitFor(10, TimeUnit.SECONDS);
			}
		}
		catch (InterruptedException e) {
			this.process.destroyForcibly();
			Thread.currentThread().interrupt();
		}
	}

}
