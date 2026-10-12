package am.ik.sluice.it;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

import org.jspecify.annotations.Nullable;

/**
 * An h2 test upstream driven frame by frame: every accepted connection reads the preface,
 * writes the SETTINGS given, records every frame the relay sends, and hands each one to
 * the handler, which writes what the test scripts. A connection reading the relay's EOF
 * closes its socket, or holds it -- writing a PING every 20ms, the only way to learn of
 * the relay's close past the EOF -- until the relay closed it.
 */
public final class ScriptedH2Upstream implements AutoCloseable {

	private final ServerSocket server;

	private final byte[] settings;

	private final BiConsumer<Connection, TestH2.Frame> handler;

	private final boolean holdOnEof;

	private final List<Connection> connections = new CopyOnWriteArrayList<>();

	private ScriptedH2Upstream(byte[] settings, BiConsumer<Connection, TestH2.Frame> handler, boolean holdOnEof)
			throws IOException {
		this.settings = settings;
		this.handler = handler;
		this.holdOnEof = holdOnEof;
		this.server = new ServerSocket();
		this.server.bind(new InetSocketAddress("127.0.0.1", 0), 128);
		Thread.ofVirtual().name("e2e-scripted-h2-accept").start(this::accept);
	}

	/**
	 * Starts an upstream announcing the SETTINGS payload and closing on the relay's EOF.
	 */
	public static ScriptedH2Upstream start(byte[] settings, BiConsumer<Connection, TestH2.Frame> handler)
			throws IOException {
		return new ScriptedH2Upstream(settings, handler, false);
	}

	/**
	 * Starts an upstream announcing the SETTINGS payload and keeping its socket open
	 * after the relay's EOF, until the relay closes the connection.
	 */
	public static ScriptedH2Upstream holdingOnEof(byte[] settings, BiConsumer<Connection, TestH2.Frame> handler)
			throws IOException {
		return new ScriptedH2Upstream(settings, handler, true);
	}

	/** Answers every request that ended with a 200 whose body is {@code body}. */
	public static BiConsumer<Connection, TestH2.Frame> answering(String body) {
		return (connection, frame) -> {
			boolean ended = (frame.type() == TestH2.HEADERS || frame.type() == TestH2.DATA)
					&& (frame.flags() & TestH2.FLAG_END_STREAM) != 0;
			if (ended) {
				connection.answer(frame.streamId(), body);
			}
		};
	}

	public int port() {
		return this.server.getLocalPort();
	}

	/** The connections accepted so far, in order. */
	public List<Connection> connections() {
		return List.copyOf(this.connections);
	}

	/** Waits until the {@code index}th connection (from 0) was accepted. */
	public Connection awaitConnection(int index, Duration timeout) throws InterruptedException {
		long deadline = System.nanoTime() + timeout.toNanos();
		while (this.connections.size() <= index) {
			if (System.nanoTime() > deadline) {
				throw new IllegalStateException("connection %d not accepted".formatted(index));
			}
			Thread.sleep(2);
		}
		return this.connections.get(index);
	}

	private void accept() {
		while (!this.server.isClosed()) {
			try {
				Socket socket = this.server.accept();
				Connection connection = new Connection(socket);
				this.connections.add(connection);
				Thread.ofVirtual().name("e2e-scripted-h2").start(connection::serve);
			}
			catch (IOException e) {
				return;
			}
		}
	}

	@Override
	public void close() {
		try {
			this.server.close();
		}
		catch (IOException e) {
			// ignore
		}
		this.connections.forEach(Connection::close);
	}

	/** One accepted connection of the upstream. */
	public final class Connection {

		private final Socket socket;

		private final List<TestH2.Frame> frames = new CopyOnWriteArrayList<>();

		private final CountDownLatch eof = new CountDownLatch(1);

		private final CountDownLatch closed = new CountDownLatch(1);

		private Connection(Socket socket) {
			this.socket = socket;
		}

		private void serve() {
			try {
				InputStream in = this.socket.getInputStream();
				if (in.readNBytes(TestH2.PREFACE.length).length < TestH2.PREFACE.length) {
					return;
				}
				write(TestH2.SETTINGS, 0, 0, ScriptedH2Upstream.this.settings);
				TestH2.@Nullable Frame frame;
				while ((frame = TestH2.readFrame(in)) != null) {
					this.frames.add(frame);
					if (frame.type() == TestH2.SETTINGS && (frame.flags() & TestH2.FLAG_ACK) == 0) {
						write(TestH2.SETTINGS, TestH2.FLAG_ACK, 0, new byte[0]);
					}
					ScriptedH2Upstream.this.handler.accept(this, frame);
				}
				this.eof.countDown();
				if (ScriptedH2Upstream.this.holdOnEof) {
					// past the EOF only a write tells the relay's close: the reset it
					// gets
					OutputStream out = this.socket.getOutputStream();
					while (true) {
						synchronized (this) {
							TestH2.writeFrame(out, TestH2.PING, 0, 0, new byte[8]);
						}
						Thread.sleep(20);
					}
				}
			}
			catch (IOException e) {
				// the relay went away
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			finally {
				this.eof.countDown();
				this.closed.countDown();
				close();
			}
		}

		/** Writes one frame. */
		public void write(int type, int flags, int streamId, byte[] payload) {
			try {
				ByteArrayOutputStream bytes = new ByteArrayOutputStream();
				TestH2.writeFrame(bytes, type, flags, streamId, payload);
				raw(bytes.toByteArray());
			}
			catch (IOException e) {
				// the relay went away
			}
		}

		/** Writes the bytes as they are. */
		public void raw(byte[] bytes) {
			try {
				OutputStream out = this.socket.getOutputStream();
				synchronized (this) {
					out.write(bytes);
					out.flush();
				}
			}
			catch (IOException e) {
				// the relay went away
			}
		}

		/** A response head ({@code :status 200}), then DATA with END_STREAM. */
		public void answer(int streamId, String body) {
			head(streamId, false);
			write(TestH2.DATA, TestH2.FLAG_END_STREAM, streamId,
					body.getBytes(java.nio.charset.StandardCharsets.UTF_8));
		}

		/** A response head ({@code :status 200}, HPACK static index 8). */
		public void head(int streamId, boolean endStream) {
			write(TestH2.HEADERS, TestH2.FLAG_END_HEADERS | (endStream ? TestH2.FLAG_END_STREAM : 0), streamId,
					new byte[] { (byte) 0x88 });
		}

		/** The frames the relay sent, in order. */
		public List<TestH2.Frame> frames() {
			return new ArrayList<>(this.frames);
		}

		/** The first frame the relay sent matching, waiting up to {@code timeout}. */
		public TestH2.@Nullable Frame await(Predicate<TestH2.Frame> match, Duration timeout)
				throws InterruptedException {
			long deadline = System.nanoTime() + timeout.toNanos();
			while (true) {
				for (TestH2.Frame frame : this.frames) {
					if (match.test(frame)) {
						return frame;
					}
				}
				if (System.nanoTime() > deadline) {
					return null;
				}
				Thread.sleep(2);
			}
		}

		/** Waits for the relay's EOF (its half-close or its close). */
		public boolean awaitEof(Duration timeout) throws InterruptedException {
			return this.eof.await(timeout.toNanos(), TimeUnit.NANOSECONDS);
		}

		/** Waits until the relay closed the connection for good. */
		public boolean awaitClosed(Duration timeout) throws InterruptedException {
			return this.closed.await(timeout.toNanos(), TimeUnit.NANOSECONDS);
		}

		/** Closes the socket with a reset (SO_LINGER 0) instead of a FIN. */
		public void abort() {
			try {
				this.socket.setSoLinger(true, 0);
				this.socket.close();
			}
			catch (IOException e) {
				// ignore
			}
		}

		public void close() {
			try {
				this.socket.close();
			}
			catch (IOException e) {
				// ignore
			}
		}

	}

}
