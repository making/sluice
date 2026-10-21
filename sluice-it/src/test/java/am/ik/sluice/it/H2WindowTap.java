package am.ik.sluice.it;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import org.jspecify.annotations.Nullable;

/**
 * A loopback TCP proxy to a plaintext HTTP/2 target (h2c with prior knowledge, as gRPC
 * speaks it), relaying the frames of both directions as they come and recording the flow
 * control windows each side announces: the initial stream window of its SETTINGS and its
 * connection window -- RFC 9113's 65535 bytes, plus what its WINDOW_UPDATEs on stream 0
 * grant, less the DATA the other side sent. Relays the first accepted connection only.
 */
public final class H2WindowTap implements AutoCloseable {

	/** RFC 9113's initial connection window. */
	public static final int INITIAL_CONNECTION_WINDOW = 65535;

	private static final int SETTINGS_INITIAL_WINDOW_SIZE = 0x4;

	private final ServerSocket server;

	private final int targetPort;

	private final List<Socket> sockets = new CopyOnWriteArrayList<>();

	private final Side connecting = new Side();

	private final Side target = new Side();

	private H2WindowTap(int targetPort, int port) throws IOException {
		this.targetPort = targetPort;
		this.server = new ServerSocket();
		this.server.bind(new InetSocketAddress("127.0.0.1", port), 16);
		Thread.ofVirtual().name("e2e-h2-window-tap-accept").start(this::accept);
	}

	/**
	 * A tap on {@code 127.0.0.1:port} (taken from {@link TestPorts}) relaying to
	 * {@code 127.0.0.1:targetPort}.
	 */
	public static H2WindowTap start(int targetPort, int port) throws IOException {
		return new H2WindowTap(targetPort, port);
	}

	/** The port the tap listens on. */
	public int port() {
		return this.server.getLocalPort();
	}

	/** What the connecting side (the HTTP/2 client) announced. */
	public Side connecting() {
		return this.connecting;
	}

	/** What the target (the HTTP/2 server) announced. */
	public Side target() {
		return this.target;
	}

	private void accept() {
		try {
			Socket from = this.server.accept();
			Socket to = new Socket("127.0.0.1", this.targetPort);
			from.setTcpNoDelay(true);
			to.setTcpNoDelay(true);
			this.sockets.add(from);
			this.sockets.add(to);
			Thread.ofVirtual()
				.name("e2e-h2-window-tap-out")
				.start(() -> relay(from, to, true, this.connecting, this.target));
			Thread.ofVirtual()
				.name("e2e-h2-window-tap-in")
				.start(() -> relay(to, from, false, this.target, this.connecting));
		}
		catch (IOException e) {
			// closed
		}
	}

	/**
	 * Relays the frames read from {@code from} to {@code to}, the client's preface first
	 * if {@code preface}, recording what they announce for {@code sending} and take of
	 * {@code receiving}'s connection window.
	 */
	private void relay(Socket from, Socket to, boolean preface, Side sending, Side receiving) {
		try (InputStream in = from.getInputStream(); OutputStream out = to.getOutputStream()) {
			if (preface) {
				byte[] bytes = in.readNBytes(TestH2.PREFACE.length);
				out.write(bytes);
				if (bytes.length < TestH2.PREFACE.length) {
					return;
				}
			}
			TestH2.@Nullable Frame frame;
			while ((frame = TestH2.readFrame(in)) != null) {
				if (frame.type() == TestH2.SETTINGS && (frame.flags() & TestH2.FLAG_ACK) == 0) {
					ByteBuffer settings = ByteBuffer.wrap(frame.payload());
					while (settings.remaining() >= 6) {
						int id = settings.getShort() & 0xffff;
						long value = settings.getInt() & 0xffffffffL;
						if (id == SETTINGS_INITIAL_WINDOW_SIZE) {
							sending.initialWindow.set(value);
						}
					}
				}
				else if (frame.type() == TestH2.WINDOW_UPDATE && frame.streamId() == 0) {
					sending.grant(ByteBuffer.wrap(frame.payload()).getInt() & 0x7fffffff);
				}
				else if (frame.type() == TestH2.DATA) {
					receiving.grant(-frame.payload().length);
				}
				TestH2.writeFrame(out, frame.type(), frame.flags(), frame.streamId(), frame.payload());
			}
		}
		catch (IOException e) {
			// closed
		}
		close();
	}

	@Override
	public void close() {
		closeQuietly(this.server);
		this.sockets.forEach(H2WindowTap::closeQuietly);
	}

	private static void closeQuietly(AutoCloseable closeable) {
		try {
			closeable.close();
		}
		catch (Exception e) {
			// closed already
		}
	}

	/** The windows one side announced to the other. */
	public static final class Side {

		/** -1 until the side's SETTINGS announced it. */
		private final AtomicLong initialWindow = new AtomicLong(-1);

		private final AtomicLong connectionWindow = new AtomicLong(INITIAL_CONNECTION_WINDOW);

		private final AtomicLong maxConnectionWindow = new AtomicLong(INITIAL_CONNECTION_WINDOW);

		private final AtomicLong largestGrant = new AtomicLong(0);

		private void grant(long increment) {
			this.largestGrant.accumulateAndGet(increment, Math::max);
			this.maxConnectionWindow.accumulateAndGet(this.connectionWindow.addAndGet(increment), Math::max);
		}

		/** The initial stream window the side's SETTINGS announced; -1 for none yet. */
		public long initialWindow() {
			return this.initialWindow.get();
		}

		/** The largest connection window the side granted. */
		public long maxConnectionWindow() {
			return this.maxConnectionWindow.get();
		}

		/** The largest single connection-window grant the side sent. */
		public long largestGrant() {
			return this.largestGrant.get();
		}

	}

}
