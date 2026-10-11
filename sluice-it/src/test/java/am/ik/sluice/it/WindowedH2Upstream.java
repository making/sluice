package am.ik.sluice.it;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.jspecify.annotations.Nullable;

/**
 * One connection of an h2 test upstream keeping the flow-control books of RFC 9113 6.9 on
 * both sides. Receiving: the windows it advertised, a changed initial stream window
 * applied once the relay acknowledged the SETTINGS carrying it (the relay may send under
 * the old value until it processed them, 6.9.3); every DATA frame is checked against both
 * windows and the largest excess kept, above 0 a violation. Sending: {@link #sendData}
 * waits for the windows the relay advertised, which only a thread calling {@link #next}
 * concurrently can replenish.
 */
public final class WindowedH2Upstream implements AutoCloseable {

	/** The spec initial window of streams and of the connection. */
	public static final int DEFAULT_WINDOW = 65535;

	public static final int SETTINGS_INITIAL_WINDOW_SIZE = 0x4;

	private static final int MAX_FRAME_SIZE = 16 * 1024;

	/** Marks a SETTINGS frame without an initial window in {@link #unacked}. */
	private static final long NO_WINDOW = -1;

	private final Socket socket;

	private final InputStream in;

	private final OutputStream out;

	/** Serializes the socket writes; never held while waiting for a window. */
	private final Object writeLock = new Object();

	private long initialWindow = DEFAULT_WINDOW;

	/** The initial windows of the SETTINGS sent and not yet acknowledged, in order. */
	private final ArrayDeque<Long> unacked = new ArrayDeque<>();

	private long connectionWindow = DEFAULT_WINDOW;

	private final Map<Integer, Long> streamWindows = new HashMap<>();

	private final Map<Integer, Long> received = new HashMap<>();

	private long maxExcess = Long.MIN_VALUE;

	private boolean windowSettingAcked;

	private long minStreamWindowAfterAck = Long.MAX_VALUE;

	private long minConnectionWindow = DEFAULT_WINDOW;

	private long sendInitialWindow = DEFAULT_WINDOW;

	private long sendConnectionWindow = DEFAULT_WINDOW;

	private long connectionGrantsReceived;

	private final Map<Integer, Long> sendStreamWindows = new HashMap<>();

	private boolean eof;

	private WindowedH2Upstream(Socket socket) throws IOException {
		this.socket = socket;
		this.in = socket.getInputStream();
		this.out = socket.getOutputStream();
	}

	/** Reads the client preface; {@code null} when the relay went away first. */
	public static @Nullable WindowedH2Upstream accept(Socket socket) throws IOException {
		WindowedH2Upstream upstream = new WindowedH2Upstream(socket);
		if (upstream.in.readNBytes(TestH2.PREFACE.length).length < TestH2.PREFACE.length) {
			return null;
		}
		return upstream;
	}

	/** Sends SETTINGS, announcing the initial stream window when given. */
	public void settings(@Nullable Integer initialWindow) throws IOException {
		byte[] payload = initialWindow == null ? new byte[0]
				: setting(SETTINGS_INITIAL_WINDOW_SIZE, initialWindow.longValue());
		synchronized (this) {
			this.unacked.addLast(initialWindow == null ? NO_WINDOW : initialWindow.longValue());
		}
		write(TestH2.SETTINGS, 0, 0, payload);
	}

	/** One SETTINGS entry. */
	public static byte[] setting(int id, long value) {
		return new byte[] { (byte) (id >>> 8), (byte) id, (byte) (value >>> 24), (byte) (value >>> 16),
				(byte) (value >>> 8), (byte) value };
	}

	/**
	 * Reads the next frame and accounts it; acknowledges the relay's SETTINGS. Returns
	 * {@code null} at EOF.
	 */
	public TestH2.@Nullable Frame next() throws IOException {
		TestH2.Frame frame;
		try {
			frame = TestH2.readFrame(this.in);
		}
		catch (IOException e) {
			frame = null;
		}
		boolean ack = false;
		synchronized (this) {
			if (frame == null) {
				this.eof = true;
				notifyAll();
				return null;
			}
			int streamId = frame.streamId();
			switch (frame.type()) {
				case TestH2.HEADERS -> {
					this.streamWindows.putIfAbsent(streamId, this.initialWindow);
					this.sendStreamWindows.putIfAbsent(streamId, this.sendInitialWindow);
				}
				case TestH2.DATA -> onData(streamId, frame.payload().length);
				case TestH2.SETTINGS -> {
					if ((frame.flags() & TestH2.FLAG_ACK) != 0) {
						onSettingsAck();
					}
					else {
						onRelaySettings(frame.payload());
						ack = true;
					}
				}
				case TestH2.WINDOW_UPDATE -> {
					long increment = TestH2.increment(frame);
					if (streamId == 0) {
						this.sendConnectionWindow += increment;
						this.connectionGrantsReceived += increment;
					}
					else {
						this.sendStreamWindows.merge(streamId, this.sendInitialWindow + increment,
								(window, ignored) -> window + increment);
					}
				}
				default -> {
					// not accounted
				}
			}
			notifyAll();
		}
		if (ack) {
			write(TestH2.SETTINGS, TestH2.FLAG_ACK, 0, new byte[0]);
		}
		return frame;
	}

	private void onData(int streamId, int length) {
		if (length > 0) {
			long streamBefore = this.streamWindows.getOrDefault(streamId, this.initialWindow);
			this.maxExcess = Math.max(this.maxExcess, Math.max(length - this.connectionWindow, length - streamBefore));
			this.connectionWindow -= length;
			this.streamWindows.put(streamId, streamBefore - length);
			this.minConnectionWindow = Math.min(this.minConnectionWindow, this.connectionWindow);
			if (this.windowSettingAcked) {
				this.minStreamWindowAfterAck = Math.min(this.minStreamWindowAfterAck, streamBefore - length);
			}
		}
		this.received.merge(streamId, (long) length, Long::sum);
	}

	private void onSettingsAck() {
		Long window = this.unacked.pollFirst();
		if (window == null || window == NO_WINDOW) {
			return;
		}
		long delta = window - this.initialWindow;
		this.initialWindow = window;
		this.streamWindows.replaceAll((id, value) -> value + delta);
		this.windowSettingAcked = true;
		for (long value : this.streamWindows.values()) {
			this.minStreamWindowAfterAck = Math.min(this.minStreamWindowAfterAck, value);
		}
	}

	private void onRelaySettings(byte[] payload) {
		for (int i = 0; i + 6 <= payload.length; i += 6) {
			int id = (payload[i] & 0xff) << 8 | (payload[i + 1] & 0xff);
			long value = (payload[i + 2] & 0xffL) << 24 | (payload[i + 3] & 0xff) << 16 | (payload[i + 4] & 0xff) << 8
					| (payload[i + 5] & 0xff);
			if (id == SETTINGS_INITIAL_WINDOW_SIZE) {
				long delta = value - this.sendInitialWindow;
				this.sendInitialWindow = value;
				this.sendStreamWindows.replaceAll((stream, window) -> window + delta);
			}
		}
	}

	/** Grants the relay {@code increment} more on the stream, or the connection for 0. */
	public void grant(int streamId, int increment) throws IOException {
		synchronized (this) {
			if (streamId == 0) {
				this.connectionWindow += increment;
			}
			else {
				this.streamWindows.merge(streamId, this.initialWindow + increment,
						(window, ignored) -> window + increment);
			}
		}
		write(TestH2.WINDOW_UPDATE, 0, streamId, TestH2.int32(increment));
	}

	/** Sends a response head ({@code :status 200}). */
	public void headers(int streamId, boolean endStream) throws IOException {
		write(TestH2.HEADERS, TestH2.FLAG_END_HEADERS | (endStream ? TestH2.FLAG_END_STREAM : 0), streamId,
				new byte[] { (byte) 0x88 });
	}

	/**
	 * Sends the payload as DATA frames within the windows the relay advertised, waiting
	 * for its grants; an empty payload sends one empty frame.
	 */
	public void sendData(int streamId, byte[] payload, boolean endStream) throws IOException, InterruptedException {
		int offset = 0;
		do {
			int n;
			synchronized (this) {
				while (true) {
					if (this.eof) {
						throw new IOException("the relay went away");
					}
					long available = Math.min(this.sendConnectionWindow,
							this.sendStreamWindows.getOrDefault(streamId, this.sendInitialWindow));
					n = (int) Math.min(Math.min(payload.length - offset, MAX_FRAME_SIZE), Math.max(0, available));
					if (n > 0 || offset == payload.length) {
						break;
					}
					wait();
				}
				int sent = n;
				this.sendConnectionWindow -= sent;
				this.sendStreamWindows.merge(streamId, this.sendInitialWindow - sent,
						(window, ignored) -> window - sent);
			}
			boolean last = offset + n == payload.length;
			write(TestH2.DATA, last && endStream ? TestH2.FLAG_END_STREAM : 0, streamId,
					Arrays.copyOfRange(payload, offset, offset + n));
			offset += n;
		}
		while (offset < payload.length);
	}

	/** Writes a frame as given. */
	public void write(int type, int flags, int streamId, byte[] payload) throws IOException {
		synchronized (this.writeLock) {
			TestH2.writeFrame(this.out, type, flags, streamId, payload);
		}
	}

	/** The largest excess of a DATA frame over a receive window; above 0 a violation. */
	public synchronized long maxExcess() {
		return this.maxExcess;
	}

	/** The DATA payload received on the stream. */
	public synchronized long received(int streamId) {
		return this.received.getOrDefault(streamId, 0L);
	}

	/** The DATA payload received on every stream. */
	public synchronized Map<Integer, Long> received() {
		return Map.copyOf(this.received);
	}

	/** The receive window of the stream as the books stand. */
	public synchronized long streamWindow(int streamId) {
		return this.streamWindows.getOrDefault(streamId, this.initialWindow);
	}

	/** The connection receive window as the books stand. */
	public synchronized long connectionWindow() {
		return this.connectionWindow;
	}

	/** The streams opened so far, with their receive windows. */
	public synchronized Map<Integer, Long> streamWindows() {
		return Map.copyOf(this.streamWindows);
	}

	/** Whether the relay acknowledged a SETTINGS carrying an initial window. */
	public synchronized boolean windowSettingAcked() {
		return this.windowSettingAcked;
	}

	/** The lowest stream receive window seen once a changed initial window applied. */
	public synchronized long minStreamWindowAfterAck() {
		return this.minStreamWindowAfterAck;
	}

	/** The lowest connection receive window seen. */
	public synchronized long minConnectionWindow() {
		return this.minConnectionWindow;
	}

	/** The connection-level grants the relay sent for this upstream's DATA. */
	public synchronized long connectionGrantsReceived() {
		return this.connectionGrantsReceived;
	}

	@Override
	public void close() throws IOException {
		this.socket.close();
	}

}
