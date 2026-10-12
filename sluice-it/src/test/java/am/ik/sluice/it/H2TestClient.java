package am.ik.sluice.it;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.jspecify.annotations.Nullable;

/**
 * A prior-knowledge h2 client for the E2E tests that honors the flow-control windows the
 * relay grants: {@link #data} waits until both the stream and the connection window admit
 * the bytes. The reader thread records the relay's settings, window grants, resets,
 * GOAWAY, the stream ends, and whether any frame landed inside a header block (RFC 9113
 * 6.10).
 * <p>
 * As a receiver it keeps its connection window: every DATA frame received is charged to
 * it, padding included, and the largest excess over what it granted is kept. By default
 * it grants back every frame's bytes on receipt; {@link #paceConnectionGrants} and
 * {@link #withholdConnectionWindow} make it sparser. Stream windows are never granted:
 * the initial stream window it announces bounds each stream.
 */
public final class H2TestClient implements AutoCloseable {

	/** The spec initial window; the relay announces no other. */
	public static final int INITIAL_WINDOW = 65535;

	private static final int MAX_FRAME_SIZE = 16 * 1024;

	private final Socket socket;

	private final OutputStream out;

	private final Map<Integer, Long> settings = new ConcurrentHashMap<>();

	private final AtomicBoolean settingsReceived = new AtomicBoolean();

	private final AtomicLong connectionWindow = new AtomicLong(INITIAL_WINDOW);

	private final AtomicLong connectionGranted = new AtomicLong();

	private final Map<Integer, AtomicLong> streamWindows = new ConcurrentHashMap<>();

	private final Map<Integer, AtomicLong> streamGranted = new ConcurrentHashMap<>();

	private final Map<Integer, AtomicLong> streamSent = new ConcurrentHashMap<>();

	/** The largest {@code sent - window} ever seen on any stream; above 0 is a breach. */
	private final AtomicLong maxOvershoot = new AtomicLong(Long.MIN_VALUE);

	private final Map<Integer, Integer> resets = new ConcurrentHashMap<>();

	private final Set<Integer> ended = ConcurrentHashMap.newKeySet();

	private final Set<Integer> collected;

	private final Map<Integer, List<TestH2.Frame>> frames = new ConcurrentHashMap<>();

	private final AtomicLong dataReceived = new AtomicLong();

	private final AtomicInteger goAwayCode = new AtomicInteger(-1);

	private final AtomicInteger goAwayLastStreamId = new AtomicInteger(-1);

	private final AtomicReference<String> interleaving = new AtomicReference<>("");

	private final AtomicInteger headerBlocks = new AtomicInteger();

	private final List<byte[]> pingAcks = new CopyOnWriteArrayList<>();

	private final AtomicBoolean closed = new AtomicBoolean();

	/** The connection window the relay may still send into, as granted. */
	private final AtomicLong receiveWindow = new AtomicLong(INITIAL_WINDOW);

	/** The largest {@code received - granted} ever seen; above 0 is a breach. */
	private final AtomicLong maxReceiveExcess = new AtomicLong(Long.MIN_VALUE);

	private final Map<Integer, AtomicLong> streamReceived = new ConcurrentHashMap<>();

	private final Object grantLock = new Object();

	/**
	 * The connection grant policy: 0 grants every frame on receipt, a positive step is
	 * paced by {@link #paceConnectionGrants}, -1 grants nothing; guarded by
	 * {@link #grantLock}.
	 */
	private int grantStep;

	/** Received and not granted back yet; guarded by {@link #grantLock}. */
	private long ungranted;

	private H2TestClient(Socket socket, Set<Integer> collected) throws IOException {
		this.socket = socket;
		this.out = socket.getOutputStream();
		this.collected = collected;
	}

	/**
	 * Connects, sends the preface and an empty SETTINGS, and starts the reader. Frames of
	 * the {@code collected} streams are kept; the others are only counted.
	 */
	public static H2TestClient connect(int port, Integer... collected) throws IOException {
		H2TestClient client = new H2TestClient(new Socket("127.0.0.1", port), Set.of(collected));
		Thread.ofVirtual().name("e2e-h2-client-reader").start(client::read);
		synchronized (client.out) {
			client.out.write(TestH2.PREFACE);
			TestH2.writeFrame(client.out, TestH2.SETTINGS, 0, 0, new byte[0]);
		}
		return client;
	}

	private void read() {
		try {
			InputStream in = this.socket.getInputStream();
			int blockOf = -1;
			TestH2.@Nullable Frame frame;
			while ((frame = TestH2.readFrame(in)) != null) {
				if (blockOf != -1 && (frame.type() != TestH2.CONTINUATION || frame.streamId() != blockOf)) {
					this.interleaving.compareAndSet("", "type=%d stream=%d inside the header block of stream %d"
						.formatted(frame.type(), frame.streamId(), blockOf));
				}
				if (frame.type() == TestH2.HEADERS || frame.type() == TestH2.CONTINUATION) {
					boolean endHeaders = (frame.flags() & TestH2.FLAG_END_HEADERS) != 0;
					blockOf = endHeaders ? -1 : frame.streamId();
					if (endHeaders) {
						this.headerBlocks.incrementAndGet();
					}
				}
				onFrame(frame);
			}
		}
		catch (IOException e) {
			// reset counts as closed
		}
		this.closed.set(true);
	}

	private void onFrame(TestH2.Frame frame) {
		int streamId = frame.streamId();
		if (this.collected.contains(streamId)) {
			this.frames.computeIfAbsent(streamId, id -> new CopyOnWriteArrayList<>()).add(frame);
		}
		switch (frame.type()) {
			case TestH2.SETTINGS -> {
				if ((frame.flags() & TestH2.FLAG_ACK) == 0) {
					byte[] p = frame.payload();
					for (int i = 0; i + 6 <= p.length; i += 6) {
						int id = (p[i] & 0xff) << 8 | (p[i + 1] & 0xff);
						long value = (p[i + 2] & 0xffL) << 24 | (p[i + 3] & 0xff) << 16 | (p[i + 4] & 0xff) << 8
								| (p[i + 5] & 0xff);
						this.settings.put(id, value);
					}
					this.settingsReceived.set(true);
				}
			}
			case TestH2.WINDOW_UPDATE -> {
				int increment = TestH2.increment(frame);
				if (streamId == 0) {
					this.connectionGranted.addAndGet(increment);
					this.connectionWindow.addAndGet(increment);
				}
				else {
					grantedOf(streamId).addAndGet(increment);
					windowOf(streamId).addAndGet(increment);
				}
			}
			case TestH2.DATA -> {
				this.dataReceived.addAndGet(frame.payload().length);
				this.streamReceived.computeIfAbsent(streamId, id -> new AtomicLong()).addAndGet(frame.payload().length);
				if ((frame.flags() & TestH2.FLAG_END_STREAM) != 0) {
					this.ended.add(streamId);
				}
				onDataReceived(frame.payload().length);
			}
			case TestH2.HEADERS -> {
				if ((frame.flags() & TestH2.FLAG_END_STREAM) != 0) {
					this.ended.add(streamId);
				}
			}
			case TestH2.RST_STREAM -> this.resets.put(streamId, TestH2.errorCode(frame));
			case TestH2.GOAWAY -> {
				this.goAwayLastStreamId.set(TestH2.increment(frame));
				this.goAwayCode.set(TestH2.errorCode(frame));
			}
			case TestH2.PING -> {
				if ((frame.flags() & TestH2.FLAG_ACK) != 0) {
					this.pingAcks.add(frame.payload());
				}
			}
			default -> {
				// not tracked
			}
		}
	}

	/** Charges the connection window and grants back as the policy says. */
	private void onDataReceived(int length) {
		long window = this.receiveWindow.addAndGet(-length);
		this.maxReceiveExcess.accumulateAndGet(-window, Math::max);
		synchronized (this.grantLock) {
			this.ungranted += length;
			grantDueLocked();
		}
	}

	private void grantDueLocked() {
		if (this.grantStep == 0 && this.ungranted > 0) {
			grantConnection(this.ungranted);
			this.ungranted = 0;
		}
	}

	private void grantConnection(long increment) {
		try {
			frame(TestH2.WINDOW_UPDATE, 0, 0, TestH2.int32((int) increment));
			this.receiveWindow.addAndGet(increment);
		}
		catch (IOException e) {
			// the connection is gone, or half-closed: nothing was granted
		}
	}

	/**
	 * Grants every DATA frame's bytes on receipt from now on, and what was received and
	 * not granted so far at once.
	 */
	public void grantConnectionWindowOnReceipt() {
		synchronized (this.grantLock) {
			this.grantStep = 0;
			grantDueLocked();
		}
	}

	/**
	 * Grants the connection window {@code step} bytes at a time, at most one grant per
	 * {@code interval} and only for bytes received and not granted yet: a reader
	 * consuming slowly, which a sender ignoring the window outpaces.
	 */
	public void paceConnectionGrants(int step, Duration interval) {
		synchronized (this.grantLock) {
			this.grantStep = step;
		}
		Thread.ofVirtual().name("e2e-h2-client-granter").start(() -> {
			while (!this.closed.get()) {
				try {
					Thread.sleep(interval);
				}
				catch (InterruptedException e) {
					return;
				}
				synchronized (this.grantLock) {
					if (this.grantStep != step) {
						return;
					}
					if (this.ungranted >= step) {
						grantConnection(step);
						this.ungranted -= step;
					}
				}
			}
		});
	}

	/** Grants no connection window from now on: the relay keeps what it holds. */
	public void withholdConnectionWindow() {
		synchronized (this.grantLock) {
			this.grantStep = -1;
		}
	}

	/**
	 * The largest excess of the DATA received over the connection window granted; at most
	 * 0 when the relay honored it.
	 */
	public long maxReceiveExcess() {
		return this.maxReceiveExcess.get();
	}

	/** The DATA payload received on the stream, padding included. */
	public long dataReceived(int streamId) {
		AtomicLong received = this.streamReceived.get(streamId);
		return received == null ? 0 : received.get();
	}

	private AtomicLong windowOf(int streamId) {
		return this.streamWindows.computeIfAbsent(streamId, id -> new AtomicLong(INITIAL_WINDOW));
	}

	private AtomicLong grantedOf(int streamId) {
		return this.streamGranted.computeIfAbsent(streamId, id -> new AtomicLong());
	}

	/** Sends a request head, split to HEADERS plus CONTINUATION frames as needed. */
	public void headers(int streamId, boolean endStream, byte[] block) throws IOException {
		synchronized (this.out) {
			int offset = 0;
			boolean first = true;
			do {
				int length = Math.min(MAX_FRAME_SIZE, block.length - offset);
				boolean last = offset + length >= block.length;
				int flags = (first && endStream ? TestH2.FLAG_END_STREAM : 0) | (last ? TestH2.FLAG_END_HEADERS : 0);
				byte[] fragment = new byte[length];
				System.arraycopy(block, offset, fragment, 0, length);
				TestH2.writeFrame(this.out, first ? TestH2.HEADERS : TestH2.CONTINUATION, flags, streamId, fragment);
				first = false;
				offset += length;
			}
			while (offset < block.length);
		}
		windowOf(streamId);
	}

	/**
	 * Sends {@code length} payload bytes on the stream within both windows, waiting up to
	 * {@code timeout} for every chunk's grant; {@code false} when the grant did not come
	 * or the connection closed.
	 */
	public boolean data(int streamId, int length, boolean endStream, Duration timeout)
			throws IOException, InterruptedException {
		int left = length;
		do {
			long deadline = System.nanoTime() + timeout.toNanos();
			int n;
			while (true) {
				long available = Math.min(this.connectionWindow.get(), windowOf(streamId).get());
				n = (int) Math.min(Math.min(left, MAX_FRAME_SIZE), available);
				if (n > 0 || left == 0) {
					break;
				}
				if (this.closed.get() || System.nanoTime() > deadline) {
					return false;
				}
				Thread.sleep(1);
			}
			int flags = endStream && n == left ? TestH2.FLAG_END_STREAM : 0;
			sendData(streamId, new byte[n], flags);
			left -= n;
		}
		while (left > 0);
		return true;
	}

	/**
	 * Sends on the stream whatever both windows admit, until no grant came for
	 * {@code quiet} or the connection closed; returns the payload bytes sent.
	 */
	public long fill(int streamId, Duration quiet) throws IOException, InterruptedException {
		long sent = 0;
		long deadline = System.nanoTime() + quiet.toNanos();
		while (!this.closed.get() && System.nanoTime() < deadline) {
			long available = Math.min(this.connectionWindow.get(), windowOf(streamId).get());
			if (available <= 0) {
				Thread.sleep(1);
				continue;
			}
			int n = (int) Math.min(available, MAX_FRAME_SIZE);
			sendData(streamId, new byte[n], 0);
			sent += n;
			deadline = System.nanoTime() + quiet.toNanos();
		}
		return sent;
	}

	/**
	 * Sends a DATA frame as given, windows ignored; the windows are charged with the
	 * whole payload, padding included (RFC 9113 6.9.1).
	 */
	public void sendData(int streamId, byte[] payload, int flags) throws IOException {
		synchronized (this.out) {
			TestH2.writeFrame(this.out, TestH2.DATA, flags, streamId, payload);
		}
		this.connectionWindow.addAndGet(-payload.length);
		long window = windowOf(streamId).addAndGet(-payload.length);
		this.streamSent.computeIfAbsent(streamId, id -> new AtomicLong()).addAndGet(payload.length);
		this.maxOvershoot.accumulateAndGet(-window, Math::max);
	}

	/** Writes the bytes as they are: frames the client would never send whole. */
	public void raw(byte[] bytes) throws IOException {
		synchronized (this.out) {
			this.out.write(bytes);
			this.out.flush();
		}
	}

	public void frame(int type, int flags, int streamId, byte[] payload) throws IOException {
		synchronized (this.out) {
			TestH2.writeFrame(this.out, type, flags, streamId, payload);
		}
	}

	/** Waits until the relay's SETTINGS arrived. */
	public boolean awaitSettings(Duration timeout) throws InterruptedException {
		return awaitTrue(this.settingsReceived::get, timeout);
	}

	/** Waits until the stream ended (END_STREAM received). */
	public boolean awaitEnd(int streamId, Duration timeout) throws InterruptedException {
		return awaitTrue(() -> this.ended.contains(streamId), timeout);
	}

	public boolean awaitTrue(java.util.function.BooleanSupplier condition, Duration timeout)
			throws InterruptedException {
		long deadline = System.nanoTime() + timeout.toNanos();
		while (!condition.getAsBoolean()) {
			if (System.nanoTime() > deadline) {
				return false;
			}
			Thread.sleep(TimeUnit.MILLISECONDS.toMillis(2));
		}
		return true;
	}

	public @Nullable Long setting(int id) {
		return this.settings.get(id);
	}

	/** The payload both the stream's and the connection's windows admit now. */
	public long sendable(int streamId) {
		return Math.min(this.connectionWindow.get(), windowOf(streamId).get());
	}

	/** The stream window the client may still send into, as the relay granted it. */
	public long streamWindow(int streamId) {
		return windowOf(streamId).get();
	}

	/** The connection window the client may still send into, as the relay granted it. */
	public long connectionWindow() {
		return this.connectionWindow.get();
	}

	public long connectionGranted() {
		return this.connectionGranted.get();
	}

	public long streamGranted(int streamId) {
		return grantedOf(streamId).get();
	}

	public long streamSent(int streamId) {
		AtomicLong sent = this.streamSent.get(streamId);
		return sent == null ? 0 : sent.get();
	}

	/** The largest {@code sent - window} of any stream; at most 0 for a compliant run. */
	public long maxOvershoot() {
		return this.maxOvershoot.get();
	}

	public @Nullable Integer reset(int streamId) {
		return this.resets.get(streamId);
	}

	/** Every RST_STREAM received: stream id to error code. */
	public Map<Integer, Integer> resets() {
		return Map.copyOf(this.resets);
	}

	public int goAwayCode() {
		return this.goAwayCode.get();
	}

	/** The last stream id of the GOAWAY received, or -1. */
	public int goAwayLastStreamId() {
		return this.goAwayLastStreamId.get();
	}

	public boolean closed() {
		return this.closed.get();
	}

	public boolean ended(int streamId) {
		return this.ended.contains(streamId);
	}

	/** The first frame seen inside a header block, or empty. */
	public String interleaving() {
		return this.interleaving.get();
	}

	public int headerBlocks() {
		return this.headerBlocks.get();
	}

	public long dataReceived() {
		return this.dataReceived.get();
	}

	public List<byte[]> pingAcks() {
		return this.pingAcks;
	}

	/** The collected frames of the stream, in arrival order. */
	public List<TestH2.Frame> frames(int streamId) {
		List<TestH2.Frame> list = this.frames.get(streamId);
		return list == null ? List.of() : new ArrayList<>(list);
	}

	/** The concatenated header block fragments of the stream's first block. */
	public byte[] firstHeaderBlock(int streamId) {
		ByteArrayOutputStream block = new ByteArrayOutputStream();
		for (TestH2.Frame frame : frames(streamId)) {
			if (frame.type() == TestH2.HEADERS || frame.type() == TestH2.CONTINUATION) {
				block.writeBytes(frame.payload());
				if ((frame.flags() & TestH2.FLAG_END_HEADERS) != 0) {
					break;
				}
			}
		}
		return block.toByteArray();
	}

	/** Ends the client's sending side of the TCP connection; reading goes on. */
	public void halfClose() throws IOException {
		synchronized (this.out) {
			this.socket.shutdownOutput();
		}
	}

	@Override
	public void close() throws IOException {
		this.socket.close();
	}

}
