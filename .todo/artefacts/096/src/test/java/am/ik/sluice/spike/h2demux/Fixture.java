package am.ik.sluice.spike.h2demux;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.LongSupplier;
import java.util.function.UnaryOperator;

import io.netty.handler.codec.http2.Http2Settings;

/**
 * One relay with its upstreams and one client connected to it over loopback TCP; each leg
 * reaches its upstream over a {@link TunnelPair}.
 */
final class Fixture implements AutoCloseable {

	/** The socket buffers of the client connection, to keep their slack small. */
	static final int SOCKET_BUFFER = 16 * 1024;

	final Map<String, TestUpstream> upstreams;

	final NettyDemuxRelay relay;

	final TestClient client;

	final Timeline timeline = new Timeline();

	private static final java.util.concurrent.atomic.AtomicInteger RUNS = new java.util.concurrent.atomic.AtomicInteger();

	private Fixture(Builder builder) throws IOException {
		this.upstreams = Map.copyOf(builder.byAuthority);
		Map<String, TestUpstream> byAuthority = this.upstreams;
		try (ServerSocket server = new ServerSocket()) {
			server.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
			Socket socket = new Socket();
			socket.setReceiveBufferSize(SOCKET_BUFFER);
			socket.connect(server.getLocalSocketAddress());
			Socket accepted = server.accept();
			accepted.setSendBufferSize(SOCKET_BUFFER);
			NettyDemuxRelay.Spec spec = builder.relay.apply(NettyDemuxRelay.spec()
				.dialer(authority -> Objects.requireNonNull(byAuthority.get(authority), authority)
					.accept(TunnelPair.open(TestUpstream.TUNNEL_WINDOW), this.timeline))
				.timeline(this.timeline));
			this.relay = NettyDemuxRelay.start(accepted, spec);
			this.client = new TestClient(socket, builder.clientSettings, this.timeline);
		}
	}

	static Builder builder() {
		return new Builder();
	}

	TestUpstream upstream(String authority) {
		return this.upstreams.get(authority);
	}

	/**
	 * Polls until the values stopped changing for 500ms (10s at most), and returns the
	 * first: the point where flow control has stopped every sender.
	 */
	static long settled(LongSupplier... values) throws InterruptedException {
		long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
		long[] last = sample(values);
		long since = System.nanoTime();
		while (System.nanoTime() < deadline) {
			Thread.sleep(50);
			long[] now = sample(values);
			if (!Arrays.equals(now, last)) {
				last = now;
				since = System.nanoTime();
			}
			else if (System.nanoTime() - since >= Duration.ofMillis(500).toNanos()) {
				return now[0];
			}
		}
		throw new AssertionError("still changing after 10s: " + Arrays.toString(last));
	}

	private static long[] sample(LongSupplier... values) {
		long[] sample = new long[values.length];
		for (int i = 0; i < values.length; i++) {
			sample[i] = values[i].getAsLong();
		}
		return sample;
	}

	@Override
	public void close() {
		try {
			java.nio.file.Path dir = java.nio.file.Path.of("target", "timelines");
			java.nio.file.Files.createDirectories(dir);
			java.nio.file.Files.write(dir.resolve(RUNS.incrementAndGet() + ".log"), this.timeline.events());
		}
		catch (IOException e) {
			throw new java.io.UncheckedIOException(e);
		}
		this.client.close();
		this.relay.close();
		this.upstreams.values().forEach(TestUpstream::close);
	}

	static final class Builder {

		private final Map<String, TestUpstream> byAuthority = new LinkedHashMap<>();

		private UnaryOperator<NettyDemuxRelay.Spec> relay = UnaryOperator.identity();

		private Http2Settings clientSettings = new Http2Settings();

		private Builder() {
		}

		/** Routes the authority to the upstream. */
		Builder route(String authority, TestUpstream upstream) {
			this.byAuthority.put(authority, upstream);
			return this;
		}

		Builder relay(UnaryOperator<NettyDemuxRelay.Spec> relay) {
			this.relay = relay;
			return this;
		}

		Builder clientSettings(Http2Settings clientSettings) {
			this.clientSettings = clientSettings;
			return this;
		}

		Fixture start() throws IOException {
			return new Fixture(this);
		}

	}

}
