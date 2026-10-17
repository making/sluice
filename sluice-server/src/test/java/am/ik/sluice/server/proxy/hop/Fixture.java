package am.ik.sluice.server.proxy.hop;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import am.ik.sluice.tunnel.DuplexPipe;
import io.netty.handler.codec.http2.Http2Settings;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A {@link TestBridge} with its upstreams and one {@link TestClient} connected to it over
 * loopback TCP; each leg reaches its upstream over a {@link TunnelPair}. Closing it
 * closes every hop and waits until all their threads ended.
 */
final class Fixture implements AutoCloseable {

	/** The socket buffers of the client connection, to keep their slack small. */
	static final int SOCKET_BUFFER = 16 * 1024;

	private static final Duration DONE_TIMEOUT = Duration.ofSeconds(10);

	final Map<String, TestUpstream> upstreams;

	final TestBridge relay;

	final TestClient client;

	private Fixture(Map<String, TestUpstream> upstreams, Http2Settings clientSettings) {
		this.upstreams = Map.copyOf(upstreams);
		try (ServerSocket server = new ServerSocket()) {
			server.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
			Socket socket = new Socket();
			socket.setReceiveBufferSize(SOCKET_BUFFER);
			socket.connect(server.getLocalSocketAddress());
			Socket accepted = server.accept();
			accepted.setSendBufferSize(SOCKET_BUFFER);
			this.relay = new TestBridge(DuplexPipe.of(accepted),
					authority -> Objects.requireNonNull(this.upstreams.get(authority), authority)
						.accept(TunnelPair.open()));
			this.client = new TestClient(socket, clientSettings);
		}
		catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	/**
	 * Upstreams {@code a} and {@code b} for the authorities {@code a.test} and
	 * {@code b.test}.
	 */
	static Fixture twoRoutes() {
		return twoRoutes(new Http2Settings());
	}

	static Fixture twoRoutes(Http2Settings clientSettings) {
		Map<String, TestUpstream> upstreams = new LinkedHashMap<>();
		upstreams.put("a.test", new TestUpstream("a"));
		upstreams.put("b.test", new TestUpstream("b"));
		return new Fixture(upstreams, clientSettings);
	}

	TestUpstream upstream(String authority) {
		return Objects.requireNonNull(this.upstreams.get(authority), authority);
	}

	/** Every failure any loop reported. */
	List<RecordingSink.Failure> failures() {
		List<RecordingSink.Failure> failures = new ArrayList<>(this.relay.failures());
		failures.addAll(this.client.failures());
		this.upstreams.values().forEach(upstream -> failures.addAll(upstream.failures()));
		return failures;
	}

	/** The state of every hop and loop failure: what a failed test prints. */
	String describe() {
		StringBuilder state = new StringBuilder();
		List<Hop> hops = new ArrayList<>(this.relay.hops());
		hops.add(this.client.hop());
		this.upstreams.values().forEach(upstream -> hops.addAll(upstream.connections()));
		for (Hop hop : hops) {
			state.append("%n%s open=%b writable=%b queued=%d read=%d written=%d".formatted(hop, hop.open(),
					hop.writable(), hop.queuedBytes(), hop.bytesRead(), hop.bytesWritten()));
			state.append(" ").append(hop.debugState());
			state.append(hop.loop().call(() -> {
				StringBuilder flow = new StringBuilder();
				io.netty.handler.codec.http2.Http2Connection c = hop.connection();
				flow.append("%n   conn local=%d remote=%d active=%d".formatted(
						hop.localFlow().windowSize(c.connectionStream()),
						hop.encoder().flowController().windowSize(c.connectionStream()), c.numActiveStreams()));
				try {
					c.forEachActiveStream(st -> {
						flow.append("%n   s%d %s local=%d unconsumed=%d remote=%d".formatted(st.id(), st.state(),
								hop.localFlow().windowSize(st), hop.localFlow().unconsumedBytes(st),
								hop.encoder().flowController().windowSize(st)));
						return true;
					});
				}
				catch (io.netty.handler.codec.http2.Http2Exception e) {
					throw new IllegalStateException(e);
				}
				return flow.toString();
			}));
		}
		state.append("%nfailures=%s".formatted(failures()));
		return state.toString();
	}

	@Override
	public void close() {
		this.client.close();
		this.relay.close();
		this.upstreams.values().forEach(TestUpstream::close);
	}

	/** Asserts that every hop's reader and writer end soon (their pipes closed). */
	static void awaitDone(List<Hop> hops) {
		for (Hop hop : hops) {
			try {
				assertThat(hop.awaitDone(DONE_TIMEOUT)).as("%s ended", hop).isTrue();
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new IllegalStateException(e);
			}
		}
	}

	/**
	 * Polls until the values stopped changing for 500ms (20s at most), and returns the
	 * first: the point where flow control has stopped every sender.
	 */
	static long settled(LongSupplier... values) throws InterruptedException {
		long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
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
		throw new AssertionError("still changing after 20s: " + Arrays.toString(last));
	}

	static void awaitTrue(BooleanSupplier condition, Supplier<String> state) throws InterruptedException {
		long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
		while (!condition.getAsBoolean()) {
			if (System.nanoTime() > deadline) {
				throw new AssertionError("condition not met within 20s: " + state.get());
			}
			Thread.sleep(10);
		}
	}

	private static long[] sample(LongSupplier... values) {
		long[] sample = new long[values.length];
		for (int i = 0; i < values.length; i++) {
			sample[i] = values[i].getAsLong();
		}
		return sample;
	}

}
