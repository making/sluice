package am.ik.sluice.server.proxy.hop;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.function.UnaryOperator;

import am.ik.sluice.tunnel.DuplexPipe;
import io.netty.handler.codec.http2.Http2FrameAdapter;

/**
 * One hop on a loop of its own over one end of a loopback TCP connection, for tests of a
 * single hop against a peer: another endpoint, or a raw socket.
 */
final class Endpoint implements AutoCloseable {

	/**
	 * The send buffer of the accepted socket, to keep the slack of what a hop writes to
	 * its peer small. The connecting socket keeps the system's receive buffer: one this
	 * small on loopback (MSS near 64KiB) lets Linux stall the connection with the
	 * receiver's buffer empty -- its window stays below the sender's MSS, the sender
	 * waits for a larger one, and the receiver, reading nothing, announces none (seen on
	 * CI: 20s rwnd-limited, 20-33KiB not sent).
	 */
	static final int SEND_BUFFER = 16 * 1024;

	final RecordingSink sink = new RecordingSink();

	final Loop loop;

	final Hop hop;

	private Endpoint(String name, Socket socket, UnaryOperator<Hop.Builder> customizer) {
		this.loop = new Loop(name, this.sink);
		this.hop = this.loop.call(
				() -> customizer
					.apply(Hop.builder()
						.name(name)
						.loop(this.loop)
						.pipe(DuplexPipe.of(socket))
						.listener(hop -> new Http2FrameAdapter()))
					.build());
	}

	/** A hop over the socket. */
	static Endpoint over(String name, Socket socket, UnaryOperator<Hop.Builder> customizer) {
		return new Endpoint(name, socket, customizer);
	}

	/** Two connected loopback sockets: the accepted one first. */
	static Socket[] socketPair() {
		try (ServerSocket server = new ServerSocket()) {
			server.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
			Socket socket = new Socket();
			socket.connect(server.getLocalSocketAddress());
			Socket accepted = server.accept();
			accepted.setSendBufferSize(SEND_BUFFER);
			return new Socket[] { accepted, socket };
		}
		catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	/** Closes the loop (and so the hop) and waits for the hop's threads. */
	@Override
	public void close() {
		this.loop.close();
		Fixture.awaitDone(List.of(this.hop));
	}

}
