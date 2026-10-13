package am.ik.sluice.it;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.Arrays;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The test client's own bookkeeping: a stream ended by a header block counts as ended
 * only once the whole block arrived, so a test reading the block once the stream ended
 * reads all of it.
 */
class H2TestClientTest {

	@Test
	void aHeaderBlockEndingItsStreamEndsItOnlyWithItsLastFrame() throws Exception {
		try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
				H2TestClient client = H2TestClient.connect(server.getLocalPort(), 1);
				Socket peer = server.accept()) {
			InputStream in = peer.getInputStream();
			OutputStream out = peer.getOutputStream();
			in.readNBytes(TestH2.PREFACE.length);
			byte[] block = TestH2.headerBlock(":status=200", "x-long=" + "v".repeat(100));
			int split = block.length / 2;
			byte[] first = Arrays.copyOfRange(block, 0, split);
			byte[] rest = Arrays.copyOfRange(block, split, block.length);
			TestH2.writeFrame(out, TestH2.HEADERS, TestH2.FLAG_END_STREAM, 1, first);
			TestH2.writeFrame(out, TestH2.PING, 0, 0, new byte[8]);
			// a frame inside the block: once the client saw it, it handled the HEADERS
			assertThat(client.awaitTrue(() -> !client.interleaving().isEmpty(), Duration.ofSeconds(5))).isTrue();
			assertThat(client.ended(1)).as("not ended before the block's last frame").isFalse();
			TestH2.writeFrame(out, TestH2.CONTINUATION, TestH2.FLAG_END_HEADERS, 1, rest);
			assertThat(client.awaitEnd(1, Duration.ofSeconds(5))).as("ended with the block's last frame").isTrue();
			assertThat(client.firstHeaderBlock(1)).isEqualTo(block);
		}
	}

}
