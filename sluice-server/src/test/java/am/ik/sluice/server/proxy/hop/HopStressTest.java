package am.ik.sluice.server.proxy.hop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integrity across the bridge under concurrency: many uploads and downloads at once over
 * two legs, each body checked by CRC. A hop whose writer reads the channel's ByteBufs
 * outside the lock instead of the heap copy garbles what it writes; this test then fails
 * (a frame header arrives garbled and the connection ends, or a CRC differs).
 */
@Tag("netty-hop")
@ExtendWith(LeakCheck.class)
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class HopStressTest {

	private static final int ROUNDS = 3;

	private static final int STREAMS_PER_KIND_AND_LEG = 4;

	private static final int BODY = 512 * 1024;

	@org.junit.jupiter.api.RepeatedTest(value = 150, failureThreshold = 1)
	void concurrentUploadsAndDownloadsOverTwoLegsArriveIntact() throws Exception {
		try (Fixture fixture = Fixture.twoRoutes()) {
			long seed = 1;
			for (int round = 0; round < ROUNDS; round++) {
				List<Check> checks = new ArrayList<>();
				for (int i = 0; i < STREAMS_PER_KIND_AND_LEG; i++) {
					for (String authority : List.of("a.test", "b.test")) {
						byte[] upload = Payloads.of(seed++, BODY);
						TestClient.Sent sent = fixture.client.send(authority, "/echo", upload);
						String upstream = authority.substring(0, 1);
						checks.add(new Check(sent, "upstream=%s stream=%d bytes=%d crc=%s".formatted(upstream,
								sent.streamId(), BODY, Payloads.crc(upload))));
						long downloadSeed = seed++;
						checks.add(new Check(
								fixture.client.send(authority, "/download/" + BODY + "/" + downloadSeed, new byte[0]),
								Payloads.crc(Payloads.of(downloadSeed, BODY))));
					}
				}

				for (Check check : checks) {
					TestClient.Response response = await(fixture, check.sent());
					assertThat(response.status()).isEqualTo("200");
					String actual = check.expected().startsWith("upstream=") ? response.text()
							: Payloads.crc(response.body());
					assertThat(actual).as("stream %d", check.sent().streamId()).isEqualTo(check.expected());
				}
			}
			assertThat(fixture.failures()).isEmpty();
		}
	}

	private static TestClient.Response await(Fixture fixture, TestClient.Sent sent) throws Exception {
		try {
			return sent.response().get(20, TimeUnit.SECONDS);
		}
		catch (TimeoutException | ExecutionException e) {
			// temporary: the kernel's view of the loopback sockets
			try {
				Process ss = new ProcessBuilder("sh", "-c", "ss -tnmie state established 2>&1")
					.redirectErrorStream(true)
					.start();
				System.out.println("=== ss\n" + new String(ss.getInputStream().readAllBytes()));
			}
			catch (IOException ignored) {
			}
			throw new AssertionError(
					"stream %d got no response (%s): %s".formatted(sent.streamId(), e, fixture.describe()), e);
		}
	}

	/** A response and what its body (or its CRC) must be. */
	private record Check(TestClient.Sent sent, String expected) {
	}

}
