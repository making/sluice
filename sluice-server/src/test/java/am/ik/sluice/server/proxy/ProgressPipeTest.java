package am.ik.sluice.server.proxy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.Test;

import am.ik.sluice.tunnel.DuplexPipe;

import static org.assertj.core.api.Assertions.assertThat;

class ProgressPipeTest {

	private static final int PIECE = ConnectionLimits.KeepAliveDeadline.WRITE_PIECE;

	private final List<Long> reported = new CopyOnWriteArrayList<>();

	@Test
	void aWriteIsMadeAndReportedInPieces() throws Exception {
		ByteArrayOutputStream written = new ByteArrayOutputStream();
		OutputStream sink = ProgressPipe.of(pipe(InputStream.nullInputStream(), written), this.reported::add).sink();
		byte[] bytes = new byte[2 * PIECE + 100];
		bytes[bytes.length - 1] = 7;
		sink.write(bytes, 0, bytes.length);
		sink.write(9);
		assertThat(this.reported).containsExactly((long) PIECE, (long) PIECE, 100L, 1L);
		assertThat(written.toByteArray()).hasSize(bytes.length + 1).endsWith(7, 9);
	}

	@Test
	void everyReadReturningBytesIsReported() throws Exception {
		InputStream source = ProgressPipe
			.of(pipe(new ByteArrayInputStream(new byte[] { 1, 2, 3, 4 }), OutputStream.nullOutputStream()),
					this.reported::add)
			.source();
		assertThat(source.read()).isEqualTo(1);
		assertThat(source.read(new byte[8])).isEqualTo(3);
		assertThat(source.read(new byte[8])).isEqualTo(-1);
		assertThat(source.read()).isEqualTo(-1);
		assertThat(this.reported).containsExactly(1L, 3L);
	}

	private static DuplexPipe pipe(InputStream source, OutputStream sink) {
		return new DuplexPipe() {

			@Override
			public InputStream source() {
				return source;
			}

			@Override
			public OutputStream sink() {
				return sink;
			}

			@Override
			public void shutdownOutput() {
			}

			@Override
			public void close() {
			}

		};
	}

}
