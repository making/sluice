package am.ik.sluice.client.tunnel;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Freeing a direct memory chunk of the pooled allocator gRPC's Netty transport uses. On a
 * native image Netty has no Unsafe and frees direct memory by closing a shared
 * {@code Arena}, which the image must be built to support; without it the release fails
 * and takes the tunnel's gRPC connection down. Run on a native image with
 * {@code ./mvnw -PnativeTest -pl sluice-client -am test -Dtest=DirectBufferReleaseTest -Dsurefire.failIfNoSpecifiedTests=false}.
 */
class DirectBufferReleaseTest {

	@Test
	void releasingAChunkFreesItsDirectMemory() {
		PooledByteBufAllocator allocator = new PooledByteBufAllocator(true);
		// larger than a chunk: allocated as a chunk of its own, destroyed on release
		ByteBuf buffer = allocator.directBuffer(32 * 1024 * 1024);
		buffer.writeZero(buffer.capacity());

		assertThat(buffer.release()).isTrue();
		assertThat(allocator.metric().usedDirectMemory()).isZero();
	}

}
