package am.ik.sluice.server.proxy;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Where a connection's share of the queue budget counts as reached: the boundary the
 * relay withholds its windows at, stops charging a peer's window at, and refuses header
 * blocks at.
 */
class NettyHttp2DemuxRelayShareTest {

	private static final long ALLOWANCE = NettyHttp2DemuxRelay.CONNECTION_ALLOWANCE;

	private static final long SHARE = NettyHttp2DemuxRelay.CONNECTION_SHARE;

	private static final long BLOCK_SHARE = NettyHttp2DemuxRelay.BLOCK_SHARE;

	@Test
	void aShareExactlyAtTheConnectionShareIsReached() {
		assertThat(SHARE).as("the share the boundary is pinned against").isPositive();
		assertThat(NettyHttp2DemuxRelay.atShare(SHARE)).isTrue();
	}

	@Test
	void aShareOneByteUnderTheConnectionShareIsNotReached() {
		assertThat(NettyHttp2DemuxRelay.atShare(SHARE - 1)).isFalse();
		assertThat(NettyHttp2DemuxRelay.atShare(0)).isFalse();
	}

	@Test
	void aSharePastTheConnectionShareIsReached() {
		assertThat(NettyHttp2DemuxRelay.atShare(SHARE + 1)).isTrue();
	}

	@Test
	void theShareIsWhatIsQueuedBeyondTheAllowancePlusTheChargedWindow() {
		assertThat(ALLOWANCE).isPositive();
		assertThat(NettyHttp2DemuxRelay.share(0, 0)).isZero();
		assertThat(NettyHttp2DemuxRelay.share(ALLOWANCE, 0)).as("the allowance is free").isZero();
		assertThat(NettyHttp2DemuxRelay.share(ALLOWANCE - 1, 0)).isZero();
		assertThat(NettyHttp2DemuxRelay.share(ALLOWANCE + 7, 0)).isEqualTo(7);
		assertThat(NettyHttp2DemuxRelay.share(0, 11)).as("a window held with nothing queued").isEqualTo(11);
		assertThat(NettyHttp2DemuxRelay.share(ALLOWANCE + 7, 11)).isEqualTo(18);
	}

	@Test
	void aHeaderBlockExactlyAtTwiceTheShareIsAdmitted() {
		assertThat(BLOCK_SHARE).as("twice the share").isEqualTo(2 * SHARE);
		assertThat(NettyHttp2DemuxRelay.pastBlockShare(BLOCK_SHARE)).isFalse();
		assertThat(NettyHttp2DemuxRelay.pastBlockShare(BLOCK_SHARE + 1)).isTrue();
	}

	@Test
	void theWindowAloneTakesAHeaderBlockPastTwiceTheShare() {
		long window = 512 * 1024;
		long block = 32 * 1024;
		// queued so that, without the window, the block makes exactly twice the share
		long queued = ALLOWANCE + BLOCK_SHARE - block;
		assertThat(NettyHttp2DemuxRelay.pastBlockShare(NettyHttp2DemuxRelay.share(queued + block, 0)))
			.as("without the window the block fits")
			.isFalse();
		assertThat(NettyHttp2DemuxRelay.pastBlockShare(NettyHttp2DemuxRelay.share(queued + block, window)))
			.as("with the window it is past")
			.isTrue();
		assertThat(NettyHttp2DemuxRelay.pastBlockShare(NettyHttp2DemuxRelay.share(queued + block, 0) + 1))
			.as("one byte more is enough")
			.isTrue();
	}

}
