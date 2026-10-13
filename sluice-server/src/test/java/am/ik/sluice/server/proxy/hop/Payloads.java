package am.ik.sluice.server.proxy.hop;

import java.util.SplittableRandom;
import java.util.zip.CRC32;

/**
 * Bodies whose bytes depend on their offset (pseudo-random from a seed), so a chunk
 * landing at the wrong offset changes the CRC.
 */
final class Payloads {

	private Payloads() {
	}

	static byte[] of(long seed, int length) {
		byte[] bytes = new byte[length];
		new SplittableRandom(seed).nextBytes(bytes);
		return bytes;
	}

	static String crc(byte[] bytes) {
		CRC32 crc = new CRC32();
		crc.update(bytes);
		return Long.toHexString(crc.getValue());
	}

}
