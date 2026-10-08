package am.ik.sluice.server.proxy;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Optional;

import org.jspecify.annotations.Nullable;

/**
 * A CIDR range or bare IP literal ({@code 10.0.0.0/8}, {@code 2001:db8::/32},
 * {@code 127.0.0.1}). The network is masked to the prefix at construction; matches
 * normalize IPv4-mapped IPv6 addresses to plain IPv4 so {@code ::ffff:127.0.0.1} matches
 * {@code 127.0.0.0/8} and vice versa.
 */
record Cidr(byte[] network, int prefix) {

	/**
	 * Parses a CIDR or bare address; empty when the value is malformed. Literal addresses
	 * only.
	 */
	static Optional<Cidr> of(String value) {
		if (value == null || value.isBlank()) {
			return Optional.empty();
		}
		String[] parts = value.split("/", 2);
		try {
			byte[] address = InetAddress.getByName(parts[0].trim()).getAddress();
			int prefix = parts.length == 2 ? Integer.parseInt(parts[1].trim()) : address.length * 8;
			if (prefix < 0 || prefix > address.length * 8) {
				return Optional.empty();
			}
			return Optional.of(new Cidr(masked(address, prefix), prefix));
		}
		catch (Exception e) {
			return Optional.empty();
		}
	}

	/**
	 * Whether the given address is in this network; a null peer never matches.
	 */
	boolean matches(@Nullable InetAddress peer) {
		if (peer == null) {
			return false;
		}
		byte[] peerBytes = ipv4MappedToIpv4(peer.getAddress());
		byte[] networkBytes = ipv4MappedToIpv4(this.network);
		if (peerBytes.length != networkBytes.length) {
			return false;
		}
		// bits the shortening removed count against the prefix
		int prefix = this.prefix - (this.network.length - networkBytes.length) * 8;
		for (int i = 0; i < networkBytes.length; i++) {
			int bits = Math.max(0, Math.min(8, prefix - i * 8));
			if (bits == 0) {
				break;
			}
			int mask = bits == 8 ? 0xFF : 0xFF << (8 - bits) & 0xFF;
			if ((peerBytes[i] & mask) != (networkBytes[i] & mask)) {
				return false;
			}
		}
		return true;
	}

	private static byte[] masked(byte[] address, int prefix) {
		byte[] masked = address.clone();
		for (int i = 0; i < masked.length; i++) {
			int bits = Math.max(0, Math.min(8, prefix - i * 8));
			if (bits == 8) {
				continue;
			}
			masked[i] &= bits == 0 ? 0 : 0xFF << (8 - bits) & 0xFF;
		}
		return masked;
	}

	/**
	 * {@code ::ffff:a.b.c.d} as 4 bytes; any other address unchanged.
	 */
	private static byte[] ipv4MappedToIpv4(byte[] address) {
		if (address.length != 16) {
			return address;
		}
		for (int i = 0; i < 10; i++) {
			if (address[i] != 0) {
				return address;
			}
		}
		if (address[10] != (byte) 0xff || address[11] != (byte) 0xff) {
			return address;
		}
		return Arrays.copyOfRange(address, 12, 16);
	}

}
