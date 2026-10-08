package am.ik.sluice.server.proxy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.jspecify.annotations.Nullable;

/**
 * Consumes a PROXY protocol header (the v1 human-readable or the v2 binary format of
 * <a href="http://www.haproxy.org/download/1.8/doc/proxy-protocol.txt">the
 * specification</a>) from the head of an incoming connection and exposes the original
 * (pre-NAT) endpoints. Presence is detected by the v2 signature or the {@code PROXY }
 * prefix, so headerless connections pass untouched; once a header is detected it is
 * validated strictly -- a truncated or malformed one fails the parse and, with it, the
 * connection. The v2 header is read exactly to its declared length and the v1 header to
 * its terminating CRLF, so no payload byte is consumed; the probe of a headerless stream
 * is pushed back.
 */
public final class ProxyProtocolParser {

	/** The 12-byte signature every v2 header starts with. */
	static final byte[] V2_SIGNATURE = { 0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A };

	private static final byte[] V1_PREFIX = "PROXY ".getBytes(StandardCharsets.US_ASCII);

	/** The longest permitted v1 header, terminating CRLF included. */
	static final int V1_MAX_LENGTH = 108;

	/** The most read ahead while detecting a header; also the pushback buffer size. */
	private static final int PROBE_LIMIT = 16;

	private static final int V2_ADDRESS_HEADER_LENGTH = 4;

	private static final int CMD_LOCAL = 0x0;

	private static final int CMD_PROXY = 0x1;

	private static final int FAMILY_UNSPEC = 0x0;

	private static final int FAMILY_INET = 0x1;

	private static final int FAMILY_INET6 = 0x2;

	private static final int PORT_LENGTH = 2;

	private static final int IPV4_LENGTH = 4;

	private static final int IPV6_LENGTH = 16;

	private static final int TLV_HEADER_LENGTH = 3;

	/**
	 * The original connection endpoints carried by the header; {@code null} addresses (a
	 * v1 {@code UNKNOWN} line or a v2 {@code LOCAL} command) leave the peer unchanged.
	 *
	 * @param source the original source (client) address
	 * @param sourcePort the original source port, {@code -1} when unknown
	 * @param dest the original destination address
	 * @param destPort the original destination port, {@code -1} when unknown
	 */
	public record Header(@Nullable InetAddress source, int sourcePort, @Nullable InetAddress dest, int destPort) {

	}

	/**
	 * The outcome of the detection: the parsed header ({@code null} when the stream does
	 * not begin with one) and the stream to continue reading from -- past the consumed
	 * header, or positioned back at its first byte.
	 *
	 * @param header the parsed header
	 * @param input the remaining stream
	 */
	public record Result(@Nullable Header header, InputStream input) {

	}

	/**
	 * Consumes a PROXY protocol header from the head of the stream when one is present.
	 * @throws IOException on EOF inside a header, or when a detected header is malformed
	 */
	public Result parse(InputStream in) throws IOException {
		PushbackInputStream pushback = new PushbackInputStream(in, PROBE_LIMIT);
		int first = pushback.read();
		if (first < 0) {
			return new Result(null, pushback);
		}
		ByteArrayOutputStream probe = new ByteArrayOutputStream(PROBE_LIMIT);
		probe.write(first);
		if (first == V2_SIGNATURE[0] && matches(pushback, probe, V2_SIGNATURE)) {
			return new Result(parseV2(pushback), pushback);
		}
		if (first == V1_PREFIX[0] && matches(pushback, probe, V1_PREFIX)) {
			return new Result(parseV1(pushback, probe), pushback);
		}
		pushback.unread(probe.toByteArray());
		return new Result(null, pushback);
	}

	private static Header parseV1(PushbackInputStream in, ByteArrayOutputStream probe) throws IOException {
		while (probe.size() < V1_MAX_LENGTH) {
			int b = in.read();
			if (b < 0) {
				throw new IOException("truncated PROXY protocol v1 header");
			}
			probe.write(b);
			if (b == '\n') {
				byte[] line = probe.toByteArray();
				if (line[line.length - 2] != '\r') {
					throw new IOException("PROXY protocol v1 header not terminated by CRLF");
				}
				return v1HeaderOf(line);
			}
		}
		throw new IOException("PROXY protocol v1 header exceeds %d bytes".formatted(V1_MAX_LENGTH));
	}

	/**
	 * Parses the fields of a consumed v1 header (the terminating CRLF included).
	 */
	private static Header v1HeaderOf(byte[] line) throws IOException {
		// the prefix is guaranteed by the detection, the CRLF by the caller
		String body = new String(line, 0, line.length - 2, StandardCharsets.US_ASCII).substring(V1_PREFIX.length);
		if (body.equals("UNKNOWN") || body.startsWith("UNKNOWN ")) {
			return new Header(null, -1, null, -1);
		}
		String[] tokens = body.split(" ", -1);
		if (tokens.length != 5) {
			throw new IOException("malformed PROXY protocol v1 header: " + body);
		}
		boolean ipv4 = switch (tokens[0]) {
			case "TCP4" -> true;
			case "TCP6" -> false;
			default -> throw new IOException("unsupported PROXY protocol v1 protocol: " + tokens[0]);
		};
		InetAddress source = addressOf(tokens[1], ipv4);
		InetAddress dest = addressOf(tokens[2], ipv4);
		return new Header(source, portOf(tokens[3], "source"), dest, portOf(tokens[4], "destination"));
	}

	private static Header parseV2(InputStream in) throws IOException {
		byte[] header = new byte[V2_ADDRESS_HEADER_LENGTH];
		if (in.readNBytes(header, 0, header.length) < header.length) {
			throw new IOException("truncated PROXY protocol v2 header");
		}
		int version = (header[0] & 0xF0) >> 4;
		int command = header[0] & 0x0F;
		if (version != 0x2) {
			throw new IOException("unsupported PROXY protocol v2 version: " + version);
		}
		if (command != CMD_LOCAL && command != CMD_PROXY) {
			throw new IOException("unsupported PROXY protocol v2 command: " + command);
		}
		int family = (header[1] & 0xF0) >> 4;
		int transport = header[1] & 0x0F;
		if (transport > 0x2) { // UNSPEC, STREAM, DGRAM
			throw new IOException("unsupported PROXY protocol v2 transport: " + transport);
		}
		int addressLength = switch (family) {
			case FAMILY_UNSPEC -> 0;
			case FAMILY_INET -> 2 * IPV4_LENGTH + 2 * PORT_LENGTH;
			case FAMILY_INET6 -> 2 * IPV6_LENGTH + 2 * PORT_LENGTH;
			default -> throw new IOException("unsupported PROXY protocol v2 address family: " + family);
		};
		if (family == FAMILY_UNSPEC && command == CMD_PROXY) {
			// with no address family there are no endpoints to proxy
			throw new IOException("PROXY protocol v2 UNSPEC family requires the LOCAL command");
		}
		int length = unsigned16(header, 2);
		byte[] payload = new byte[length];
		if (in.readNBytes(payload, 0, length) < length) {
			throw new IOException("truncated PROXY protocol v2 payload");
		}
		if (length < addressLength) {
			throw new IOException("truncated PROXY protocol v2 address block");
		}
		validateTlvs(payload, addressLength);
		if (command == CMD_LOCAL) {
			return new Header(null, -1, null, -1);
		}
		int addressLength2 = (addressLength - 2 * PORT_LENGTH) / 2;
		InetAddress source = addressOf(Arrays.copyOfRange(payload, 0, addressLength2));
		InetAddress dest = addressOf(Arrays.copyOfRange(payload, addressLength2, 2 * addressLength2));
		int sourcePort = unsigned16(payload, 2 * addressLength2);
		int destPort = unsigned16(payload, 2 * addressLength2 + PORT_LENGTH);
		return new Header(source, sourcePort, dest, destPort);
	}

	/**
	 * Every byte after the address block must be a well-formed TLV: a type, a 2-byte
	 * value length and that many value bytes.
	 */
	private static void validateTlvs(byte[] payload, int offset) throws IOException {
		int pos = offset;
		while (pos < payload.length) {
			if (pos + TLV_HEADER_LENGTH > payload.length) {
				throw new IOException("truncated PROXY protocol v2 TLV");
			}
			int end = pos + TLV_HEADER_LENGTH + unsigned16(payload, pos + 1);
			if (end > payload.length) {
				throw new IOException("truncated PROXY protocol v2 TLV value");
			}
			pos = end;
		}
	}

	/**
	 * Reads the pattern's remaining bytes into the probe; {@code false} (with every read
	 * byte kept in the probe, ready to be pushed back) on EOF or mismatch.
	 */
	private static boolean matches(PushbackInputStream in, ByteArrayOutputStream probe, byte[] pattern)
			throws IOException {
		for (int i = probe.size(); i < pattern.length; i++) {
			int b = in.read();
			if (b < 0) {
				return false;
			}
			probe.write(b);
			if (b != (pattern[i] & 0xff)) {
				return false;
			}
		}
		return true;
	}

	private static InetAddress addressOf(String token, boolean ipv4) throws IOException {
		if (ipv4) {
			String[] octets = token.split("\\.", -1);
			if (octets.length != IPV4_LENGTH) {
				throw new IOException("malformed PROXY protocol IPv4 address: " + token);
			}
			byte[] bytes = new byte[IPV4_LENGTH];
			for (int i = 0; i < IPV4_LENGTH; i++) {
				bytes[i] = (byte) unsignedIntOf(octets[i], 3, 255, "IPv4 octet");
			}
			return InetAddress.getByAddress(bytes);
		}
		// a colon-containing token is parsed as a literal, never resolved by name
		if (!token.matches("[0-9a-fA-F:.]+") || token.indexOf(':') < 0) {
			throw new IOException("malformed PROXY protocol IPv6 address: " + token);
		}
		try {
			return InetAddress.getByName(token);
		}
		catch (UnknownHostException e) {
			throw new IOException("malformed PROXY protocol IPv6 address: " + token, e);
		}
	}

	private static InetAddress addressOf(byte[] bytes) throws IOException {
		try {
			return InetAddress.getByAddress(bytes);
		}
		catch (UnknownHostException e) {
			throw new IOException("malformed PROXY protocol v2 address", e);
		}
	}

	private static int unsignedIntOf(String token, int maxDigits, int max, String name) throws IOException {
		if (!token.matches("\\d{1,%d}".formatted(maxDigits))) {
			throw new IOException("malformed PROXY protocol %s: %s".formatted(name, token));
		}
		int value = Integer.parseInt(token);
		if (value > max) {
			throw new IOException("PROXY protocol %s out of range: %s".formatted(name, token));
		}
		return value;
	}

	private static int portOf(String token, String name) throws IOException {
		return unsignedIntOf(token, 5, 0xFFFF, name + " port");
	}

	private static int unsigned16(byte[] bytes, int pos) {
		return ((bytes[pos] & 0xff) << 8) | (bytes[pos + 1] & 0xff);
	}

}
