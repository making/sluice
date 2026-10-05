package am.ik.sluice.server.proxy;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;

import org.jspecify.annotations.Nullable;

/**
 * Peeks the TLS ClientHello of an incoming connection and extracts the SNI host name (the
 * first server_name entry of type host_name) without terminating TLS. Only the record
 * layer and the ClientHello structure are parsed; the bytes are returned verbatim so they
 * can be replayed to the upstream as the beginning of the relayed stream.
 */
public final class SniHeadParser {

	private static final int RECORD_HEADER_LENGTH = 5;

	/** TLS record content type 0x16: handshake. */
	public static final int TLS_HANDSHAKE_RECORD = 0x16;

	private static final int HANDSHAKE_HEADER_LENGTH = 4;

	/** Handshake message type 0x01: ClientHello. */
	public static final int HANDSHAKE_CLIENT_HELLO = 0x01;

	/** Extension type 0x0000: server_name. */
	public static final int EXTENSION_SERVER_NAME = 0x0000;

	/** server_name entry type 0: host_name. */
	public static final int NAME_TYPE_HOST_NAME = 0x00;

	private final int maxHeadSize;

	public SniHeadParser(int maxHeadSize) {
		if (maxHeadSize <= 0) {
			throw new IllegalArgumentException("maxHeadSize must be positive");
		}
		this.maxHeadSize = maxHeadSize;
	}

	/**
	 * The extracted SNI host name ({@code null} when the ClientHello carries none) and
	 * the verbatim consumed bytes.
	 */
	public record Head(@Nullable String sni, byte[] bytes) {

	}

	/**
	 * Reads from the stream until a ClientHello has been consumed; empty on EOF, error,
	 * or when the bytes are not a TLS handshake record.
	 */
	public Optional<Head> parse(InputStream in) {
		ByteArrayOutputStream buffer = new ByteArrayOutputStream(1024);
		byte[] chunk = new byte[1024];
		try {
			int n;
			while (buffer.size() < this.maxHeadSize && (n = in.read(chunk)) > 0) {
				buffer.write(chunk, 0, n);
				byte[] bytes = buffer.toByteArray();
				ClientHello hello = clientHelloOf(bytes);
				if (hello != null) {
					byte[] consumed = Arrays.copyOf(bytes, hello.consumed());
					return Optional.of(new Head(serverNameOf(hello.handshake()), consumed));
				}
			}
		}
		catch (Exception e) {
			return Optional.empty();
		}
		return Optional.empty();
	}

	/**
	 * A reassembled ClientHello handshake message and the number of record bytes consumed
	 * to obtain it (record headers included).
	 */
	private record ClientHello(byte[] handshake, int consumed) {

	}

	/**
	 * Walks consecutive TLS handshake records and reassembles the ClientHello;
	 * {@code null} while the handshake is still incomplete, or when the bytes are not a
	 * ClientHello.
	 */
	private static @Nullable ClientHello clientHelloOf(byte[] bytes) {
		ByteArrayOutputStream handshake = new ByteArrayOutputStream(256);
		int handshakeLength = -1;
		int pos = 0;
		while (true) {
			if (pos + RECORD_HEADER_LENGTH > bytes.length) {
				return null; // record header not buffered yet
			}
			if ((bytes[pos] & 0xff) != TLS_HANDSHAKE_RECORD) {
				return null; // not a TLS handshake
			}
			int recordLength = unsigned16(bytes, pos + 3);
			pos += RECORD_HEADER_LENGTH;
			if (pos + recordLength > bytes.length) {
				return null; // record payload not buffered yet
			}
			if (handshakeLength < 0) {
				if (recordLength < HANDSHAKE_HEADER_LENGTH || (bytes[pos] & 0xff) != HANDSHAKE_CLIENT_HELLO) {
					return null; // first handshake message is not a ClientHello
				}
				handshakeLength = HANDSHAKE_HEADER_LENGTH + unsigned24(bytes, pos + 1);
			}
			int remaining = handshakeLength - handshake.size();
			handshake.write(bytes, pos, Math.min(recordLength, remaining));
			pos += recordLength;
			if (handshake.size() >= handshakeLength) {
				return new ClientHello(Arrays.copyOf(handshake.toByteArray(), handshakeLength), pos);
			}
		}
	}

	/**
	 * Extracts the first host_name of the server_name extension; {@code null} when the
	 * ClientHello carries no SNI.
	 */
	static @Nullable String serverNameOf(byte[] hello) {
		int pos = HANDSHAKE_HEADER_LENGTH;
		pos += 2; // legacy version
		pos += 32; // random
		if (pos >= hello.length) {
			return null;
		}
		pos += 1 + (hello[pos] & 0xff); // session id
		if (pos + 2 > hello.length) {
			return null;
		}
		pos += 2 + unsigned16(hello, pos); // cipher suites
		if (pos >= hello.length) {
			return null;
		}
		pos += 1 + (hello[pos] & 0xff); // compression methods
		if (pos + 2 > hello.length) {
			return null;
		}
		int extensionsEnd = Math.min(pos + 2 + unsigned16(hello, pos), hello.length);
		pos += 2;
		while (pos + 4 <= extensionsEnd) {
			int type = unsigned16(hello, pos);
			int length = unsigned16(hello, pos + 2);
			int body = pos + 4;
			pos = body + length;
			if (type != EXTENSION_SERVER_NAME) {
				continue;
			}
			int namesEnd = Math.min(pos, body + 2 + unsigned16(hello, body));
			int entry = body + 2;
			while (entry + 3 <= namesEnd) {
				int nameType = hello[entry] & 0xff;
				int nameLength = unsigned16(hello, entry + 1);
				int name = entry + 3;
				entry = name + nameLength;
				if (nameType != NAME_TYPE_HOST_NAME || name > namesEnd) {
					continue;
				}
				return new String(hello, name, nameLength, StandardCharsets.US_ASCII);
			}
		}
		return null;
	}

	private static int unsigned16(byte[] bytes, int pos) {
		return ((bytes[pos] & 0xff) << 8) | (bytes[pos + 1] & 0xff);
	}

	private static int unsigned24(byte[] bytes, int pos) {
		return ((bytes[pos] & 0xff) << 16) | ((bytes[pos + 1] & 0xff) << 8) | (bytes[pos + 2] & 0xff);
	}

}
