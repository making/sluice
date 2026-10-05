package am.ik.sluice.server.proxy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link SniHeadParser} over hand-crafted ClientHello records; the real
 * handshake path is covered by the SNI routing E2E test in sluice-it.
 */
class SniHeadParserTest {

	private static final SniHeadParser PARSER = new SniHeadParser(64 * 1024);

	@Test
	void extractsServerName() {
		byte[] hello = clientHello("a.sni.test");
		SniHeadParser.Head head = PARSER.parse(new ByteArrayInputStream(hello)).orElseThrow();
		assertThat(head.sni()).isEqualTo("a.sni.test");
		assertThat(head.bytes()).isEqualTo(hello);
	}

	@Test
	void returnsEmptyWhenServerNameExtensionIsAbsent() {
		byte[] hello = clientHello(null);
		SniHeadParser.Head head = PARSER.parse(new ByteArrayInputStream(hello)).orElseThrow();
		assertThat(head.sni()).isNull();
		assertThat(head.bytes()).isEqualTo(hello);
	}

	@Test
	void reassemblesHandshakeFragmentedAcrossRecords() {
		byte[] hello = clientHello("b.sni.test");
		// split the record payload in two, each in its own record header
		int split = (hello.length - 5) / 2;
		byte[] bytes = concat(record(hello, 5, split), record(hello, 5 + split, hello.length - 5 - split));
		SniHeadParser.Head head = PARSER.parse(new ByteArrayInputStream(bytes)).orElseThrow();
		assertThat(head.sni()).isEqualTo("b.sni.test");
		assertThat(head.bytes()).isEqualTo(bytes);
	}

	@Test
	void returnsEmptyForNonTlsBytes() {
		assertThat(PARSER.parse(new ByteArrayInputStream("GET / HTTP/1.1\r\n\r\n".getBytes(StandardCharsets.US_ASCII))))
			.isEmpty();
	}

	@Test
	void returnsEmptyOnEof() {
		assertThat(PARSER.parse(InputStream.nullInputStream())).isEmpty();
	}

	/** Builds a minimal ClientHello record carrying a single server_name, or none. */
	private static byte[] clientHello(@Nullable String serverName) {
		ByteArrayOutputStream body = new ByteArrayOutputStream();
		body.write(0x03); // legacy version major
		body.write(0x03); // legacy version minor
		body.writeBytes(new byte[32]); // random
		body.write(0); // empty session id
		body.writeBytes(new byte[] { 0x00, 0x02, 0x13, 0x01 }); // one cipher suite
																// (TLS_AES_128_GCM_SHA256)
		body.write(1); // compression methods
		body.write(0); // null compression
		ByteArrayOutputStream extensions = new ByteArrayOutputStream();
		extensions.writeBytes(new byte[] { 0x00, 0x17, 0x00, 0x00 }); // extended master
																		// secret, empty
		if (serverName != null) {
			byte[] name = serverName.getBytes(StandardCharsets.US_ASCII);
			ByteArrayOutputStream list = new ByteArrayOutputStream();
			list.write(SniHeadParser.NAME_TYPE_HOST_NAME);
			writeUnsigned16(list, name.length);
			list.writeBytes(name);
			ByteArrayOutputStream extension = new ByteArrayOutputStream();
			writeUnsigned16(extension, list.size());
			extension.writeBytes(list.toByteArray());
			extensions.writeBytes(new byte[] { (byte) (SniHeadParser.EXTENSION_SERVER_NAME >> 8),
					(byte) SniHeadParser.EXTENSION_SERVER_NAME });
			writeUnsigned16(extensions, extension.size());
			extensions.writeBytes(extension.toByteArray());
		}
		body.writeBytes(extensions.size() > 0 ? prefixLength16(extensions) : new byte[] { 0, 0 });
		byte[] handshake = concat(new byte[] { (byte) SniHeadParser.HANDSHAKE_CLIENT_HELLO },
				prefixLength24(body.toByteArray()));
		// record: content type 0x16 (handshake), version 0x0301 (TLS 1.2 legacy), length
		return concat(recordHeader(handshake.length), handshake);
	}

	/**
	 * A TLS record header: 0x16 (handshake), version 0x03 0x01 (TLS 1.2 legacy), length.
	 */
	private static byte[] recordHeader(int length) {
		return new byte[] { (byte) SniHeadParser.TLS_HANDSHAKE_RECORD, 0x03, 0x01, (byte) (length >> 8),
				(byte) length };
	}

	/**
	 * Wraps {@code hello[ offset, offset+length)} as the payload of a fresh TLS record.
	 */
	private static byte[] record(byte[] hello, int offset, int length) {
		return concat(recordHeader(length), java.util.Arrays.copyOfRange(hello, offset, offset + length));
	}

	private static void writeUnsigned16(ByteArrayOutputStream out, int value) {
		out.write((value >> 8) & 0xff);
		out.write(value & 0xff);
	}

	private static byte[] prefixLength16(ByteArrayOutputStream body) {
		return prefixLength16(body.toByteArray());
	}

	private static byte[] prefixLength16(byte[] body) {
		return concat(new byte[] { (byte) ((body.length >> 8) & 0xff), (byte) (body.length & 0xff) }, body);
	}

	private static byte[] prefixLength24(byte[] body) {
		return concat(new byte[] { (byte) ((body.length >> 16) & 0xff), (byte) ((body.length >> 8) & 0xff),
				(byte) (body.length & 0xff) }, body);
	}

	private static byte[] concat(byte[]... arrays) {
		int length = 0;
		for (byte[] array : arrays) {
			length += array.length;
		}
		byte[] bytes = new byte[length];
		int pos = 0;
		for (byte[] array : arrays) {
			System.arraycopy(array, 0, bytes, pos, array.length);
			pos += array.length;
		}
		return bytes;
	}

}
