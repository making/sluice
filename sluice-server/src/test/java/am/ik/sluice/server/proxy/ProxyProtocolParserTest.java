package am.ik.sluice.server.proxy;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIOException;

class ProxyProtocolParserTest {

	private static final byte[] V2_SIGNATURE = { 0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54,
			0x0A };

	private static final byte[] PAYLOAD = "GET / HTTP/1.1\r\nHost: example.local\r\n\r\n"
		.getBytes(StandardCharsets.US_ASCII);

	@Test
	void v1Tcp4HeaderIsParsedWithThePayloadIntact() throws Exception {
		ProxyProtocolParser.Result result = parse(withPayload(
				"PROXY TCP4 255.255.255.255 255.255.255.255 65535 65535\r\n".getBytes(StandardCharsets.US_ASCII)));
		ProxyProtocolParser.Header header = headerOf(result);
		assertThat(header.source()).isEqualTo(InetAddress.getByName("255.255.255.255"));
		assertThat(header.sourcePort()).isEqualTo(65535);
		assertThat(header.dest()).isEqualTo(InetAddress.getByName("255.255.255.255"));
		assertThat(header.destPort()).isEqualTo(65535);
		assertThat(remaining(result)).isEqualTo(PAYLOAD);
	}

	@Test
	void v1Tcp6HeaderIsParsed() throws Exception {
		ProxyProtocolParser.Result result = parse(withPayload(
				"PROXY TCP6 2001:db8:0:0:0:0:2:1 2001:db8::2 1234 80\r\n".getBytes(StandardCharsets.US_ASCII)));
		ProxyProtocolParser.Header header = headerOf(result);
		assertThat(header.source()).isEqualTo(InetAddress.getByName("2001:db8:0:0:0:0:2:1"));
		assertThat(header.sourcePort()).isEqualTo(1234);
		assertThat(header.dest()).isEqualTo(InetAddress.getByName("2001:db8::2"));
		assertThat(header.destPort()).isEqualTo(80);
		assertThat(remaining(result)).isEqualTo(PAYLOAD);
	}

	@Test
	void v1UnknownLeavesThePeerUnchanged() throws Exception {
		ProxyProtocolParser.Result result = parse(withPayload("PROXY UNKNOWN\r\n".getBytes(StandardCharsets.US_ASCII)));
		ProxyProtocolParser.Header header = headerOf(result);
		assertThat(header.source()).isNull();
		assertThat(header.sourcePort()).isEqualTo(-1);
		assertThat(remaining(result)).isEqualTo(PAYLOAD);
	}

	@Test
	void v1HeaderOfTheMaximumLengthIsAccepted() throws Exception {
		// 108 bytes including the terminating CRLF: the longest header the spec mandates
		String line = "PROXY UNKNOWN".concat(" ".repeat(108 - 2 - 13)).concat("\r\n");
		assertThat(line.length()).isEqualTo(108);
		ProxyProtocolParser.Result result = parse(line.getBytes(StandardCharsets.US_ASCII));
		assertThat(headerOf(result).source()).isNull();
	}

	@Test
	void v2Tcp4HeaderIsParsedWithThePayloadIntact() throws Exception {
		// the spec's TCP4 example: 192.0.2.1:47263 -> 192.0.2.3:12345, one TLV behind
		byte[] header = ByteBuffer.allocate(V2_SIGNATURE.length + 4 + 12 + 4)
			.put(V2_SIGNATURE)
			.put((byte) 0x21) // version 2, command PROXY
			.put((byte) 0x11) // family INET, transport STREAM
			.putShort((short) 16) // 12 address bytes + one 4-byte TLV
			.put(new byte[] { (byte) 192, 0, 2, 1 })
			.put(new byte[] { (byte) 192, 0, 2, 3 })
			.putShort((short) 47263)
			.putShort((short) 12345)
			.put((byte) 0x01) // PP2_TYPE_ALPN
			.putShort((short) 1)
			.put((byte) 0x02)
			.array();
		ProxyProtocolParser.Result result = parse(withPayload(header));
		ProxyProtocolParser.Header parsed = headerOf(result);
		assertThat(parsed.source()).isEqualTo(InetAddress.getByName("192.0.2.1"));
		assertThat(parsed.sourcePort()).isEqualTo(47263);
		assertThat(parsed.dest()).isEqualTo(InetAddress.getByName("192.0.2.3"));
		assertThat(parsed.destPort()).isEqualTo(12345);
		assertThat(remaining(result)).isEqualTo(PAYLOAD);
	}

	@Test
	void v2Tcp6HeaderIsParsed() throws Exception {
		byte[] source = InetAddress.getByName("2001:db8::1").getAddress();
		byte[] dest = InetAddress.getByName("2001:db8::2").getAddress();
		byte[] header = ByteBuffer.allocate(V2_SIGNATURE.length + 4 + 36)
			.put(V2_SIGNATURE)
			.put((byte) 0x21)
			.put((byte) 0x21) // family INET6, transport STREAM
			.putShort((short) 36)
			.put(source)
			.put(dest)
			.putShort((short) 65535)
			.putShort((short) 1)
			.array();
		ProxyProtocolParser.Result result = parse(header);
		ProxyProtocolParser.Header parsed = headerOf(result);
		assertThat(parsed.source()).isEqualTo(InetAddress.getByName("2001:db8::1"));
		assertThat(parsed.sourcePort()).isEqualTo(65535);
		assertThat(parsed.dest()).isEqualTo(InetAddress.getByName("2001:db8::2"));
		assertThat(parsed.destPort()).isEqualTo(1);
	}

	@Test
	void v2LocalCommandLeavesThePeerUnchanged() throws Exception {
		byte[] header = ByteBuffer.allocate(V2_SIGNATURE.length + 4)
			.put(V2_SIGNATURE)
			.put((byte) 0x20) // version 2, command LOCAL
			.put((byte) 0x00) // family UNSPEC, transport UNSPEC
			.putShort((short) 0)
			.array();
		ProxyProtocolParser.Result result = parse(withPayload(header));
		assertThat(headerOf(result).source()).isNull();
		assertThat(remaining(result)).isEqualTo(PAYLOAD);
	}

	private static byte[] withPayload(byte[] header) {
		return ByteBuffer.allocate(header.length + PAYLOAD.length).put(header).put(PAYLOAD).array();
	}

	@Test
	void v2UdpTransportIsAccepted() throws Exception {
		byte[] header = ByteBuffer.allocate(V2_SIGNATURE.length + 4 + 12)
			.put(V2_SIGNATURE)
			.put((byte) 0x21)
			.put((byte) 0x12) // family INET, transport DGRAM
			.putShort((short) 12)
			.put(new byte[] { 10, 0, 0, 1 })
			.put(new byte[] { 10, 0, 0, 2 })
			.putShort((short) 1)
			.putShort((short) 2)
			.array();
		assertThat(headerOf(parse(header)).source()).isEqualTo(InetAddress.getByName("10.0.0.1"));
	}

	@Test
	void aStreamWithoutAHeaderIsUntouched() throws Exception {
		ProxyProtocolParser.Result result = parse(PAYLOAD);
		assertThat(result.header()).isNull();
		assertThat(remaining(result)).isEqualTo(PAYLOAD);
	}

	@Test
	void aStreamThatMerelyStartsLikeTheSignatureIsUntouched() throws Exception {
		byte[] bytes = ByteBuffer.allocate(PAYLOAD.length)
			.put(V2_SIGNATURE, 0, 6)
			.put(PAYLOAD, 6, PAYLOAD.length - 6)
			.array();
		ProxyProtocolParser.Result result = parse(bytes);
		assertThat(result.header()).isNull();
		assertThat(remaining(result)).isEqualTo(bytes);
	}

	@Test
	void anEmptyStreamHasNoHeader() throws Exception {
		ProxyProtocolParser.Result result = parse(new byte[0]);
		assertThat(result.header()).isNull();
		assertThat(result.input().read()).isEqualTo(-1);
	}

	@Test
	void v1HeadersFailOnMalformedFields() {
		assertThatIOException()
			.isThrownBy(() -> parse("PROXY UDP 1.2.3.4 5.6.7.8 1 2\r\n".getBytes(StandardCharsets.US_ASCII)));
		assertThatIOException()
			.isThrownBy(() -> parse("PROXY TCP4 1.2.3 5.6.7.8 1 2\r\n".getBytes(StandardCharsets.US_ASCII)));
		assertThatIOException()
			.isThrownBy(() -> parse("PROXY TCP4 1.2.3.4.5 5.6.7.8 1 2\r\n".getBytes(StandardCharsets.US_ASCII)));
		assertThatIOException()
			.isThrownBy(() -> parse("PROXY TCP4 1.2.3.4 5.6.7.8 70000 2\r\n".getBytes(StandardCharsets.US_ASCII)));
		assertThatIOException()
			.isThrownBy(() -> parse("PROXY TCP4 1.2.3.4 5.6.7.8 1\r\n".getBytes(StandardCharsets.US_ASCII)));
		assertThatIOException()
			.isThrownBy(() -> parse("PROXY  TCP4 1.2.3.4 5.6.7.8 1 2\r\n".getBytes(StandardCharsets.US_ASCII)));
		assertThatIOException()
			.isThrownBy(() -> parse("PROXY TCP4 1.2.3.4 5.6.7.8 1 2\n".getBytes(StandardCharsets.US_ASCII)));
	}

	@Test
	void v1HeadersFailWhenTruncatedOrTooLong() {
		assertThatIOException().isThrownBy(() -> parse("PROXY TCP4 1.2.3.4".getBytes(StandardCharsets.US_ASCII)));
		String tooLong = "PROXY TCP4 1.2.3.4 5.6.7.8 1 2".concat(" ".repeat(80)).concat("\r\n");
		assertThat(tooLong.length()).isGreaterThan(108);
		assertThatIOException().isThrownBy(() -> parse(tooLong.getBytes(StandardCharsets.US_ASCII)));
	}

	@Test
	void v2HeadersFailWhenMalformed() {
		// unsupported version / command / family
		assertThatIOException().isThrownBy(() -> parse(header(0x11, 0x11, 12, new byte[12])));
		assertThatIOException().isThrownBy(() -> parse(header(0x22, 0x11, 12, new byte[12])));
		assertThatIOException().isThrownBy(() -> parse(header(0x21, 0x31, 12, new byte[12])));
		// PROXY with the UNSPEC family has no endpoints to proxy
		assertThatIOException().isThrownBy(() -> parse(header(0x21, 0x01, 0, new byte[0])));
		// address block / TLV / payload truncated
		assertThatIOException().isThrownBy(() -> parse(header(0x21, 0x11, 13, new byte[13])));
		assertThatIOException().isThrownBy(() -> parse(header(0x21, 0x11, 12, new byte[11])));
		assertThatIOException().isThrownBy(() -> parse(truncated(0x21, 0x11, 20)));
	}

	private static byte[] header(int vercmd, int familyProto, int length, byte[] payload) {
		return ByteBuffer.allocate(V2_SIGNATURE.length + 4 + payload.length)
			.put(V2_SIGNATURE)
			.put((byte) vercmd)
			.put((byte) familyProto)
			.putShort((short) length)
			.put(payload)
			.array();
	}

	private static byte[] truncated(int vercmd, int familyProto, int length) {
		return ByteBuffer.allocate(V2_SIGNATURE.length + 4)
			.put(V2_SIGNATURE)
			.put((byte) vercmd)
			.put((byte) familyProto)
			.putShort((short) length)
			.array();
	}

	private static ProxyProtocolParser.Header headerOf(ProxyProtocolParser.Result result) {
		return java.util.Objects.requireNonNull(result.header(), "header is required");
	}

	private static ProxyProtocolParser.Result parse(byte... bytes) throws IOException {
		return new ProxyProtocolParser().parse(new ByteArrayInputStream(bytes));
	}

	private static byte[] remaining(ProxyProtocolParser.Result result) throws IOException {
		return result.input().readAllBytes();
	}

}
