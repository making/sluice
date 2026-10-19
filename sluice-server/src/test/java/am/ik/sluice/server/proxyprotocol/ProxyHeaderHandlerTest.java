package am.ik.sluice.server.proxyprotocol;

import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import io.netty.buffer.AbstractByteBufAllocator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCounted;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;

class ProxyHeaderHandlerTest {

	private static final Duration DEADLINE = Duration.ofSeconds(10);

	private static final byte[] V2_SIGNATURE = ProxyProtocolParser.V2_SIGNATURE;

	/** A plaintext h2 client's preface: shares "PR" with a v1 header. */
	private static final byte[] H2_PREFACE = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".getBytes(StandardCharsets.US_ASCII);

	/** The first bytes of a TLS ClientHello. */
	private static final byte[] CLIENT_HELLO = { 0x16, 0x03, 0x01, 0x00, 0x05, 0x01, 0x00, 0x00, 0x01, 0x00 };

	static Stream<Arguments> heads() {
		return Stream.of(Arguments.of("v2 header", concat(v2(0x21, 0x11), H2_PREFACE), "203.0.113.7:32100", H2_PREFACE),
				Arguments.of("v1 header",
						concat("PROXY TCP4 203.0.113.7 198.51.100.7 32101 8001\r\n".getBytes(StandardCharsets.US_ASCII),
								CLIENT_HELLO),
						"203.0.113.7:32101", CLIENT_HELLO),
				Arguments.of("v2 LOCAL header", concat(v2(0x20, 0x00), CLIENT_HELLO), null, CLIENT_HELLO),
				Arguments.of("headerless h2 preface", H2_PREFACE, null, H2_PREFACE),
				Arguments.of("headerless ClientHello", CLIENT_HELLO, null, CLIENT_HELLO));
	}

	/**
	 * Fed a byte at a time, the handler holds back the negotiation event and the bytes
	 * until the head is decided, then passes the event on before the bytes after any
	 * header, sets the header's source and leaves the pipeline; its deadline no longer
	 * applies.
	 */
	@ParameterizedTest(name = "{0}")
	@MethodSource("heads")
	void decidesTheHeadByteByByte(String name, byte[] input, @Nullable String source, byte[] passed) throws Exception {
		Fixture fixture = new Fixture();
		fixture.channel.pipeline().fireUserEventTriggered("negotiation");
		List<ByteBuf> fed = new ArrayList<>();
		for (byte b : input) {
			ByteBuf chunk = Unpooled.wrappedBuffer(new byte[] { b });
			fed.add(chunk);
			fixture.channel.writeInbound(chunk);
			if (fixture.channel.pipeline().get(ProxyHeaderHandler.class) != null) {
				assertThat(fixture.downstream.log).isEmpty();
			}
		}
		assertThat(fixture.channel.pipeline().get(ProxyHeaderHandler.class)).isNull();
		assertThat(fixture.downstream.log).containsExactly("event:negotiation", "bytes");
		assertThat(fixture.downstream.bytes.toByteArray()).isEqualTo(passed);
		InetSocketAddress actual = fixture.channel.attr(ProxyHeaderHandler.SOURCE).get();
		assertThat(actual == null ? null : actual.getAddress().getHostAddress() + ":" + actual.getPort())
			.isEqualTo(source);
		fixture.channel.advanceTimeBy(DEADLINE.toMillis() * 2, TimeUnit.MILLISECONDS);
		fixture.channel.runScheduledPendingTasks();
		assertThat(fixture.channel.isOpen()).isTrue();
		assertThat(fed).allSatisfy(chunk -> assertThat(chunk.refCnt()).isZero());
		assertThat(fixture.allocator.allocated).allSatisfy(buffer -> assertThat(buffer.refCnt()).isZero());
	}

	static Stream<Arguments> failures() {
		return Stream.of(Arguments.of("unsupported v2 family", v2(0x21, 0x31), false),
				Arguments.of("unsupported v1 protocol",
						"PROXY TCP5 203.0.113.7 198.51.100.7 1 2\r\n".getBytes(StandardCharsets.US_ASCII), false),
				Arguments.of("v1 header past its maximum length",
						("PROXY TCP4 " + "1".repeat(ProxyProtocolParser.V1_MAX_LENGTH))
							.getBytes(StandardCharsets.US_ASCII),
						false),
				Arguments.of("partial header", "PROXY TCP4 203.0".getBytes(StandardCharsets.US_ASCII), true),
				Arguments.of("no bytes at all", new byte[0], true));
	}

	/**
	 * A malformed header closes the connection at once, a head undecided by the deadline
	 * at the deadline; nothing reaches the handlers behind, and what was held is
	 * released.
	 */
	@ParameterizedTest(name = "{0}")
	@MethodSource("failures")
	void closesTheConnection(String name, byte[] input, boolean atTheDeadline) throws Exception {
		Fixture fixture = new Fixture();
		HeldEvent event = new HeldEvent();
		fixture.channel.pipeline().fireUserEventTriggered(event);
		ByteBuf chunk = Unpooled.wrappedBuffer(input);
		if (input.length > 0) {
			fixture.channel.writeInbound(chunk);
		}
		if (atTheDeadline) {
			fixture.channel.advanceTimeBy(DEADLINE.toMillis() - 1, TimeUnit.MILLISECONDS);
			fixture.channel.runScheduledPendingTasks();
			assertThat(fixture.channel.isOpen()).isTrue();
			fixture.channel.advanceTimeBy(1, TimeUnit.MILLISECONDS);
			fixture.channel.runScheduledPendingTasks();
		}
		fixture.channel.runPendingTasks();
		assertThat(fixture.channel.isOpen()).isFalse();
		assertThat(fixture.downstream.log).isEmpty();
		assertThat(fixture.channel.attr(ProxyHeaderHandler.SOURCE).get()).isNull();
		assertThat(event.refCnt()).isZero();
		if (input.length > 0) {
			assertThat(chunk.refCnt()).isZero();
		}
		assertThat(fixture.allocator.allocated).allSatisfy(buffer -> assertThat(buffer.refCnt()).isZero());
	}

	/** A v2 header for 203.0.113.7:32100 -> 198.51.100.7:8001 (INET STREAM). */
	private static byte[] v2(int versionCommand, int familyTransport) {
		boolean inet = familyTransport == 0x11;
		ByteBuffer header = ByteBuffer.allocate(V2_SIGNATURE.length + 4 + (inet ? 12 : 0))
			.put(V2_SIGNATURE)
			.put((byte) versionCommand)
			.put((byte) familyTransport)
			.putShort((short) (inet ? 12 : 0));
		if (inet) {
			header.put(new byte[] { (byte) 203, 0, (byte) 113, 7 })
				.put(new byte[] { (byte) 198, 51, 100, 7 })
				.putShort((short) 32100)
				.putShort((short) 8001);
		}
		return header.array();
	}

	private static byte[] concat(byte[] first, byte[] second) {
		return ByteBuffer.allocate(first.length + second.length).put(first).put(second).array();
	}

	/** A channel with the handler first, a recorder behind it, on a frozen clock. */
	private static final class Fixture {

		final RecordingAllocator allocator = new RecordingAllocator();

		final Downstream downstream = new Downstream();

		final EmbeddedChannel channel = new EmbeddedChannel(false, false);

		Fixture() throws Exception {
			this.channel.config().setAllocator(this.allocator);
			this.channel.pipeline().addLast(this.downstream);
			this.channel.register();
			// the deadline is scheduled as the handler is added, on the frozen clock
			this.channel.freezeTime();
			this.channel.pipeline().addFirst(new ProxyHeaderHandler(DEADLINE));
		}

	}

	/** Records the events and the bytes that pass the handler, in order. */
	private static final class Downstream extends ChannelInboundHandlerAdapter {

		final List<String> log = new ArrayList<>();

		final ByteArrayOutputStream bytes = new ByteArrayOutputStream();

		@Override
		public void channelRead(ChannelHandlerContext ctx, Object msg) {
			ByteBuf buffer = (ByteBuf) msg;
			if (this.log.isEmpty() || !this.log.getLast().equals("bytes")) {
				this.log.add("bytes");
			}
			this.bytes.writeBytes(ByteBufUtil.getBytes(buffer));
			buffer.release();
		}

		@Override
		public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
			this.log.add("event:" + evt);
		}

	}

	/** An event that must be released when the handler drops it. */
	private static final class HeldEvent extends AbstractReferenceCounted {

		@Override
		protected void deallocate() {
		}

		@Override
		public ReferenceCounted touch(Object hint) {
			return this;
		}

	}

	/** Heap buffers whose reference counts the test checks afterwards. */
	private static final class RecordingAllocator extends AbstractByteBufAllocator {

		final List<ByteBuf> allocated = new ArrayList<>();

		@Override
		protected ByteBuf newHeapBuffer(int initialCapacity, int maxCapacity) {
			ByteBuf buffer = Unpooled.buffer(initialCapacity, maxCapacity);
			this.allocated.add(buffer);
			return buffer;
		}

		@Override
		protected ByteBuf newDirectBuffer(int initialCapacity, int maxCapacity) {
			return this.newHeapBuffer(initialCapacity, maxCapacity);
		}

		@Override
		public boolean isDirectBufferPooled() {
			return false;
		}

	}

}
