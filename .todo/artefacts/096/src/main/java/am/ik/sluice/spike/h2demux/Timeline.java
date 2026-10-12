package am.ik.sluice.spike.h2demux;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http2.Http2Flags;
import io.netty.handler.codec.http2.Http2FrameLogger;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2Settings;
import io.netty.handler.logging.LogLevel;

/**
 * Every frame every hop read or wrote, in order, with the hop and the thread: the
 * timeline a failed run dumps.
 */
public final class Timeline {

	private final long start = System.nanoTime();

	private final List<String> events = new CopyOnWriteArrayList<>();

	public void note(String hop, String event) {
		this.events.add("%8.3fms %-24s %-28s %s".formatted((System.nanoTime() - this.start) / 1e6, hop,
				Thread.currentThread().getName(), event));
	}

	public List<String> events() {
		return List.copyOf(this.events);
	}

	/** A frame logger recording the hop's frames here. */
	public Http2FrameLogger logger(String hop) {
		return new Http2FrameLogger(LogLevel.DEBUG, hop) {

			@Override
			public boolean isEnabled() {
				return true;
			}

			@Override
			public void logData(Direction direction, ChannelHandlerContext ctx, int streamId, ByteBuf data, int padding,
					boolean endStream) {
				note(hop, "%s DATA stream=%d length=%d end=%b".formatted(direction, streamId, data.readableBytes(),
						endStream));
			}

			@Override
			public void logHeaders(Direction direction, ChannelHandlerContext ctx, int streamId, Http2Headers headers,
					int padding, boolean endStream) {
				note(hop, "%s HEADERS stream=%d end=%b %s".formatted(direction, streamId, endStream, headers));
			}

			@Override
			public void logHeaders(Direction direction, ChannelHandlerContext ctx, int streamId, Http2Headers headers,
					int streamDependency, short weight, boolean exclusive, int padding, boolean endStream) {
				logHeaders(direction, ctx, streamId, headers, padding, endStream);
			}

			@Override
			public void logPriority(Direction direction, ChannelHandlerContext ctx, int streamId, int streamDependency,
					short weight, boolean exclusive) {
				note(hop, "%s PRIORITY stream=%d".formatted(direction, streamId));
			}

			@Override
			public void logRstStream(Direction direction, ChannelHandlerContext ctx, int streamId, long errorCode) {
				note(hop, "%s RST_STREAM stream=%d code=%d".formatted(direction, streamId, errorCode));
			}

			@Override
			public void logSettingsAck(Direction direction, ChannelHandlerContext ctx) {
				note(hop, "%s SETTINGS ack".formatted(direction));
			}

			@Override
			public void logSettings(Direction direction, ChannelHandlerContext ctx, Http2Settings settings) {
				note(hop, "%s SETTINGS %s".formatted(direction, settings));
			}

			@Override
			public void logPing(Direction direction, ChannelHandlerContext ctx, long data) {
				note(hop, "%s PING".formatted(direction));
			}

			@Override
			public void logPingAck(Direction direction, ChannelHandlerContext ctx, long data) {
				note(hop, "%s PING ack".formatted(direction));
			}

			@Override
			public void logPushPromise(Direction direction, ChannelHandlerContext ctx, int streamId,
					int promisedStreamId, Http2Headers headers, int padding) {
				note(hop, "%s PUSH_PROMISE stream=%d".formatted(direction, streamId));
			}

			@Override
			public void logGoAway(Direction direction, ChannelHandlerContext ctx, int lastStreamId, long errorCode,
					ByteBuf debugData) {
				note(hop, "%s GOAWAY last=%d code=%d".formatted(direction, lastStreamId, errorCode));
			}

			@Override
			public void logWindowsUpdate(Direction direction, ChannelHandlerContext ctx, int streamId,
					int windowSizeIncrement) {
				note(hop,
						"%s WINDOW_UPDATE stream=%d increment=%d".formatted(direction, streamId, windowSizeIncrement));
			}

			@Override
			public void logUnknownFrame(Direction direction, ChannelHandlerContext ctx, byte frameType, int streamId,
					Http2Flags flags, ByteBuf data) {
				note(hop, "%s UNKNOWN type=%d stream=%d".formatted(direction, frameType, streamId));
			}

		};
	}

}
