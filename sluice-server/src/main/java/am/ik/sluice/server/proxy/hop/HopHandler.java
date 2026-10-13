package am.ik.sluice.server.proxy.hop;

import java.util.Map;
import java.util.Objects;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http2.AbstractHttp2ConnectionHandlerBuilder;
import io.netty.handler.codec.http2.DecoratingHttp2FrameWriter;
import io.netty.handler.codec.http2.Http2CodecUtil;
import io.netty.handler.codec.http2.Http2Connection;
import io.netty.handler.codec.http2.Http2ConnectionDecoder;
import io.netty.handler.codec.http2.Http2ConnectionEncoder;
import io.netty.handler.codec.http2.Http2ConnectionHandler;
import io.netty.handler.codec.http2.Http2Error;
import io.netty.handler.codec.http2.Http2Exception;
import io.netty.handler.codec.http2.Http2Flags;
import io.netty.handler.codec.http2.Http2FrameListener;
import io.netty.handler.codec.http2.Http2FrameLogger;
import io.netty.handler.codec.http2.Http2FrameWriter;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2HeadersEncoder;
import io.netty.handler.codec.http2.Http2Settings;
import io.netty.handler.codec.http2.Http2Stream;
import io.netty.handler.logging.LogLevel;
import org.jspecify.annotations.Nullable;

/**
 * netty's connection handler of a hop, with the limit on the peer's stream errors in
 * place of netty's limit on the resets an endpoint sends.
 * <p>
 * netty's server endpoint (4.2.19) counts every RST_STREAM it writes other than
 * {@code CANCEL} and {@code NO_ERROR} -- the bridging code's own resets and those it
 * relays included -- and ends the connection with GOAWAY {@code ENHANCE_YOUR_CALM} past
 * 200 within 30s. The limit is meant for a peer making the endpoint reset streams by
 * misbehaving ("MadeYouReset"), so a hop counts the stream errors netty's codec raises
 * reading the peer's own frames instead -- frames on a stream netty already forgot
 * included, but those the peer had in flight when the hop reset it (below) -- and past
 * the limit within its window ends the connection the same way. netty's encoder limit is
 * off on every hop; its limit on the resets a server endpoint receives ("Rapid Reset")
 * stays on server hops and is off on client hops, as netty sets it by role.
 * <p>
 * <b>Resets go out at once.</b> netty closes a reset stream -- and drops the DATA its
 * flow controller holds for it -- only once the RST_STREAM is written, and the flow
 * controller writes held DATA whenever a window opens: at the handler's flush (after each
 * read of the peer), and inside a read raising the initial window (a SETTINGS). A
 * RST_STREAM still waiting for a flush then has DATA of its stream written behind it (RFC
 * 9113 5.1). Every RST_STREAM netty writes -- through {@code resetStream}, which the
 * encoder's {@code writeRstStream} and netty's own stream errors go through -- is
 * therefore flushed past the handler right away: its stream closes inside the reset call
 * ({@code HopResetOrderTest}). A reset written while the channel is flushing already (by
 * a listener of a write that flush completes) is not complete when the reset call
 * returns: it closes its stream once that flush or the next wrote it, before the hop
 * reads again.
 * <p>
 * <b>Frames in flight after a reset are ignored.</b> netty forgets a stream once its
 * reset is written, and answers a DATA or HEADERS frame on a stream it forgot with a
 * stream error {@code STREAM_CLOSED}. A frame the peer sent before it read the hop's
 * reset is no error: RFC 9113 5.1 has the endpoint that sent RST_STREAM ignore it. So the
 * hop remembers each stream it writes a reset of while the peer could still send on it
 * (its remote side open), but those netty's codec resets answering a stream error of the
 * peer's frames: of each, the DATA its stream window held at the reset -- an empty frame
 * not ending the stream costing one byte -- and one header list of header blocks (as
 * HPACK counts them), until a frame ends the stream, are ignored: neither answered nor
 * counted. netty returned the bytes of such DATA to the connection window before raising
 * the error. A frame past that, or on a stream forgotten, is answered and counted as any
 * other ({@code HopFramesAfterResetTest}). The hop remembers the last
 * {@link Hop.Builder#rememberedResets} streams, a number meant to be the peer's
 * concurrency limit: the peer reads the resets in the order they were written, and each
 * one it has not read yet is of a stream it still counts against that limit. The frame
 * raising a stream error is the last one the hop's frame logger saw
 * ({@link InboundFrames}): netty's frame reader hands the logger each frame before the
 * decoder.
 */
public final class HopHandler extends Http2ConnectionHandler {

	private final @Nullable StreamErrorLimit streamErrorLimit;

	private final Http2FrameWriter frameWriter;

	private final ResetStreams resetStreams;

	private final InboundFrames inbound;

	/** The header list size limit the hop announced, in HPACK bytes. */
	private final int maxHeaderListSize;

	/** The stream whose stream error of the peer netty's codec is answering; 0 = none. */
	private int answering;

	/** The limit was passed: the connection is ending. */
	private boolean limitReached;

	private HopHandler(Http2ConnectionDecoder decoder, Http2ConnectionEncoder encoder, Builder builder,
			Http2Settings initialSettings) {
		super(decoder, encoder, initialSettings, builder.decoupled(), builder.flushesPreface());
		this.streamErrorLimit = builder.streamErrorLimit;
		this.frameWriter = new ResetFlushingFrameWriter(encoder.frameWriter());
		this.resetStreams = new ResetStreams(builder.rememberedResets);
		this.inbound = Objects.requireNonNull(builder.inbound, "the frame logger is not installed");
		Long maxHeaderListSize = initialSettings.maxHeaderListSize();
		this.maxHeaderListSize = (int) Math.min(Integer.MAX_VALUE,
				(maxHeaderListSize != null) ? maxHeaderListSize : Http2CodecUtil.DEFAULT_HEADER_LIST_SIZE);
	}

	/** The frame writer netty's resets and GOAWAYs go through. */
	@Override
	protected Http2FrameWriter frameWriter() {
		return this.frameWriter;
	}

	/**
	 * Ignores a frame the peer had in flight on a stream the hop reset (see the class
	 * comment); else counts a stream error of the peer -- one netty's codec raised
	 * reading the peer's frames, not one of a write -- once netty answered it; past the
	 * limit, the connection ends with GOAWAY {@code ENHANCE_YOUR_CALM}.
	 */
	@Override
	protected void onStreamError(ChannelHandlerContext ctx, boolean outbound, Throwable cause,
			Http2Exception.StreamException http2Ex) {
		if (!outbound && inFlight(http2Ex)) {
			// RFC 9113 5.1: ignored
			return;
		}
		this.answering = outbound ? 0 : http2Ex.streamId();
		try {
			super.onStreamError(ctx, outbound, cause, http2Ex);
		}
		finally {
			this.answering = 0;
		}
		StreamErrorLimit limit = this.streamErrorLimit;
		if (outbound || limit == null || this.limitReached || limit.count(System.nanoTime())) {
			return;
		}
		this.limitReached = true;
		onError(ctx, false, Http2Exception.connectionError(Http2Error.ENHANCE_YOUR_CALM,
				"Maximum number %d of stream errors reached within %s", limit.maxErrors(), limit.window()));
	}

	/**
	 * Whether the peer's stream error is a frame on a stream netty forgot that the peer
	 * may have had in flight when the hop reset the stream; charges it to the stream.
	 */
	private boolean inFlight(Http2Exception.StreamException error) {
		int streamId = error.streamId();
		return error.error() == Http2Error.STREAM_CLOSED && connection().stream(streamId) == null
				&& this.inbound.admittedBy(streamId, this.resetStreams);
	}

	/**
	 * Remembers a stream whose reset is being written while the peer could still send on
	 * it, unless netty's codec is answering a stream error of the peer's on it.
	 */
	private void rememberReset(int streamId) {
		if (streamId == this.answering) {
			return;
		}
		Http2Stream stream = connection().stream(streamId);
		if (stream == null || !stream.state().remoteSideOpen()) {
			return;
		}
		this.resetStreams.remember(streamId, decoder().flowController().windowSize(stream), this.maxHeaderListSize);
	}

	/**
	 * Remembers each stream reset ({@link #rememberReset}) and flushes the RST_STREAM
	 * past the handler -- not through the handler's flush, which has the flow controller
	 * write the DATA it holds first -- so the write completes, and netty closes the
	 * stream, as the reset call goes on.
	 */
	private final class ResetFlushingFrameWriter extends DecoratingHttp2FrameWriter {

		ResetFlushingFrameWriter(Http2FrameWriter delegate) {
			super(delegate);
		}

		@Override
		public ChannelFuture writeRstStream(ChannelHandlerContext ctx, int streamId, long errorCode,
				ChannelPromise promise) {
			rememberReset(streamId);
			ChannelFuture future = super.writeRstStream(ctx, streamId, errorCode, promise);
			ctx.flush();
			return future;
		}

	}

	/**
	 * The hop's frame logger: keeps the last frame read -- the stream, the kind, the DATA
	 * bytes or the header block, whether it ended the stream -- and hands every frame on
	 * to the logger the hop's codec configuration set, if any.
	 */
	static final class InboundFrames extends Http2FrameLogger {

		private final @Nullable Http2FrameLogger delegate;

		/** The stream of the last frame read; 0 = none, or not DATA or HEADERS. */
		private int streamId;

		/** The flow-controlled bytes of the last frame read, if DATA. */
		private int dataBytes;

		/** The header block of the last frame read, if HEADERS. */
		private @Nullable Http2Headers headers;

		private boolean endStream;

		InboundFrames(@Nullable Http2FrameLogger delegate) {
			super(LogLevel.TRACE, HopHandler.class);
			this.delegate = delegate;
		}

		/**
		 * Whether the last frame read is on the stream and one the peer may have had in
		 * flight, as the streams reset tell; charges it to them.
		 */
		boolean admittedBy(int streamId, ResetStreams resetStreams) {
			if (streamId != this.streamId) {
				return false;
			}
			this.streamId = 0;
			Http2Headers headers = this.headers;
			this.headers = null;
			return (headers != null) ? resetStreams.admitHeaders(streamId, headerBytes(headers), this.endStream)
					: resetStreams.admitData(streamId, this.dataBytes, this.endStream);
		}

		/** The bytes of the header block as HPACK counts them (RFC 7541 4.1). */
		private static long headerBytes(Http2Headers headers) {
			long bytes = 0;
			for (Map.Entry<CharSequence, CharSequence> header : headers) {
				bytes += header.getKey().length() + header.getValue().length() + 32;
			}
			return bytes;
		}

		private void read(Direction direction) {
			if (direction == Direction.INBOUND) {
				this.streamId = 0;
				this.headers = null;
			}
		}

		@Override
		public boolean isEnabled() {
			Http2FrameLogger delegate = this.delegate;
			return delegate != null && delegate.isEnabled();
		}

		@Override
		public void logData(Direction direction, ChannelHandlerContext ctx, int streamId, ByteBuf data, int padding,
				boolean endStream) {
			if (direction == Direction.INBOUND) {
				this.streamId = streamId;
				this.dataBytes = data.readableBytes() + padding;
				this.headers = null;
				this.endStream = endStream;
			}
			if (this.delegate != null) {
				this.delegate.logData(direction, ctx, streamId, data, padding, endStream);
			}
		}

		@Override
		public void logHeaders(Direction direction, ChannelHandlerContext ctx, int streamId, Http2Headers headers,
				int padding, boolean endStream) {
			headersRead(direction, streamId, headers, endStream);
			if (this.delegate != null) {
				this.delegate.logHeaders(direction, ctx, streamId, headers, padding, endStream);
			}
		}

		@Override
		public void logHeaders(Direction direction, ChannelHandlerContext ctx, int streamId, Http2Headers headers,
				int streamDependency, short weight, boolean exclusive, int padding, boolean endStream) {
			headersRead(direction, streamId, headers, endStream);
			if (this.delegate != null) {
				this.delegate.logHeaders(direction, ctx, streamId, headers, streamDependency, weight, exclusive,
						padding, endStream);
			}
		}

		private void headersRead(Direction direction, int streamId, Http2Headers headers, boolean endStream) {
			if (direction == Direction.INBOUND) {
				this.streamId = streamId;
				this.headers = headers;
				this.endStream = endStream;
			}
		}

		@Override
		public void logPriority(Direction direction, ChannelHandlerContext ctx, int streamId, int streamDependency,
				short weight, boolean exclusive) {
			read(direction);
			if (this.delegate != null) {
				this.delegate.logPriority(direction, ctx, streamId, streamDependency, weight, exclusive);
			}
		}

		@Override
		public void logRstStream(Direction direction, ChannelHandlerContext ctx, int streamId, long errorCode) {
			read(direction);
			if (this.delegate != null) {
				this.delegate.logRstStream(direction, ctx, streamId, errorCode);
			}
		}

		@Override
		public void logSettingsAck(Direction direction, ChannelHandlerContext ctx) {
			read(direction);
			if (this.delegate != null) {
				this.delegate.logSettingsAck(direction, ctx);
			}
		}

		@Override
		public void logSettings(Direction direction, ChannelHandlerContext ctx, Http2Settings settings) {
			read(direction);
			if (this.delegate != null) {
				this.delegate.logSettings(direction, ctx, settings);
			}
		}

		@Override
		public void logPing(Direction direction, ChannelHandlerContext ctx, long data) {
			read(direction);
			if (this.delegate != null) {
				this.delegate.logPing(direction, ctx, data);
			}
		}

		@Override
		public void logPingAck(Direction direction, ChannelHandlerContext ctx, long data) {
			read(direction);
			if (this.delegate != null) {
				this.delegate.logPingAck(direction, ctx, data);
			}
		}

		@Override
		public void logPushPromise(Direction direction, ChannelHandlerContext ctx, int streamId, int promisedStreamId,
				Http2Headers headers, int padding) {
			read(direction);
			if (this.delegate != null) {
				this.delegate.logPushPromise(direction, ctx, streamId, promisedStreamId, headers, padding);
			}
		}

		@Override
		public void logGoAway(Direction direction, ChannelHandlerContext ctx, int lastStreamId, long errorCode,
				ByteBuf debugData) {
			read(direction);
			if (this.delegate != null) {
				this.delegate.logGoAway(direction, ctx, lastStreamId, errorCode, debugData);
			}
		}

		@Override
		public void logWindowsUpdate(Direction direction, ChannelHandlerContext ctx, int streamId,
				int windowSizeIncrement) {
			read(direction);
			if (this.delegate != null) {
				this.delegate.logWindowsUpdate(direction, ctx, streamId, windowSizeIncrement);
			}
		}

		@Override
		public void logUnknownFrame(Direction direction, ChannelHandlerContext ctx, byte frameType, int streamId,
				Http2Flags flags, ByteBuf data) {
			read(direction);
			if (this.delegate != null) {
				this.delegate.logUnknownFrame(direction, ctx, frameType, streamId, flags, data);
			}
		}

	}

	/**
	 * Builds a hop's handler: what the hop's codec configuration may set is public, the
	 * connection, the frame listener and the limits are the hop's.
	 */
	public static final class Builder extends AbstractHttp2ConnectionHandlerBuilder<HopHandler, Builder> {

		private @Nullable StreamErrorLimit streamErrorLimit;

		private int rememberedResets = Hop.DEFAULT_REMEMBERED_RESETS;

		/** The frame logger the hop's codec configuration set; null = none. */
		private @Nullable Http2FrameLogger frameLogger;

		/** The frame logger installed, set as the handler is built. */
		private @Nullable InboundFrames inbound;

		Builder() {
		}

		@Override
		public Builder initialSettings(Http2Settings settings) {
			return super.initialSettings(settings);
		}

		@Override
		public Builder gracefulShutdownTimeoutMillis(long gracefulShutdownTimeoutMillis) {
			return super.gracefulShutdownTimeoutMillis(gracefulShutdownTimeoutMillis);
		}

		@Override
		public Builder validateHeaders(boolean validateHeaders) {
			return super.validateHeaders(validateHeaders);
		}

		/**
		 * A frame logger, handed every frame by the hop's own ({@link InboundFrames}).
		 */
		@Override
		public Builder frameLogger(Http2FrameLogger frameLogger) {
			this.frameLogger = Objects.requireNonNull(frameLogger, "frameLogger");
			return this;
		}

		@Override
		public Builder encoderEnforceMaxConcurrentStreams(boolean encoderEnforceMaxConcurrentStreams) {
			return super.encoderEnforceMaxConcurrentStreams(encoderEnforceMaxConcurrentStreams);
		}

		@Override
		public Builder headerSensitivityDetector(Http2HeadersEncoder.SensitivityDetector headerSensitivityDetector) {
			return super.headerSensitivityDetector(headerSensitivityDetector);
		}

		/**
		 * The hop's connection, its role taken from it: netty's builder would treat every
		 * endpoint given a connection as a server for its limits.
		 */
		Builder hopConnection(Http2Connection connection) {
			super.connection(connection);
			super.encoderEnforceMaxRstFramesPerWindow(0, 0);
			if (!connection.isServer()) {
				super.decoderEnforceMaxRstFramesPerWindow(0, 0);
			}
			return this;
		}

		Builder hopListener(Http2FrameListener listener) {
			return super.frameListener(listener);
		}

		/** The limit on the peer's stream errors; null = none. */
		Builder streamErrorLimit(@Nullable StreamErrorLimit streamErrorLimit) {
			this.streamErrorLimit = streamErrorLimit;
			return this;
		}

		/** The streams the handler remembers it reset; 0 = none. */
		Builder rememberedResets(int rememberedResets) {
			this.rememberedResets = rememberedResets;
			return this;
		}

		private boolean decoupled() {
			return decoupleCloseAndGoAway();
		}

		private boolean flushesPreface() {
			return flushPreface();
		}

		HopHandler buildHandler() {
			InboundFrames inbound = new InboundFrames(this.frameLogger);
			this.inbound = inbound;
			super.frameLogger(inbound);
			return super.build();
		}

		@Override
		protected HopHandler build(Http2ConnectionDecoder decoder, Http2ConnectionEncoder encoder,
				Http2Settings initialSettings) {
			return new HopHandler(decoder, encoder, this, initialSettings);
		}

	}

}
