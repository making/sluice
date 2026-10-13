package am.ik.sluice.server.proxy.hop;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http2.AbstractHttp2ConnectionHandlerBuilder;
import io.netty.handler.codec.http2.DecoratingHttp2FrameWriter;
import io.netty.handler.codec.http2.Http2Connection;
import io.netty.handler.codec.http2.Http2ConnectionDecoder;
import io.netty.handler.codec.http2.Http2ConnectionEncoder;
import io.netty.handler.codec.http2.Http2ConnectionHandler;
import io.netty.handler.codec.http2.Http2Error;
import io.netty.handler.codec.http2.Http2Exception;
import io.netty.handler.codec.http2.Http2FrameListener;
import io.netty.handler.codec.http2.Http2FrameLogger;
import io.netty.handler.codec.http2.Http2FrameWriter;
import io.netty.handler.codec.http2.Http2HeadersEncoder;
import io.netty.handler.codec.http2.Http2Settings;
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
 * included -- and past the limit within its window ends the connection the same way.
 * netty's encoder limit is off on every hop; its limit on the resets a server endpoint
 * receives ("Rapid Reset") stays on server hops and is off on client hops, as netty sets
 * it by role.
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
 */
public final class HopHandler extends Http2ConnectionHandler {

	private final @Nullable StreamErrorLimit streamErrorLimit;

	private final Http2FrameWriter frameWriter;

	/** The limit was passed: the connection is ending. */
	private boolean limitReached;

	private HopHandler(Http2ConnectionDecoder decoder, Http2ConnectionEncoder encoder, Builder builder,
			Http2Settings initialSettings) {
		super(decoder, encoder, initialSettings, builder.decoupled(), builder.flushesPreface());
		this.streamErrorLimit = builder.streamErrorLimit;
		this.frameWriter = new ResetFlushingFrameWriter(encoder.frameWriter());
	}

	/** The frame writer netty's resets and GOAWAYs go through. */
	@Override
	protected Http2FrameWriter frameWriter() {
		return this.frameWriter;
	}

	/**
	 * Counts a stream error of the peer -- one netty's codec raised reading the peer's
	 * frames, not one of a write -- once netty answered it; past the limit, the
	 * connection ends with GOAWAY {@code ENHANCE_YOUR_CALM}.
	 */
	@Override
	protected void onStreamError(ChannelHandlerContext ctx, boolean outbound, Throwable cause,
			Http2Exception.StreamException http2Ex) {
		super.onStreamError(ctx, outbound, cause, http2Ex);
		StreamErrorLimit limit = this.streamErrorLimit;
		if (outbound || limit == null || this.limitReached || limit.count(System.nanoTime())) {
			return;
		}
		this.limitReached = true;
		onError(ctx, false, Http2Exception.connectionError(Http2Error.ENHANCE_YOUR_CALM,
				"Maximum number %d of stream errors reached within %s", limit.maxErrors(), limit.window()));
	}

	/**
	 * Flushes each RST_STREAM past the handler -- not through the handler's flush, which
	 * has the flow controller write the DATA it holds first -- so the write completes,
	 * and netty closes the stream, as the reset call goes on.
	 */
	private static final class ResetFlushingFrameWriter extends DecoratingHttp2FrameWriter {

		ResetFlushingFrameWriter(Http2FrameWriter delegate) {
			super(delegate);
		}

		@Override
		public ChannelFuture writeRstStream(ChannelHandlerContext ctx, int streamId, long errorCode,
				ChannelPromise promise) {
			ChannelFuture future = super.writeRstStream(ctx, streamId, errorCode, promise);
			ctx.flush();
			return future;
		}

	}

	/**
	 * Builds a hop's handler: what the hop's codec configuration may set is public, the
	 * connection, the frame listener and the limits are the hop's.
	 */
	public static final class Builder extends AbstractHttp2ConnectionHandlerBuilder<HopHandler, Builder> {

		private @Nullable StreamErrorLimit streamErrorLimit;

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

		@Override
		public Builder frameLogger(Http2FrameLogger frameLogger) {
			return super.frameLogger(frameLogger);
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

		private boolean decoupled() {
			return decoupleCloseAndGoAway();
		}

		private boolean flushesPreface() {
			return flushPreface();
		}

		HopHandler buildHandler() {
			return super.build();
		}

		@Override
		protected HopHandler build(Http2ConnectionDecoder decoder, Http2ConnectionEncoder encoder,
				Http2Settings initialSettings) {
			return new HopHandler(decoder, encoder, this, initialSettings);
		}

	}

}
