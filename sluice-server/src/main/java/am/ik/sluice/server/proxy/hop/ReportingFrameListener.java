package am.ik.sluice.server.proxy.hop;

import java.util.function.Consumer;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http2.Http2Exception;
import io.netty.handler.codec.http2.Http2Flags;
import io.netty.handler.codec.http2.Http2FrameListener;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2Settings;

/**
 * Hands an unchecked exception of the listener to the hop's failure handling before netty
 * sees it. Netty turns such an exception into a connection error (GOAWAY
 * {@code INTERNAL_ERROR}, then close) without reporting it anywhere; an
 * {@link Http2Exception} the listener throws is a protocol decision and passes as it is.
 */
final class ReportingFrameListener implements Http2FrameListener {

	private final Http2FrameListener delegate;

	private final Consumer<Throwable> failure;

	ReportingFrameListener(Http2FrameListener delegate, Consumer<Throwable> failure) {
		this.delegate = delegate;
		this.failure = failure;
	}

	@Override
	public int onDataRead(ChannelHandlerContext ctx, int streamId, ByteBuf data, int padding, boolean endOfStream)
			throws Http2Exception {
		try {
			return this.delegate.onDataRead(ctx, streamId, data, padding, endOfStream);
		}
		catch (RuntimeException | Error e) {
			this.failure.accept(e);
			throw e;
		}
	}

	@Override
	public void onHeadersRead(ChannelHandlerContext ctx, int streamId, Http2Headers headers, int padding,
			boolean endOfStream) throws Http2Exception {
		try {
			this.delegate.onHeadersRead(ctx, streamId, headers, padding, endOfStream);
		}
		catch (RuntimeException | Error e) {
			this.failure.accept(e);
			throw e;
		}
	}

	@Override
	public void onHeadersRead(ChannelHandlerContext ctx, int streamId, Http2Headers headers, int streamDependency,
			short weight, boolean exclusive, int padding, boolean endOfStream) throws Http2Exception {
		try {
			this.delegate.onHeadersRead(ctx, streamId, headers, streamDependency, weight, exclusive, padding,
					endOfStream);
		}
		catch (RuntimeException | Error e) {
			this.failure.accept(e);
			throw e;
		}
	}

	@Override
	public void onPriorityRead(ChannelHandlerContext ctx, int streamId, int streamDependency, short weight,
			boolean exclusive) throws Http2Exception {
		try {
			this.delegate.onPriorityRead(ctx, streamId, streamDependency, weight, exclusive);
		}
		catch (RuntimeException | Error e) {
			this.failure.accept(e);
			throw e;
		}
	}

	@Override
	public void onRstStreamRead(ChannelHandlerContext ctx, int streamId, long errorCode) throws Http2Exception {
		try {
			this.delegate.onRstStreamRead(ctx, streamId, errorCode);
		}
		catch (RuntimeException | Error e) {
			this.failure.accept(e);
			throw e;
		}
	}

	@Override
	public void onSettingsAckRead(ChannelHandlerContext ctx) throws Http2Exception {
		try {
			this.delegate.onSettingsAckRead(ctx);
		}
		catch (RuntimeException | Error e) {
			this.failure.accept(e);
			throw e;
		}
	}

	@Override
	public void onSettingsRead(ChannelHandlerContext ctx, Http2Settings settings) throws Http2Exception {
		try {
			this.delegate.onSettingsRead(ctx, settings);
		}
		catch (RuntimeException | Error e) {
			this.failure.accept(e);
			throw e;
		}
	}

	@Override
	public void onPingRead(ChannelHandlerContext ctx, long data) throws Http2Exception {
		try {
			this.delegate.onPingRead(ctx, data);
		}
		catch (RuntimeException | Error e) {
			this.failure.accept(e);
			throw e;
		}
	}

	@Override
	public void onPingAckRead(ChannelHandlerContext ctx, long data) throws Http2Exception {
		try {
			this.delegate.onPingAckRead(ctx, data);
		}
		catch (RuntimeException | Error e) {
			this.failure.accept(e);
			throw e;
		}
	}

	@Override
	public void onPushPromiseRead(ChannelHandlerContext ctx, int streamId, int promisedStreamId, Http2Headers headers,
			int padding) throws Http2Exception {
		try {
			this.delegate.onPushPromiseRead(ctx, streamId, promisedStreamId, headers, padding);
		}
		catch (RuntimeException | Error e) {
			this.failure.accept(e);
			throw e;
		}
	}

	@Override
	public void onGoAwayRead(ChannelHandlerContext ctx, int lastStreamId, long errorCode, ByteBuf debugData)
			throws Http2Exception {
		try {
			this.delegate.onGoAwayRead(ctx, lastStreamId, errorCode, debugData);
		}
		catch (RuntimeException | Error e) {
			this.failure.accept(e);
			throw e;
		}
	}

	@Override
	public void onWindowUpdateRead(ChannelHandlerContext ctx, int streamId, int windowSizeIncrement)
			throws Http2Exception {
		try {
			this.delegate.onWindowUpdateRead(ctx, streamId, windowSizeIncrement);
		}
		catch (RuntimeException | Error e) {
			this.failure.accept(e);
			throw e;
		}
	}

	@Override
	public void onUnknownFrame(ChannelHandlerContext ctx, byte frameType, int streamId, Http2Flags flags,
			ByteBuf payload) throws Http2Exception {
		try {
			this.delegate.onUnknownFrame(ctx, frameType, streamId, flags, payload);
		}
		catch (RuntimeException | Error e) {
			this.failure.accept(e);
			throw e;
		}
	}

}
