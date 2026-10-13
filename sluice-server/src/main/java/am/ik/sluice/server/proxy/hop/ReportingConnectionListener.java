package am.ik.sluice.server.proxy.hop;

import java.util.function.Consumer;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http2.Http2Connection;
import io.netty.handler.codec.http2.Http2Stream;

/**
 * Hands an exception of the listener to the hop's failure handling: netty's
 * {@code DefaultHttp2Connection} catches what a listener throws and only logs it.
 */
final class ReportingConnectionListener implements Http2Connection.Listener {

	private final Http2Connection.Listener delegate;

	private final Consumer<Throwable> failure;

	ReportingConnectionListener(Http2Connection.Listener delegate, Consumer<Throwable> failure) {
		this.delegate = delegate;
		this.failure = failure;
	}

	@Override
	public void onStreamAdded(Http2Stream stream) {
		try {
			this.delegate.onStreamAdded(stream);
		}
		catch (RuntimeException | Error e) {
			this.failure.accept(e);
		}
	}

	@Override
	public void onStreamActive(Http2Stream stream) {
		try {
			this.delegate.onStreamActive(stream);
		}
		catch (RuntimeException | Error e) {
			this.failure.accept(e);
		}
	}

	@Override
	public void onStreamHalfClosed(Http2Stream stream) {
		try {
			this.delegate.onStreamHalfClosed(stream);
		}
		catch (RuntimeException | Error e) {
			this.failure.accept(e);
		}
	}

	@Override
	public void onStreamClosed(Http2Stream stream) {
		try {
			this.delegate.onStreamClosed(stream);
		}
		catch (RuntimeException | Error e) {
			this.failure.accept(e);
		}
	}

	@Override
	public void onStreamRemoved(Http2Stream stream) {
		try {
			this.delegate.onStreamRemoved(stream);
		}
		catch (RuntimeException | Error e) {
			this.failure.accept(e);
		}
	}

	@Override
	public void onGoAwaySent(int lastStreamId, long errorCode, ByteBuf debugData) {
		try {
			this.delegate.onGoAwaySent(lastStreamId, errorCode, debugData);
		}
		catch (RuntimeException | Error e) {
			this.failure.accept(e);
		}
	}

	@Override
	public void onGoAwayReceived(int lastStreamId, long errorCode, ByteBuf debugData) {
		try {
			this.delegate.onGoAwayReceived(lastStreamId, errorCode, debugData);
		}
		catch (RuntimeException | Error e) {
			this.failure.accept(e);
		}
	}

}
