package am.ik.sluice.spike.h2demux;

import java.util.Objects;

import io.netty.handler.codec.http2.DefaultHttp2Connection;
import io.netty.handler.codec.http2.DefaultHttp2LocalFlowController;
import io.netty.handler.codec.http2.Http2ConnectionHandler;
import io.netty.handler.codec.http2.Http2ConnectionHandlerBuilder;
import io.netty.handler.codec.http2.Http2FrameListener;
import io.netty.handler.codec.http2.Http2FrameLogger;
import io.netty.handler.codec.http2.Http2Settings;
import org.jspecify.annotations.Nullable;

/**
 * Builds the netty HTTP/2 endpoints of the spike: framing, HPACK, stream states, both
 * flow controllers and SETTINGS / PING / GOAWAY handling are netty's, with its default
 * protections (control frame and RST flood limits).
 */
public final class Http2Handlers {

	/** The window update ratio of netty's local flow controller default. */
	private static final float WINDOW_UPDATE_RATIO = 0.5f;

	private Http2Handlers() {
	}

	public static Spec spec() {
		return new Spec();
	}

	public static final class Spec {

		private boolean server;

		private @Nullable Http2FrameListener listener;

		private Http2Settings settings = new Http2Settings();

		private boolean autoRefillConnectionWindow;

		private boolean bufferStreams;

		private @Nullable Http2FrameLogger frameLogger;

		private Spec() {
		}

		public Spec server(boolean server) {
			this.server = server;
			return this;
		}

		public Spec listener(Http2FrameListener listener) {
			this.listener = listener;
			return this;
		}

		public Spec settings(Http2Settings settings) {
			this.settings = settings;
			return this;
		}

		/**
		 * Grants the connection window back as soon as DATA arrives, so only the stream
		 * windows bound what the endpoint holds (netty's {@code AutoRefillState}).
		 */
		public Spec autoRefillConnectionWindow(boolean autoRefillConnectionWindow) {
			this.autoRefillConnectionWindow = autoRefillConnectionWindow;
			return this;
		}

		/**
		 * Streams beyond the peer's SETTINGS_MAX_CONCURRENT_STREAMS wait in the endpoint
		 * ({@code StreamBufferingEncoder}); client role only.
		 */
		public Spec bufferStreams(boolean bufferStreams) {
			this.bufferStreams = bufferStreams;
			return this;
		}

		/** Records every frame read and written. */
		public Spec frameLogger(@Nullable Http2FrameLogger frameLogger) {
			this.frameLogger = frameLogger;
			return this;
		}

		public Http2ConnectionHandler build() {
			DefaultHttp2Connection connection = new DefaultHttp2Connection(this.server);
			connection.local()
				.flowController(new DefaultHttp2LocalFlowController(connection, WINDOW_UPDATE_RATIO,
						this.autoRefillConnectionWindow));
			Http2ConnectionHandlerBuilder builder = new Http2ConnectionHandlerBuilder();
			if (this.frameLogger != null) {
				builder.frameLogger(this.frameLogger);
			}
			return builder.connection(connection)
				.frameListener(Objects.requireNonNull(this.listener, "listener is required"))
				.initialSettings(this.settings)
				.encoderEnforceMaxConcurrentStreams(this.bufferStreams)
				.build();
		}

	}

}
