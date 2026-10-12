package am.ik.sluice.server.tunnel;

import org.jspecify.annotations.Nullable;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.grpc.server.ServerBuilderCustomizer;

import am.ik.sluice.v1.proto.TunnelGrpc;
import io.grpc.Context;
import io.grpc.Metadata;
import io.grpc.ServerStreamTracer;
import io.grpc.Status;
import io.grpc.netty.NettyServerBuilder;

/**
 * Lets the handler of a tunnel stream reset it. A server call has no public cancel, and
 * closing it with a status sends the trailers behind the frames already buffered for the
 * client, which a client that stopped reading never takes. gRPC resets the stream when
 * the call's context is cancelled with {@code DEADLINE_EXCEEDED}, but that context is not
 * reachable from the handler ({@link Context#current()} is an uncancellable surrogate).
 * So a stream tracer gives every tunnel call a cancellable parent context, carried in the
 * call's context under {@link #KEY}: cancelling it cancels the call's context, and gRPC
 * resets the stream ({@code RST_STREAM CANCEL}, which the client sees as
 * {@code CANCELLED}).
 */
final class TunnelStreamReset extends ServerStreamTracer.Factory {

	private static final Context.Key<Context.CancellableContext> KEY = Context.key("sluice-tunnel-stream-reset");

	private static final String CONNECT = TunnelGrpc.getConnectMethod().getFullMethodName();

	private static final ServerStreamTracer NOOP = new ServerStreamTracer() {
	};

	@Override
	public ServerStreamTracer newServerStreamTracer(String fullMethodName, Metadata headers) {
		if (!CONNECT.equals(fullMethodName)) {
			return NOOP;
		}
		return new ServerStreamTracer() {

			@Override
			public Context filterContext(Context context) {
				Context.CancellableContext resettable = context.withCancellation();
				return resettable.withValue(KEY, resettable);
			}

		};
	}

	/**
	 * The reset of the current tunnel call, captured on the call's thread; null when the
	 * call carries none (the tracer is not installed).
	 */
	static @Nullable Runnable current() {
		Context.CancellableContext resettable = KEY.get();
		if (resettable == null) {
			return null;
		}
		return () -> resettable
			.cancel(Status.DEADLINE_EXCEEDED.withDescription("tunnel stream stalled").asRuntimeException());
	}

	@Configuration(proxyBeanMethods = false)
	static class TunnelStreamResetConfiguration {

		@Bean
		ServerBuilderCustomizer<NettyServerBuilder> tunnelStreamReset() {
			return builder -> builder.addStreamTracerFactory(new TunnelStreamReset());
		}

	}

}
