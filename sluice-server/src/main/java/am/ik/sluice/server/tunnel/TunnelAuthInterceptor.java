package am.ik.sluice.server.tunnel;

import java.util.UUID;

import am.ik.sluice.server.auth.TokenValidator;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

/**
 * Authenticates tunnel clients via the {@code authorization} metadata and resolves the
 * client id from {@code x-sluice-id} (a UUID is assigned when absent). Both values are
 * exposed to the service handler through the {@link io.grpc.Context}.
 */
import org.springframework.grpc.server.GlobalServerInterceptor;
import org.springframework.stereotype.Component;

@GlobalServerInterceptor
@Component
public class TunnelAuthInterceptor implements ServerInterceptor {

	/** Metadata key of the bearer token. */
	static final Metadata.Key<String> AUTHORIZATION = Metadata.Key.of("authorization",
			Metadata.ASCII_STRING_MARSHALLER);

	/** Metadata key of the client identity. */
	static final Metadata.Key<String> CLIENT_ID_HEADER = Metadata.Key.of("x-sluice-id",
			Metadata.ASCII_STRING_MARSHALLER);

	/** Context key carrying the resolved client id. */
	public static final Context.Key<String> CLIENT_ID = Context.key("sluice-client-id");

	private final TokenValidator tokenValidator;

	public TunnelAuthInterceptor(TokenValidator tokenValidator) {
		this.tokenValidator = tokenValidator;
	}

	@Override
	public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers,
			ServerCallHandler<ReqT, RespT> next) {
		String authorization = headers.get(AUTHORIZATION);
		if (!this.tokenValidator.isValid(authorization)) {
			call.close(Status.UNAUTHENTICATED.withDescription("invalid or missing token"), new Metadata());
			return new ServerCall.Listener<>() {
			};
		}
		String clientId = headers.get(CLIENT_ID_HEADER);
		if (clientId == null || clientId.isBlank()) {
			clientId = UUID.randomUUID().toString();
		}
		Context context = Context.current().withValue(CLIENT_ID, clientId);
		return Contexts.interceptCall(context, call, headers, next);
	}

}
