package am.ik.sluice.server.livelog;

import java.util.concurrent.TimeUnit;

import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import org.jspecify.annotations.Nullable;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.stereotype.Component;

import am.ik.sluice.server.config.GrpcChannels;
import am.ik.sluice.server.config.SluiceServerProperties;
import am.ik.sluice.v1.proto.FlumeGrpc;

/**
 * The channel to the flume of {@code sluice.live-log.url}, shared by this node's feed and
 * its console's watches; none with the live access log off. The channel connects on the
 * first call and reconnects on its own, so nothing here waits for the flume.
 */
@Component
public class FlumeConnection implements DisposableBean {

	private static final Metadata.Key<String> AUTHORIZATION = Metadata.Key.of("authorization",
			Metadata.ASCII_STRING_MARSHALLER);

	private final String url;

	private final @Nullable ManagedChannel channel;

	private final Metadata headers = new Metadata();

	FlumeConnection(SluiceServerProperties properties, SslBundles sslBundles) {
		SluiceServerProperties.LiveLog liveLog = properties.liveLog();
		this.url = liveLog.url();
		// the keepalive fails the streams of a flume gone without a word instead of
		// leaving them hanging
		this.channel = liveLog.enabled()
				? GrpcChannels.builder(liveLog.url(), liveLog.tlsBundle(), liveLog.insecure(), sslBundles)
					.keepAliveTime(30, TimeUnit.SECONDS)
					.keepAliveTimeout(10, TimeUnit.SECONDS)
					.build()
				: null;
		this.headers.put(AUTHORIZATION, "Bearer " + liveLog.token());
	}

	public boolean enabled() {
		return this.channel != null;
	}

	/** The flume's address, empty with the live access log off. */
	public String url() {
		return this.url;
	}

	/**
	 * A stub carrying the token.
	 * @throws IllegalStateException with the live access log off
	 */
	public FlumeGrpc.FlumeStub stub() {
		if (this.channel == null) {
			throw new IllegalStateException("the live access log is off: sluice.live-log.url is not set");
		}
		return FlumeGrpc.newStub(this.channel)
			.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(this.headers));
	}

	@Override
	public void destroy() {
		if (this.channel != null) {
			this.channel.shutdownNow();
		}
	}

}
