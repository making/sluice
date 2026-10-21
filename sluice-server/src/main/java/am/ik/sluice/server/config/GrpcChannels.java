package am.ik.sluice.server.config;

import java.net.URI;
import java.net.URISyntaxException;

import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

import org.springframework.boot.ssl.SslBundles;

/**
 * Channels this server dials as a gRPC client: the peer nodes' control planes, the live
 * access log's flume.
 */
public final class GrpcChannels {

	private GrpcChannels() {
	}

	/**
	 * A channel to a {@code grpc://} (plaintext) or {@code grpcs://} address: verified
	 * against the SSL bundle when one is named (its keystore presented for mTLS), not
	 * verified at all when {@code insecure}, else against the JVM's trust store.
	 */
	public static NettyChannelBuilder builder(String url, String tlsBundle, boolean insecure, SslBundles sslBundles) {
		HostPort target = HostPort.parse(url);
		NettyChannelBuilder builder = NettyChannelBuilder.forAddress(target.host(), target.port());
		if (!url.startsWith("grpcs://")) {
			return builder.usePlaintext();
		}
		try {
			if (!tlsBundle.isEmpty()) {
				var managers = sslBundles.getBundle(tlsBundle).getManagers();
				return builder.sslContext(GrpcSslContexts.forClient()
					.keyManager(managers.getKeyManagerFactory())
					.trustManager(managers.getTrustManagerFactory())
					.build());
			}
			if (insecure) {
				return builder
					.sslContext(GrpcSslContexts.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE).build());
			}
			return builder.useTransportSecurity();
		}
		catch (Exception e) {
			throw new IllegalStateException("failed to build the TLS context for " + url, e);
		}
	}

	/** {@code host} and port of a {@code grpc(s)://host[:port]} address. */
	record HostPort(String host, int port) {

		static HostPort parse(String url) {
			try {
				URI uri = new URI(url);
				if (uri.getHost() == null) {
					throw new IllegalArgumentException("no host in the gRPC address: " + url);
				}
				int port = uri.getPort();
				return new HostPort(uri.getHost(), port > 0 ? port : "grpcs".equals(uri.getScheme()) ? 443 : 80);
			}
			catch (URISyntaxException e) {
				throw new IllegalArgumentException("unparseable gRPC address: " + url, e);
			}
		}

	}

}
