package am.ik.sluice.client.tunnel;

import org.jspecify.annotations.Nullable;

import java.net.Socket;
import java.net.URI;
import java.util.Map;
import java.util.Optional;

import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dials the local upstream for a tunnel CONNECT request. In strict mode only addresses
 * matching a declared upstream are dialable (the port of makeFilter); upstream URLs with
 * the {@code https://} scheme are wrapped in a trust-on-first-use style TLS socket
 * mirroring the original's InsecureSkipVerify transport.
 */
public class LocalConnector {

	private static final Logger log = LoggerFactory.getLogger(LocalConnector.class);

	private final Map<String, String> upstreams;

	private final boolean strict;

	private final boolean insecure;

	public LocalConnector(Map<String, String> upstreams, boolean strict, boolean insecure) {
		this.upstreams = upstreams;
		this.strict = strict;
		this.insecure = insecure;
	}

	/**
	 * Permissive filter used when strict forwarding is disabled.
	 */
	public static boolean allowsAll(String address) {
		return true;
	}

	/**
	 * Strict filter: only hosts declared in the upstream map are dialable (the port of
	 * makeFilter, normalized to host comparison).
	 */
	public boolean permits(String address) {
		if (!strict) {
			return true;
		}
		return resolve(address).isPresent();
	}

	private Optional<UpstreamEndpoint> resolve(String address) {
		for (String targetUrl : this.upstreams.values()) {
			UpstreamEndpoint endpoint = UpstreamEndpoint.of(targetUrl);
			if (endpoint == null) {
				continue;
			}
			if (endpoint.matches(address)) {
				return Optional.of(endpoint);
			}
		}
		return Optional.empty();
	}

	/**
	 * Opens a plain or TLS socket to the upstream addressed by a CONNECT frame.
	 */
	public Socket dial(String address) throws Exception {
		Optional<UpstreamEndpoint> endpoint = resolve(address);
		String scheme = endpoint.map(UpstreamEndpoint::scheme).orElse("http");
		String host;
		int port;
		int colon = address.lastIndexOf(':');
		if (colon > address.lastIndexOf(']')) {
			host = address.substring(0, colon);
			port = Integer.parseInt(address.substring(colon + 1));
		}
		else {
			host = address;
			port = switch (scheme) {
				case "https" -> 443;
				default -> 80;
			};
		}
		if (this.strict && endpoint.isEmpty()) {
			throw new IllegalArgumentException("upstream not permitted: " + address);
		}
		Socket socket = new Socket(host, port);
		socket.setTcpNoDelay(true);
		if ("https".equals(scheme)) {
			SSLSocketFactory factory = sslFactory();
			SSLSocket sslSocket = (SSLSocket) factory.createSocket(socket, host, port, true);
			sslSocket.startHandshake();
			return sslSocket;
		}
		return socket;
	}

	private SSLSocketFactory sslFactory() throws Exception {
		if (!this.insecure) {
			return (SSLSocketFactory) SSLSocketFactory.getDefault();
		}
		TrustManager trustAll = new X509TrustManager() {

			@Override
			public void checkClientTrusted(java.security.cert.X509Certificate[] chain, String authType) {
			}

			@Override
			public void checkServerTrusted(java.security.cert.X509Certificate[] chain, String authType) {
			}

			@Override
			public java.security.cert.X509Certificate[] getAcceptedIssuers() {
				return new java.security.cert.X509Certificate[0];
			}

		};
		javax.net.ssl.SSLContext context = javax.net.ssl.SSLContext.getInstance("TLS");
		javax.net.ssl.SSLContext ssl = context;
		ssl.init(null, new TrustManager[] { trustAll }, null);
		return ssl.getSocketFactory();
	}

	/** Parsed form of an upstream URL. */
	record UpstreamEndpoint(String scheme, String host, int port) {

		static @Nullable UpstreamEndpoint of(String targetUrl) {
			try {
				URI uri = new URI(targetUrl.trim());
				if (uri.getHost() == null) {
					return null;
				}
				return new UpstreamEndpoint(uri.getScheme() == null ? "http" : uri.getScheme(), uri.getHost(),
						uri.getPort());
			}
			catch (Exception e) {
				log.debug("unparseable upstream url: {}", targetUrl);
				return null;
			}
		}

		boolean matches(String address) {
			int colon = address.lastIndexOf(':');
			boolean hasPort = colon > address.lastIndexOf(']');
			int effectivePort = this.port > 0 ? this.port : switch (this.scheme) {
				case "https" -> 443;
				default -> 80;
			};
			if (hasPort) {
				if (!address.substring(0, colon).equalsIgnoreCase(this.host)) {
					return false;
				}
				try {
					return Integer.parseInt(address.substring(colon + 1)) == effectivePort;
				}
				catch (NumberFormatException e) {
					return false;
				}
			}
			return address.equalsIgnoreCase(this.host);
		}

	}

}
