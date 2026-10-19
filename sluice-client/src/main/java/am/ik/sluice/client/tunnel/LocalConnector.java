package am.ik.sluice.client.tunnel;

import org.jspecify.annotations.Nullable;

import java.net.InetSocketAddress;
import java.net.ProtocolException;
import java.net.Socket;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import am.ik.sluice.client.config.SluiceClientProperties;
import am.ik.sluice.tunnel.TcpKeepAlive;

/**
 * Dials the local upstream for a tunnel CONNECT request. In strict mode only addresses
 * matching a declared upstream are dialable (the port of makeFilter); upstream URLs with
 * the {@code https://} scheme are wrapped in a TLS socket -- verified against the JDK's
 * default trust store, or trusting any certificate when {@code insecure} -- that
 * negotiates the protocol the server speaks on the connection over ALPN; every other
 * scheme ({@code http://}, {@code tcp://}, ...) is a raw dial whose bytes are relayed
 * untouched.
 */
public class LocalConnector {

	private static final Logger log = LoggerFactory.getLogger(LocalConnector.class);

	private static final String ALPN_H2 = "h2";

	private static final String ALPN_HTTP_1_1 = "http/1.1";

	private final List<String> upstreams;

	private final boolean strict;

	private final boolean insecure;

	private final Duration tcpKeepAlive;

	private final Duration connectTimeout;

	private LocalConnector(Builder builder) {
		this.upstreams = Objects.requireNonNull(builder.upstreams, "upstreams is required");
		this.strict = builder.strict;
		this.insecure = builder.insecure;
		this.tcpKeepAlive = builder.tcpKeepAlive;
		this.connectTimeout = builder.connectTimeout;
	}

	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Builder for {@link LocalConnector}.
	 */
	public static final class Builder {

		@Nullable private List<String> upstreams;

		private boolean strict;

		private boolean insecure;

		private Duration tcpKeepAlive = SluiceClientProperties.DEFAULT_TCP_KEEP_ALIVE;

		private Duration connectTimeout = SluiceClientProperties.DEFAULT_CONNECT_TIMEOUT;

		private Builder() {
		}

		public Builder upstreams(List<String> upstreams) {
			this.upstreams = upstreams;
			return this;
		}

		public Builder strict(boolean strict) {
			this.strict = strict;
			return this;
		}

		public Builder insecure(boolean insecure) {
			this.insecure = insecure;
			return this;
		}

		/** The TCP keepalive idle time of the upstream connections; zero = off. */
		public Builder tcpKeepAlive(Duration tcpKeepAlive) {
			this.tcpKeepAlive = tcpKeepAlive;
			return this;
		}

		/**
		 * How long the TCP connect may take, and then each wait of the TLS handshake of
		 * an {@code https} upstream.
		 */
		public Builder connectTimeout(Duration connectTimeout) {
			this.connectTimeout = connectTimeout;
			return this;
		}

		public LocalConnector build() {
			return new LocalConnector(this);
		}

	}

	/**
	 * Permissive filter used when strict forwarding is disabled.
	 */
	public static boolean allowsAll(String address) {
		return true;
	}

	/**
	 * Strict filter: only targets declared in the upstream list are dialable (the port of
	 * makeFilter, normalized to host comparison).
	 */
	public boolean permits(String address) {
		if (!strict) {
			return true;
		}
		return resolve(address).isPresent();
	}

	private Optional<UpstreamEndpoint> resolve(String address) {
		for (String targetUrl : this.upstreams) {
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
	 * Opens a plain or TLS socket to the upstream addressed by a CONNECT frame, under the
	 * connect timeout; on failure nothing opened stays open. The connection carries bytes
	 * the server does not interpret: a TLS upstream is offered no ALPN protocol.
	 */
	public Socket dial(String address) throws Exception {
		return this.dial(address, "");
	}

	/**
	 * Opens a socket as {@link #dial(String)} does, for a connection on which the server
	 * speaks the given protocol (an ALPN id; empty = none). A TLS upstream must negotiate
	 * it: {@code h2} is offered along with {@code http/1.1} so that an upstream without
	 * h2 answers rather than failing the handshake, and the dial fails unless it picked
	 * {@code h2}; an upstream negotiating no protocol speaks {@code http/1.1}.
	 */
	public Socket dial(String address, String applicationProtocol) throws Exception {
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
		int timeout = (int) Math.clamp(this.connectTimeout.toMillis(), 1, Integer.MAX_VALUE);
		Socket socket = new Socket();
		try {
			socket.connect(new InetSocketAddress(host, port), timeout);
			socket.setTcpNoDelay(true);
			TcpKeepAlive.enable(socket, this.tcpKeepAlive);
			if (!"https".equals(scheme)) {
				return socket;
			}
			SSLSocket sslSocket = (SSLSocket) sslFactory().createSocket(socket, host, port, true);
			if (!applicationProtocol.isEmpty()) {
				SSLParameters parameters = sslSocket.getSSLParameters();
				parameters.setApplicationProtocols(ALPN_H2.equals(applicationProtocol)
						? new String[] { ALPN_H2, ALPN_HTTP_1_1 } : new String[] { applicationProtocol });
				sslSocket.setSSLParameters(parameters);
			}
			sslSocket.setSoTimeout(timeout);
			sslSocket.startHandshake();
			if (!applicationProtocol.isEmpty()) {
				String negotiated = Objects.requireNonNullElse(sslSocket.getApplicationProtocol(), "");
				if (!(negotiated.isEmpty() ? ALPN_HTTP_1_1 : negotiated).equals(applicationProtocol)) {
					String mismatch = "upstream %s negotiated %s over ALPN, not %s".formatted(address,
							negotiated.isEmpty() ? "no protocol" : negotiated, applicationProtocol);
					log.warn("{}; an upstream without h2 over TLS needs force-http1 on its route", mismatch);
					throw new ProtocolException(mismatch);
				}
			}
			// the relay waits on its upstream for as long as the connection lives
			sslSocket.setSoTimeout(0);
			return sslSocket;
		}
		catch (Exception e) {
			socket.close();
			throw e;
		}
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
