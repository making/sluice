package am.ik.sluice.server.proxy;

import org.jspecify.annotations.Nullable;

/**
 * What the data plane's TLS termination saw of a client's handshake: the SNI host name
 * ({@code null} when the client sent none, as a client dialing an IP literal does) and
 * whether its ALPN offer included http/1.1.
 */
record TlsHandshake(@Nullable String sni, boolean http1Offered) {

	/**
	 * Whether the handshake was the host's own -- its SNI names the host (a port in
	 * {@code host} aside), or it named none -- rather than one of another host the client
	 * coalesced the host onto.
	 */
	boolean isFor(@Nullable String host) {
		if (this.sni == null) {
			return true;
		}
		if (host == null) {
			return false;
		}
		int portSeparator = host.lastIndexOf(':');
		// not an IPv6 suffix
		String bareHost = portSeparator > host.lastIndexOf(']') ? host.substring(0, portSeparator) : host;
		return this.sni.equalsIgnoreCase(bareHost);
	}

}
