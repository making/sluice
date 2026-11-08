package am.ik.sluice.server.proxyprotocol;

import java.net.InetAddress;

import org.jspecify.annotations.Nullable;

/**
 * Whether the PROXY protocol header a connection carried may be believed: the source of
 * the header is judged by the socket peer, which the header itself cannot change.
 */
public interface TrustedProxy {

	/**
	 * Whether a header from the peer is believed. Implementations return {@code true}
	 * when no trusted proxy is configured at all (enabling the header is itself the
	 * opt-in).
	 * @param peer the socket peer, {@code null} when the transport has none
	 */
	boolean trustsProxySource(@Nullable InetAddress peer);

}
