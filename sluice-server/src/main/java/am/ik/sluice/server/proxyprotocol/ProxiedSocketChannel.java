package am.ik.sluice.server.proxyprotocol;

import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

import io.netty.channel.socket.nio.NioSocketChannel;

/**
 * An accepted connection whose pipeline starts with a {@link ProxyHeaderHandler}; its
 * remote address is the source of its PROXY header once one was read, the socket peer
 * otherwise.
 */
final class ProxiedSocketChannel extends NioSocketChannel {

	ProxiedSocketChannel(ProxiedServerSocketChannel parent, SocketChannel socket) {
		super(parent, socket);
		pipeline().addFirst(new ProxyHeaderHandler(parent.headTimeout()));
	}

	@Override
	public InetSocketAddress remoteAddress() {
		InetSocketAddress source = attr(ProxyHeaderHandler.SOURCE).get();
		return source != null ? source : super.remoteAddress();
	}

}
