package am.ik.sluice.server.proxyprotocol;

import java.nio.channels.SocketChannel;
import java.time.Duration;
import java.util.List;

import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.internal.SocketUtils;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

/**
 * A server socket channel whose accepted connections are {@link ProxiedSocketChannel}s:
 * each one starts with a {@link ProxyHeaderHandler} and reports the source of its PROXY
 * header as its remote address. The accept mirrors {@link NioServerSocketChannel}'s.
 */
final class ProxiedServerSocketChannel extends NioServerSocketChannel {

	private static final InternalLogger logger = InternalLoggerFactory.getInstance(ProxiedServerSocketChannel.class);

	private final Duration headTimeout;

	ProxiedServerSocketChannel(Duration headTimeout) {
		this.headTimeout = headTimeout;
	}

	@Override
	protected int doReadMessages(List<Object> buf) throws Exception {
		SocketChannel ch = SocketUtils.accept(javaChannel());
		try {
			if (ch != null) {
				buf.add(new ProxiedSocketChannel(this, ch));
				return 1;
			}
		}
		catch (Throwable t) {
			logger.warn("Failed to create a new channel from an accepted socket.", t);
			try {
				ch.close();
			}
			catch (Throwable t2) {
				logger.warn("Failed to close a socket.", t2);
			}
		}
		return 0;
	}

	Duration headTimeout() {
		return this.headTimeout;
	}

}
