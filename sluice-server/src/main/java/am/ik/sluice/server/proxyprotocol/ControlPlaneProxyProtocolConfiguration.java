package am.ik.sluice.server.proxyprotocol;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.util.concurrent.DefaultThreadFactory;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBooleanProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.grpc.server.ServerBuilderCustomizer;

import am.ik.sluice.server.config.SluiceServerProperties;

/**
 * Parses the PROXY protocol header a front end prepends on the gRPC control plane
 * ({@code sluice.control-proxy-protocol=true}): the tunnel session's remote address
 * (console, logs, per-address tunnel limits) becomes the header's source.
 * <p>
 * gRPC offers no hook ahead of its TLS handshake on a server built from credentials, so
 * the server's channel type is replaced: its connections start with a
 * {@link ProxyHeaderHandler} and report the header's source as their remote address. A
 * custom channel type requires event loops of its own; the control plane then runs on
 * NIO, also where gRPC would pick epoll.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnBooleanProperty("sluice.control-proxy-protocol")
class ControlPlaneProxyProtocolConfiguration {

	@Bean
	ProxiedControlPlane proxiedControlPlane(SluiceServerProperties properties) {
		return new ProxiedControlPlane(properties.limits().headTimeout());
	}

	/**
	 * Installs the proxied channel type and owns its event loops, shut down with the
	 * context (after the gRPC server stopped).
	 */
	static final class ProxiedControlPlane implements ServerBuilderCustomizer<NettyServerBuilder>, DisposableBean {

		private final Duration headTimeout;

		private final EventLoopGroup boss = new MultiThreadIoEventLoopGroup(1,
				new DefaultThreadFactory("sluice-grpc-boss", true), NioIoHandler.newFactory());

		private final EventLoopGroup worker = new MultiThreadIoEventLoopGroup(0,
				new DefaultThreadFactory("sluice-grpc-worker", true), NioIoHandler.newFactory());

		ProxiedControlPlane(Duration headTimeout) {
			this.headTimeout = headTimeout;
		}

		@Override
		public void customize(NettyServerBuilder builder) {
			builder.channelFactory(() -> new ProxiedServerSocketChannel(this.headTimeout))
				.bossEventLoopGroup(this.boss)
				.workerEventLoopGroup(this.worker);
		}

		@Override
		public void destroy() {
			this.boss.shutdownGracefully(0, 5, TimeUnit.SECONDS);
			this.worker.shutdownGracefully(0, 5, TimeUnit.SECONDS);
		}

		EventLoopGroup boss() {
			return this.boss;
		}

		EventLoopGroup worker() {
			return this.worker;
		}

	}

}
