package am.ik.sluice.server.tunnel;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.grpc.server.ServerBuilderCustomizer;

import am.ik.sluice.tunnel.TunnelBudget;
import io.grpc.netty.NettyServerBuilder;

/**
 * Fixes the HTTP/2 flow control window of the gRPC server's connections at the tunnel
 * budget's {@link TunnelBudget#transportWindow()}, which turns gRPC's own window tuning
 * off. Left to it, the window starts at 1MiB and grows only when one round trip carried
 * more than half of it; as gRPC grants the window back at every half of it read, a
 * transfer can settle with half of it in flight and half of it read but not granted back,
 * where the tuning sees exactly half the window per round trip and never grows it. The
 * tunnel then moved 512KiB per round trip, whatever its connections' windows held.
 */
@Configuration(proxyBeanMethods = false)
class TunnelTransportWindowConfiguration {

	@Bean
	ServerBuilderCustomizer<NettyServerBuilder> tunnelTransportWindow(TunnelService tunnel) {
		return builder -> builder.flowControlWindow(tunnel.transportWindow());
	}

}
