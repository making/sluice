package am.ik.sluice.server.config;

import java.util.Objects;

import javax.net.ssl.SSLContext;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Provides the {@link SSLContext} of the data plane TLS termination, resolved from the
 * Spring Boot SSL bundle named by {@code sluice.data-tls-bundle}.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty("sluice.data-tls-bundle")
class DataPlaneSslConfiguration {

	@Bean
	SSLContext dataPlaneSslContext(SslBundles sslBundles, SluiceServerProperties properties) {
		String bundleName = Objects.requireNonNull(properties.dataTlsBundle(), "dataTlsBundle is required");
		try {
			return sslBundles.getBundle(bundleName).createSslContext();
		}
		catch (Exception e) {
			throw new IllegalStateException("failed to load ssl bundle '%s' for the data plane".formatted(bundleName),
					e);
		}
	}

}
