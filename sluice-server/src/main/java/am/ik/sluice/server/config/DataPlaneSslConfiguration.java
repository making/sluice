package am.ik.sluice.server.config;

import javax.net.ssl.SSLContext;

import org.jspecify.annotations.Nullable;

import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Provides the {@link SSLContext} of the data plane TLS termination, resolved from the
 * Spring Boot SSL bundle named by {@code sluice.data-tls-bundle}.
 */
@Configuration(proxyBeanMethods = false)
class DataPlaneSslConfiguration {

	/**
	 * The bundle is resolved in this bean body instead of a
	 * {@code @ConditionalOnProperty} bean: conditions are evaluated at build time in the
	 * native image, which would pin the feature to the properties of the build machine
	 * (the configuration vanished whenever the image was built without
	 * {@code sluice.data-tls-bundle}, so runtime configuration was silently ignored). A
	 * {@code null} becomes a NullBean that {@code ObjectProvider#getIfAvailable} reports
	 * as absent.
	 */
	@Bean
	@Nullable SSLContext dataPlaneSslContext(SslBundles sslBundles, SluiceServerProperties properties) {
		String bundleName = properties.dataTlsBundle();
		if (bundleName == null || bundleName.isBlank()) {
			return null;
		}
		try {
			return sslBundles.getBundle(bundleName).createSslContext();
		}
		catch (Exception e) {
			throw new IllegalStateException("failed to load ssl bundle '%s' for the data plane".formatted(bundleName),
					e);
		}
	}

}
