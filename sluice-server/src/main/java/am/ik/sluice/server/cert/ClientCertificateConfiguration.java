package am.ik.sluice.server.cert;

import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.GeneralSecurityException;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Enumeration;

import org.springframework.boot.ssl.SslBundle;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import am.ik.sluice.server.config.SluiceServerProperties;

/**
 * Wires the client certificate issuer when a CA is configured: {@code sluice.ca-bundle}
 * names an SSL bundle whose keystore holds the issuer private key and certificate. The
 * gRPC control plane already trusts that CA ({@code client-auth=REQUIRE} + truststore),
 * so certificates issued later are accepted without a restart.
 */
@Configuration(proxyBeanMethods = false)
class ClientCertificateConfiguration {

	/**
	 * The issuer is resolved in this bean body instead of a
	 * {@code @ConditionalOnProperty} bean: conditions are evaluated at build time in the
	 * native image, which would pin the feature to the properties of the build machine.
	 */
	@Bean
	ClientCertificateIssuers clientCertificateIssuers(SslBundles sslBundles, SluiceServerProperties properties) {
		String bundleName = properties.caBundle();
		if (bundleName == null || bundleName.isBlank()) {
			return new ClientCertificateIssuers(null);
		}
		return new ClientCertificateIssuers(issuer(sslBundles, bundleName));
	}

	private static ClientCertificateIssuer issuer(SslBundles sslBundles, String bundleName) {
		SslBundle bundle = sslBundles.getBundle(bundleName);
		KeyStore keyStore = bundle.getStores().getKeyStore();
		if (keyStore == null) {
			throw new IllegalStateException("the CA bundle " + bundleName + " has no keystore");
		}
		String password = bundle.getStores().getKeyStorePassword();
		char[] keyPassword = password == null ? new char[0] : password.toCharArray();
		try {
			for (Enumeration<String> aliases = keyStore.aliases(); aliases.hasMoreElements();) {
				String alias = aliases.nextElement();
				if (!keyStore.isKeyEntry(alias)) {
					continue;
				}
				Certificate certificate = keyStore.getCertificate(alias);
				if (keyStore.getKey(alias, keyPassword) instanceof PrivateKey privateKey
						&& certificate instanceof X509Certificate x509) {
					return new ClientCertificateIssuer(privateKey, x509);
				}
			}
		}
		catch (GeneralSecurityException e) {
			throw new IllegalStateException("reading the CA key pair from bundle " + bundleName + " failed", e);
		}
		throw new IllegalStateException("the CA bundle " + bundleName + " holds no private key entry");
	}

}
