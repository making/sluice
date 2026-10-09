package am.ik.sluice.server.cert;

import org.jspecify.annotations.Nullable;

/**
 * The console client certificate issuer, present even when no CA is configured so the
 * decision can be taken in a bean body: bean conditions are evaluated at build time in
 * the native image, which would pin the feature to the properties of the build machine.
 *
 * @param issuer the issuer for the configured CA; {@code null} without
 * {@code sluice.ca-bundle}
 */
public record ClientCertificateIssuers(@Nullable ClientCertificateIssuer issuer) {

	/** The issuer for the configured CA; {@code null} when no CA bundle is set. */
	public @Nullable ClientCertificateIssuer find() {
		return this.issuer;
	}

}
