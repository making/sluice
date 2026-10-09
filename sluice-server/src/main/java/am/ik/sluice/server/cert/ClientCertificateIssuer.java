package am.ik.sluice.server.cert;

import java.io.ByteArrayInputStream;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.spec.ECGenParameterSpec;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import javax.security.auth.x500.X500Principal;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;

/**
 * Issues client certificates for the mTLS control plane, signed by the configured
 * certificate authority. Each issuance creates a fresh key pair, so the private key never
 * leaves this node unencrypted: the caller receives the certificate and the PKCS#8 key as
 * PEM and hands both to the client operator.
 */
public final class ClientCertificateIssuer {

	/** Last instant representable as a UTCTime, keeping validity below the year 2050. */
	private static final Instant UTC_TIME_LIMIT = Instant.parse("2049-12-31T23:59:59Z");

	private static final SecureRandom RANDOM = new SecureRandom();

	private final PrivateKey caKey;

	private final X509Certificate caCertificate;

	public ClientCertificateIssuer(PrivateKey caKey, X509Certificate caCertificate) {
		this.caKey = caKey;
		this.caCertificate = caCertificate;
	}

	/**
	 * The subject of the CA, shown in the console as the signer of issued certificates.
	 */
	public String caSubject() {
		return this.caCertificate.getSubjectX500Principal().getName();
	}

	/** The CA certificate, PEM encoded: the truststore anchor on the client side. */
	public String caCertificatePem() {
		try {
			return pem("CERTIFICATE", this.caCertificate.getEncoded());
		}
		catch (java.security.cert.CertificateEncodingException e) {
			throw new IllegalStateException("encoding the CA certificate failed", e);
		}
	}

	/**
	 * Issues a certificate for a fresh key pair.
	 * @param commonName subject CN; characters outside {@code [A-Za-z0-9._-]} are
	 * dropped, an empty remainder is rejected
	 * @param validityDays days until notAfter; values below one day are raised to one,
	 * and the notAfter instant never reaches the year 2050 (UTCTime boundary)
	 */
	public IssuedClientCertificate issue(String commonName, int validityDays) {
		String name = sanitize(commonName);
		if (name.isEmpty()) {
			throw new IllegalArgumentException("common name is empty after sanitization: " + commonName);
		}
		Instant notBefore = Instant.now().truncatedTo(ChronoUnit.SECONDS);
		Instant notAfter = notBefore.plus(Duration.ofDays(Math.max(1, validityDays)));
		if (notAfter.isAfter(UTC_TIME_LIMIT)) {
			notAfter = UTC_TIME_LIMIT;
		}
		KeyPair identity = identityKeyPair();
		try {
			byte[] der = X509CertificateSigner.encode(new X500Principal("CN=" + name), identity.getPublic(),
					this.caCertificate.getSubjectX500Principal(), this.caKey, notBefore, notAfter);
			CertificateFactory factory = CertificateFactory.getInstance("X.509");
			X509Certificate certificate = (X509Certificate) factory.generateCertificate(new ByteArrayInputStream(der));
			// round trip through the parser and against the CA before anything is handed
			// out
			certificate.verify(this.caCertificate.getPublicKey());
			return new IssuedClientCertificate(pem("CERTIFICATE", certificate.getEncoded()),
					pem("PRIVATE KEY", identity.getPrivate().getEncoded()));
		}
		catch (GeneralSecurityException e) {
			throw new IllegalStateException("issuing the certificate for CN=" + name + " failed", e);
		}
	}

	/** Strips every character outside {@code [A-Za-z0-9._-]} from the common name. */
	public static String sanitize(String commonName) {
		return commonName.replaceAll("[^A-Za-z0-9._-]", "");
	}

	/**
	 * A key pair of the same family as the CA key, so the signature algorithm carries
	 * over.
	 */
	private KeyPair identityKeyPair() {
		try {
			String algorithm = this.caKey.getAlgorithm();
			KeyPairGenerator generator = KeyPairGenerator.getInstance(algorithm);
			if (isEllipticCurve(algorithm)) {
				generator.initialize(new ECGenParameterSpec("secp256r1"), RANDOM);
			}
			else {
				generator.initialize(2048, RANDOM);
			}
			return generator.generateKeyPair();
		}
		catch (GeneralSecurityException e) {
			throw new IllegalStateException("generating the client key pair failed", e);
		}
	}

	private static boolean isEllipticCurve(String algorithm) {
		return algorithm.equals("EC") || algorithm.equals("ECDSA");
	}

	private static String pem(String label, byte[] der) {
		return "-----BEGIN " + label + "-----\n" + Base64.getMimeEncoder(64, new byte[] { '\n' }).encodeToString(der)
				+ "\n-----END " + label + "-----\n";
	}

}
