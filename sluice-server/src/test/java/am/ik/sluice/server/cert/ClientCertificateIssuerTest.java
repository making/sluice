package am.ik.sluice.server.cert;

import java.io.ByteArrayInputStream;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.ECGenParameterSpec;
import java.security.spec.PKCS8EncodedKeySpec;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import javax.security.auth.x500.X500Principal;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Round trips the hand-rolled encoder through {@code CertificateFactory}: every issued
 * certificate must parse, verify against its CA and carry the client auth profile.
 */
class ClientCertificateIssuerTest {

	@Test
	void issuedCertificateVerifiesAgainstTheCa() throws Exception {
		Ca ca = authority("sluice-test-ca", "RSA");
		ClientCertificateIssuer issuer = new ClientCertificateIssuer(ca.key(), ca.certificate());

		IssuedClientCertificate issued = issuer.issue("client-1", 365);
		X509Certificate certificate = certificate(issued.certificatePem());

		certificate.verify(ca.certificate().getPublicKey());
		assertThat(certificate.getVersion()).isEqualTo(3);
		assertThat(certificate.getIssuerX500Principal()).isEqualTo(ca.certificate().getSubjectX500Principal());
		assertThat(certificate.getSubjectX500Principal().getName()).contains("CN=client-1");
		assertThat(certificate.getSigAlgOID()).isEqualTo(X509CertificateSigner.OID_SHA256_WITH_RSA);
	}

	@Test
	void theCertificateCarriesTheClientAuthProfile() throws Exception {
		Ca ca = authority("sluice-test-ca", "RSA");
		ClientCertificateIssuer issuer = new ClientCertificateIssuer(ca.key(), ca.certificate());

		X509Certificate certificate = certificate(issuer.issue("client-2", 365).certificatePem());

		assertThat(certificate.getBasicConstraints()).isEqualTo(-1);
		assertThat(certificate.getCriticalExtensionOIDs()).containsExactlyInAnyOrder(
				X509CertificateSigner.OID_BASIC_CONSTRAINTS, X509CertificateSigner.OID_KEY_USAGE);
		assertThat(certificate.getNonCriticalExtensionOIDs())
			.containsExactly(X509CertificateSigner.OID_EXTENDED_KEY_USAGE);
		assertThat(certificate.getExtendedKeyUsage()).containsExactly(X509CertificateSigner.OID_CLIENT_AUTH);
		boolean[] keyUsage = certificate.getKeyUsage();
		assertThat(keyUsage[0]).isTrue();
		assertThat(keyUsage[1]).isFalse();
		assertThat(keyUsage[2]).isTrue();
	}

	@Test
	void serialNumbersArePositiveAndDistinct() throws Exception {
		Ca ca = authority("sluice-test-ca", "RSA");
		ClientCertificateIssuer issuer = new ClientCertificateIssuer(ca.key(), ca.certificate());

		X509Certificate first = certificate(issuer.issue("client-3", 365).certificatePem());
		X509Certificate second = certificate(issuer.issue("client-4", 365).certificatePem());

		assertThat(first.getSerialNumber().signum()).isPositive();
		assertThat(second.getSerialNumber().signum()).isPositive();
		assertThat(first.getSerialNumber()).isNotEqualTo(second.getSerialNumber());
	}

	@Test
	void validityCoversTheRequestedDays() throws Exception {
		Ca ca = authority("sluice-test-ca", "RSA");
		ClientCertificateIssuer issuer = new ClientCertificateIssuer(ca.key(), ca.certificate());
		Instant before = Instant.now();

		X509Certificate certificate = certificate(issuer.issue("client-5", 30).certificatePem());

		assertThat(certificate.getNotBefore().toInstant()).isAfter(before.minus(1, ChronoUnit.MINUTES));
		assertThat(certificate.getNotAfter().toInstant())
			.isEqualTo(certificate.getNotBefore().toInstant().plus(30, ChronoUnit.DAYS));
	}

	@Test
	void validityStaysRepresentableAsUtcTime() throws Exception {
		Ca ca = authority("sluice-test-ca", "RSA");
		ClientCertificateIssuer issuer = new ClientCertificateIssuer(ca.key(), ca.certificate());

		X509Certificate certificate = certificate(issuer.issue("client-6", 100_000).certificatePem());

		assertThat(certificate.getNotAfter().toInstant()).isEqualTo(Instant.parse("2049-12-31T23:59:59Z"));
	}

	@Test
	void theEnclosedPrivateKeyMatchesTheCertificate() throws Exception {
		Ca ca = authority("sluice-test-ca", "RSA");
		ClientCertificateIssuer issuer = new ClientCertificateIssuer(ca.key(), ca.certificate());

		IssuedClientCertificate issued = issuer.issue("client-7", 365);
		PrivateKey privateKey = KeyFactory.getInstance("RSA")
			.generatePrivate(new PKCS8EncodedKeySpec(der("PRIVATE KEY", issued.privateKeyPem())));
		X509Certificate certificate = certificate(issued.certificatePem());
		byte[] probe = "sluice".getBytes();

		Signature signature = Signature.getInstance("SHA256withRSA");
		signature.initSign(privateKey);
		signature.update(probe);
		Signature verifier = Signature.getInstance("SHA256withRSA");
		verifier.initVerify(certificate.getPublicKey());
		verifier.update(probe);
		assertThat(verifier.verify(signature.sign())).isTrue();
	}

	@Test
	void anEllipticCurveCaSignsWithTheEcdsaAlgorithm() throws Exception {
		Ca ca = authority("sluice-test-ca", "EC");
		ClientCertificateIssuer issuer = new ClientCertificateIssuer(ca.key(), ca.certificate());

		IssuedClientCertificate issued = issuer.issue("client-8", 365);
		X509Certificate certificate = certificate(issued.certificatePem());

		certificate.verify(ca.certificate().getPublicKey());
		assertThat(certificate.getPublicKey().getAlgorithm()).isIn("EC", "ECDSA");
		assertThat(certificate.getSigAlgOID()).isEqualTo(X509CertificateSigner.OID_SHA256_WITH_ECDSA);
	}

	@Test
	void theDownloadArchiveHoldsSeparatePemFiles() throws Exception {
		Ca ca = authority("sluice-test-ca", "RSA");
		ClientCertificateIssuer issuer = new ClientCertificateIssuer(ca.key(), ca.certificate());
		IssuedClientCertificate issued = issuer.issue("client-9", 365);

		Map<String, String> entries = entries(issued.zip("client-9", issuer.caCertificatePem()));

		assertThat(entries.keySet()).containsExactlyInAnyOrder("client-9.crt.pem", "client-9.key.pem", "ca.crt.pem");
		String certificatePem = entries.get("client-9.crt.pem");
		String keyPem = entries.get("client-9.key.pem");
		assertThat(certificatePem).isNotNull();
		assertThat(keyPem).isNotNull();
		X509Certificate certificate = certificate(certificatePem);
		certificate.verify(ca.certificate().getPublicKey());
		assertThat(certificate.getSubjectX500Principal().getName()).contains("CN=client-9");
		PrivateKey privateKey = KeyFactory.getInstance("RSA")
			.generatePrivate(new PKCS8EncodedKeySpec(der("PRIVATE KEY", keyPem)));
		assertThat(privateKey.getAlgorithm()).containsIgnoringCase("rsa");
		assertThat(entries.get("ca.crt.pem")).isEqualTo(issuer.caCertificatePem());
	}

	private static Map<String, String> entries(byte[] archive) throws Exception {
		Map<String, String> entries = new HashMap<>();
		try (ZipInputStream zip = new ZipInputStream(new ByteArrayInputStream(archive))) {
			for (ZipEntry entry; (entry = zip.getNextEntry()) != null;) {
				entries.put(entry.getName(), new String(zip.readAllBytes(), StandardCharsets.UTF_8));
			}
		}
		return entries;
	}

	@Test
	void theCommonNameIsSanitizedAndEmptinessRejected() throws Exception {
		Ca ca = authority("sluice-test-ca", "RSA");
		ClientCertificateIssuer issuer = new ClientCertificateIssuer(ca.key(), ca.certificate());

		X509Certificate certificate = certificate(issuer.issue("client nine/1", 365).certificatePem());

		assertThat(certificate.getSubjectX500Principal().getName()).contains("CN=clientnine1");
		assertThatIllegalArgumentException().isThrownBy(() -> issuer.issue("?", 365));
	}

	private static Ca authority(String commonName, String algorithm) throws Exception {
		KeyPair pair = keyPair(algorithm);
		Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
		// the self-signed marker CA is built by the signer under test itself
		byte[] der = X509CertificateSigner.encode(new X500Principal("CN=" + commonName), pair.getPublic(),
				new X500Principal("CN=" + commonName), pair.getPrivate(), now, now.plus(3650, ChronoUnit.DAYS));
		X509Certificate certificate = (X509Certificate) CertificateFactory.getInstance("X.509")
			.generateCertificate(new ByteArrayInputStream(der));
		return new Ca(pair.getPrivate(), certificate);
	}

	/** A CA key pair with its self-signed certificate. */
	private record Ca(PrivateKey key, X509Certificate certificate) {

	}

	private static KeyPair keyPair(String algorithm) throws Exception {
		KeyPairGenerator generator = KeyPairGenerator.getInstance(algorithm);
		if (algorithm.equals("EC")) {
			generator.initialize(new ECGenParameterSpec("secp256r1"));
		}
		else {
			generator.initialize(2048);
		}
		return generator.generateKeyPair();
	}

	private static X509Certificate certificate(String pem) throws Exception {
		byte[] der = der("CERTIFICATE", pem);
		return (X509Certificate) CertificateFactory.getInstance("X.509")
			.generateCertificate(new ByteArrayInputStream(der));
	}

	private static byte[] der(String label, String pem) {
		String body = Pattern.compile("-----BEGIN " + label + "-----(.*?)-----END " + label + "-----", Pattern.DOTALL)
			.matcher(pem)
			.results()
			.findFirst()
			.orElseThrow(() -> new AssertionError("no " + label + " block in the PEM"))
			.group(1);
		return Base64.getMimeDecoder().decode(body);
	}

}
