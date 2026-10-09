package am.ik.sluice.server.cert;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * A certificate issued by {@link ClientCertificateIssuer}: the certificate and its PKCS#8
 * private key, each PEM encoded, ready to drop into an SSL bundle next to the CA
 * certificate.
 *
 * @param certificatePem the certificate, PEM encoded with the {@code CERTIFICATE} label
 * @param privateKeyPem the private key, PEM encoded with the {@code PRIVATE KEY} label
 * (PKCS#8, as produced by {@code PrivateKey#getEncoded()})
 */
public record IssuedClientCertificate(String certificatePem, String privateKeyPem) {

	/**
	 * The console download: a zip archive of separate PEM files named
	 * {@code baseName.crt.pem}, {@code baseName.key.pem} and {@code ca.crt.pem} (the
	 * truststore anchor), so the client bundle properties map one to one.
	 */
	public byte[] zip(String baseName, String caCertificatePem) {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		try (ZipOutputStream zip = new ZipOutputStream(out)) {
			entry(zip, baseName + ".crt.pem", this.certificatePem);
			entry(zip, baseName + ".key.pem", this.privateKeyPem);
			entry(zip, "ca.crt.pem", caCertificatePem);
		}
		catch (IOException e) {
			throw new IllegalStateException("building the certificate archive failed", e);
		}
		return out.toByteArray();
	}

	private static void entry(ZipOutputStream zip, String name, String content) throws IOException {
		zip.putNextEntry(new ZipEntry(name));
		zip.write(content.getBytes(StandardCharsets.UTF_8));
		zip.closeEntry();
	}

}
