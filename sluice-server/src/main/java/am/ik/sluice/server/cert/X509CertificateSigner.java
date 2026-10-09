package am.ik.sluice.server.cert;

import java.io.ByteArrayOutputStream;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Signature;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import javax.security.auth.x500.X500Principal;

/**
 * Minimal X.509 v3 certificate encoder: the wrapper fields the JDK offers no builder for
 * are hand-rolled in DER, while keys and signatures stay with the default JCA provider.
 * The profile is fixed to a client certificate for the sluice mTLS control plane:
 * basicConstraints CA:FALSE (critical), keyUsage digitalSignature + keyEncipherment
 * (critical) and extendedKeyUsage clientAuth, signed with SHA-256 (RSA or EC).
 */
final class X509CertificateSigner {

	static final String OID_SHA256_WITH_RSA = "1.2.840.113549.1.1.11";

	static final String OID_SHA256_WITH_ECDSA = "1.2.840.10045.4.3.2";

	static final String OID_BASIC_CONSTRAINTS = "2.5.29.19";

	static final String OID_KEY_USAGE = "2.5.29.15";

	static final String OID_EXTENDED_KEY_USAGE = "2.5.29.37";

	static final String OID_CLIENT_AUTH = "1.3.6.1.5.5.7.3.2";

	private static final DateTimeFormatter UTC_TIME = DateTimeFormatter.ofPattern("yyMMddHHmmss'Z'")
		.withZone(ZoneOffset.UTC);

	private static final SecureRandom RANDOM = new SecureRandom();

	private X509CertificateSigner() {
	}

	/**
	 * Encodes and signs a certificate for {@code subjectPublicKey}, issued by the holder
	 * of {@code caKey}. A self-signed certificate passes the same pair as subject and
	 * issuer identity.
	 * @return the DER encoding of the complete certificate
	 */
	static byte[] encode(X500Principal subject, PublicKey subjectPublicKey, X500Principal issuer, PrivateKey caKey,
			Instant notBefore, Instant notAfter) {
		String keyAlgorithm = subjectPublicKey.getAlgorithm();
		boolean rsa = switch (keyAlgorithm) {
			case "RSA" -> true;
			case "EC", "ECDSA" -> false;
			default -> throw new IllegalStateException("unsupported key algorithm: " + keyAlgorithm);
		};
		String signatureAlgorithm = rsa ? "SHA256withRSA" : "SHA256withECDSA";
		byte[] signatureAlgorithmIdentifier = rsa ? sequence(oid(OID_SHA256_WITH_RSA), nullValue())
				: sequence(oid(OID_SHA256_WITH_ECDSA));
		byte[] validity = sequence(utcTime(notBefore), utcTime(notAfter));
		byte[] extensions = tag(0xA3,
				sequence(extension(OID_BASIC_CONSTRAINTS, true, sequence()),
						extension(OID_KEY_USAGE, true, bitString(new byte[] { (byte) 0xA0 }, 5)),
						extension(OID_EXTENDED_KEY_USAGE, false, sequence(oid(OID_CLIENT_AUTH)))));
		byte[] tbsCertificate = sequence(tag(0xA0, integer(BigInteger.TWO)), integer(positiveSerial()),
				signatureAlgorithmIdentifier, issuer.getEncoded(), validity, subject.getEncoded(),
				subjectPublicKey.getEncoded(), extensions);
		return sequence(tbsCertificate, signatureAlgorithmIdentifier,
				bitString(sign(tbsCertificate, caKey, signatureAlgorithm), 0));
	}

	private static byte[] sign(byte[] tbsCertificate, PrivateKey caKey, String signatureAlgorithm) {
		try {
			Signature signature = Signature.getInstance(signatureAlgorithm);
			signature.initSign(caKey, RANDOM);
			signature.update(tbsCertificate);
			return signature.sign();
		}
		catch (GeneralSecurityException e) {
			throw new IllegalStateException("signing with " + signatureAlgorithm + " failed", e);
		}
	}

	/** A positive serial number, unique per issuer by randomness alone. */
	private static BigInteger positiveSerial() {
		return new BigInteger(62, RANDOM).add(BigInteger.ONE);
	}

	/** One extension: SEQUENCE of OID, optional critical flag and the DER value. */
	private static byte[] extension(String oid, boolean critical, byte... value) {
		return critical ? sequence(oid(oid), booleanTrue(), octetString(value))
				: sequence(oid(oid), octetString(value));
	}

	private static byte[] sequence(byte[]... parts) {
		return tag(0x30, concat(parts));
	}

	private static byte[] integer(BigInteger value) {
		return tag(0x02, value.toByteArray());
	}

	private static byte[] booleanTrue() {
		return new byte[] { 0x01, 0x01, (byte) 0xFF };
	}

	private static byte[] nullValue() {
		return new byte[] { 0x05, 0x00 };
	}

	private static byte[] octetString(byte[] content) {
		return tag(0x04, content);
	}

	/**
	 * A BIT STRING carrying the given content, with the given count of unused final bits.
	 */
	private static byte[] bitString(byte[] content, int unusedBits) {
		byte[] body = new byte[content.length + 1];
		body[0] = (byte) unusedBits;
		System.arraycopy(content, 0, body, 1, content.length);
		return tag(0x03, body);
	}

	private static byte[] utcTime(Instant instant) {
		return tag(0x17,
				ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).format(UTC_TIME).getBytes(StandardCharsets.US_ASCII));
	}

	private static byte[] tag(int identifier, byte[] content) {
		ByteArrayOutputStream out = new ByteArrayOutputStream(content.length + 8);
		out.write(identifier);
		int length = content.length;
		if (length < 0x80) {
			out.write(length);
		}
		else {
			byte[] encoded = BigInteger.valueOf(length).toByteArray();
			int significant = encoded[0] == 0 ? encoded.length - 1 : encoded.length;
			out.write(0x80 | significant);
			out.write(encoded, encoded.length - significant, significant);
		}
		out.write(content, 0, length);
		return out.toByteArray();
	}

	private static byte[] oid(String dotted) {
		String[] arcs = dotted.split("\\.");
		ByteArrayOutputStream body = new ByteArrayOutputStream(arcs.length);
		body.write(Integer.parseInt(arcs[0]) * 40 + Integer.parseInt(arcs[1]));
		for (int i = 2; i < arcs.length; i++) {
			long arc = Long.parseLong(arcs[i]);
			byte[] encoded = new byte[10];
			int length = 0;
			do {
				encoded[length++] = (byte) (arc & 0x7F);
				arc >>>= 7;
			}
			while (arc > 0);
			for (int j = length - 1; j >= 0; j--) {
				body.write(j == 0 ? encoded[j] : encoded[j] | 0x80);
			}
		}
		return tag(0x06, body.toByteArray());
	}

	private static byte[] concat(byte[]... parts) {
		int length = 0;
		for (byte[] part : parts) {
			length += part.length;
		}
		byte[] all = new byte[length];
		int offset = 0;
		for (byte[] part : parts) {
			System.arraycopy(part, 0, all, offset, part.length);
			offset += part.length;
		}
		return all;
	}

}
