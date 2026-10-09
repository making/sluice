Difficulty: High

# Client certificate issuance from the console

Issue client certificates for the server<->client mTLS from the management console
(`/console`), enabled only when a CA is configured. No BouncyCastle: sign via
`java.security`, hand-roll only the minimal DER encoding (~250 lines) of the X.509
wrapper. The design was investigated on 2026-10-09; key facts:

## Configuration

- New CA bundle key, e.g. `sluice.ca-bundle` (issuer private key + certificate, PEM via
  Spring Boot SSL bundle syntax). Feature activates only when present
  (`ObjectProvider`-style conditional wiring).
- No server restart needed after issuance: the gRPC control plane already trusts the CA
  via `spring.grpc.server.ssl.client-auth=REQUIRE` + truststore.

## Issuance flow

- Console form (CN, validity days) -> POST -> sign with CA -> download PEM
  (certificate + PKCS#8 private key). No CSR parsing: plain form fields only, so no
  untrusted-DER parser is written. Sanitize CN to `[A-Za-z0-9._-]`.
- Console POST needs CSRF (`{{#_csrf}}` pattern from `login.mustache`); first non-GET
  endpoints in the console.
- New dependency-free encoder must set: matching sigAlg OID / actual signature
  (`sha256WithRSAEncryption` or EC equivalent; signature itself via default JDK
  provider, no BC provider registration), `SecureRandom` positive serial,
  basicConstraints CA:FALSE critical, keyUsage, extendedKeyUsage clientAuth. UTCTime
  only (validity kept under the 2050 boundary). Copy issuer/subject DN bytes from
  `X500Principal#getEncoded()`; SPKI / PKCS#8 from `key.getEncoded()`.

## Verification

- Unit: issue -> `CertificateFactory#generateCertificate` parse + `verify(ca)` round
  trip; UTCTime boundary test.
- E2E: extend the `ClusterGrpcMutualTlsE2ETests` pattern -- issue via the console
  endpoint, connect the client with the issued cert (`sluice.tls-bundle`), assert the
  mTLS handshake; optionally `openssl verify` in the mix.
- Native image (`-Pnative`): run the console issuance flow once against the native
  build; the encoder is plain Java so no new reflection config is expected.

## Non-goals

- Revocation (CRL/OCSP) and renewal flow: not in this item.
- Pushing the cert to the client over the tunnel: the client fetches the PEM files
  out-of-band (download endpoint) and restarts/reconnects.
