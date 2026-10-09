Difficulty: Low

# sluice-wasmlet: mTLS client certificate (PEM keypair flags)

Follow-up to 020 (TLS control plane, done): present a client certificate on the
`grpcs://` connection. The Java side uses an SSL bundle (`sluice.tls-bundle`); a PEM
cert+key flag pair is enough for the PoC:

- `--tls-cert <file>` / `--tls-key <file>` (PEM), mapped to
  `ClientTlsConfig::identity(Identity::from_pem(cert, key))`
- server side: `spring.grpc.server.ssl.client-auth=REQUIRE` + truststore, as in
  `ClusterGrpcMutualTlsE2ETests`

`tonic::transport::Identity::from_pem` and the `identity` field of `ClientTlsConfig`
(already wired through `sluice-wasmlet/wasmlet/src/tls.rs`) are the pieces; rustls is
in the tree.

Acceptance: `sluice-wasmlet --server grpcs://... --tls-cert ... --tls-key ...` against
a server with `client-auth=REQUIRE`; without the flags the handshake is rejected.
