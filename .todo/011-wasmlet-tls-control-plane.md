Difficulty: Medium

# sluice-wasmlet: TLS control plane (grpcs / mTLS / insecure)

The wasmlet client only speaks plaintext `grpc://`. Port the transport options of the
Java client (`sluice-client` `NodeConnection#buildChannel`):

- `grpcs://` scheme in `--server`, with certificate verification
- `sluice.insecure` equivalent (skip verification) for self-signed setups
- mTLS via a client certificate (the Java side uses an SSL bundle; a PEM cert+key pair
  flag pair is enough for the PoC follow-up)

`Endpoint::tls_config` / `tonic::transport::ClientTlsStream` are the pieces; rustls is
already in the tree via reqwest.

Acceptance: `sluice-wasmlet --server grpcs://...` against a server with
`spring.grpc.server.ssl.bundle` configured, plus the insecure variant; the wasm data
path is unaffected.

Current state: `wasmlet/src/main.rs` `run()` strips `grpc://` only and rejects nothing
else explicitly.
