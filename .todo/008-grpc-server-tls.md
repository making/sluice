Difficulty: Medium

# Server gRPC TLS (SNI routing support)

Serve the control plane over TLS (`spring.grpc.server.ssl.bundle`) so a front end can route
per node by SNI without terminating TLS.

- Verify the server property works with the client's `grpcs://` (ALPN h2) -- including
  `sluice.insecure` against a self-signed server cert
- Cover with an E2E test in `sluice-it` (server with a pem bundle, client via `grpcs://`)
- Keep the plaintext path working; document the bundle keys next to the existing
  `sluice.data-tls-bundle` example in `sluice-server/src/main/resources/application.properties`

Part of the former todo 007 item 9 (split out on 2026-10-06 when the rest of 007 landed).
Deployment context: see the k8s + HAProxy example in git history
(`.todo/007-scale-out-client-fan-out.md` at its last revision).
