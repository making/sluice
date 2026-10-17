Difficulty: Medium

# sluice-client: the native image fails with a NullPointerException

Reported 2026-10-17: the client built with `-Pnative` does not work; it throws a
NullPointerException. Stack trace and trigger (startup, connect, first relayed request)
not captured yet.

## Work

1. Build the native client (`./mvnw -Pnative ...` for `sluice-client`) and run it against a
   local `sluice-server` (everything stays local; do not connect to production) to reproduce.
   Capture the full stack trace and the phase it fails in.
2. Find the cause (missing reflection / resource / proxy metadata, class initialized at build
   time, a code path that behaves differently under native, ...). Look at what changed
   since the native client last worked: new config properties records, the tunnel budget,
   `TunnelTransportWindowConfiguration`, `NettyChannelBuilder.flowControlWindow`, the
   reconnect backoff.
3. Fix it. No failing test first is required for this item: the native run in step 4 is
   the verification.
4. Verify locally: the native client connects to a local server, advertises, and relays
   h1.1 and h2 requests to a local upstream (`sluice-example-upstream`); reconnects after a
   server restart.

## Done when

- The native client runs the scenarios in step 4 without exceptions.
- The cause is recorded in the commit.

## Not reproduced locally (2026-10-17, at 40db6f6)

The native client (`-Pnative`, GraalVM 25.0.3, macOS aarch64) ran every scenario below
without an exception; so did the published arm64 images `sluice-client:native` (800d093)
and `native_12f5a3c…` (the image current when this was reported) in Docker, also with
`-m 128m`. Server: JVM, and the native server for the plaintext runs.

- connect + advertise, h1.1 and h2 (prior knowledge) through `sluice-example-upstream`
- reconnect after a server restart; start with the server down, then up
- `grpcs://` with `insecure`, with a `tls-bundle` truststore, with mTLS (keystore +
  `client-auth=REQUIRE`), with the default trust path (`-Djavax.net.ssl.trustStore`);
  RSA and EC P-384 server certificates
- configuration from env vars only (`SLUICE_*`), structured logging (`ecs`)
- two-node cluster (`sluice.cluster.nodes`): the second stream via membership, both nodes
  restarted
- 50MiB downloads and uploads over one keep-alive connection with idle gaps (window growth
  and reclaim), an `https://` upstream, a tcp `listen-port` route, TLS passthrough
- `/actuator/health`, `/actuator/prometheus`, SIGTERM shutdown

The amd64 image cannot run here (Rosetta lacks AVX/AVX2, which `-march` of the CI build
requires), so a Linux amd64 host remains untested.

Needed to go on: the stack trace, the exact command / env (image tag or build commit,
`sluice.*` settings, server URL scheme), and the host (OS / arch).
