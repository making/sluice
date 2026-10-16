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
