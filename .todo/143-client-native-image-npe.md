Difficulty: Medium

# sluice-client: the native image fails with a NullPointerException

Reported 2026-10-17: the client built with `-Pnative` does not work; it throws a
NullPointerException. Stack trace and trigger (startup, connect, first relayed request)
not captured yet.

## Work

1. Build the native client (`./mvnw -Pnative ...` for `sluice-client`) and run it against a
   real server (local `sluice-server`, then the production server read-only) to reproduce.
   Capture the full stack trace and the phase it fails in.
2. Find the cause (missing reflection / resource / proxy metadata, class initialized at build
   time, a code path that behaves differently under native, ...). Look at what changed
   since the native client last worked: new config properties records, the tunnel budget,
   `TunnelTransportWindowConfiguration`, `NettyChannelBuilder.flowControlWindow`, the
   reconnect backoff.
3. Fix it, with a test that fails without the fix where possible (e.g. a check that the
   metadata covers the classes, or a native smoke run in CI if feasible; if a native test
   is not practical in CI, record why).
4. Verify on the real machine: the native client connects, advertises, and relays h1.1 and
   h2 requests through the production routes; reconnects after a server restart.

## Done when

- The native client runs the scenarios in step 4 without exceptions.
- The cause and the guard against its recurrence are recorded in the commit.
