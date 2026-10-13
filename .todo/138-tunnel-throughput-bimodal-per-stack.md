Difficulty: Medium

# Tunnel throughput at a 20ms RTT is bimodal per stack

Found while measuring 134 (`.todo/artefacts/106/RESULTS.md`, "After todo 134").

## Problem

With the 106 harness at a 20ms tunnel RTT (`DelayProxy` between sluice-client and the
server's gRPC port), throughput comes in two modes, in the build before 134 and after it
alike:

- one h1 download: about 20.5 or about 39 MiB/s per 12s run, the mode changing between runs
  of one stack and between stacks;
- all downloads of a stack: one stack of each build moved every download at about
  21 MiB/s whatever the number of connections (c8, c32, c100: 20.8 to 27.1 MiB/s), while
  other stacks moved 60 to 166 MiB/s.

20.5 MiB/s is 1MiB per 50ms, a cap over the whole tunnel stream, so it is not a virtual
connection's window. Neither side configures the gRPC transport's flow control window
(`NettyServerBuilder` / `NettyChannelBuilder` defaults; whether BDP auto-tuning is on was
not checked), nor the `SessionSender`'s readiness threshold.

## Done when

- The cause is found with a timeline (gRPC HTTP/2 connection and stream windows, the
  sender's `isReady` waits, the `DelayProxy`'s own buffering), and the harness shows which
  mode a stack is in.
- If it is the transport window: it is set (or auto-tuned) so one tunnel moves well past
  1MiB per round trip, with a test counting round trips, and the 106 harness numbers are
  recorded before/after.
