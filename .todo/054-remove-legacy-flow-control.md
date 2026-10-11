Difficulty: Medium

# Remove the legacy (unwindowed) connection mode

Follow-up of `.todo/041-tunnel-head-of-line-blocking.md` (section "Follow-up"), filed when
046 landed. Do it only once no pre-046 client or server and no pre-047 wasmlet is
deployed: until then a legacy peer needs the old path. Depends on 047.

## Problem

A connection is windowed only when both peers announce a window (`Advertise` /
`Connect.connection_window > 0`). Otherwise it runs in legacy mode, which keeps the
stream-wide stall of F1: `VirtualConnection#acceptData` blocks the gRPC receive callback
while 128 chunks are buffered, so one slow consumer stalls every connection of a legacy
peer.

## Scope

- Java (`sluice-proto`): drop the legacy constructor `VirtualConnection(long,
  FrameWriter)`, the blocking `acceptData` branch and `inboundPermits`, and the legacy
  branches of `Sink#write`, `close()` and `Source#read`. Every connection is windowed.
- Server (`TunnelSession#open`) and client (`NodeConnection`, `CONNECT`): a peer that
  announces no window is rejected (server: fail the stream with a clear status;
  client: answer the CONNECT with ERROR), or given the default window -- decide, and log
  it either way.
- wasmlet: the legacy branches 047 keeps (bounded ctrl channel, forwarding task).
- Proto: keep the fields (0 stays meaningful as "no window" for rejection); update their
  comments.
- Tests: `LegacyClientInteropE2ETest` / `LegacyServerInteropE2ETest` change to assert
  the rejection; the legacy unit tests in `VirtualConnectionTest` go; tests that build
  `new VirtualConnection(id, writer)` switch to the builder.
- README: drop the sentence on legacy peers.

## Tests (watch each fail first)

- A client advertising no window is rejected as decided, with the log line.
- A CONNECT without a window is answered as decided.
- Everything in `sluice-it` stays green with only windowed connections.
