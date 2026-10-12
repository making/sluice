Difficulty: Medium

# Remove the legacy (unwindowed) connection mode

Follow-up of `.todo/041-tunnel-head-of-line-blocking.md` (section "Follow-up"), filed when
046 landed. Do it only once no pre-046 client or server and no pre-047 wasmlet is
deployed: until then a legacy peer needs the old path. Depends on 047.

## Problem

A connection is windowed only when both peers announce a window (`Advertise` /
`Connect.connection_window > 0`). Otherwise it runs in legacy mode, which keeps the
stream-wide stall of F1: `VirtualConnection#acceptData` blocks the gRPC receive callback
while 128 frames are buffered, so one slow consumer stalls every connection of a legacy
peer.

## Scope

- Java (`sluice-proto`): drop the legacy constructor `VirtualConnection(long,
  FrameWriter)`, the blocking `acceptData` branch and `inboundPermits`, the
  uncoalesced (`coalesce == false`) branch of `append`, and the legacy branches of
  `Sink#write`, `close()` and `Source#take`. Every connection is windowed.
- Server (`TunnelSession#open`) and client (`NodeConnection`, `CONNECT`): a peer that
  announces no window is rejected (server: fail the stream with a clear status;
  client: answer the CONNECT with ERROR), or given the default window -- decide, and log
  it either way.
- wasmlet (`tunnel.rs`): `Ctrl`, the `legacy` channel of `Conn` / `TunnelIo`,
  `Deferred` and `LEGACY_BUFFER` (the bounded channel whose send the receive loop
  awaits after the dispatch), the `server_window == 0` branch of
  `Connections::register`, and the legacy branches of `TunnelIo::fill` / `poll_read` /
  `poll_write` / `consumed` and of `eof` / `aborted`. `Connections::deliver` then
  never awaits and can become the plain `dispatch`. A CONNECT without a window is
  answered as decided for the Java client.
- Proto: keep the fields (0 stays meaningful as "no window" for rejection); update their
  comments.
- Tests: `LegacyClientInteropE2ETest` / `LegacyServerInteropE2ETest` change to assert
  the rejection; the legacy unit tests in `VirtualConnectionTest` go; tests that build
  `new VirtualConnection(id, writer)` switch to the builder. wasmlet: the `a_legacy_*`
  tests in `tunnel.rs` and `wasm_host.rs` and the `window = 0` cases go.
- README: drop the sentence on legacy peers.

## Tests (watch each fail first)

- A client advertising no window is rejected as decided, with the log line.
- A CONNECT without a window is answered as decided.
- Everything in `sluice-it` stays green with only windowed connections.

## Decision (2026-10-12)

- A peer announcing no window is rejected, not given the default window (a legacy peer
  never sends WindowUpdate, so a window would stall it): the server fails the stream with
  `FAILED_PRECONDITION` and a message naming the minimum version; the client and the
  wasmlet answer such a CONNECT with ERROR. Each logs one line per rejection.
- Do it last in the queue. Before starting, check that the deployed server includes 046
  and 047 (`curl https://console.sluice.ik.am/actuator/info | jq .git.commit`; on
  2026-10-12 it was 18a4ff4, after both). Clients and wasmlets of older versions are
  then rejected with that message and need an upgrade.
- Depends on 084 (peer build reporting): deploy it first, then confirm the console shows
  no legacy-window and no unknown-build sessions instead of assuming no old peer remains.
