Difficulty: High

# h2 demux: the relay's windows cap a connection at about 64 KiB per round trip

Found by the measurements of 106 (`.todo/artefacts/106/RESULTS.md`, finding 1).

## Problem

The relay announces netty's defaults on both faces: a 65535-byte stream window and a 65535-byte
connection window. netty refills the connection window as DATA arrives and sends the
WINDOW_UPDATE once half of it is consumed. So each hop where the relay receives holds a whole
connection to about 64 KiB in flight per round trip. On the leg that bounds downloads by the
tunnel RTT; on the client face it bounds uploads by the client RTT. Uploads are further bound
by the upstream's own window over the tunnel RTT, which the relay cannot change.

Measured with the harness (20 ms RTT from a delay proxy):

| | h2 now | h1 |
|---|---|---|
| download, tunnel RTT 20 ms, 1 stream | 1.7 MiB/s | 41.7 MiB/s |
| download, tunnel RTT 20 ms, 8 streams of one connection | 1.4 MiB/s | 49.0 MiB/s (8 conns) |
| upload, client RTT 20 ms, 1 stream | 1.7 MiB/s | 477 MiB/s |
| upload, client RTT 20 ms, 8 streams of one connection | 1.4 MiB/s | - |
| download, loopback, 1 stream | 258 MiB/s | 1670 MiB/s |

Experiment: a patched build (`.todo/artefacts/106/harness/patches/window-experiment.patch`)
with a 1 MiB stream window and a 16 MiB connection window on both faces:

- download at a 20 ms tunnel RTT: 22.3 MiB/s with 1 stream, 31.8 MiB/s with 8 streams;
- download on loopback: 526 MiB/s;
- small requests: unchanged (145k req/s on one connection with 100 streams);
- upload: unchanged at 1.7 MiB/s while the upstream announces 65535; 22.4 MiB/s with an
  upstream announcing 1 MiB.

Browsers send their requests over h2 whenever ALPN offers it, so every download through an
h2 route is capped like this.

## Constraints

- The memory bounds derive from the 65535 windows. They are `STREAM_WINDOW`,
  `WITHHELD_ARRIVALS` (which assumes a peer holds at most 65535 of connection window once it
  is withheld) and `UNBUDGETED_BYTES`, and through them `ConnectionLimits.connectionBytes`
  (3340 KiB per slot; production derives 54 connections from a 409 MiB heap). A held h2
  connection is 163 to 228 KiB of live heap (106), so the derived bound is a worst case.
  Re-derive it for the windows chosen, and say in README what the limit becomes.
- `PendingPayload.append` grows the backlog array by copying on every append, which is
  quadratic in the frames a backlog holds. With 1 MiB windows the server allocated 18.4 bytes
  per relayed byte instead of 6.9 (106). Fix it before raising the stream window: grow the
  array geometrically or keep a chunk list, and keep the accounting.
- The connection window alone (refilled on arrival) bounds no memory today, so raising it
  costs only the `WITHHELD_ARRIVALS` re-derivation. On its own it lifts the 8-stream case,
  not the single stream.

## Done when

- The relay announces larger windows on both faces (a stream window and a connection
  window, as properties or derived constants), with the bounds re-derived and
  `ConnectionLimitsTest` updated.
- A test shows the gain where it matters: a stream through a tunnel with injected latency
  moves its bytes in fewer round trips than the 64 KiB windows allow. The test must fail on
  the current windows.
- The budget, withholding and stall tests still hold with the new bounds.
- README documents the windows and the per-connection throughput bound.
