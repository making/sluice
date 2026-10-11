Difficulty: Medium

# h2 demux: flow control towards the upstream

Found in the 051 review. Both items predate 051; 051 states the first one in the
`Http2DemuxRelay` class doc.

## Problem

1. **The upstream's stream windows are not tracked.** The client's stream window is the
   relay's (spec default 65535, plus what the relay grants), and the leg writer writes
   whatever is queued. An upstream that announces `SETTINGS_INITIAL_WINDOW_SIZE` below
   65535 (or lowers it later) can receive more DATA on a stream than its window allows,
   and must answer with `FLOW_CONTROL_ERROR`. Same for the upstream's connection window,
   which the relay assumes is never exhausted.
2. **The upstream's connection window is not released after the client's TCP
   half-close.** At client EOF each leg's writer half-closes the upstream and exits; the
   connection-level WINDOW_UPDATEs the leg inbound queues afterwards are dropped (the sink
   is closed). An upstream that keeps the half-closed connection and honors its window
   stalls a response larger than that window. Before 051 the same write failed and tore
   the relay down.

## Design sketch

- Track per leg the upstream's connection window and per stream its stream window
  (initial from the upstream's SETTINGS, plus its WINDOW_UPDATEs). The writer takes a
  stream's DATA only as far as both allow; a stream without upstream window waits
  without blocking the leg's other entries. Keep the 051 rules (header block atomic,
  priority lane, per-stream budget).
- Half-close: defer the upstream half-close while the leg has open streams, or keep
  sending control frames until the leg's inbound ends. Choose by what a real upstream
  (Netty, Go, Envoy) does with a half-closed h2 connection.

## Tests (`sluice-it`, watch each fail first)

- An upstream announcing `INITIAL_WINDOW_SIZE=16384` never receives more than its
  window on a stream, and the request completes.
- After the client half-closes, a 1MiB response from an upstream that keeps the
  connection and honors its window reaches the client.
