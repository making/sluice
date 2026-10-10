Difficulty: High

# Per-request re-routing: HTTP/2 stream demux

Sibling of 037 (same problem statement; read it first). HTTP/2 multiplexes concurrent
streams with distinct `:authority` values on one connection -- including Chrome's connection
coalescing, the confirmed production symptom. Unlike h1 keep-alive, streams interleave, so
the relay must demux per stream:

- Parse the h2 preface / HEADERS on the relayed stream far enough to read `:authority` per
  stream (`Http2Rewriter` already decodes HPACK headers for rewriting -- reuse its parser
  state machine; rewrite-host=false connections currently skip parsing entirely, decide
  whether the router needs it regardless)
- Per stream: resolve the route at HEADERS time and relay that stream's frames to a virtual
  connection of the matching session. DATA frames carry no stream->route state, so the demux
  must track stream id -> virtual connection for the life of the stream
- Flow control: the connection-level WINDOW_UPDATE semantics span all streams -- demuxing
  streams of one connection to different virtual connections must not deadlock the shared
  flow-control window; study how the per-window accounting maps before writing code
- Trailers, RST_STREAM, GOAWAY, and half-close mapping per stream to the tunnel CLOSE frame
- Access log per stream (`type=req` with route) as in 037
- E2E in `sluice-it`: one h2 connection, concurrent streams to two different authorities,
  each stream's response from its own upstream (watch it fail first)
- Degradation fallback is acceptable where the demux cannot be proven (e.g. answer
  non-matching authorities with a redirect or error rather than silently misrouting); silent
  misroute is the bug being fixed -- do not ship a variant that still misroutes
- h2 upgrade / prior-knowledge entry paths both go through `ConnectionHeadParser`; keep one
  demux behind both
