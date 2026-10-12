Difficulty: High

# Feasibility: hand the h2 demux's framing and stream state to netty's HTTP/2 codec

A study, not a migration. Decide with the user whether to migrate before working the open
h2 demux todos (066, 073, 075, 078, 080, 085, 088, 089, 090, 091, 092, 093): a migration
would change or subsume most of them.

## Problem

`Http2DemuxRelay` (about 4,300 lines) hand-writes HTTP/2: frame parsing and writing, stream
state (RFC 9113 5.1), flow control, SETTINGS, GOAWAY, and the error scope. netty
(`netty-codec-http2`, already a dependency) is used only for HPACK. Each fix exposes the
next unvalidated path: 082, 088, 089 and 092 are validation that a mature codec does, and
090 and 093 are flow-control accounting. The hand-written state machine keeps producing todos.

## Questions to answer

- **Layer:** which netty layer fits?
  - `Http2FrameReader` / `Http2FrameWriter` only (validated framing; keep our state).
  - Plus `DefaultHttp2Connection` and the flow controllers (stream state and windows).
  - Or the full `Http2ConnectionHandler` / `Http2FrameCodec` / `Http2MultiplexHandler` in a
    channel pipeline.
- **I/O:** I/O model fit. The relay runs blocking I/O on virtual threads. The client side is
  a raw socket handed over by `DataProxyServer` after head inspection (TLS terminated or
  plain). Each upstream leg is a tunnel `VirtualConnection`, not a socket. netty pipelines
  need a channel: an `EmbeddedChannel` driven by our threads, or a custom `Channel` over
  `VirtualConnection`, or moving the data plane onto netty event loops. Cost and threading
  risk of each.
- **Behaviours to keep:** what in the current behaviour netty would have to support or let
  us plug in:
  - Stream-by-stream rerouting to different legs and routes, and leg retirement.
  - The shared queue budget, window withholding and `refuseWaiting` (083).
  - The stall timeout (071), the idle and keep-alive timeouts.
  - The upstream error scope (081): a 502 before the head, `RST_STREAM` after it,
    `REFUSED_STREAM`, GOAWAY handling.
  - Upstream `MAX_CONCURRENT_STREAMS` waiting (067), the client-face SETTINGS, header
    re-encoding, and the metrics.
- **Alternatives:** other libraries (e.g. Jetty's low-level http2), or keeping our state
  machine behind a validated netty frame layer. Native image impact.
- **Acceptance suite:** the `sluice-it` `H2Demux*E2ETest` classes and the other h2 E2E
  tests are the behavioural contract. List any test that encodes behaviour netty cannot
  give.

## Deliverable

- A short design note in this file, with a recommendation (migrate / partial / stay) and a
  migration plan in steps that keep `sluice-it` green.
- Per open h2 todo: whether the migration subsumes it, changes it, or leaves it.
- A spike under `.todo/artefacts/096/` that proves the riskiest point (likely: one client
  connection demuxed to two legs over `VirtualConnection` with netty doing the framing and
  flow control), with a test.
- The user decides; file the migration todos only after that.
