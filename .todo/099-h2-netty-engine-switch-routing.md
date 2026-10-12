Difficulty: High

# netty h2 demux, step 2: engine switch, routing and the parameterized suite

Step 2 of the netty h2 demux migration (context and user decisions: 098). Depends on 098.

## Scope

- A server setting selecting the h2 demux engine, legacy or netty, default legacy (name it
  after the existing `sluice.limits.*` / `sluice.*` conventions). `DataProxyServer` builds
  either relay.
- The netty relay feeds the seed head (bytes `DataProxyServer` already read) first and
  keeps the first route resolved once.
- Per-stream routing and leg choice, one leg per route, stream ids kept; rewrite-host
  (edit `Http2Headers`); the no-route / forbidden pages through netty's flow controller;
  TLS-terminated h2. Header list limit 64KiB (netty's default is 8KiB).
- Parameterize the h2 E2E classes in `sluice-it` by engine. A class or method that the
  netty engine does not support yet is excluded for netty only, with the step that adds it
  named in the exclusion; legacy runs everything.

## Tests

- The routing, rewrite-host, page and TLS h2 subsets pass on both engines.
- A test pins that the setting actually selects the netty relay.

## Note: CONNECT and heads that are no request (from the legacy fix, 2026-10-13)

- `H2DemuxConnectE2ETest` (sluice-it) belongs to the routing subset: a connection opening
  with a CONNECT is demuxed (idle / stall timeouts apply); a CONNECT stream is routed and
  access-checked by its `:authority` and logged with it as its path
  (`ConnectionHeadParser.http2RequestOf`); a head that is no request -- no `:method`, no
  `:path` but for a CONNECT, a CONNECT without `:authority` -- gets RST_STREAM
  PROTOCOL_ERROR and reaches no upstream, the connection's first head included.
  `DataProxyServer` demuxes every h2-preface connection (`Head#http2Connection`), not only
  those whose first head is a request.
- netty 4.2.17 `DefaultHttp2ConnectionDecoder#validateRequiredPseudoHeaders` raises the same
  stream errors (CONNECT without `:authority`, else a missing `:method` / `:scheme` /
  `:path`). Differences: it also rejects a missing `:scheme`, which legacy forwards; neither
  rejects a CONNECT carrying `:scheme` / `:path` without `:protocol` (malformed per RFC 9113
  8.5). The seed head must go through the codec so the validation covers the first stream.
- An undecodable header block: legacy resets the stream PROTOCOL_ERROR (its decoder
  swallows every exception, untested); netty makes it the connection error
  COMPRESSION_ERROR (RFC 9113 4.3), which is the correct answer.
