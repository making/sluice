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
- A header block that does not decode is the connection error COMPRESSION_ERROR (RFC 9113
  4.3) on both engines: legacy answers a first head so in `DataProxyServer`
  (`Head#undecodable`, GOAWAY naming stream 0) and a later one in the relay (GOAWAY naming
  the last stream processed); netty's HPACK decoder raises it natively. The two
  `anUndecodable*` tests of `H2DemuxConnectE2ETest` pin it, including the
  `route=- method=- path=-` request line; netty must log that line too.
- A field the HPACK validation rejects (a connection-specific field) is a stream error
  (netty defers it past the whole block, so the decoder state holds): a later head gets
  RST_STREAM PROTOCOL_ERROR on legacy (pinned); a first head is left unroutable by
  `ConnectionHeadParser` and answered 503 no-route with a GOAWAY unless a catch-all route
  takes it to the relay, which resets it (read from the code, untested) -- netty's codec would reset it in either case.

## Note: force-http1 routes answer 421 (from the legacy fix, 2026-10-13)

- `ForceHttp1MisdirectedE2ETest` (sluice-it) joins the routing subset. An h2 stream whose
  route is `force-http1` (upstream reached over HTTP/1.1 only) is never relayed: a bodiless
  `421` (HEADERS END_STREAM, `content-length: 0`), then RST_STREAM NO_ERROR if the request
  is still being sent; no leg is opened, the connection goes on. Checked after the
  no-route / access-control refusals, for every stream including the connection's first
  (TLS SNI route with h2-only ALPN, h2c prior knowledge). Production cause: browsers
  coalesce a force-http1 host onto an h2 connection of another `*.apps` host; relayed as
  h2 it got 502 and the browser kept reusing the connection.
- The netty engine must do the same: log `type=req ... status=421 reason=force-http1`
  (`AccessLogger.Connection#misdirected`) and count
  `sluice.h2demux.streams.misdirected{reason=force-http1}` (`H2DemuxMeters`).

## Notes from 098 (the hop infrastructure, 2026-10-13)

- `am.ik.sluice.server.proxy.hop`: `Hop` (one netty HTTP/2 endpoint in an
  `EmbeddedChannel` over a `DuplexPipe`, own reader / writer virtual threads, started by
  `build()`) and `Loop` (the lock, the settle after each action, the ticker, the exception
  sink). The netty relay goes in `proxy` and depends on `hop`, never the reverse.
  `TestBridge` (sluice-server tests, same package) is the smallest relay on it: legs
  dialed inside the client listener, DATA forwarded as `retainedSlice` and consumed with
  `hop.newPromise(...)` once written, `addConnectionListener` for stream close.
- A leg's pipe: `DuplexPipe.of(VirtualConnection)` (added in sluice-proto). The seed head
  needs no hop API: `DuplexPipe.of(SequenceInputStream(seed, socketIn), socket)`.
- Settings, header list size (64KiB), validation, `encoderEnforceMaxConcurrentStreams`:
  through `Hop.Builder#codec`; auto refill of the connection window is on by default.
- Close a hop with `closeLocked()` (netty's graceful close: GOAWAY, then close when the
  streams ended or the handler's graceful timeout ran) or `abortLocked()` (now). Never
  `EmbeddedChannel#close`: it cancels every scheduled task of the channel, and keeps
  cancelling later ones (LoopTest pins the graceful timer).
- TLS: `SSLSocket#shutdownOutput` throws; the hop's writer then closes the pipe at the
  close grace instead of half-closing. Untested.
- The sink is called with the loop's lock held: log or count, never block.
- Hop tests run in the surefire execution `hop` (`@Tag("netty-hop")`, paranoid leak
  detector, `LeakCheck`); run them through the reactor, e.g.
  `./mvnw test -pl sluice-proto,sluice-server -Dtest=Hop* -Dsurefire.failIfNoSpecifiedTests=false`.
- netty is pinned to 4.2.19.Final in the root `pom.xml` (4.2.18 turned HTTP/2 header
  value validation on by default).
