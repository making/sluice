Difficulty: Medium

# Data plane h2: a connection whose first request is a CONNECT is relayed verbatim

Found while working on 071. `ConnectionHeadParser` takes an h2 connection head as a request
only when its first HEADERS carries `:method` and `:path`; a CONNECT (RFC 9113 8.5; the
extended CONNECT of RFC 8441 carries `:path` and is not affected) has none, so the head yields no
request and `DataProxyServer#relay` relays the whole connection verbatim on the route of its
`:authority`, like TLS passthrough. Confirmed in a throwaway run of
`H2DemuxStallTimeoutE2ETest` with the CONNECT on stream 1: the access log shows
`transport=h1`, a `StreamRelay`.

Consequences: every later stream of that connection goes to the CONNECT's route whatever its
authority (no demux, no rerouting, no access control per stream), and the connection keeps
none of the HTTP deadlines -- no idle timeout, no stall timeout (only the verbatim-relay idle
timeout of 070, once done).

## Direction

Take an h2 head whose first block is a CONNECT with `:authority` and no `:path` as an h2
request, so the connection is demuxed like any other; check `Http2DemuxRelay` routes and
logs a CONNECT stream (`requestOf` returns null without `:path`).

## Tests (`sluice-it`; watch each fail first)

- An h2 connection opening with a CONNECT is demuxed: a second stream with another authority
  reaches its own route (`trackedStreams`), and the connection idles out at the idle timeout.
- The CONNECT stream itself is relayed as before (200, data both ways).
