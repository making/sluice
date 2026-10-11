Difficulty: Medium

# h2 demux: honor the upstream's SETTINGS_MAX_CONCURRENT_STREAMS

Found while working on 059. `Http2DemuxRelay.Leg#applySettings` applies only the
upstream's `SETTINGS_INITIAL_WINDOW_SIZE`; the relay opens every client stream of a route
on the route's leg up to its own `MAX_CONCURRENT_STREAMS` (100). An upstream announcing
fewer refuses the excess with `RST_STREAM(REFUSED_STREAM)`, which the relay passes on to
the client.

The wasmlet announces half its `--max-instances` per connection (one with
`--max-instances 2`). Seen by hand (server jar of 1dd85e2 plus the 059 wasmlet, hello.wasm,
`curl --http2-prior-knowledge --parallel --parallel-max 2` of `/stall` twice): the relay
sent both streams on one leg, the wasmlet refused the second, and curl retried it on a
new connection (access log: two `type=req` on conn 13, the retry on conn 14). A client
that does not retry a refused stream sees an error, and one that retries opens
connections the relay could have avoided.

## Direction

Track the upstream's announced limit per leg; a stream beyond it waits in the relay
(its head queued, within the queue budget of 057) until a stream of the leg ends, or is
refused by the relay itself when it cannot wait. Decide which; RFC 9113 5.1.2.

## Tests (`sluice-it`; watch each fail first)

- An upstream announcing `MAX_CONCURRENT_STREAMS` 1: two concurrent client streams on
  one connection both complete over one leg, and the upstream never sees two open
  streams at once (assert on the upstream's peak).
- The limit lowered by a later SETTINGS applies to streams opened after it.
