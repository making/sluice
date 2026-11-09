Difficulty: High

# sluice server: a client's share full passes its turn (requests in flight)

After 260. Clients each carrying a weight (a wicket's route: its `instances`) share a host by smooth weighted
round robin (`WeightedRoundRobinStrategy`), each node on its own: static shares. A wicket whose share is full
(a wasm route's `instances` x `concurrency` busy, a 503 after the budget; a Firecracker pool at its VMs) still
gets its turn while another has room: its instances cold after a restart, its `--max-instances` held by its
other apps, requests of uneven cost.

## To do

- Measure first: an app over two wasi wickets (`concurrency: 1`, 2 + 1 instances) through a server, a load near
  their capacity, requests of uneven cost, one wicket's instances held by another app; the 503s and the
  latency per wicket with the weights alone. Nothing to build if no share fills while another has room: say so
  in the root README's Scale-out and close this
- Else the fewest in flight per weight, ties by the weighted rotation (nginx's `least_conn` with weights): per
  node, per key and candidate, a request from its pick to its response's end. The virtual connections per
  client do not tell it: an HTTP/1.1 leg stays open idle between requests, an h2 leg carries many streams.
  Counted where each relay ends a request: `ReroutingRelay` (a leg's request sent to its response completed,
  or the leg's end), `NettyHttp2DemuxRelay` (a stream to its close or reset), a connection relayed verbatim
  (TLS passthrough, a tcp route, an upgrade) for its life. Released exactly once on every path, a refused or
  failed one included (a leak skews the spread for good): a ticket from the pick, closed by the relay
- A key not every client of which carries a weight stays the strategy's

## Tests

`RouterTest`: a candidate with more in flight per weight passes its turn, and gets it back once they end. Each
relay's release on its paths (answered, refused, reset, failed, upgraded), in its existing tests.
