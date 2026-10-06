Difficulty: Low

# Client: multiple upstreams sharing one host silently collapse

`SluiceClientProperties#upstreamMap` keys the host-to-target map by `Upstream#host`, so two
upstreams declared with the same host (e.g. an http route and a tcp route with a listen
port) collapse to the last one. `LocalConnector` (strict mode) then refuses to dial the
dropped target and the server-side relay dies with an empty response. Found while writing
`RoundRobinE2ETest` (worked around by giving the tcp upstream a distinct host).

- Make the dial path resolve per-upstream (key by host + listen-port, or match against the
  declared upstream list instead of a map)
- Decide the intended semantics when the same host is declared twice (error, or both
  routes active) and validate / document it
- Cover with a client-side unit test plus an E2E where one client declares http + tcp
  upstreams on the same host
