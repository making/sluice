Difficulty: High

# Per-request re-routing: HTTP/1.1 keep-alive

The data plane resolves the route once per TCP connection from the first request head
(`DataProxyServer#relay` -> `Router#lookup`) and relays the rest byte-blind. Any party that
multiplexes distinct Hosts over one connection is pinned to the first host's route. Confirmed
in production (2026-10-10): Chrome HTTP/2 connection coalescing (same IP, one certificate
whose SANs cover every hosted name) makes every later navigation land on the first visited
app; a console host served an app's 404 JSON body.

This item: re-route HTTP/1.1 keep-alive connections per request.

- Detect the authority of every request head on the relayed stream. `HostRewritingPipe`
  already parses the request stream incrementally (`Http1Rewriter`) -- extend or sibling it
  into a routing variant; keep the rewriting behavior intact
- When the authority's route differs from the connection's: open a new virtual connection to
  the new route's session/address and switch the outbound leg at the request boundary
  (requests on one keep-alive connection are strictly sequential, so no demux is needed;
  the in-flight response must complete before the switch)
- Access log: a `type=req` per request head with its own route, instead of one per connection
- The connection-close / half-close bookkeeping (`session.remove`, relay `onComplete`) must
  hold per virtual connection segment
- E2E in `sluice-it`: keep-alive connection where the second request carries a different Host
  routed to the other client's upstream (watch the test fail first); pin that both responses
  come from the correct upstreams and the connection survives
- Failure paths: mid-head disconnect, upstream route disappearing between requests
  (`Router#lookup` empty -> error response per `ErrorResponse`), session gone -> same
- h2 variant is sibling todo 038

Observed on the live deployment: app A, then app B, then the console host -- the console
rendered app B's root JSON; the data plane saw no new connection for it (it rode the shared
h2 connection, unlogged because only the first head is `type=req`-logged).
