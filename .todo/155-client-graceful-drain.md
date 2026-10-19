Difficulty: High

# Client graceful drain on shutdown

Clients are updated by hand on each site (`docker compose`). Stopping a client today
(`TunnelClient#stop`) shuts its node connections down at once: the in-flight virtual connections
are cut, and requests that were already resolved to it fail even when another client serves the
same host. The server has a node-level drain (`DRAIN` frame, `sluice.cluster.drain-grace`); the
client has nothing comparable.

## Goal

On SIGTERM the client withdraws its routes on every node, keeps relaying its in-flight connections
until they finish or a grace period (`sluice.client.drain-grace`, new) runs out, then closes. With a
second client serving the same host, `docker compose up -d` of a new client build loses no request.

## Starting points (verify before designing)

- The server already applies a later `Advertise` as a replacement: `TunnelSession#advertise` runs
  `router.register(clientId, upstreams)` and `tcpRoutes.reconcile(...)` each time. An `Advertise`
  with no upstreams might therefore withdraw every route and unbind the tcp ports while keeping the
  session (and its live connections) up, on servers already deployed. Confirm: route removal,
  port release, the AdvertiseAck, the session staying registered, and what the console shows for a
  session with no routes.
- If a dedicated frame is cleaner (e.g. a client -> server drain), note that the server ignores an
  unknown body (`TunnelSession#handle` default branch), so an old server would simply keep routing
  to the client: the client must then still wait the grace before closing. Decide between the two.
- `TunnelClient` is a `SmartLifecycle`: use `stop(Runnable)` so the drain runs before the context
  closes; check its phase against Spring Boot's graceful shutdown.
- `docker compose stop` sends SIGKILL after 10s by default: document `stop_grace_period` against
  `drain-grace` in the README.

## Failure modes to handle and test

- Grace expires with connections still open: they are closed (as today), logged with a count.
- A node connection drops / the server drains during the client drain: no reconnect, no
  re-advertise.
- The client is the only candidate for a host: new requests get 503 from the moment of withdrawal.
  Decide whether that is acceptable or whether the client keeps its routes until no peer can take
  them; state the choice in the README.
- Long-lived connections (WebSocket, tcp routes) hold the drain for the whole grace.
- Second SIGTERM / SIGINT during the drain: stop at once.
- sluice-wicket-wasi is a client too: decide whether it follows here or in its own todo.

## Acceptance

- sluice-it E2E (extend an existing multi-client test where one fits): two clients serve one host;
  a slow request is in flight on client A; A is stopped. The in-flight request completes, requests
  issued after the stop go to B, none fails. Assert on the access log / counters, not timing.
- Grace expiry closes the remaining connections (pin the grace setting took effect).
- README: client drain, the new property, the compose `stop_grace_period` note.
