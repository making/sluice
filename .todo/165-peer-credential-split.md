Difficulty: Medium

# Separate the peer credential from the tunnel token

The control plane accepts one token (`sluice.token`, `TokenValidator`) for every RPC, checked by
`TunnelAuthInterceptor`. Tunnel clients and peer nodes share it: `RouteGatePropagator` sends
`TokenValidator.authorizationHeader()`. So any tunnel client can call `SetRouteGate` and open or
shut any route on any node.

## Scope

- A peer credential, e.g. `sluice.cluster.token` (plus a `-file` variant like `sluice.token-file`).
  `SetRouteGate` accepts only it; `Connect` / `ListNodes` keep the tunnel token.
  `RouteGatePropagator` sends it.
- Rolling update: nodes update one by one while mixed versions run. Decide the transition (e.g.
  `SetRouteGate` also accepts the tunnel token while `sluice.cluster.token` is unset, and the
  receiver logs a WARN), so gate propagation never breaks mid-rollout. Clients need no change.
- Consider moving peer-only RPCs to their own `Cluster` service in the proto; if so, keep the old
  `Tunnel/SetRouteGate` served during the transition.
- `application.properties` (alphabetical, comment above the key), README properties table, the
  cluster section of the docs.

## Acceptance

- `RouteGateE2ETest` (extend): the tunnel token is refused on `SetRouteGate` once the peer token
  is set, the peer token is accepted, propagation between two nodes still works; the transition
  mode accepts the tunnel token.
