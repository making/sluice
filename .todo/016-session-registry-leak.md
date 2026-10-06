Difficulty: Low

# Closed tunnel sessions stay in SessionRegistry

`SessionRegistry.remove` has no production caller (only `TcpPortGatewayPreemptTest`).
`TunnelSession.close()` releases routes, tcp listeners and virtual connections, but the
session stays registered until the same client id connects again. Present since the
initial import.

Observed: two clients stopped; the server logged `closing session for client ...` and
unbound their ports, yet the console kept listing them minutes later.

Consequences:

- the console and the health `clients` detail count disconnected clients
- clients without `sluice.client.id` get a random id per process, so every restart leaks
  one session (with its closed `SessionSender`) for the lifetime of the server
- `MembershipBroadcaster` / `ClusterLifecycle.drain` keep sending to closed senders
- `TcpPortGateway.reconcile` treats a gone owner as live (`sessions.find(...)`)

Fix: `close()` removes itself from the registry (`remove(clientId, session)` keeps a
newer session of the same id). Failing test first: connect and stop a client, assert
`SessionRegistry.count()` returns to 0 (E2E) / registry is empty after `close()` (unit).
