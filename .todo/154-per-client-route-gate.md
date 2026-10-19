Difficulty: Medium

# Per-client route gate

Route gates (87b3ea4, drawing 5bd1fd8) work per route key: the domain, the host pattern, `*`,
`tcp:<port>`. Shutting `demo.local` drops every candidate serving it at once
(`Router#opened` returns null before load balancing), so the request falls through to the
pattern / catch-all. There is no way to take one client out of a key that several clients serve.

## Why

Incident isolation: a client stays connected while its upstream misbehaves. Today the only lever is
stopping that client, which also drops every other route it serves. A gate on (route key, client id)
removes just that candidate; load balancing moves the traffic to the remaining ones.

Planned maintenance is covered by 155 (client graceful drain), not by this.

## Scope

- State: `RouteGate` / `RouteGateStore` hold a gate per (route key, client id) next to the per-key
  gate. Decide the key form (e.g. `demo.local@client-a`) and the startup property
  (`sluice.route.gate.[<key>]` or a sibling map).
- Routing: `Router` filters the candidate list before `httpStrategy.pick` / `tcpStrategy.pick`.
  Every candidate gated = the key is gated (falls through / 503 / tcp close), same as the per-key
  gate. Check the strategies with state (round robin and the like) over a filtered list, and that
  "smallest client id" stays deterministic across nodes.
- `resolve` (console lookup, routes table Serving / Standby, outlet "Serves ..." in the drawing)
  must agree with `lookup`: the next candidate shows as Serving.
- Cluster: `SetRouteGateRequest` gains the client id; propagation as for the per-key gate.
- Console: a toggle per candidate row in the routes table; the shut candidate row is marked.
- Connections already relayed to the gated client: same semantics as the per-key gate (state which,
  and pin it in a test). Keep-alive rerouting (per-request route resolution) must not send a later
  request on the connection to the gated client.

## Acceptance

- `RouterTest`: two candidates, gate one -> the other is picked; gate both -> fall-through.
- `RouteGateE2ETest` (extend, do not add a class): two clients on one host, gate one from node 1's
  console -> both nodes route to the other; reopen -> back to the strategy's pick.
- `ConsoleE2ETest`: the candidate toggle round-trips and the Serving / Standby text follows.
- Native image: register any new console view record in `reflect-config.json`.
- README: the gate section describes both granularities.
