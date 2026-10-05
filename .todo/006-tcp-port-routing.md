Difficulty: High

## TCP port routing

Route by listen port instead of connection head: the server opens one listener per
route, and each accepted connection is relayed verbatim (no head parsing, no rewriting).
Enables tunneling arbitrary TCP protocols (ssh, postgres, redis, ...), not just HTTP.

### Spike result (2026-10-05, artefacts/006-tcp-port-routing/TcpRouteSpike.java — run: `java .todo/artefacts/006-tcp-port-routing/TcpRouteSpike.java`)

- Dynamic per-port `ServerSocket` bind/unbind while running works; `SO_REUSEADDR` +
  a `ConcurrentMap<port, ServerSocket>` is enough. Unbind must only close the listener:
  in-flight relayed connections survive it (each runs on its own accepted socket).
  New connects after unbind are refused; immediate re-bind of the same port works.
- The relay is the trivial half of this feature (plain copy + half-close, no parser);
`SocketRelay`/`StreamRelay` apply as-is with an empty prefix.

### The hard part is the route model -- decide before implementing

1. Who declares the public port?
   - server-side config (`sluice.tcp-routes[.port,clientId,host]`): simple, but the
     server must know clients' upstream names; breaks the "client announces everything"
     model used by ADVERTISE.
   - client-declared via ADVERTISE (extend proto `Upstream` with an optional listen
     port): consistent with the existing design; server allocates/binds on registration
     and releases on disconnect. Preferred.
2. Port allocation & conflict: fixed ports from config vs server-assigned from a range
   (`sluice.data-port-range`), published back to the client. Fixed-first is simpler;
   assigned ports need a registry + ADVERTISE response carrying the assigned value.
3. Lifecycle: listener per registered route -- bind on `register`, close on
   `remove`/session end; server restart re-binds from the registry. Handle bind failure
   (port taken) by failing the advertise, not the whole session.
4. Ops: the data port range must be exposed/published (docker -p, firewall) -- document
   in README; this is the main deployment delta vs the single-port design.

### Implementation sketch

- `PortListenerRegistry` (server): mirrors the spike -- bind/unbind, accept loop per
  port, routes resolved through a port-keyed view of `Router`.
- `DataProxyServer` grows a sibling or is generalized: same `StreamRelay` relay with
  no head parse and no rewrite (`preserveHost` is meaningless here).
- `Router`: add `byPort` map alongside `byDomain`; registration/unregistration shared.
- E2E in sluice-it: raw TCP echo upstream, connect to allocated port, assert bidirectional
  echo + half-close; reconnect test (server-side listener restored after client
  re-advertise).
