Difficulty: Medium

# Server shutdown waits out the gRPC grace; tcp listeners accept during drain

Reproduced on 4b6d024. SIGTERM to exit with connected clients: 30s; with held tcp route
connections: 40s (`drain-grace` 10s + `spring.grpc.server.shutdown.grace-period` 30s).
Every `sluice-it` fork also hits surefire's "kill self fork JVM ... 30 seconds after
System.exit(0)".

Root cause:

- The server never completes the tunnel streams. `ClusterLifecycle.drain()` only sends
  `Drain` and waits; `TunnelSession.close()` / `SessionSender.close()` never call
  `outbound.onCompleted()`. The client keeps the stream open on `Drain` by design
  (`NodeConnection`: "the server drains in-flight connections before it closes the
  stream itself"), so `Server.shutdown()` always runs to the grace timeout and then
  `shutdownNow`s.
- `TcpPortGateway` has no lifecycle (only `AutoCloseable`); its listeners accept and route
  new connections until the sessions are torn down at the very end. `DataProxyServer`
  also accepts during drain. New connections keep `connectionCount()` above 0, so the
  drain wait runs its full grace under steady traffic.

Fix direction:

- After the drain wait, close every session from the server side; make
  `SessionSender.close()` flush and `onCompleted()` the stream
- Stop accepting on the data port and all tcp route ports at drain start (optionally after
  a short delay for load balancer deregistration), so the in-flight count only falls
- Make `TcpPortGateway` a `SmartLifecycle`
- Add a failing E2E first: time from context close to exit with a connected client
- `SameHostUpstreamsE2ETest` starts a client per test but stops only the last one; the
  leaked stream makes the next test class wait out the 30s grace when the cached context
  closes
