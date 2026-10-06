Difficulty: High

# Scale-out: client fan-out to every server node

Run sluice-server as N nodes. Every client keeps one tunnel stream to every node, so each node holds
the full route table locally: no shared store, no inter-node hop.

Requirements on the deployment (independent of the front end in use):

- each node's control plane is reachable from clients at its own address (per-node URL)
- data plane connections may land on any node (plain L4 balancing is enough)

Chosen over shared-registry forwarding (needs a store + an extra hop) and domain sharding (the front
end would need a domain -> node table at runtime, and plaintext HTTP would need L7 parsing there).
If connection count (clients x nodes) ever becomes the bottleneck, move to shared-registry forwarding;
node id, node list and drain below carry over.

## Single-node assumptions to remove

- `Router` / `SessionRegistry`: in-memory only; a data plane connection reaches only clients connected to the same node
- client: one `sluice.server-url`, one stream; `x-sluice-id` is a fresh UUID per session (`TunnelClient.java:260`)
- `TcpPortGateway`: port conflicts ("held by another connected client") are resolved per node
- `Router.lookup`: first registered target wins, so with several clients on one domain each node may pick a different one
- `TunnelHealthIndicator`: DOWN with zero clients, so a new node never enters a load balancer rotation
- `TokenValidator`: the auto-generated token differs per node

## Work items

1. Node identity: `sluice.node.id` (default hostname) and `sluice.node.public-url`
   (e.g. `grpcs://sluice-0.tunnel.example.com`). Tag logs and metrics with the node id.
2. Membership on the server, behind a provider interface: static list, and DNS SRV (e.g. a k8s
   headless Service; no k8s API / RBAC) mapping each node name to its public URL via a template
   (`grpcs://{node}.tunnel.example.com`). Poll; on change push the new list to connected clients.
3. Proto:
   - unary `ListNodes` RPC (authenticated) returning `[{node_id, public_url}]` plus a membership version
   - `AdvertiseAck.node_id`, so the client can detect landing on a node it already has a stream to
   - server -> client frame for membership updates, and a `Drain` frame
4. Client:
   - process-stable instance id (`sluice.client.id`, default random once per process) sent on every stream; required for the deterministic tie-break in item 7
   - `sluice.server-url` becomes the bootstrap (accept several): `ListNodes`, then one stream per node via its public URL; open/close streams on membership updates
   - single-node compatibility: a server without clustering returns itself with no public URL -> keep using the bootstrap URL
   - refactor `TunnelClient` state (`connections`, `connected`) into a per-stream session; backoff per node
   - health: UP while at least one stream is up, per-node status as details; metrics tagged by node
5. Server health: split liveness / readiness (health groups). Readiness is false during a warm-up after
   start (`sluice.cluster.warmup`, longer than membership push + client connect) and while draining.
   Client count becomes a detail, not the status.
6. Drain (SmartLifecycle stop / preStop): readiness false, send `Drain`, stop accepting data plane
   connections, wait for in-flight virtual connections up to the grace period, then close streams.
   The client keeps other nodes' streams and reconnects to the drained node only once it is listed again.
7. Cross-node route consistency (decide):
   - domain served by several clients: deterministic choice (order by client instance id) or balance across live sessions
   - tcp listen port claimed by several clients: deterministic tie-break with preemption (the winner arriving later takes the port over on every node), or static port ownership in server config
8. Cluster mode refuses to start without an explicit `sluice.token` / `sluice.token-file`.
9. Server gRPC TLS, so a front end can route per node by SNI without terminating TLS: verify `spring.grpc.server.ssl.bundle` works with the
   client's `grpcs://` (ALPN h2) and cover it with an E2E test.
10. Tests (sluice-it): 2 server nodes + 1 client in one JVM -- request via either node, stop one node
    and the other keeps serving, add a node and the client connects after the membership push, a tcp
    port route works on both nodes, drain lets in-flight connections finish.
11. Docs: README cluster section stating the two deployment requirements; the k8s + HAProxy example below as one sample.

Already in place: a data plane connection with no route gets an immediate 503 / close
(`DataProxyServer#relay`), which is the right answer during warm-up gaps; a front end cannot
re-route after sluice has read the head.

## Reference: example deployment (k8s + in-cluster HAProxy)

One way to meet the requirements; nothing above depends on it. HAProxy runs in the cluster behind a
`type: LoadBalancer` Service as the only entry point.

```
client / browser
  -> LoadBalancer Service (HAProxy, externalTrafficPolicy: Local)
     -> HAProxy (mode tcp, SNI routing)
        sluice-N.tunnel.example.com -> sluice-N.sluice-headless:8001  (control plane, per pod)
        tunnel.example.com          -> any pod :8001                  (bootstrap / ListNodes)
        other SNI / plaintext       -> every pod :8000                (data plane)
        existing apps' SNI          -> Traefik
```

- StatefulSet `serviceName: sluice-headless`; headless Service (`clusterIP: None`) with named ports `grpc` (8001), `data` (8000), `management` (8081). `podManagementPolicy: Parallel`, no PVC.
- `preStop` triggers drain; `terminationGracePeriodSeconds` covers in-flight connections.
- The HAProxy LoadBalancer Service must list 443, 80 and every port of `sluice.tcp-port-range` one by one (Service ports have no ranges).
- A ConfigMap change restarts HAProxy and drops tunnel streams (clients reconnect); run several HAProxy replicas with rolling updates and keep the config static (`server-template`, optionally `do-resolve`).

## Reference: HAProxy config

```
global
    log stdout format raw local0

defaults
    mode tcp
    log global
    timeout connect 5s
    timeout client 1h
    timeout server 1h

resolvers k8s
    parse-resolv-conf
    hold valid 10s

frontend fe_tls
    bind :443
    tcp-request inspect-delay 5s
    tcp-request content accept if { req.ssl_hello_type 1 }
    use_backend be_sluice_ctrl     if { req.ssl_sni -m reg ^sluice-[0-9]+\.tunnel\.example\.com$ }
    use_backend be_sluice_ctrl_any if { req.ssl_sni -i tunnel.example.com }
    use_backend be_traefik         if { req.ssl_sni -i app1.example.com }
    default_backend be_sluice_data

frontend fe_plain
    bind :80
    default_backend be_sluice_data

frontend fe_tcp_routes
    bind :9000-9010
    default_backend be_sluice_tcp

# Control plane, one server per ordinal; pre-define up to the max replica count
# (init-addr none lets HAProxy start while a pod does not exist yet).
backend be_sluice_ctrl
    use-server sluice-0 if { req.ssl_sni -i sluice-0.tunnel.example.com }
    use-server sluice-1 if { req.ssl_sni -i sluice-1.tunnel.example.com }
    use-server sluice-2 if { req.ssl_sni -i sluice-2.tunnel.example.com }
    server sluice-0 sluice-0.sluice-headless.<ns>.svc.cluster.local:8001 check resolvers k8s init-addr none
    server sluice-1 sluice-1.sluice-headless.<ns>.svc.cluster.local:8001 check resolvers k8s init-addr none
    server sluice-2 sluice-2.sluice-headless.<ns>.svc.cluster.local:8001 check resolvers k8s init-addr none

backend be_sluice_ctrl_any
    balance roundrobin
    server-template sluice 10 _grpc._tcp.sluice-headless.<ns>.svc.cluster.local resolvers k8s check init-addr none

# Data plane, follows the pod count via SRV records. Health check against the readiness group
# once item 5 is done; add send-proxy-v2 once the data plane parses the PROXY protocol.
backend be_sluice_data
    balance leastconn
    option httpchk GET /actuator/health/readiness
    server-template sluice 10 _data._tcp.sluice-headless.<ns>.svc.cluster.local resolvers k8s check port 8081 init-addr none

# TCP port routes: no port on the server line = same port the client connected to.
backend be_sluice_tcp
    balance leastconn
    option httpchk GET /actuator/health/readiness
    server-template sluice 10 sluice-headless.<ns>.svc.cluster.local resolvers k8s check port 8081 init-addr none

backend be_traefik
    server traefik traefik.<traefik-ns>.svc.cluster.local:443 check resolvers k8s init-addr none
```

Alternative for `be_sluice_ctrl` without per-ordinal lines (no config change on scale-out). The regex
must stay strict: a loose one lets outsiders resolve arbitrary in-cluster names.

```
frontend fe_tls
    ...
    tcp-request content set-var(txn.pod) req.ssl_sni,regsub(^(sluice-[0-9]+)\.tunnel\.example\.com$,\1.sluice-headless.<ns>.svc.cluster.local) if { req.ssl_sni -m reg ^sluice-[0-9]+\.tunnel\.example\.com$ }
    tcp-request content do-resolve(txn.ip,k8s,ipv4) var(txn.pod) if { var(txn.pod) -m found }
    use_backend be_sluice_ctrl_dyn if { var(txn.ip) -m found }

backend be_sluice_ctrl_dyn
    tcp-request content set-dst var(txn.ip)
    tcp-request content set-dst-port int(8001)
    server dyn 0.0.0.0:0
```

The HAProxy directives (`server-template` with SRV, `do-resolve` / `set-dst` in tcp mode, port-range
`bind` with portless `server`) need verifying against the deployed HAProxy version.
