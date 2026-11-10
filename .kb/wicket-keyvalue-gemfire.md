# The gemfire store on a production-like cluster

What a wicket's `gemfire` store needs to reach a Tanzu GemFire cluster set up as the production
one, and what such a cluster was seen to do. Every address, name, path and secret here is a
placeholder (`192.0.2.0/24` is a documentation range, `.example` a reserved domain): the real ones
stay out of the repository.

## The cluster

- Three hosts, `192.0.2.20`, `192.0.2.30` and `192.0.2.40`, a locator and a server on each:
  Tanzu GemFire 10.3 with Tanzu GemFire for gRPC 1.0, its `.gfm` in every member's `extensions/`.
  The members take it by restarting all at once: one without it fails one with it to start
  (`Unknown DataSerializableFixedID: 2320`).
- Each member's gRPC address, two JVM properties: `gemfire.grpc.bind-address` and
  `gemfire.grpc.advertised-address`, `192.0.2.20:50051` for a locator (its xDS),
  `192.0.2.20:50052` for a server (the `GemFire` service). The locators name the servers by their
  advertised addresses, the hosts' IPs: a wicket must reach those too. The members' names
  (`*.gemfire.example`) lead to a front end forwarding neither port.
- TLS: `ssl-enabled-components=all` covers `grpc`, with the members' keystore and truststore. A
  locator's port takes no client certificate; a server's requires one.
- One CA; each member's certificate names its IP address and `*.gemfire.example`.
- No `security-manager`: no token.
- The region, made with gfsh (the store makes none): `create region --name=sluice
  --type=PARTITION_PERSISTENT`. A `PARTITION` region loses its entries when the cluster restarts.

## The store

```yaml
keyvalue:
  stores:
    gemfire:
      type: gemfire
      locators: [grpcs://192.0.2.20:50051, grpcs://192.0.2.30:50051, grpcs://192.0.2.40:50051]
      region: sluice
      ca-cert: /etc/sluice/gemfire/ca.pem
      client-cert: /etc/sluice/gemfire/client.pem
      client-key: /etc/sluice/gemfire/client.key

routes:
  - host: counter.example
    component: counter.wasm
    sandbox:
      wasi: [keyvalue]
      keyvalue:
        "": gemfire             # the namespace defaults to the host
```

- `locators`: all three, the next asked when one fails; the store follows the servers they name.
  `servers: [grpcs://192.0.2.20:50052, grpcs://192.0.2.30:50052, grpcs://192.0.2.40:50052]`
  names the servers instead, no locator asked.
- No `server-name`: the certificates name the addresses. A wicket reaching the members by other
  addresses (through a NAT) verifies a name the certificates name:
  `server-name: grpc.gemfire.example`.
- `ca-cert`: the cluster's CA. `client-cert` / `client-key`: a certificate the servers' truststore
  trusts and its key, PEM, the key unencrypted: one the CA issued for the wickets, or a member's,
  out of its PKCS#12 keystore:

  ```bash
  openssl pkcs12 -in member.p12 -passin env:P12_PASSWORD -clcerts -nokeys -out client.pem
  openssl pkcs12 -in member.p12 -passin env:P12_PASSWORD -nocerts -nodes -out client.key
  ```

- No `token`; `timeout` the default, 5s.

## Checking it

Two wickets count on a namespace no earlier check used, then a third with no route purges it (a
console refuses a namespace one of its routes maps). `counter.wasm` is the wicket's
`examples/counter`; `check.yaml` the config above with `"": gemfire/${NS}`, `purge.yaml` its store
alone with `routes: []`.

```bash
export NS=check-$(date +%s)
sluice-wicket-runtime-wasi --listen 127.0.0.1:18080 --config check.yaml &
sluice-wicket-runtime-wasi --listen 127.0.0.1:18081 --config check.yaml &
curl -H 'host: counter.example' http://127.0.0.1:18080/   # hits 1
curl -H 'host: counter.example' http://127.0.0.1:18081/   # hits 2
sluice-wicket-runtime-wasi --listen 127.0.0.1:18082 --config purge.yaml \
  --console 127.0.0.1:19090 --console-token t0ken &
curl -X DELETE -H 'authorization: Bearer t0ken' http://127.0.0.1:19090/api/keyvalue/gemfire/$NS
# {"store":"gemfire","namespace":"check-...","keys":1}; a second time "keys":0
```

## What the cluster does

What the store and its mock rely on:

- `ReplaceIf` compares bytes by their content (an expected value equal to the stored one, sent anew,
  replaces); an expected `bytes` value never equals a stored `string` one. An increment over a value
  another client stored as a string would retry forever, so a value of another type fails the call
  (`'k' holds a string value, not bytes`) instead. `PutIfAbsent` over a key there and `ReplaceIf` on
  a key absent answer false; an empty `bytes` value is stored and compared as any other.
- A region not there fails every call `FAILED_PRECONDITION` (`Region not found: <name>`), never
  `NOT_FOUND`. `GetStream` leaves the keys absent out; `KeySetStream` streams the key set in
  batches.
- A locator answers the EDS subscription to `gemfire_grpc` with one `ClusterLoadAssignment` naming
  every server, of health `UNKNOWN`, and keeps the ADS stream open after the ACK. Its port takes
  no client certificate (as the documentation says), the servers' require one: without it, TLS 1.3
  completes on the client's side and the first read fails (`received fatal alert:
  CertificateRequired`), a failure after the request may have left.
- Two wickets counting on one namespace at once (20 increments and 20 cas, through the locators or
  the servers named) end exact; 250 keys more list as 252 in 3 pages; a purge deletes 252, a second
  none.
