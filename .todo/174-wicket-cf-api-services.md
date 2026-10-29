Difficulty: Medium

# sluice-wicket-cf-api: capabilities and keyvalue stores as services (`cf bind-service`)

After 173. The marketplace offers what the wicket can grant a route; binding an instance
grants it to the app.

```text
$ cf marketplace
offering   plans                                                 broker
keyvalue   local, shared                                         sluice-wicket
wasi       cli, inherit-network, allow-ip-name-lookup, tcp, udp  sluice-wicket

cf create-service keyvalue local kv
cf bind-service counter kv -c '{"identifier":"sessions"}'   # the identifier defaults to ""
cf restart counter
```

- no broker: the cf-api serves the offerings itself (broker `sluice-wicket`). The plans
  follow the wicket's listing: `capabilities` (keyvalue aside) and `keyvalue_stores` (172)
- a `wasi` plan grants the capability it is named after; a network plan grants `cli` too
  (sockets are linked only with it; `cli` preopens no directory)
- a `keyvalue` instance is the namespace `cf/<instance guid>` of its plan's store; a binding
  maps its `identifier` to it. Two bindings of one app on one identifier: 422. Renaming the
  instance or moving the app's routes never touches the data (unlike a console route's,
  todo 171)
- a binding applies at the next start, restart or restage, as the rest of the sandbox (the
  CLI's TIP says so); manifest `services:` (a name, or a name and parameters) binds as
  `bind-service` does
- `cf delete-service` is refused while bound, as on CC; it removes the namespace's data: the
  wicket gains `DELETE /api/keyvalue/<store>/<namespace>` (sqlite: the rows and the usage
  accounting; redis: SCAN + DEL of the escaped prefix)
- `VCAP_SERVICES` lists the bindings (credentials: store, namespace, identifier): `cf env`
  shows it, the guest gets it with 175
- instance and binding operations complete at once (their jobs are COMPLETE when answered);
  `cf service` needs `relationships/shared_spaces` (spike of 173)

## Acceptance

- the integration test of 173, extended: `counter` without a binding fails to start, naming
  `wasi=keyvalue`; create, bind, restart: it counts, and keeps counting over a restart;
  unbind, delete-service, a new instance and binding: it counts from 1 again. `relay` is
  denied its socket without an `inherit-network` binding and relays with one (a local TCP
  listener)
