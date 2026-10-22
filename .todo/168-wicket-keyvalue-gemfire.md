Difficulty: High

# wicket-wasi: `gemfire` keyvalue store

The Tanzu GemFire store of the wicket's wasi:keyvalue (166: the `Backend` trait in
`sluice-wicket-wasi/src/keyvalue.rs`, README "Key-value stores"). Do 167 (redis) first:
it settles the networked-store questions (timeouts, connection loss, config shape).

## Design (from 166)

- cargo feature `gemfire`, off in the default image
- the C++ native client via FFI, or the REST API (decide: the native client needs its
  libraries in the image; REST is plain HTTP but slower and coarser)
- a region per store, keys `<namespace>:<key>`; swap = `replace(k, old, new)` /
  `putIfAbsent`; increment as the trait's default swap loop
- blocking calls run on `spawn_blocking`, bounded by the client's own operation timeout (a
  dropped future does not stop a blocking call)

## Acceptance

- the counter example counts on a GemFire region; two wickets sharing it count correctly
- list-keys pages; the namespaces stay apart
