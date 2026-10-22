Difficulty: Medium

# wicket-wasi: wasi:keyvalue capability with swappable backends (experimental)

Guests get `wasi:keyvalue@0.2.0-draft2`; the store behind it is the host's choice and
invisible to the guest. Default backend: SQLite, a file on the host. Planned: Redis, Tanzu GemFire.
Local checks run on the wicket itself (`--listen`, no sluice server), not on
`wasmtime serve`. Spike and measurements: `.todo/artefacts/166/` (README there).

Experimental: the interface follows WASI keyvalue as it moves (an async WASI 0.3 version
replaces draft2 once it exists); the wicket's guest interface changes with it.

## Findings

- WASI's newest is `wasi:keyvalue@0.2.0-draft2` (WebAssembly/wasi-keyvalue main `aa972c8`,
  untagged): `store` (bucket get/set/delete/exists/list-keys with a string cursor),
  `atomics` (s64 increment, `cas` resource + `swap`), `batch`, and the exported `watcher`.
  No WASI 0.3 (async) version; asyncifying is only proposed (WebAssembly/wasi-keyvalue#68).
  wasmCloud's `wasmcloud:keyvalue` is not WASI: not used.
- `wasmtime serve -Skeyvalue` (v49 / v50, `wasmtime-wasi-keyvalue`) links only the older
  `0.2.0-draft` (u64 cursor, no cas): a draft2 guest fails there ("matching implementation
  was not found"; prereleases never match each other). Its store is in memory, bucket `""`
  only, and each `open` copies the `-Skeyvalue-in-memory-data` preset, so nothing persists
  across requests. Hence the local mode below instead of serve compatibility.
- keyvalue functions are plain `func`: in a p3 guest the call blocks its instance while the
  host awaits the backend (bindgen `async`, fiber). Measured: other tasks of the same
  instance (e.g. a streaming response body) stall for the call; other requests (other
  stores) do not. Lifted only by an async WASI version.
- Dropping the store mid-call (the budget watchdog `supervise`) is clean.
- wasmtime 49 -> 50: the wicket compiles unchanged and `cargo test` passes (154 tests, run
  on a scratch copy).

## Design

### Local mode (`--listen`)

- `--listen <addr>` (`SLUICE_WICKET_WASI_LISTEN`, config `listen:`) serves the routes over
  plain TCP, http/1.1 and h2c sniffed as on the tunnel, with the same handlers: sandbox,
  pools, budgets, reload, keyvalue. `--server` becomes optional with it; both together
  serve the tunnel and the listener.
- Routing by the request's `Host` (port stripped); with a single route every request goes
  to it (`curl localhost:8080/` works as with `wasmtime serve`). Unknown host -> 404.
- `wasm_host::serve` takes the connection IO generically: `TunnelIo` keeps its
  `failed` / `ending` hooks, a `TcpStream` has none. The per-connection `Binding` (target
  fixed at CONNECT) becomes a per-request lookup for the listener.
- README "Run": the local loop (`--listen 127.0.0.1:8080 --wasm app.wasm?wasi=keyvalue`).

### Guest view

- `wasi=keyvalue` (named as `wasmtime serve -Skeyvalue`) links draft2's `store`, `atomics`,
  `batch`. Without it a component importing them fails at startup with the
  `wasi=keyvalue` hint, as for `cli`. A `0.2.0-draft` component does not link (README).
- `store.open(identifier)` resolves through the route's bucket map; an unmapped identifier
  is `no-such-store`. Default map: `""` -> the default store, namespace = the route's host.
  Routes are isolated unless the config maps two of them to one namespace.

### Config

```yaml
keyvalue:                       # process-wide; changes apply on restart (as other settings)
  default: local                # optional when there is one store
  stores:
    local:
      type: sqlite
      path: keyvalue.db         # relative to the config file
    shared:
      type: redis
      url: redis://redis:6379/0
routes:
  - host: demo.local
    component: app.wasm
    sandbox:
      wasi: [keyvalue]
      keyvalue:                 # identifier -> store[/namespace]; reloaded with the routes
        "": local               # namespace defaults to the host
        sessions: shared/demo-sessions
```

- No `keyvalue` section: one `sqlite` store at `keyvalue.db` (next to the config file, the
  working directory without one); `--keyvalue-file` / `SLUICE_WICKET_WASI_KEYVALUE_FILE`
  move it. The image needs a volume for it (README).
- `--wasm ...?wasi=keyvalue` gets the default map only; custom maps need the config file.
- The bucket map is part of the sandbox, hence of the advertised target query (a changed
  map is a new target, as a changed sandbox). `Sandbox` stops being `Copy`.
- A map naming an unknown store rejects the reload / startup.

### Host layering

- `src/keyvalue.rs`: the draft2 bindings, the `Bucket` / `Cas` resources and the
  identifier -> (backend, namespace) resolution. `Ctx` gains the route's map
  (`Option<Arc<..>>`); the linker / compile cache key gains `keyvalue` beside `cli`. A
  WASI version change touches this file only.
- `src/keyvalue/<backend>.rs`: one module per backend behind an object-safe
  `#[async_trait] trait Backend: Send + Sync` taking the resolved namespace, never the guest
  identifier, and shaped by no WIT version (spike: `artefacts/166/host/src/main.rs`):
  - required: `get`, `set`, `delete`, `list_keys(cursor: Option<String>) -> (keys, next)`
    (cursor opaque, backend-defined), `swap(key, expected: Option<bytes>, value) -> Done |
    Stale(current)`
  - defaulted, overridden where the backend has a primitive: `exists` (get),
    `increment` (swap loop), `get_many` / `set_many` / `delete_many` (per key)
- CAS = value equality: `cas.new` reads the value into the handle, `swap` writes only while
  the key still holds it; a lost race returns `cas-failed` with a handle carrying the
  current value. ABA is invisible to a guest that only sees values (SQLite immediate transaction,
  Redis script, GemFire `replace(k, old, new)` / `putIfAbsent`).
- increment: the value is the decimal ASCII of an i64 (wasmtime's in-memory and Redis
  `INCRBY` alike); a non-integer value is `other`.
- Backend errors map to `other(message)` (draft2 has no finer case); logged with the route.
- Limits per store: `max-key-bytes`, `max-value-bytes` (rejected with `other`). A guest
  must not fill the host disk through the default store.
- Pooled units: a bucket / cas handle kept across requests leaves the table dirty and
  retires the unit (existing rule); guests open per request (cheap: a table entry).

### Backends

- `sqlite` (default; rusqlite `bundled`): one table `kv(bucket, key, value)` keyed by
  `(bucket, key)`, `WITHOUT ROWID`; WAL, `synchronous=NORMAL` (a power loss may drop the
  last commits, never corrupts); cursor = last key returned (`key > ? ORDER BY key`);
  increment / swap in a `BEGIN IMMEDIATE` transaction; blocking calls on `spawn_blocking`.
  Chosen over redb (spike-compared, `artefacts/166/README.md`): a file format stable for
  decades (redb dropped its v2 format in 3.0 and has had a major per ~6 months), the
  `sqlite3` CLI to inspect / edit a local store, and several processes on one host may
  share the file (redb refuses a second opener). The build already compiles C (`ring`),
  so `bundled` adds no toolchain. Init must retry `SQLITE_BUSY`: switching a new file to
  WAL races a second opener without the busy handler (seen in the spike).
- Sharing across hosts is a networked backend's job.
- `redis` (cargo feature, follow-up): keys `<prefix><namespace>:<key>`; `INCRBY`; swap as a
  Lua script (`SET NX` for an absent expected value); `SCAN MATCH` for list-keys;
  pipelines for batch.
- `gemfire` (cargo feature, follow-up, off in the default image): the C++ native client via
  FFI, or the REST API. Blocking calls run on `spawn_blocking`, bounded by the backend's own
  operation timeout (a dropped future does not stop a blocking call).
- Not in scope: the `watcher` export (needs a change source: Redis keyspace
  notifications, GemFire CQ).

## Steps

1. wasmtime 49 -> 50 (`Cargo.toml`, the CLAUDE.md / README "wasmtime 49" mentions).
2. Local mode (`--listen`): generic IO in `wasm_host::serve`, Host routing, config / flag.
3. Vendor the draft2 WIT (`artefacts/166/wit/keyvalue`) under `wit/deps/`; `keyvalue`
   capability, bucket map, `keyvalue` config section and flag.
4. `keyvalue.rs` + the `sqlite` backend.
5. `examples/counter` guest (README: needs `wasi=keyvalue`; the local-mode command line).
6. README: Run (local mode), Capabilities (keyvalue, experimental), Configuration file,
   image volume; `.kb` if behavior detail grows.

## Acceptance

- `--listen` alone (no `--server`) serves `examples/hello` and `examples/counter`; a sole
  route answers any Host, several are routed by Host, an unknown one is 404.
- On a `wasi=keyvalue` route the counter survives a restart.
- Two routes with the default map do not see each other's keys; a shared namespace does.
- Unmapped identifier -> `no-such-store`; a component importing keyvalue on a route
  without the capability fails at startup with the hint.
- cas retry loop under contention (two concurrent requests) ends with the right count.
- list-keys pages through more keys than one page; oversized key / value rejected;
  non-integer increment is `other`.
- Two wickets on one host sharing a `sqlite` file count correctly (increment and cas).
- Tests extend the existing wasm_host / config tests where they reach the behavior.
