# 166 spike: wasi:keyvalue on wasmtime 50

- `wit/keyvalue/`: `wasi:keyvalue@0.2.0-draft2`, WebAssembly/wasi-keyvalue main `aa972c8`
  (untagged; the only tag is the older `v0.2.0-draft`)
- `wit/keyvalue-draft/`: `wasi:keyvalue@0.2.0-draft`, the version `wasmtime serve -Skeyvalue`
  links (wasmtime v49 / v50 `crates/wasi-keyvalue`, identical to the tag)
- `host/`: wasmtime 50 host; draft2 bindgen (`async | trappable`), the `Backend` trait,
  a redb backend and a SQLite one (`src/sqlite.rs`, `KV_BACKEND=sqlite`), a `Slow` wrapper (300ms reads) standing in for network latency
- `guest/`: wasi:http 0.3 handler (wasm32-wasip3, the wicket examples' nightly pin) driving
  every draft2 function
- `guest-draft/`: the same on `0.2.0-draft` (the version `wasmtime serve -Skeyvalue` links),
  checked against serve before 166 settled on draft2 + the wicket's local mode; the host links
  both versions (`host/src/draft.rs`)

```text
(cd guest && cargo build --release)
(cd host && cargo build --release)
host/target/release/kv-spike-host guest/target/wasm32-wasip3/release/kv_spike_guest.wasm \
    /tmp/kv.redb incr/_/hits set/_/a=alpha get/_/a list/_ cas/_/n batch/_ get/nope/a \
    concurrent/slow '!get/slow/a' 'concurrent/slow+concurrent/_'
```

Path = `op/bucket/key[=body]`, `_` the bucket `""`; `!` cancels the request after 100ms;
`a+b` runs two requests at once on separate stores.

Results (2026-10-22):

| check | result |
|---|---|
| get / set / delete / exists, list-keys paging (page 2), increment, cas retry loop, batch, unknown bucket -> `no-such-store` | as specified |
| persistence across a process restart | `hits` continued 2 -> 3 |
| resource table after each request | empty (pooled units stay warm) |
| guest task progress while a keyvalue call is pending | **stalls**: the whole instance waits (tick gap 52ms -> 323ms) |
| another request (another store) meanwhile | unaffected |
| request dropped mid-call (budget watchdog) | clean; the next request works |
| redb write commit | ~4ms each (fsync per transaction, macOS) |

`0.2.0-draft` guest (2026-10-22):

```text
wasmtime serve -Skeyvalue -Skeyvalue-in-memory-data=a=preset \
    guest-draft/target/wasm32-wasip3/release/kv_spike_guest_draft.wasm
curl 127.0.0.1:8080/get/_/a
```

| check | wasmtime serve 49 | spike host (redb) |
|---|---|---|
| links and serves (p3 guest, no other flag) | yes | yes |
| preset / stored value read | `preset` | `alpha` |
| `incr` twice, then after a restart | 1, 1 (nothing persists across requests) | 1, 2, then 3 |
| set, then get in the next request | `None` | the value |
| list-keys | all keys, cursor none | page of 2, cursor `Some(2)` |
| unknown bucket | `no-such-store` | `no-such-store` |

redb vs SQLite (same draft2 guest, 2026-10-22):

| check | redb 4.4 | SQLite (rusqlite 0.40 bundled, WAL, synchronous=NORMAL) |
|---|---|---|
| every operation, restart persistence | ok | ok |
| 3 concurrent cas on one key | -- | 1, 2, 3 attempts; final value right |
| write commit | ~4ms (fsync per commit) | <1ms |
| second process opening the file | `DatabaseAlreadyOpen` | shares it: 2 x (50 incr + 50 cas) -> 100 / 100, cas retries seen |
| two processes creating a new file at once | -- | the second fails `SQLITE_BUSY` switching to WAL (busy handler not used): init must retry |
| inspect | none | `sqlite3 kv.db "SELECT bucket, key, CAST(value AS TEXT) FROM kv"` |
