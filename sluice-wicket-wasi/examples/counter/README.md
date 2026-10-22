# counter

State in `wasi:keyvalue` (0.2.0-draft2): a counter, and the store's operations one path
each. Its route needs `wasi=keyvalue`; `?store=<identifier>` opens another identifier
of the route's map than the default `""`.

```text
cargo build --release -p counter   # from examples/
sluice-wicket-wasi --listen 127.0.0.1:8080 \
    --wasm 'counter.local=examples/target/wasm32-wasip3/release/counter.wasm?wasi=keyvalue'
```

| request | demonstrates |
|---|---|
| `GET /` | `atomics.increment` of `hits`: the count, kept across restarts |
| `GET /cas` | read-modify-write of `cas-hits` through a `cas` handle, retried on a lost race |
| `GET /cas?race` | the same, with a write between the read and the first swap: that swap fails, the retry wins |
| `PUT` / `GET` / `DELETE /kv/<key>` | `set` (the body) / `get` / `delete` |
| `POST /fill/<n>` | `batch.set-many` of `n` keys at once (all or none) |
| `GET /keys` | `list-keys` page by page: the keys and pages counted |
| `GET /?store=other` | an identifier the route does not map: `no-such-store` |

```text
curl 127.0.0.1:8080/                       # hits 1, hits 2, ...
curl '127.0.0.1:8080/cas?race'             # cas-hits 2 after 2 attempt(s)
curl -X PUT -d hello 127.0.0.1:8080/kv/greeting
curl 127.0.0.1:8080/kv/greeting
curl -X POST 127.0.0.1:8080/fill/250 && curl 127.0.0.1:8080/keys
sqlite3 keyvalue.db "SELECT namespace, key, CAST(value AS TEXT) FROM kv"
```

An error of the store answers 500 with it (`increment: Error::Other("...")`).
