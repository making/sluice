Difficulty: Low

# sluice-wicket-runtime-wasi: a client quiet on `--listen` holds one of its connections for good

Seen while bounding the consoles and the cf-api. `listener::run` of the wasi wicket gives up sniffing after
`SNIFF_TIMEOUT`, then `wasm_host::serve_http` serves the connection on hyper's builders without a timer: a client
connected and sending nothing, or idle between requests, keeps its slot of `--max-connections` until it leaves. TCP
keepalive ends only one that is gone. The firecracker wicket closes a `--listen` connection whose head takes over 10s,
but its relay leaves an idle one to the guest.

- bound the wait for a request's head on http/1.1 and an idle h2 connection (`http2::Builder::keep_alive_*` and the
  timer) with `sluice_wicket_server::http::QUIET` or an option; a request in progress, a WebSocket and an upgrade stay
- `serve_http` serves the tunnel's connections too: decide whether the bound holds there, or the server's limits are
  enough (`.kb/connection-limits.md`)
- a request's body and an unread answer: see `sluice_wicket_server::http`; a guest may hold them as it likes, so a
  bound may belong to the route
