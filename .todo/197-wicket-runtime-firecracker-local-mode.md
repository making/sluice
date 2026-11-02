Difficulty: Medium

# sluice-wicket-runtime-firecracker: local mode routing every route by host

`--listen` serves the sole route only (`listen serves the sole route: N given`,
`config.rs`): the wicket relays raw bytes and reads nothing of a connection, so
`--listen` has no host to route by. sluice-wicket-runtime-wasi's local mode
(its README, "Local mode") serves every route on one listener, by the `Host`
header or the h2 authority, a single route taking any host, an unknown host
answered 404: a local loop with no sluice server for several routes, and the
same routing the server's data plane does.

Nothing in the VMs stands in the way: the wicket has to read the connection's
start to pick the route, then relay it verbatim, the bytes read included.

## First: share the listener with sluice-wicket-runtime-wasi

Start by moving what the two local modes have alike into `sluice-wicket-tunnel`
(a `listener` module), the wasm wicket moved onto it first, its tests unchanged:

- the accept loop: `--max-connections` held by a semaphore (one beyond waits in
  the backlog), an accept error retried after 100ms (`sluice-wicket-runtime-wasi`
  `src/listener.rs::run`; the Firecracker wicket's `src/listener.rs` has neither
  the bound nor the retry), the connection handed to the wicket's serve
- the replay stream: the bytes read to decide replayed first, the rest from the
  socket (the wasm wicket's `Sniffed`, made generic over its prefix: the
  Firecracker wicket reads the whole head, the wasm wicket `PRI`)
- the routing rule over the advertised table (`routes::Table::advertised`, both
  wickets' tables implement it): the route of the host, compared ignoring case,
  its port left out, or the sole route whatever the host (the wasm wicket's
  `Table::by_host` and `request_host`; the Firecracker wicket's `Table::sole`
  is its sole-route case), so the wickets cannot drift apart

What stays each wicket's own: the wasm wicket terminates HTTP (hyper) and routes
each request, an h2 stream or a keep-alive request to another host going to its
own route; the Firecracker wicket relays bytes and routes the connection once,
by its first request, so it parses the head itself (below). The 404 for an
unknown host goes with that: hyper's answer in one, written raw in the other.

## Then: the Firecracker wicket's routing

- http/1.1: read the request head (bounded, as the server's data plane does), its
  `Host`, the port left out; relay the head on
- h2c (prior knowledge): the preface, then the first HEADERS frame's
  `:authority` (HPACK: the decoder's state is the connection's, so the bytes go on
  as read and the guest decodes them again); the frames before it (SETTINGS)
  relayed alike. A connection's later streams to another host stay on the route
  of the first (the server demuxes them, `.kb/h2-demux.md`: say what local mode
  does not)
- an unknown host: `404` (http/1.1) / `GOAWAY` or a 404 stream (h2), as the wasm
  wicket answers; a head past the bound or not HTTP: closed
- the reloads keep working (`Routes` publishes the table; the listener takes the
  table of the moment); `--max-connections` bounds the listener's connections
  (the wasm wicket's: apart from the tunnel's)
- `X-Forwarded-*`: the server adds them; local mode adds none today (the
  sample's `x-forwarded-for -`). Decide whether it does, as the server would,
  given it reads the head anyway

Extend the VM test (`tests/vm.rs`) rather than adding one: its routes served
through `--listen` as well, by host. Update the README (Run: `--listen`,
`scripts/try.sh`) and the `--help` text.
