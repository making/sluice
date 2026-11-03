Difficulty: Medium

# sluice-wicket-runtime-firecracker: a client quiet on `--listen` holds one of its connections

Seen while bounding the wasi wicket's `--listen` (`.kb/connection-limits.md`). `listener::serve` closes a connection
whose head takes over 10s, but a sole route reads no head at all, and past the head the relay is verbatim: a client
connected and quiet, or idle between requests, keeps its slot of `--max-connections` for as long as its guest keeps the
connection and the route sets no `idle-ms`. On the tunnel the server's `idle-timeout` closes it; on `--listen` nothing
does. 256 of them stop the listener answering, and a per-connection or reuse route holds a VM for each.

- `idle-ms` (off by default) cuts live peers too: a long poll, a WebSocket, a guest speaking first. A default on
  `--listen` alone needs to tell those from a client that is merely quiet, which a verbatim relay cannot; decide between
  a default bound on the route, HTTP awareness in the relay (`head.rs` sees only the first head) or leaving it to the guest
- a client not reading: a relay's write pending past `sluice_wicket_server::http::QUIET` is its fault whatever the
  protocol (`Unread`), so that part needs no such decision
