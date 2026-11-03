Difficulty: Medium

# sluice-wicket-runtime-wasi: a client gone from `--listen` holds its slot until the budget ends

Seen while building todo 215, whose test holds an instance with `/stream` instead of `/stall` for this reason: over
`--listen`, a guest that writes nothing does not learn its client left, and keeps its instance slot until the
budget runs out. `half_close(true)` (`wasm_host.rs`) is why: a client's FIN is a half-close, so a request already
received is still answered, and hyper does not read between a request's end and its response's end. A connection's
failure is watched beside hyper over the tunnel; find what `--listen` has of that.

It mattered less while a route had no limit. With `cf scale -i 1` and `concurrency: 1`, one client leaving a slow
request blocks the app's next one for up to the budget (10s by default), then 503.

- a FIN alone stays a half-close (`a_request_followed_by_the_servers_half_close_is_answered`, the README): it
  cannot be told from a client leaving, and nginx's `proxy_ignore_client_abort` off shows the other choice.
  Weigh it, and take the one that holds for the long term, also for the tunnel, where a FIN arrives as CLOSE
- what can be told apart: an RST or a write failing on `--listen`, the tunnel's ERROR. These end the instance
  at once, as the tunnel's path does
- the test: an extension of the listener's tests in `tests/`, a slot freed before the budget when the client
  resets, seen failing first. `sluice-wicket-cf-api/tests/cli.rs` may then go back to `/stall`, if it reads better
