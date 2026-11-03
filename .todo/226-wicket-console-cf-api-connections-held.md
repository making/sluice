Difficulty: Low

# Wickets' consoles and sluice-wicket-cf-api: a client vanished or quiet holds one of the 64 connection slots for good

Seen while adding TCP keepalive to `--listen` and the firecracker wicket's egress. `sluice_wicket_console::serve::run`
and `sluice_wicket_cf_api::api::run` each take a slot of `MAX_CONNECTIONS` (64) per connection, set no keepalive, and
build hyper's `http1::Builder` without a timer, so no header read timeout applies: 64 connections gone silently, or
merely left open and idle, stop the console or the API answering until the process restarts. A `--console` on a
non-loopback address, and the cf-api, are reachable by anyone who can open a TCP connection, signed in or not.

- keepalive on each accepted socket, as `sluice_wicket_tunnel::listener::keep_alive`: neither crate depends on
  sluice-wicket-tunnel, so decide where the few lines live
- a bound on a quiet connection too (`Builder::timer` + `header_read_timeout` bounds the wait for a next request's
  head; a long poll or an upload in progress must stay unaffected)
- the test: extend an existing test of each server; the socket's options as accepted, and a connection sending
  nothing closed within the bound (paused time, if hyper's timer allows it)
