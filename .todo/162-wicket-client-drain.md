Difficulty: Medium

# sluice-wicket-wasi: drain on shutdown

`sluice-client` drains on SIGTERM / SIGINT (README "Client shutdown (drain)"): it sends
`Advertise{withdraw: true}` (its routes repeated in `upstreams`) on every node stream, stops
reconnecting, keeps serving the connections open, ends a stream once the server answered
`AdvertiseAck{withdrawn: true}` and no connection is left on it, and closes the rest after
`sluice.client.drain-grace`. A second signal ends the drain at once; an ack without `withdrawn` to
the withdrawing advertise means the server predates it: the stream is closed at once.

`sluice-wicket-wasi` handles no signal: stopping it cuts every in-flight request, also when another
client serves the same host.

## Goal

The same drain in `wicket-wasi` (tokio signal handling in `main.rs`, the per-node stream in
`tunnel.rs`), with a `--drain-grace` option (and the config file key) defaulting to 20s, `0` = off.
Count the acks per stream as `NodeConnection` does to tell an old server's ack apart.

## Acceptance

- A Rust test against a fake server: the withdrawing advertise is sent, a CONNECT accepted before
  the `withdrawn` ack is served to the end, the stream ends after the ack once the last connection
  closed; the grace closes a connection still open; a plain ack ends the drain at once.
- The wicket README documents the drain, the option and the compose `stop_grace_period` note.
