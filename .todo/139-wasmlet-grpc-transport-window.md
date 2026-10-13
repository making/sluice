Difficulty: Low

# The wasmlet's gRPC receive window caps what the server sends it

Found while fixing the tunnel-wide cap of the Java stacks (the server and sluice-client now
announce a fixed HTTP/2 window of twice their tunnel budget, gRPC's own tuning off; see
README "transport flow control").

## Problem

sluice-wasmlet's tonic `Endpoint` sets no window (`wasmlet/src/main.rs`, `attempt`), so it
announces hyper's client defaults: 2MiB per stream, 5MiB per connection, no adaptive
window. The tunnel is one stream, so everything the server sends a wasmlet (request bodies
of every virtual connection) shares 2MiB per round trip, while each virtual connection's
window is a fixed 1MiB (`tunnel::WINDOW`): two connections at full window fill it. h2
grants back once the unclaimed capacity reaches half of the window the peer still holds,
so up to a third of it can sit unreturned; at a 20ms round trip that is roughly 65 to
100MiB/s for the whole tunnel. The direction wasmlet -> server is the server's window and
is no longer capped.

## Done when

- The wasmlet announces a stream and connection window sized from what its connections may
  have in flight (the connection limit times `tunnel::WINDOW`, or a configured bound),
  with a test counting round trips (as `TunnelTransportWindowE2ETest` does for the Java
  client), and the README states it.
