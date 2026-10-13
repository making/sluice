Difficulty: Low

# netty h2 demux: a leg that fails to open has no test

Found while giving the netty relay its leg slots. `NettyHttp2DemuxRelay#legLocked` has two
failure paths that no test reaches: `TunnelSession#open` throwing (the slot is released at
once) and the leg's hop failing to build or configure (the hop is aborted, the virtual
connection closed and removed from the session, the slot released once its queued frames
left). Either way the stream is refused with `REFUSED_STREAM`. Neither throws in practice
today, so a test needs a seam (a hop builder or session that fails on demand).

## Tests

- A leg whose hop fails to build: the stream is refused, the connection goes on, the
  session holds no connection of it, and `sluice.data.connections.admitted` comes back to
  what it was before the stream (a further leg's permit, or the connection's own slot).
- Removing the release in that path makes it fail.
