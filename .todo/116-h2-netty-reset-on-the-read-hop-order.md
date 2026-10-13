Difficulty: Medium

# netty h2 demux: a relay reset written on the hop being read may still precede held DATA

Found while replacing the write-seam race tests with `H2DemuxResetOrderE2ETest` (105).

## Problem

netty closes a reset stream -- and drops the DATA its flow controller holds for it -- only
once the RST_STREAM write completes, which in a hop is at a flush. `Hop#settleLocked` now
flushes what was written before the handler's flush has the flow controller write its
held DATA, so a reset the relay writes on another hop than the one being read is never
followed by that stream's DATA (the tests of `H2DemuxResetOrderE2ETest`). netty's own stream
errors flush right after their RST_STREAM (`Http2ConnectionHandler#onError`), so they are
safe too.

Not covered: a reset the relay writes on the hop being read, during that read. netty's
`channelReadComplete` runs the handler's flush (`writePendingBytes`, then the flush) before
the settle, so DATA the same read made writable -- a WINDOW_UPDATE ahead in the read --
goes out behind the reset. Read from the code, not shown by a test. The one such reset
today: trailers past the allowance with the budget exhausted
(`ClientListener#onHeadersRead`: RST_STREAM `ENHANCE_YOUR_CALM` on the client hop), with the
stream's response DATA held for the client's stream window and the client's grant in the
same read.

## Directions

- Flush past the handler in `Hop.LockGuard#channelReadComplete` before passing the event on
  (as the settle does), or have the relay flush its hop's context right after each reset
  it writes (as netty's `onError` does; the stream then closes inside the reset call, so
  check the relay's listeners for reentrancy).

## Tests

- First, failing: the budget trailers reset with the stream's response DATA held for a
  client stream window of 0, and one client write carrying a WINDOW_UPDATE of the stream
  and the trailers: the client must read no DATA of the stream after its RST_STREAM.

## Notes (from 115)

- Seen at the hop level: a server hop whose frame listener answered each stream with a
  head and DATA (window open) and reset it in the same call sent the DATA behind the
  RST_STREAM -- its client-role peer answered `Received DATA frame for an unknown stream`.
  No WINDOW_UPDATE was needed: DATA written in the same read waits in the flow controller
  until the handler's flush.
- More resets on the hop being read than the trailers one: `LegListener#onHeadersRead`
  resets the leg's stream on the leg being read (interim heads past `MAX_INTERIM_BYTES`:
  `ENHANCE_YOUR_CALM`; a response block over the budget: `CANCEL`), with request DATA
  possibly held for the upstream's window.
- `HopHandler` (115) now subclasses netty's handler: overriding its `flush` to flush past
  the handler before `writePendingBytes` is another place for the fix.
