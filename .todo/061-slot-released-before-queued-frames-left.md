Difficulty: Medium

# Java: a connection's slot is released while its frames are still queued

Found in the 056 review (by reading, not tested). Parent:
`.todo/041-tunnel-head-of-line-blocking.md` (memory bound of 048).

## Problem

The 048 bound (`ConnectionLimit.CONNECTION_BYTES`: receive window + send queue cap +
relay buffers per connection, times the limit) assumes a connection's buffers are gone
once its slot is free. The send queue is not: the server (`DataProxyServer`, the
`release` run when the relay completes) and the client (`NodeConnection`,
`permit.release()`) free the slot when the relay ends, while `SessionSender` may still
hold up to `DATA_QUEUE_CAPACITY` x 64KiB = 1MiB of that connection's DATA plus its
CLOSE / ERROR. While the stream is not ready (gRPC flow control, a congested transport),
connections that end keep handing their slots to new connections whose responses queue
too, so the queued bytes grow without the limit applying.

The wasmlet does it the other way since 056: the ended connection's `Permit` moves into
its outbound queue and is dropped with it (`Outbound::hold`).

## Direction

Release the slot when the connection's last queued frame was handed to gRPC, or at once
when nothing is queued; the sender's teardown releases what it still holds. Keep the
release exactly once on every path (048's `ConnectionReleaseE2ETest`).

## Tests (watch each fail first)

- `SessionSenderTest`: observer not ready, a connection queues DATA + CLOSE and ends:
  its slot stays held; after ready and the drain it is free; `close()` with frames
  queued frees it too.
- Data plane (`sluice-it`): with the stream held not ready, the limit's open count
  does not drop below the connections that still have frames queued.
