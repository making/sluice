Difficulty: Medium

# Receive buffers: per-frame overhead is not counted against the window

Found while working on 048 (by reading, not measured). A windowed receiver buffers at
most its window (1MiB) of DATA payload per connection, but stores one chunk per DATA
frame: Java `VirtualConnection#acceptWindowed` queues a `Chunk(byte[])` per frame
(deque node + record + array header, roughly 60 bytes beyond the payload), the wasmlet
one `Ctrl::Payload(Vec<u8>)` per frame. A connection fed 1-byte frames buffers up to a
million chunks, about 60MiB instead of 1MiB.

Who emits tiny frames: a relay sends what one socket read returns, so a public peer
dripping its bytes one at a time makes the server send 1-byte DATA frames, which the
client buffers while its upstream does not read. The connection-limit worst case in
README "Connection limits" (2.125MiB per connection) assumes the payload bound.

## Direction

Measure first: a test feeding 1MiB in 1-byte frames to a connection whose reader does
not read, asserting the retained heap (or a chunk counter). Then bound the overhead,
e.g. append small payloads to the tail chunk when it has room (the reader side must stay
lock-free on the receive path), on both the Java side and the wasmlet. Counting
overhead against the window is not an option: the peer computes credit from payload
bytes only.

## Tests (watch each fail first)

- Java (`sluice-proto`): 1MiB of 1-byte DATA frames into an unread windowed connection
  keeps at most a bounded number of chunks (assert on a test hook), and reads back
  byte-exact.
- wasmlet (`cargo test`): the same for `TunnelIo`.
