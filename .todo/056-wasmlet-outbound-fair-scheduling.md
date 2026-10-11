Difficulty: Medium

# wasmlet: one outbound FIFO for every connection's DATA

Found in the 047 review (by reading, not measured). The wasmlet counterpart of 041's
F4, which 044 fixed on the Java side. Parent: `.todo/041-tunnel-head-of-line-blocking.md`.

## Problem

`tunnel::Outbound` has an unbounded control lane (sent first) and one bounded data lane
(`DATA_BUFFER` = 64 frames of up to 16KiB) shared by every connection. Since 047 one
connection can queue up to its credit (1MiB, the server's window), which fills the whole
lane. DATA, CLOSE and the connection-end ERROR of every other connection then wait
behind up to 1MiB, and their writers park on the lane's capacity. The Java server's
receive path never blocks, so this costs latency (about 1MiB of transport time per
frame), not a stall.

## Design sketch

Per-connection queues drained round-robin into the request stream, as `SessionSender`
does (044): a connection's writer waits only on its own queue's cap. Keep the order of
DATA, CLOSE and ERROR within one connection; the control lane stays first.

## Test (`cargo test`; watch it fail first)

- Connection 1 has 1MiB of credit and a writer that fills it; connection 2's 1-byte
  DATA and its CLOSE leave before connection 1's backlog (assert the frame order on the
  merged stream).
