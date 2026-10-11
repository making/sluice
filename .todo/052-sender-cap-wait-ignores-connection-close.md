Difficulty: Medium

# A producer blocked at the sender's per-connection cap ignores its connection's close

Parent: `.todo/041-tunnel-head-of-line-blocking.md`. Found in the 051 review. Do it before
or with 046.

## Problem

`SessionSender#enqueueData` waits while the connection's queue holds
`DATA_QUEUE_CAPACITY` frames. The wait ends only on a pop of that queue, sender death or
call close. `VirtualConnection#close` (local close) queues CLOSE as a marker but does not
wake the waiter, so a producer blocked in `Sink#write` stays blocked until the stream
drains the queue. With a stalled tunnel (today: the legacy stream-wide stall, F1) that
can be never.

Effect on the data plane: a relay's writer thread never returns, so the relay's pending
count never reaches 0. `finishRelay` does not run: `activeConnections` stays
incremented, the access log line is not closed, `session.remove(connectionId)` does not
run.

Probe (by hand, on 051): `H2DemuxStallE2ETest#stalledLegKeepsTheOtherLegsFlowing`, then
after the client socket closed, poll `DataProxyServer#activeConnections()`: still 1 after
10s. The leg writer is parked in `SessionSender.enqueueData`. `StreamRelay`'s relay-out
thread has the same shape (`doRelayToRemote` -> `sendData`).

## Design

- Mark the connection queue closed when CLOSE or ERROR is queued for it (local close or
  half-close: no DATA may follow either), signal its cap waiters, and have
  `enqueueData` return `false` for a closed queue (`Sink#write` then throws).
- Keep the 050 invariants: the queue may be dropped once empty; a late DATA after CLOSE
  must not resurrect a queue that is never drained.
- 046's credit wait already wakes on `close()`; this is the cap wait under it.

## Tests

- `SessionSenderTest`: a producer blocked at the cap (observer not ready) returns
  `false` within 100ms of a CLOSE queued for its connection; another connection's
  producer is unaffected.
- `sluice-it`: the probe above as a test. After the client closes, `activeConnections()`
  returns to 0 within 5s while the stalled upstream still holds its socket. Watch it fail
  first.
