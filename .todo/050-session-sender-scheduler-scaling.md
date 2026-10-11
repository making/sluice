Difficulty: Medium

# SessionSender scheduler: O(1) per frame, targeted wakeups, queue lifecycle (044 review)

Parent: `.todo/041-tunnel-head-of-line-blocking.md`. Follow-up of 044 (e81ed4d). Same
class as 046; do it before or with 046. No protocol change.

## Findings

R1. **Every frame scans every live connection under the global lock.**
`nextConnectionFrame` iterates the whole `queues` TreeMap. A queue is dropped only when
empty *and* `ended` (CLOSE / ERROR queued), so every open connection that ever sent DATA
(keep-alive, WebSocket) stays in the map. Producers cannot enqueue during the scan.

R2. **Thundering herd.** One `activity` condition with `signalAll` on every enqueue and
every DATA pop wakes the worker and every producer parked on *any* connection's cap.

R3. **Queue leak.** A DATA enqueued after the connection's queue was dropped creates a
queue with `ended = false` that is never removed (and is scanned per R1 for the rest of
the session). Reachable from `VirtualConnection.Sink#write`: it checks `writeClosed`,
releases `writeLock`, then calls `sendData`, so a concurrent `close()` can put CLOSE
first. Probe: 2 leaked queues per 20000 write/close races. (DATA after CLOSE on the wire
is older than 044; the peer drops it after EOF. Only the leak is new.)

R4. **Dead sender keeps accepting.** `enqueueControl` / `enqueueMarker` do not check
`running`; after the worker dies they append to queues nobody drains (the pre-044
`enqueue` rejected them). Membership broadcasts to such a session pile up until it is
removed.

R5. **Tests do not pin what they claim.**
- Removing `it.remove()` (no queue removal at all) passes all 8 `SessionSenderTest`
  tests; `dataAfterTheQueueIsRemovedIsStillDelivered` never checks the removal.
- `closeAndErrorBypassTheCapInConnectionOrder` and the conn-2 call in
  `perConnectionCapBlocksOnlyItsOwnProducer` run blocking calls on the test thread
  without a timeout: with CLOSE subject to the cap the build hangs (> 240s) instead of
  failing.
- `secondConnectionIsServedWithinOneTurnOfTheFirst` uses `sleep(200)` for "the bulk
  producer reached its cap" and never asserts it; on a slow machine it passes vacuously.
- Per-connection order is checked only with one connection, not interleaved.

R6. **Doc overclaim.** The class javadoc and the e81ed4d message list KEEP_ALIVE as a
control frame; nothing sends KEEP_ALIVE.

Measurements (no-op always-ready observer, 16-byte frames, Apple M4 Max; old = e81ed4d~):

| scenario | old FIFO | 044 |
|---|---|---|
| 1 producer, 200k frames, 0 idle connections | 20 ms | 51 ms |
| same, 1k idle connections | 16 ms | 1704 ms |
| same, 10k idle connections | 20 ms | 15064 ms (13k frames/s) |
| 100 producers x 4000 frames | 674 ms | 2328 ms |
| 1000 producers x 400 frames | 851 ms | 5933 ms |

Probes: `.todo/artefacts/050-session-sender-scheduler-scaling/` (`ReviewProbeTest`:
R1-R3 numbers and the late-DATA leak; `ReviewRaceProbeTest`: R3 through
`VirtualConnection`). Copy into `sluice-proto/src/test/java/am/ik/sluice/tunnel/`; the
baseline is
`git show e81ed4d~:sluice-proto/src/main/java/am/ik/sluice/tunnel/SessionSender.java | sed 's/public final class SessionSender/final class OldSessionSender/; s/public SessionSender(/OldSessionSender(/' > OldSessionSender.java`.
They read private fields by reflection; replace that with the hook below.

## Design

- **Active ring**: a FIFO of the connection queues that have pending frames. A queue
  joins it on empty -> non-empty. The worker takes the head, sends one frame, re-appends
  the queue if it still has frames, otherwise removes it from the map. O(1) per frame;
  turn order becomes arrival order instead of id order.
- **Drop a queue whenever it empties**; remove `ended`. This also fixes R3. A producer
  parks only while its queue holds `DATA_QUEUE_CAPACITY` frames, so a queue with a parked
  producer is never empty and never dropped under it.
- **Conditions**: one for the worker (signaled on enqueue and onReady), one per
  connection queue for its cap waiters (signaled when that queue pops a DATA). Teardown
  (`close`, worker exit, `markCallClosed`) signals the worker condition and every queue's.
- **Reject after death**: `enqueueControl` / `enqueueMarker` drop the frame when
  `!running`.
- Fix the javadoc (R6). Keep 043/049 semantics and the CONNECT-before-DATA invariant.

## Tests (`SessionSenderTest`; watch each fail on the current code first)

- Package-private `int connectionQueues()`. After DATA + CLOSE drain: 0. After a late DATA
  for the dropped id drains: 0 (fails today: 1). An open connection whose DATA drained:
  0 (fails today: 1). 10k connections, one drained DATA each, no CLOSE: 0.
- Herd: a package-private counter of cap-waiter wakeups. Conn 1's producer parked at its
  cap while conn 2 sends 1000 frames: conn 1's producer wakes only for its own pops plus
  the 100ms timer (fails today: one wake per frame of conn 2).
- Every blocking call that must not block runs on a thread joined with a timeout and
  asserts it returned (R5).
- Fairness test: assert the bulk producer is parked on its cap (`observeBlocked`) before
  conn 2 enqueues.
- Two connections interleaved (DATA..., CLOSE each): each connection's subsequence keeps
  its order.
- Rerun the probe table; the idle rows must not grow with the idle count, and the
  fan-out rows must be no worse than old FIFO. Record the numbers in the commit message.
