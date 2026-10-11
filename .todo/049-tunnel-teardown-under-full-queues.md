Difficulty: Medium

# Tunnel teardown under full queues (043 follow-up)

Review of 981797f (043). No protocol change. R1 is a regression of 043: fix it before
046 builds on `acceptData`. R3 and the `enqueue` half of R2 are in `SessionSender`, which
044 rewrites (per-connection queues, CLOSE / ERROR bypass the cap): rerun their repros
against 044's sender first and keep only what still fails. The unit repro of R3 fills the
256-frame queue of 981797f; adapt it to 044's caps.

Repro tests (all fail on 981797f): `.todo/artefacts/049-tunnel-teardown-under-full-queues/`
- `unit-repro.patch`: `VirtualConnectionTest#acceptDataBlockedOnFullBoundReturnsAfterClose`,
  `SessionSenderTest#closeReturnsWithFullQueueOnReadyStream`;
- `ClientTeardownFullQueueReproE2ETest.java` (`sluice-it`).

## Findings

R1. **`VirtualConnection#close` never releases a producer parked in `acceptData`
(regression).** 043 replaced the `offer(100ms)` loop, which rechecked `closed`, with an
untimed `inboundPermits.acquire()`. `close()` clears `inbound` without returning the
permits, so a producer waiting on the full 128-chunk bound waits forever (unit repro;
passes on 981797f~). By reading, not yet shown end to end: the producer is the gRPC
`onNext` of the session, so the whole stream stops receiving, and since gRPC serializes
callbacks, `onError` / `onCompleted` never run and the session is never closed. Trigger:
the local side of one connection ends (its relay calls `close()`) while the remote keeps
sending to it.

R2. **Client teardown hangs on a full sender queue.** `NodeConnection#runSession` calls
`closeConnections()` before `sender.close()`, the reverse of the order 043 introduced in
`TunnelSession#close`. When the server vanishes with the client -> server direction
stalled, `VirtualConnection#close` -> `sendClose` -> `enqueue` waits forever (the
thread stays in `closeConnections`). Cause by reading: the worker waits in `awaitReady`
(`isReady()` stays false on a closed call) and `running` is only cleared by
`sender.close()`, which comes after. `runSession` never returns: no reconnect, and the upstream sockets
stay open. Repro: `ClientTeardownFullQueueReproE2ETest` (public peer never reads, upstream
floods; precondition pinned: a relay is parked in `SessionSender.enqueue`).

R3. **`SessionSender#close` hangs with a full queue on a ready stream** (predates 043, but
043's commit message claims "teardown survives a full send queue"). `offerFirst(Poison)`
fails when the bounded queue is full. The worker keeps draining (the `isReady()` fast path
of `awaitReady` does not check `running`), finds the queue empty and blocks in
`takeFirst()` forever; `close()` loops on `workerDone.await(5s)` forever. Likely trigger
(by reading): the client half-closes while server relays fill the queue. Repro:
`SessionSenderTest#closeReturnsWithFullQueueOnReadyStream`.

R4. **`TunnelBackpressureE2ETest#deadStreamReleasesSessionConnectionsAndThreads` does not
check what it names.**
- "no thread parked in `SessionSender.enqueue`" uses `Thread.getAllStackTraces()`, which
  excludes virtual threads. Relays are virtual threads, so the check always passes
  (confirmed: with relays parked in `enqueue`, `getAllStackTraces` sees none,
  `HotSpotDiagnosticMXBean#dumpThreads` sees them). Use `dumpThreads` (helper `anyThreadIn`
  in the artefact).
- `session.connectionCount() == 0` is guaranteed by `connections.clear()` in
  `TunnelSession#close`; it says nothing about the relays or sockets. Assert the public
  peer socket sees EOF / reset instead.
- The precondition "server sender queue full" is not pinned. Assert a relay is parked in
  `SessionSender.enqueue` (via `dumpThreads`) before `started.stop()`.
- The test passes with the F2 fix reverted.

R5. **Markers are unbounded.** `remoteClosed` / `remoteFailed` append a marker per call
and the reader stops taking after the first. Repeated CLOSE / ERROR frames for one
connection grow `inbound` until the connection is removed. Keep at most one `Eof` and one
`Failure` per connection.

## Fix

- R1: `close()` wakes producers waiting in `acceptData` (e.g. `tryAcquire` with a timeout
  rechecking `closed` / `readClosed`, or release enough permits in `close()`). The
  dropped chunks must not leak permits either.
- R2: in `runSession`, close the sender before `closeConnections()`, as in
  `TunnelSession#close`. Also make `awaitReady` / `enqueue` give up when the call is
  closed, not only when `running` is cleared.
- R3: `close()` must not depend on the Poison fitting into the queue; the drain loop
  checks `running` (or `close()` clears the queue before `offerFirst`). Decide and
  document whether `close()` flushes queued frames: its Javadoc says it avoids cutting
  "the final frames off", but `offerFirst` already drops everything queued.
- R4, R5 as above.

## Tests (watch each fail first)

- Apply `unit-repro.patch`; promote `ClientTeardownFullQueueReproE2ETest` into `sluice-it`
  (rename, keep the pinned precondition).
- R1 E2E: a connection whose local side closes while the remote keeps pushing; another
  connection on the same stream keeps working afterwards.
- R4: rework the test as above; verify it fails with 043's `TunnelSession#close` reorder
  reverted.
- R5: `VirtualConnectionTest`, 1000 `remoteClosed()` calls leave one marker (check via
  reader behavior or a package-private size accessor).
- After the fixes, also rerun the 041 artefact `HeadOfLineBlockingE2ETest` and confirm
  nothing regressed.
