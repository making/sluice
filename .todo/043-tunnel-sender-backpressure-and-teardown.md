Difficulty: Medium

# Tunnel sender backpressure and teardown fixes (041 child 1/6)

Parent: `.todo/041-tunnel-head-of-line-blocking.md` (read Findings F2, F3, F6, F1). No
protocol change. Order: 043 -> 044 -> 045 -> 046 -> 047.

## Scope

1. **F2 -- server ignores gRPC flow control.** `SessionSender` (`sluice-proto`) only
   recognizes `ClientCallStreamObserver`. Generalize to `io.grpc.stub.CallStreamObserver`
   (common type of the client and server observers): `setOnReadyHandler` in the
   constructor, `isReady` in `awaitReady`. On the server the handler must be set before
   `TunnelService#connect` returns -- it is, the sender is built there. On the client only
   inside `beforeStart` (already so; CLAUDE.md gRPC quirks).
2. **F3 -- `TunnelSession#close` hangs on a dead stream.** In the worker's `finally`, set
   `running = false` and wake every producer waiting in `enqueue`. Producers on a dead
   sender then fail fast. `VirtualConnection.Sink#write` throws
   `IOException("tunnel stream closed")` instead of the silent drop in
   `SessionSender#sendData` (have `sendData` report failure, e.g. a boolean or an
   exception), so relays end and close their sockets.
3. **Non-blocking markers.** `VirtualConnection#remoteClosed` / `remoteFailed` must not
   wait (`enqueueAfterDrained`). Make `inbound` an unbounded `LinkedBlockingDeque` and
   keep the 128-chunk bound for DATA only (a counter or semaphore that `acceptData` waits
   on, released by `Source#read`). Markers are appended right away and stay ordered after
   the data. `acceptData` keeps blocking for now; 046 removes that.
4. **F6 -- Java client leaks connections on stream end.** After `closed.await()` in
   `NodeConnection#runSession`, mirror `TunnelSession#close`: call
   `remoteFailed("tunnel stream closed")` and then `close()` on each `VirtualConnection`,
   then clear `connections`.

Expected side effect: with F2 fixed, a stalled client makes the server-side relay threads
block at the sender queue bound (256 frames) instead of buffering without limit. F1 still
stalls the stream; that is fixed in 046.

## Tests (watch each fail first)

- `sluice-proto`, new `SessionSenderTest`:
  - a fake `ServerCallStreamObserver` whose `isReady()` is false gets no `onNext` until
    its onReady handler runs. Pin the precondition: assert the handler was registered;
  - once the worker died (fake `onNext` throws), `sendClose` / `sendData` return within
    1s with a full queue.
- `VirtualConnectionTest`: with 128 chunks queued and no reader, `remoteClosed()` and
  `remoteFailed()` return within 100ms; a reader then sees all the data, then EOF or the
  failure.
- `sluice-it`:
  - `serverToClientDirectionIsBackpressured` from
    `.todo/artefacts/041-tunnel-head-of-line-blocking/HeadOfLineBlockingE2ETest.java`.
    Promote it into a new test class, without the `HOLDUMP` output;
  - F3: a client with a connection whose upstream never reads, while the peer keeps
    pushing until the server sender queue is full; stop the client. Assert the server
    session leaves `SessionRegistry` and its connections close within 5s, and that no
    thread is parked in `SessionSender.enqueue` (thread dump check);
  - F6: open a relayed connection, then stop the server (pattern of
    `TunnelReconnectTests`). The upstream socket must see EOF / reset within 5s.

## Docs

README line 64 ("the gRPC send path honors `isReady()`") becomes true for both sides; keep
the wording but make sure it is not claiming more than the tests show.
