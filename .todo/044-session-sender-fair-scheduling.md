Difficulty: Medium

# SessionSender: control priority and per-connection round-robin (041 child 2/6)

Parent: `.todo/041-tunnel-head-of-line-blocking.md` (Finding F4). Depends on 043 (same
class). No protocol change. 046 relies on the control queue added here.

## Problem

`SessionSender` (`sluice-proto`) has one FIFO of 256 frames x 64KiB for every
connection. A frame of an interactive connection, a CLOSE / ERROR, or a control frame
waits behind up to 16MiB of a bulk transfer. One busy producer can also take every slot,
so other connections' producers block on a shared resource.

## Design

- **Control queue**: CONNECT, WINDOW_UPDATE (added in 046), KEEP_ALIVE, ADVERTISE,
  ADVERTISE_ACK, MEMBERSHIP_UPDATE, DRAIN.
  - Enqueue never blocks: these come from the gRPC receive path and from close paths.
  - Always drained before data.
- **Per-connection queues** (keyed by conn_id): DATA, CLOSE, ERROR of that connection, in
  order. The worker drains them round-robin, one frame per turn, over connections with
  pending frames.
- CONNECT stays ahead of its connection's DATA: `TunnelSession#open` enqueues it before
  any relay thread exists, and control drains first. Note this invariant in a comment.
- **Bound**: cap DATA per connection (e.g. 16 chunks = 1MiB). Only that connection's
  producer blocks at the cap. CLOSE / ERROR bypass the cap (enqueued from the receive path
  and close paths). Keep a global bound on queued DATA only if a test shows one is needed;
  otherwise memory is bounded by the cap x connections.
- Remove a per-connection queue when it empties after its CLOSE or ERROR. A late frame for
  a removed id recreates it and drains normally.
- Keep 043's dead-worker rule: every waiting producer is woken and fails.
- The single worker thread and `awaitReady` stay: `onNext` on one gRPC call must not be
  called concurrently.

## Tests (`sluice-proto`, `SessionSenderTest`; watch each fail first)

- 100 DATA of conn 1 queued while the fake observer is not ready, then 1 DATA of conn 2.
  After ready, conn 2's frame is among the first 2 `onNext` calls.
- A control frame enqueued after queued DATA is sent first.
- The producer of conn 1 blocks at its cap (assert the thread state is WAITING /
  TIMED_WAITING, not just "not done"), while a producer of conn 2 enqueues without
  blocking.
- CLOSE and ERROR are accepted with conn 1 at its cap.
- Per connection, the frame order is unchanged (DATA..., CLOSE).
