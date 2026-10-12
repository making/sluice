Difficulty: Medium

# A tunnel stream that stays not ready holds slots with no deadline

Found in 061. Since 061 a connection's slot (server: `sluice.limits.max-connections`,
client: `sluice.client.max-connections`) is released only once the frames it queued left
the `SessionSender`, or with the session's end. Nothing ends a session whose stream stays
not ready: a peer that stops reading the gRPC stream (zero HTTP/2 window) still answers
keepalive pings. Its ended connections then keep their slots for good
(`QueuedFramesHoldSlotsE2ETest` shows them held while stalled, freed at the session end).

On the server the limit is global, so one tunnel client that stops reading takes slots
from every client's routes as public connections to its routes end. Before 061 the same
stall let the ended connections' send queues grow past the limit instead; the live
connections of a stalled stream already held their slots. Related: an HTTP/1.1
connection switching routes waits for the previous leg's queued frames with no deadline
(`ReroutingRelay.Leg#awaitQueued`), so its client gets no response meanwhile.

The data plane's stall timeout (071, `sluice.limits.stall-timeout`) does not bound this: a
relay held back by a stream not ready waits on the upstream side, which it never counts
against the client (`StallTimeoutE2ETest#anUploadHeldBackByItsUpstreamIsNotCut` pins the
same for an upstream that does not read).

## Direction

A per-session progress watchdog: frames queued, the stream not ready and no frame handed
to it for a timeout -> fail the session (the sender's teardown drops the frames and runs
the held releases). Alternatively, or as well, a per-client share of the server's slots.
User decision: the policy and the timeout default. A metric for the stalled time.

## Tests (watch each fail first)

- `sluice-it`, with a client that stops reading (as in `QueuedFramesHoldSlotsE2ETest`):
  past the timeout the session is closed and the admitted count drops to the live
  connections of other clients; a stream that is slow but keeps taking frames is not cut.
- The same on the client side with a server that stops reading
  (`ClientQueuedFramesHoldSlotsE2ETest`).

## Decision (2026-10-12)

- A per-session progress watchdog on both sides: frames queued, the stream not ready and
  no frame handed to it for the timeout -> fail the session (the teardown drops the
  frames and runs the held releases, which also ends `Leg#awaitQueued`). A stream that
  is slow but keeps taking frames is never cut.
- Default 60s, configurable on the server (`sluice.limits.*`) and the client
  (`sluice.client.*`); a metric for the stalled time and a counter of sessions failed by
  the watchdog.
- No per-client share of the server's slots: tunnel clients are authenticated, and the
  watchdog bounds how long one can hold slots. File it separately only if a test or an
  incident shows the need.
