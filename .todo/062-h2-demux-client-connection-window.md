Difficulty: Medium

# h2 demux: honor the client's connection window on the response path

Found while working on 053 (by reading, not tested). The relay writes every response
DATA frame to the client as soon as a leg's inbound reads it, and grants that upstream's
connection window for it at once. The client's connection-level WINDOW_UPDATEs are
dropped (`onClientWindowUpdate` returns for stream 0), so the client's connection window
is never tracked. Each upstream honors the client's stream windows (the client's stream
WINDOW_UPDATEs and SETTINGS are relayed), but the sum over concurrent streams, or over
several legs, is bounded by no connection window. A client that grants its connection
window more sparingly than the sum of its stream windows (Netty's default: 65535, a
WINDOW_UPDATE after half of it is consumed) can receive more than it granted and must
answer with GOAWAY `FLOW_CONTROL_ERROR`. README "Constraints and notes" states the gap.

## Direction

Track the client's connection window (65535 plus its WINDOW_UPDATEs) as the relay's send
window towards the client. A leg's inbound writes DATA only within it and grants the
upstream's connection window only for what it wrote, so the upstream is held back by its
own window. Caveats to design around: an inbound thread parked on the client window must
not stop that leg's control frames (upstream SETTINGS, PING, WINDOW_UPDATE, which the
leg writer needs for request data), nor response HEADERS/RST_STREAM, which are not flow
controlled; several legs share one client window, so hand it out fairly.

## Tests (`sluice-it`; watch each fail first)

- A client granting its connection window 16KiB at a time while two streams on two
  routes each receive 512KiB never receives more than it granted, and both complete.
- An upstream PING sent while its leg waits for client connection window is acked.

## Also left open by 053

- The upstream's SETTINGS_MAX_CONCURRENT_STREAMS is not tracked: a stream beyond it is
  refused by the upstream (RST_STREAM REFUSED_STREAM, relayed; the client may retry).
- A stream routed to a leg whose upstream retires concurrently is refused with
  REFUSED_STREAM (`onRequestHead` checks `acceptingLocked` under the lock); that race has
  no test.
