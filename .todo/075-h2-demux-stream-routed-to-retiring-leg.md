Difficulty: Low

# h2 demux: no test for a stream routed to a leg its upstream retires meanwhile

Left open by 053, carried over from 062. `Http2DemuxRelay#routeRequestHead` picks the
route's leg (`legFor`, which skips legs marked `ended`) and admits the stream under the
lock only while `Leg#admitsLocked` holds; a leg whose upstream ended its side, failed, made
a connection error or sent a GOAWAY in between refuses it with `REFUSED_STREAM`. Since 062
a leg whose upstream left first writes its responses waiting for the client's connection
window (`Leg#fail`, then `awaitResponsesWritten`, then `Leg#retire`, all from
`Http2DemuxRelay#endLeg`), so the leg is `gone` (not accepting) for longer before it
closes. The deterministic part is tested
(`H2DemuxClientWindowE2ETest.newStreamOfARouteWhoseLegDrainsGoesToANewLeg`: a stream
opened once the leg is marked goes to a new leg); the race between `legFor` and the mark
is not.

## Tests (watch each fail first)

- A stream whose head is routed to a leg that is marked gone between `legFor` and the
  admission is refused with `REFUSED_STREAM`, not admitted to the gone leg (needs a test
  hook to hold the routing between the two, e.g. a latch in `legFor`); removing the
  `admitsLocked` check must make it fail (the stream hangs, or -- since 081 -- it is
  answered with a `502` or refused when the leg fails, instead of being refused at once).
- The same for a leg its upstream's GOAWAY marks between `legFor` and the admission
  (`Leg#goingAwayLocked`): refused at once, its head never queued on that leg.
