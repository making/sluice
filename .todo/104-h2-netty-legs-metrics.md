Difficulty: Medium

# netty h2 demux, step 7: leg limits, leg retirement, metrics and access logs

Step 7 of the netty h2 demux migration (context and user decisions: 098). Depends on 103.

## Scope

- Leg limits and slots (`ConnectionLimits`) on the netty engine.
- Leg retirement: a leg is removed when its channel goes inactive (066); a stream routed to
  a leg its upstream retires meanwhile gets REFUSED_STREAM (075). Close 066 and 075 with
  this step if their tests pass on netty.
- The h2 demux metrics and access log on the netty engine, same names and tags.

## Tests

- The leg-limit, metrics and access-log subsets pass on netty; 066's and 075's tests.

## Notes from 098

- `Hop.Builder#onInactive` runs under the lock when a leg's channel closes (leg
  retirement); `Hop#bytesRead()` / `bytesWritten()` count the pipe bytes for metrics.

## Notes from 099 (engine switch and routing, 2026-10-13)

- Done already: per-stream request lines (with the route, `-` for refused and malformed
  heads, the 421 line, the `COMPRESSION_ERROR` line), the first leg's connection id, the
  `trackedStreams` meter (`H2DemuxConnectE2ETest` needs it), `misdirectedForHttp1`, and
  `sluice_h2demux_connections_total{engine}`. A leg leaves the route's map when its channel
  closes and a closed or GOAWAY'd leg takes no new stream (`takesStreamsLocked`).
- Missing: leg slots (`ConnectionLimits#tryAcquireLeg`; every leg opens on the
  connection's slot today), the byte counters (`sluice.tunnel.bytes`, `access.bytes`), the
  reset / refused / legs-failed meters. The admission is released once every leg's
  `VirtualConnection#afterQueued` ran (`settle`).
- Excluded for netty naming this step: `H2DemuxLegLimitE2ETest`. Not parameterized (mixed
  classes): `QueuedFramesHoldSlotsE2ETest`, `ConnectionReleaseE2ETest` (its
  `theEndOfAnH2RelayReleasesItsSlot`).
