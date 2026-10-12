Difficulty: Medium

# h2 demux: a coalesced stream WINDOW_UPDATE capped at 2^31-1 can lose window

Found while working on 082 (by reading, not tested). `Http2DemuxRelay.Leg#queueWindowUpdate`
coalesces the client's updates of a stream into the one still queued for its leg, capped
at 2^31-1. Since 082 the relay resets a stream whose window as the upstream sees it
(`Stream#responseWindow`) would pass 2^31-1, so the queued sum stays within 2^31-1 less
the upstream's window -- unless that window is negative: a client lowering
SETTINGS_INITIAL_WINDOW_SIZE below what the upstream already sent on the stream, then
granting close to 2^31-1 in updates coalesced while the leg writer is busy, has the
excess cut. The upstream's window ends below the client's, and the stream can stall.

## Direction

Queue a second `LegEntry.WindowUpdate` instead of capping (two at most per stream: the
sum stays below 2^32-2), and count it in `MAX_QUEUED_BYTES` / `UNBUDGETED_BYTES`.

## Tests (`sluice-it`; watch each fail first)

- The updates must be queued while the leg writer is held, which no seam allows today
  (`Http2DemuxRelay.WriteSeam` covers writes to the client only): add one on the leg
  writer, then pin that the upstream receives the client's whole grant.
