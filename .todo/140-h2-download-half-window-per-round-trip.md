Difficulty: Medium

# One h2 stream downloads 512KiB per tunnel round trip

Found while measuring the gRPC transport window fix (`.todo/artefacts/106/RESULTS.md`,
"After todo 138").

## Problem

With the 106 harness at a 20ms tunnel RTT, one h2 stream (`budget download h2 c1 s1`)
downloads 20.5 to 21.0 MiB/s in every stack, before and after the transport window fix,
while one h1 connection now moves 38 to 40 MiB/s through the same tunnel. 20.5 MiB/s is
512KiB per 25ms round trip: half of a 1MiB window, the figure the gRPC transport showed when
its receiver kept half its window consumed and not granted back (netty grants at every half
window read). Which window it is on the h2 path (the relay's upstream virtual connection,
whose reader is the demux; the relay's own stream / connection windows towards the
upstream hop) was not measured.

## Done when

- A timeline (frame logs of the relay's upstream hop and the tunnel's WINDOW_UPDATEs for
  that connection) names the window and why only half of it is in flight.
- One h2 stream moves about what one h1 connection moves at the same tunnel RTT, with a test
  counting round trips (as `TunnelTransportWindowE2ETest` / `H2DemuxWindowRoundTripE2ETest`
  do), or the reason it cannot is recorded.
