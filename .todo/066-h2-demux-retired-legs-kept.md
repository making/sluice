Difficulty: Low

# h2 demux: retired legs stay referenced until the connection ends

Found while working on 057 (by reading, not tested). A leg whose upstream ended its side,
failed, or made a connection error retires (`Http2DemuxRelay#endLeg`, then
`Http2DemuxRelay.Leg#retire`; since 081 also with responses owed, and a leg its upstream's
GOAWAY ended retires once its upstream closes): it closes, releases its slot of the
connection limit (057) and its queued bytes, but stays in the relay's `legs` list, and its virtual connection stays in `TunnelSession.connections`: only
`finishRelay` calls `session.remove`. A long-lived client connection that keeps reaching
a route whose upstream closes idle h2 connections (a common idle timeout) adds one dead
`Leg` (HPACK coder state, maps, meters) and one closed `VirtualConnection` per cycle
until the client connection ends.

## Direction

On retire, remove the leg from `legs` and its connection from the session once its two
pump threads exited (they still settle the relay's `pending` count); keep `finishRelay`
idempotent for legs already removed.

## Tests (`sluice-it`; watch each fail first)

- An upstream closing after every answer, a client sending 50 requests one after the
  other on one connection: the relay holds at most one leg and the session at most one
  connection for the route (test hooks), and every request is answered.

## Retargeted (2026-10-12)

The h2 demux moves to netty (decision in `.todo/artefacts/096/FINDINGS.md`). Do not fix this
in the legacy relay; it is done on the netty engine in 104.
