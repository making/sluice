Difficulty: Medium

# QueuedFramesHoldSlotsE2ETest: "every relay ended" timed out once on Linux

`QueuedFramesHoldSlotsE2ETest#theSessionEndFreesTheSlotsOfQueuedFrames` failed once in a
full sluice-it run in a Linux container (eclipse-temurin:25-jdk, arm64, `--cpus 4`, a
macOS verify running on the host meanwhile; 2026-10-12): the await "every relay ended"
(`session().connectionCount() == 0`) passed its 10s. Not seen on CI so far; 6 runs of the
class alone in the same container passed.

Each filler connection writes one full window (`VirtualConnection.DEFAULT_WINDOW`) and
half-closes while the stalled client reads nothing: whether the relay ends depends on the
whole window fitting the initial credit and the sender's per-connection cap before the
stream stops being ready. The run's log was not kept.

## Direction

Dump the timeline first (per filler: bytes relayed, credit left, whether the relay's
writer is parked in `awaitCredit` or at the sender cap, the stream's readiness) when the
await fails, then decide between a precondition the test cannot guarantee (the tunnel
stream turning not ready before the last bytes are queued) and a relay that does not end
on its client's FIN while its frames wait.
