Difficulty: Medium

# h2 demux: streams waiting for an upstream's concurrency limit hold queue budget

Found while working on 067. A stream beyond its upstream's
SETTINGS_MAX_CONCURRENT_STREAMS waits in the relay (`Http2DemuxRelay.Leg#nextEntryLocked`
holds its head), with whatever the client sends on it queued: up to the initial stream
window (65535) of DATA each -- `releaseLocked` grants a waiting stream nothing more --,
for up to the relay's 100 streams. That counts against the connection's 128KiB allowance
and the shared `H2DemuxBudget` like any queued byte, and nothing of it drains until the
stream opens.

- With the budget exhausted, the relay withholds the client's connection window
  (`connectionGrantLocked`): an open stream of the same connection whose request body is
  unfinished cannot finish it, so it never closes on the upstream, so no waiting stream
  opens. By this reasoning the connection stalls until the client gives up (not
  reproduced yet). Before 067 the excess streams went to the upstream, which refused
  them, and the relay dropped their data.
- The wait itself is unbounded: as long as the upstream keeps its streams open (or keeps
  a limit of 0), the waiting streams and their bytes stay. Same class as an upstream that
  never answers (no bound either). The stall timeout (071) bounds the client side only:
  it does not count a waiting stream whose client spent its window, nor the stalled
  connection of the first point, whose withheld window leaves the client nothing to send;
  a waiting stream whose client still holds window and sends nothing is cut by it.

## Direction

Options, not decided:

- Refuse (`REFUSED_STREAM`) a new stream that would wait while the connection is past its
  allowance (or past N waiting streams per leg), instead of queueing it; the client
  retries it later or elsewhere.
- Exempt the DATA of open streams from the withholding (per-stream accounting of who
  holds the budget) -- larger change to the budget model.

## Tests (`sluice-it`; watch each fail first)

- Budget small (`sluice.limits.h2-queue-budget`), upstream limit 1: stream 1 open with an
  unfinished POST, streams 3..N waiting with their initial windows filled until the
  budget is exhausted; stream 1's body must still complete (by the reasoning above it
  stalls today; not reproduced yet), and the waiting streams then go.
