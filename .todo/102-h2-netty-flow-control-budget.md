Difficulty: High

# netty h2 demux, step 5: flow-control isolation, bounds and the queue budget

Step 5 of the netty h2 demux migration (context and user decisions: 098). Depends on 101.

## Scope

- Flow control across hops: consume on the other hop's write, not on arrival; auto refill
  of the connection window so one held stream does not starve the others (both shown
  necessary by the spike's negative controls).
- The shared `H2DemuxBudget`: withhold `consumeBytes`; queued bytes = unconsumed bytes plus
  the remote-controller backlog. `refuseWaiting` (REFUSED_STREAM while the window is
  withheld).
- Re-derive the memory bounds for per-hop windows (`MAX_QUEUED_BYTES`, `UNBUDGETED_BYTES`,
  `RESPONSE_QUEUE_BYTES`, `ConnectionLimits.connectionBytes`) and the startup log of the
  effective budget.

## Tests

- The flow-control and budget subsets pass on netty; tests asserting end-to-end windows or
  exact connection grants are rewritten for per-hop semantics (list in `.todo/artefacts/096/FINDINGS.md`).
- Each bound has a negative control that breaks it.

## Notes from 098

- Per hop, beside the stream windows: the writer queue stops DATA at the high water mark
  (64KiB default) plus about one flow-controller pass (`HopWritabilityTest` measured under
  256KiB queued against a client not reading), and the reader pauses while more than
  `readPauseBytes` (1MiB default) is queued, which bounds what netty answers on its own
  (PING / SETTINGS acks, resets) towards a peer that does not read. Both enter the
  memory bounds. Each byte is copied once more into the writer queue (heap).
- `Hop#queuedBytes()` / `writable()` are there for the budget and the stall timer.
- The read pause could hold up a peer that stops reading until it can write, if both
  sides queue past their pause bytes at once (only frames netty sends on its own count:
  DATA stops at the high water mark). HTTP/2 peers read independently; the stall timeout
  (103) is the backstop. Untested.

## Notes from 099 (engine switch and routing, 2026-10-13)

- The relay forwards DATA as `TestBridge` does (retained slice, consumed on the other
  hop's write); nothing else of flow control is in yet.
- Excluded for netty naming this step: `H2DemuxFlowControlE2ETest`, `H2DemuxBudgetE2ETest`,
  `H2DemuxClientWindowE2ETest`, `H2DemuxLegWriterE2ETest`,
  `H2DemuxUpstreamFlowControlE2ETest`, `H2DemuxWaitingStreamBudgetE2ETest` (classes) and
  `H2ReroutingE2ETest#grantsConnectionWindowWhileRelayingRequestData` (netty's auto refill
  grants the connection window only past half of it).

## Notes from 100 (upstream error scope, 2026-10-13)

- A response the upstream completed outlives its leg: the forwarded DATA (retained slices
  of the closed leg's inbound buffer) waits in the client's flow controller until the
  client grants (`NettyH2DemuxRelayE2ETest#aResponseTheUpstreamCompletedOutlivesItsConnectionError`).
  The budget must count those bytes though their leg is gone.
