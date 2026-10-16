Difficulty: Medium

# h2 demux: the hop writer queues are most of what an h2 connection reserves

Split from 135 (2026-10-17).

## Problem

Since 135 an h2 connection reserves 1127KiB besides its virtual connection
(`ConnectionLimits.h2RelayBytes`): its allowance (128KiB), per hop what a peer still sends
uncharged (the 16KiB read under way, RFC 9113's 65535 bytes of window, 100 granules of
backlog rounding) and per hop a writer queue bound of 320KiB (`HOP_QUEUE_BYTES`: the read
pause plus 64KiB). The two writer queues are 640KiB of it, and each further leg reserves
another 320KiB. They sit outside the budget because what reaches a queue (netty's own
answers, header blocks past the read pause, DATA up to the writability mark) is charged
only after the fact.

## Direction

- Measure what a hop's writer queue holds in practice (the largest under the 106 harness
  and the budget tests) against the 320KiB bound.
- Either lower the read pause / water marks (with the throughput they cost measured at a
  20ms RTT), or charge what passes the writability mark to the budget before it is written,
  so the reservation keeps only a small fixed part per hop.

## Tests

- The derivation in `ConnectionLimitsTest`, and a stack test (as
  `H2DemuxWindowCreditE2ETest`) whose held bytes plus credit stay within the budget plus the
  new reservations.
