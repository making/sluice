Difficulty: High

# h2 demux: charge the connection-window credit peers hold to the budget

Split from 132 (2026-10-14).

## Problem

An h2 connection reserves 3048KiB besides its virtual connection (132):
`NettyHttp2DemuxRelay.UNBUDGETED_BYTES` + `RESPONSE_QUEUE_BYTES`, two thirds of it the 1MiB
connection window each peer may still send once its window is withheld
(`WITHHELD_ARRIVALS`). The budget counts only what the relay holds, not the credit its
peers hold, so the credit has to be reserved per connection; the 409MiB heap holds 34 h2
connections.

## Direction

- Account per relay what it holds plus the connection window its peers hold (the local
  window less what the hop owes), past a floor of RFC 9113's 65535 per hop; grant the 1MiB
  boost and every refill only once charged (the hop's gate already holds back the
  connection WINDOW_UPDATEs: grant them from the review), so arrivals are charged before
  they can happen. A relay whose boost does not fit runs at 65535 (the throughput before
  127) instead of being refused.
- The share and the block share then include the credit; rescale their tests.
- What stays per connection: the allowance, the floor credits, the read under way, the
  backlog rounding (100 granules per direction) and the hop writer queues (read pause +
  64KiB each), about 1.1MiB with one leg; the writer queues are the next lever.

## Tests

- The relay's held bytes plus outstanding credit within the budget plus the per-connection
  floor with many connections (stack test as `H2DemuxConnectionMemoryE2ETest`); a relay
  without a boost still completes its streams; the derivation in `ConnectionLimitsTest`.
