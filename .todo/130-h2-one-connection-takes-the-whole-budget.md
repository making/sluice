Difficulty: Medium

# h2 demux: one connection can take the whole shared queue budget

Found while raising the relay's windows to 1MiB (todo 127).

## Problem

The relay holds up to one stream window per stream and direction (1MiB now, 64KiB before),
and everything past a connection's 128KiB allowance is charged to the budget every h2
connection shares (`sluice.limits.h2-queue-budget`, an eighth of half the heap: about 25MiB
in production). Nothing caps one connection's share: with 100 streams a single client
connection can make the relay hold 100MiB while the budget has room
(`H2DemuxFlowControlE2ETest.compliantClientWithManyStreamsIsNotTornDown` saw 104.6MB queued
for one connection under the test JVM's large default budget). In production 25 streams of
one connection whose client reads slowly (a browser fetching large files on a slow link, or
a client that stops reading on purpose) exhaust the budget.

Memory stays bounded (the budget plus the per-slot worst case the connection limit derives).
The cost is the other connections: with the budget exhausted, every h2 connection past its
128KiB allowance has its connection windows withheld until it drains below it, so what they
hold is held near their allowance instead of their 1MiB windows (not measured), and their
new streams past the allowance are refused. With 64KiB windows one connection could hold 6.4MiB at most, so it took four
connections to do the same.

## Done when

- One connection's share of the budget is bounded (a cap per connection, derived from the
  budget, or a fair-share scheme), with tests showing a connection past its cap withheld
  while the budget has room and another connection keeping its windows.
- The interplay with the refusal and reset policies (new streams, trailers, waiting streams)
  is decided and tested.
- README says what one connection may take.
