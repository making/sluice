Difficulty: High

# netty h2 demux: header blocks across hops are charged but not refused

Found while bounding the netty engine's queue (102). Needed before the netty engine is
the default (106).

## Problem

The netty relay charges every header block it holds to the queue budget and refuses a
client's new stream or resets a stream whose trailers do not fit, but nothing bounds the
header blocks an upstream sends:

- netty takes any number of interim (1xx) heads on a stream before the final one; each
  is written to the client hop, whose writer queue grows past `HOP_QUEUE_BYTES` while
  the client does not read (only that hop's own reads pause on its queue, not the leg's
  reads that make it grow).
- A block is decoded on one hop and re-encoded as never-indexed literals on the other:
  a peer referring to its dynamic table sends a few bytes per field, the relay writes
  the whole field. One 16KiB read of such blocks can make megabytes towards the other
  hop (each block up to the 64KiB header list limit). This holds for the client's
  request heads too, where only the admission (refusal past the budget) stops it, one
  head at a time.

So `UNBUDGETED_BYTES` / `RESPONSE_QUEUE_BYTES` do not hold against an upstream sending
1xx floods or table-referencing blocks; README says so ("never refused").

## Directions

- Pause a hop's reader while the hop it writes to is past its read pause (a cross-hop
  read pause), so heads back-pressure like DATA does through windows.
- Cap interim heads per stream (a stream error past N).
- Announce `SETTINGS_HEADER_TABLE_SIZE` 0 to the upstreams (bounds the amplification to
  the static table), weighing the bytes it costs on the tunnel; or charge the
  re-encoded size before writing and refuse / reset past the budget.

## Tests

- An upstream flooding 1xx heads at a client that does not read: what the relay holds
  stays within the derived bounds (and a negative control without the fix passes them).
- Table-referencing request and response heads: the bytes queued per read stay bounded.
