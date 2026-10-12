Difficulty: High

# netty h2 demux, step 1: production hop and loop

First step of the migration of the h2 demux to netty's HTTP/2 codec, decided on 2026-10-12
from the findings of 096 (`.todo/artefacts/096/FINDINGS.md`; the spike is beside it).
Steps: 098 to 107, in order; `sluice-it` stays green after each.

User decisions (2026-10-12): migrate in steps behind an engine switch; accept per-hop
semantics (the upstream no longer sees the client's SETTINGS or stream WINDOW_UPDATEs,
padding is dropped, netty's error codes where they differ from earlier decisions, no
half-close and grace after an error GOAWAY); I/O model: `EmbeddedChannel` driven by our
virtual threads, one lock per relay.

## Scope

- Production `Hop` / `Loop` in `sluice-server` from the spike: one `Http2ConnectionHandler`
  per hop in an `EmbeddedChannel`, every channel operation of a relay under one lock.
- ByteBuf confinement: no ByteBuf crosses the lock (heap copy under the lock). The spike
  showed silent payload corruption otherwise (netty 4.2 adaptive allocator).
- Writability feedback from the writer queue, a ticker for scheduled tasks, an exception
  sink.
- Pin the netty version and note in the code where behaviour depends on it.
- Native image: `queryAllPublicMethods` entries for our handlers in `reflect-config.json`;
  one `-Pnative` build and a smoke run.

## Tests (watch each fail first)

- A CRC stress test: many concurrent uploads and downloads over two legs; it fails with
  the zero-copy writer.
- Netty's paranoid leak detector on in these tests; a deliberate leak fails the suite.
- Writability: a client not reading its socket stops the upstream (bounded read).
