Difficulty: Medium

# netty h2 demux, step 9: netty by default, measured and soaked

## Plan change (2026-10-13, the user)

Production is a test deployment; breaking changes are fine, and no code is written just to
keep two engines side by side. New order: 102 -> 107 (remove the legacy relay now, netty
only) -> 103, 104, 105 (in parallel, each re-enables its disabled tests on netty) -> 106.

No switch and no legacy comparison any more (the legacy relay is gone in 107): measure the
netty engine (throughput, latency, CPU and heap per connection, lock contention under many
streams), record the numbers, report anything the user should weigh, and watch production.

Step 9 of the netty h2 demux migration (context and user decisions: 098). Depends on 105.

## Scope

- Measure both engines before switching: throughput, latency, CPU and heap per connection,
  lock contention under many streams (the spike measured none). Record the numbers here;
  a regression the user should weigh is reported before the switch.
- Switch the default to netty; CI green, then the auto-deploy; watch production (console,
  metrics, logs) for a soak period agreed with the user.
- If lock contention or misuse shows, file the hardening step (a custom `Channel` on a
  netty event loop, option 2 of the I/O model in `.todo/artefacts/096/FINDINGS.md`).

## Notes from 098

- Cost per hop: a reader and a writer virtual thread (plus a short-lived closer at close),
  one ticker thread per loop (it sleeps until a timer is due), and one heap copy of every
  byte written (the ByteBuf confinement). Measure these.

## Notes from 099 (engine switch and routing, 2026-10-13)

- Both faces encode header blocks as never-indexed literals (`configure` in
  `NettyHttp2DemuxRelay`), so every block decodes on its own as with the legacy relay:
  many test peers decode each block with a fresh HPACK decoder. HPACK's dynamic table
  would save bytes on the tunnel; weigh it in the measurements (the test peers would need
  a decoder per connection first).
- `sluice_h2demux_connections_total{engine}` tells which engine served the connections
  during the soak.
