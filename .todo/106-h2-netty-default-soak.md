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

## Notes from 102 (flow control and budget, 2026-10-13)

- Copies per forwarded DATA byte now: the reader's copy into the channel, the relay's
  copy at the bridge (`Flow`: the first write a copy, later ones appended to the stream's
  backlog buffer), the writer queue's heap copy. Measure their cost.
- While a relay queues anything, each writer batch of its hops takes the loop's lock once
  more (`Hop#watchWrites`, for the budget review and the route gauge); and the review
  runs after every loop action. Measure the contention with many streams.
- The derived connection limit follows the netty bounds now, whatever the engine (3.26MiB
  per slot against the legacy 2.67MiB: 53 instead of 65 connections for a 400MiB heap,
  `ConnectionLimitsTest`); the read pause (256KiB) and the per-hop stream window are the
  levers if that is too few.
- 114 (header blocks across hops are not refused) should be done before the switch.

## Notes from 107 (legacy relay removed, 2026-10-13)

`sluice_h2demux_connections_total{engine}` is gone with the engine switch (the 099 note
above no longer applies).

## Notes from 103 (timeouts, half-close drain, probe, 2026-10-13)

- The h2 demux now has the idle and stall timeouts, the half-close drain and its PING
  probe (`half-close-probe-interval`). Watch in the deployment: `sluice_data_connections_idle_closed_total`
  and `_stall_closed_total` for h2 connections, and half-closed clients held by an
  upstream that does not take the request rest after its response ended (the drain waits
  for it; the client connection idles under `idle-timeout` meanwhile, untested).
