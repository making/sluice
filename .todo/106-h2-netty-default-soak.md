Difficulty: Medium

# netty h2 demux, step 9: netty by default, measured and soaked

Step 9 of the netty h2 demux migration (context and user decisions: 098). Depends on 105.

## Scope

- Measure both engines before switching: throughput, latency, CPU and heap per connection,
  lock contention under many streams (the spike measured none). Record the numbers here;
  a regression the user should weigh is reported before the switch.
- Switch the default to netty; CI green, then the auto-deploy; watch production (console,
  metrics, logs) for a soak period agreed with the user.
- If lock contention or misuse shows, file the hardening step (a custom `Channel` on a
  netty event loop, option 2 of the I/O model in `.todo/artefacts/096/FINDINGS.md`).
