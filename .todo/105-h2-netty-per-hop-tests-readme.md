Difficulty: Medium

# netty h2 demux, step 8: the remaining per-hop test rewrites and README

Step 8 of the netty h2 demux migration (context and user decisions: 098). Depends on 104.

## Scope

- Rewrite every test still excluded for the netty engine against netty's semantics (list
  in `.todo/artefacts/096/FINDINGS.md`: settings replay and end-to-end windows, raw stubs without SETTINGS
  first, exact connection grants, error codes of 088 / 089, PING ordering, GOAWAY grace,
  `WriteSeam` races replaced by tests of the lock boundary). No netty exclusion remains.
- README: each intentional behaviour change (per-hop SETTINGS and windows, padding not
  relayed, error codes, no GOAWAY grace), stated only as far as tests show it.
