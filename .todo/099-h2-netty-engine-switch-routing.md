Difficulty: High

# netty h2 demux, step 2: engine switch, routing and the parameterized suite

Step 2 of the netty h2 demux migration (context and user decisions: 098). Depends on 098.

## Scope

- A server setting selecting the h2 demux engine, legacy or netty, default legacy (name it
  after the existing `sluice.limits.*` / `sluice.*` conventions). `DataProxyServer` builds
  either relay.
- The netty relay feeds the seed head (bytes `DataProxyServer` already read) first and
  keeps the first route resolved once.
- Per-stream routing and leg choice, one leg per route, stream ids kept; rewrite-host
  (edit `Http2Headers`); the no-route / forbidden pages through netty's flow controller;
  TLS-terminated h2. Header list limit 64KiB (netty's default is 8KiB).
- Parameterize the h2 E2E classes in `sluice-it` by engine. A class or method that the
  netty engine does not support yet is excluded for netty only, with the step that adds it
  named in the exclusion; legacy runs everything.

## Tests

- The routing, rewrite-host, page and TLS h2 subsets pass on both engines.
- A test pins that the setting actually selects the netty relay.
