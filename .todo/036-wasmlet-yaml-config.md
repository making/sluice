Difficulty: Medium

# sluice-wasmlet: YAML config file

The `--wasm host=locator?query` flag packs the sandbox into a URL query
(`wasmlet/src/main.rs` `parse_route_target` / `Sandbox::parse`): literal `?` in
paths needs escaping, the flag grows unreadable, comments and env references are
impossible. Move the whole configuration to a YAML file.

## Shape

`--config wasmlet.yaml` (new flag) plus a default `wasmlet.yaml` lookup? Decide
the lookup rule; at minimum `--config <path>`.

```yaml
server: grpc://127.0.0.1:8001
token: SECRET            # ${ENV} expansion for secrets; decide scope
id: wasmlet-1
# insecure: true
# ca-cert: ca.pem

routes:
  - host: demo.local
    component: ./hello.wasm
  - host: relay.local
    component: ./relay.wasm
    sandbox:
      wasi: [cli, inherit-network]
      budget: 250ms      # human duration, not budget-ms
      memory: 64Mi       # human size, not memory-mib
      pool: 2
```

- `server` / `token` / `id` / `insecure` / `ca-cert` remain settable as flags;
  a flag overrides the YAML value (flag > file > default). `--wasm` stays for
  one-off routes, appended after the file's routes; host duplication between
  the two sources: decide (error or file wins) and pin with a test
- `budget: 250ms` / `memory: 64Mi` human-readable forms (humantime-ish
  duration, size suffixes K/M/G with optional `i`); the old `budget-ms` /
  `memory-mib` query keys remain only in the wire-format canonicalizer
  (`Sandbox::query`), not in the YAML surface
- `Route` / `Locator` / `Sandbox` gain `serde::Deserialize` impls; the
  advertised-target canonicalization and the `Shared`/dial-address machinery
  are unchanged -- the query string survives only as the wire format
- Fail fast on unknown keys and unparsable values, same messages quality as the
  flag parser; every acceptance item gets a test that fails without it
- `--server` etc. without `--config` keeps today's behavior (flags only), so
  existing invocations in docs / scripts keep working
- Crate: `serde_yml` (serde_yaml is archived). Update the README usage block
  and CLAUDE.md only if the invocation examples change
