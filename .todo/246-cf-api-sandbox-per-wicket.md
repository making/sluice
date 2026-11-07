Difficulty: Medium

# sluice-wicket-cf-api: one sandbox type per wicket, not both wickets' fields in one

## Now

`wicket.rs` `Sandbox` is the app API body of both wickets in one struct. The wasi wicket names its
fields in snake_case (`budget_ms`, `memory_mib`, `keep_ms`), the Firecracker wicket as its route
options, in kebab-case (`keep-ms`, `lifetime-ms`, `boot-ms`). Both now have a keep time, so the
struct carries `keep_ms` (serialized `keep-ms`, the Firecracker one) beside `wasm_keep_ms`
(serialized `keep_ms`, the wasi one): two fields apart by a dash, and nothing but doc comments
keeps a component's settings out of a VM's body and back. `controller.rs` (`desired`, `same`)
reads and writes the one struct for either.

## Wanted

The type says which wicket a body is for: a sandbox per wicket (or an enum of the two), each with
its own field names, so a field of the other wicket cannot be set and the keep time is `keep_ms`
in both. Weigh, too, one naming in both wickets' app APIs (the wasi wicket taking kebab-case as
its route options do, or the reverse, old names still read for a while): then the cf-api needs no
renames at all.

## To weigh

- Mixed versions: a cf-api and wickets updated apart (docker compose, by hand); a renamed field
  must be read in both spellings until both sides have it.
- `same` compares a listed body to a desired one per wicket; what the listing omits at its default.
- Tests: extend `controller.rs`'s existing ones; the CLI test covers both wickets.
