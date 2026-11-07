Difficulty: Medium

# The wickets name alike what means alike: one set of option and app API names

No compatibility: old names are refused, not read; cf-api, both wickets and their configs move
together.

## Now

Since the wasi wicket's pool grows and shrinks (`keep-ms`) and its stores take several requests at
once (`concurrency`), its options mean what the Firecracker wicket's do, under other names:

| meaning | cf-api annotation | Firecracker wicket | wasi wicket |
|---|---|---|---|
| at most | `cf scale -i` | `vms` (boot: the VMs booted) | `instances` |
| kept warm, the floor | `warm` | `warm` | `pool` |
| at once per VM / store | `concurrency` | `conns` | `concurrency` |
| a spare's idle time | `keep-ms` | `keep-ms` | `keep-ms` |
| memory | `cf push -m` | `mem` | `memory-mib` |
| process-wide cap | | `--max-vms` | `--max-instances` |

The app APIs also differ in case: the wasi wicket's JSON is snake_case (`budget_ms`, `keep_ms`), the
Firecracker wicket's its route options' kebab-case (`keep-ms`, `lifetime-ms`). So
sluice-wicket-cf-api's `wicket.rs` `Sandbox`, both wickets' bodies in one struct, carries `keep_ms`
(serialized `keep-ms`, Firecracker's) beside `wasm_keep_ms` (serialized `keep_ms`, wasi's), and
`controller.rs` (`desired`, `same`, `vm_sandbox`) translates the annotations into each wicket's
names.

## Wanted

- One name per meaning in both wickets: route options (`--wasm` / `--vm` queries), config files,
  app API JSON, consoles, listings, logs and messages. The cf-api annotations' names (`warm`,
  `concurrency`, `keep-ms`) are the user-facing ones and the natural choice; pick the rest
  (`instances` / `vms`, the memory, the process-wide cap) by what reads right on both.
- One case in both app APIs' JSON, as the route options are written (kebab-case), or the reverse.
- sluice-wicket-cf-api: no field renamed (`wasm_keep_ms` gone); the type says which wicket a body
  is for (a sandbox per wicket, or an enum), so a field of the other cannot be set.

## To weigh

- Firecracker `vms` also counts a boot route's VMs (`mode=boot`): a name fitting both pool and boot.
- `instance` as the wasi wicket's unit in logs and `cf logs` indices; `--max-instances` slots.
- What each listing omits at its default (`same` compares listed to desired).
- The configs deployed by hand (docker compose, the routes files the wickets save: `deployed.rs`):
  say in the commit what to rewrite.
- Docs: both wickets' READMEs (options, pool sections, app API), the cf-api README (the mapping and
  Instances tables), `.kb`.
- Tests: rename in place; the CLI test covers both wickets.
