Difficulty: Low

# wickets: bounds on a route's / an app's resource options

- Firecracker: a route's `vcpus`, `memory-mib`, `cpu`, `disk-mib` have no upper bound, and 0 passes to
  Firecracker (`route.rs`); only a build's request is checked (`stage.rs`: `within`), and its ceilings are
  1 TiB of memory and of scratch. The guest's `/tmp` and `/dev/shm` are tmpfs with no size
  (`guest/src/main.rs`): bounded by the guest's RAM only.
- wasi: `memory-mib`, `budget-ms`, `concurrency`, `keep-ms` have a floor of 1 and no ceiling
  (`wasm_host.rs`, `console.rs`).
- Neither has a ceiling per owner: one owner of the console may ask for the whole budget.

Give each option a range checked where it is parsed (config and console alike), with operator-set ceilings
(`--max-app-memory-mib` or the like, to decide with 300, beside `--max-cpus`; the Firecracker
wicket's memory budget is by default what the host has, 294) reported in the listing's identity.
Extend the existing option parsing tests; READMEs' option tables updated.
