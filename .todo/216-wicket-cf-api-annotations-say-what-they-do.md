Difficulty: Low

# sluice-wicket-cf-api: an annotation changes only what it names

- **`keep-ms` turns a pool's `warm` to 0.** `vm_sandbox` (`controller.rs`) has `warm` 0 when `keep-ms` is given
  on a `pool`, so annotating the wicket's own default, `keep-ms: 60000`, takes a pool from N VMs to none. The
  wicket itself keeps `warm` at `vms` with a `keep-ms` (`route.rs`). Ask for `warm` explicitly: `keep-ms` on a
  pool with no VM beyond `warm` does nothing, so refuse it naming `warm`, rather than guess. Same defaults as
  the wicket's otherwise
- **A manifest shared across both wickets.** A Firecracker app annotated `budget-ms` is refused at its next
  start (`api.rs`, `CRASHED`), though `cf push` said nothing: refuse it at the push, as the rest are

- **A refused `PATCH /v3/apps` changes nothing** (fixed in todo 213) was never seen failing: undo the fix and see
  its step in `tests/cli.rs` fail, or make it fail

The test: `vm_sandbox`'s table in `controller.rs` and the refusals in `tests/cli.rs` (the step scaling to zero
annotates `keep-ms` alone: it takes a `warm: 0` too), seen failing first. Do this after todo 215, which
settles `warm`'s default on both wickets.
