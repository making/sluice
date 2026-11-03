Difficulty: Low

# sluice-wicket-cf-api: an annotation changes only what it names

- **`keep-ms` turns a pool's `warm` to 0.** `vm_sandbox` (`controller.rs`) has `warm` 0 when `keep-ms` is given
  on a `pool`, so annotating the wicket's own default, `keep-ms: 60000`, takes a pool from N VMs to none. The
  wicket itself keeps `warm` at `vms` with a `keep-ms` (`route.rs`). Ask for `warm` explicitly: `keep-ms` on a
  pool with no VM beyond `warm` does nothing, so refuse it naming `warm`, rather than guess. Same defaults as
  the wicket's otherwise
- **What an upgrade refuses.** A Firecracker app annotated `budget-ms` or `instances` was ignored and is now
  refused at its next start (`api.rs`, `CRASHED`), as a manifest shared across both wickets would be. The README
  has the refusal, not the upgrade: say it, beside the note on apps at 0 instances

The test: `vm_sandbox`'s table in `controller.rs` and the refusals in `tests/cli.rs` (the step scaling to zero
annotates `keep-ms` alone: it takes a `warm: 0` too), seen failing first. Do this after todo 215, which
settles `warm`'s default on both wickets.
