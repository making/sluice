Difficulty: Medium

# sluice-wicket-cf-api: `cf app` says healthy while a pool cannot restore, and one `since` for all

Left by todo 217, read from its report and not reproduced here.

- **a pool's restore failing is not `down`**: a pool below its `warm` because a restore keeps failing (no room in
  `--max-vms` / `--max-memory`, a snapshot gone) shows `k/k`, or `0/0` as an app kept at none does. A shared VM
  going down and a pool's VM exiting already show `CRASHED` with the reason (`Route::down`, `console.rs`): a failing
  restore should too. Mind what is not a failure: a VM started beyond `warm`, a connection merely waiting for room
  within `wait-ms`, a restore that retries and succeeds
- **each instance's `since`**: `stats` (`views.rs`) gives every entry the app's last start (`running.since`), so
  `cf app` shows a VM cloned a second ago, or one replaced, with the app's uptime. The wicket's listing gives the
  indices alive: give each its start, on both wickets (a wasi unit's, a VM's restore)

The test: the pool steps of `tests/vm.rs` (a restore failing, then succeeding, read through the listing's `down`), and
`stats`' unit test in `views.rs`, seen failing first.
