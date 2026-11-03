Difficulty: Medium

# sluice-wicket-runtime-firecracker, sluice-wicket-cf-api: `mode: boot`, N VMs booted with no snapshot

`shared` refuses `-i 2`. Nothing requires that but a disk's one writer: a route's `Backend::Shared` holds one VM, and
only a pool spreads connections over several. So a process that cannot survive a restore (what it opened before its
snapshot is gone in a clone) cannot scale out, where `-i 3` is ordinary on CF. `.kb/wicket-firecracker-findings.md#a-scale`
weighed `-i 1` as a pool, not this.

The user chose the name: `shared`, one guest for all connections, does not fit several. No migration path.

- **`mode: boot`** replaces `shared`: `-i N` keeps N VMs, each booted, none restored. A pool but for how a VM starts:
  reuse its spreading (the VM holding the fewest) and its replacing rather than a third kind of VM. Remove `shared` from
  the annotations, the routes' `mode=` and the docs
- **refused**, naming it: `warm`, `keep-ms` (a boot is too slow to ask for one on demand; N are kept) and `concurrency`
  (with none grown past N it decides nothing), as a mode's options are refused today
- **a disk** needs `vms=1`, refused above: one writer, its `flock`. The cf-api's own VM runs so
- **a scale in place**: up boots one more and touches none of the others, connections reaching it once its guest
  accepts; down stops the highest indices after their connections. `cf scale` returns without waiting for the boot's
  12s of a CNB Java app; `cf app` lists what is alive
- **one going down**: the others serve on, its connections alone cut. Replaced with the backoff a shared VM has now
  (1s, 2s, 4s up to 60s, anew after 60s up), per VM. `cf app` shows `CRASHED` with why for that index only, and a route
  with all down fails its connections as a shared VM's does (`Route::down`, `vms.down`)
- **the budget**: N x `mem` in `--max-vms` / `--max-memory`, each VM holding its room through a replacement as a shared
  VM does
- decide and report: when `cf push` and `cf start` return (the first VM accepting, or all N), against what CF does

The test: extend the shared steps of `tests/vm.rs` (two VMs serving, one killed, a scale up and down, the refusals) and
the cf-api's unit and CLI tests. The README's tables of modes, `.kb` (the numbers against a pool: a VM's memory, a
replacement's time) and `CLAUDE.md`'s line on the wicket.
