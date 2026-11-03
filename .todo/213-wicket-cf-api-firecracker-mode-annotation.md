Difficulty: Medium

# sluice-wicket-cf-api: `cf push` choosing a Firecracker VM's mode by annotation

An app's mode cannot be chosen: `vm_sandbox` (`controller.rs`) derives it from the web process's
`instances` alone (0 `per-connection`, 1 `shared`, more `pool`), and the README's table has annotations at
"no effect" on this wicket. `reuse` cannot be reached at all, and a pool of one or a per-connection app
that `cf app` shows as running neither.

Take an annotation, `wicket.sluice.ik.am/mode`, beside `budget-ms` and `instances` (`store.rs`, set from
`cf push`'s manifest `metadata` and the v3 API alike): a setting of the sandbox is what the annotations already
hold, and a label is for selecting apps. No annotation keeps today's mapping.

- settle what `instances` means under each mode: `pool` its `vms`; `shared` above 1 and `per-connection` /
  `reuse` with a count: refused or what the count becomes, the instance state `cf app` shows in step
- a value that is no mode is refused at the push, as `manifest.rs` refuses `instances: many` (that check
  is for numbers), and by the v3 API
- `reuse`'s and the cloned modes' options (`keep-ms`, `uses`, `lifetime-ms`, `warm`, `conns`): which become
  annotations, or are left at the wicket's defaults and the README says so
- a mode a disk cannot take (a disk is `mode=shared` alone) and a wasi wicket's app: what each does with
  the annotation
- a changed annotation applies as `instances` does, or from the next start: `desired` in `controller.rs`
  compares `sandbox.mode`, so check a reload and a VM's start follow it
- the instance index of `cf logs` (todo 212) under each mode
- the test: `vm_sandbox`'s table in `controller.rs` and the firecracker steps of `tests/cli.rs`, seen failing
  first. The cf-api's README table and the wicket's README in step
