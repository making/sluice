Difficulty: Medium

# sluice-wicket-cf-api: `cf push` choosing a Firecracker VM's mode by annotation

An app's mode cannot be chosen: `vm_sandbox` (`controller.rs`) derives it from the web process's
`instances` alone (0 `per-connection`, 1 `shared`, more `pool`), and the README's table has annotations at
"no effect" on this wicket. `reuse` cannot be reached at all.

The user's direction: **`cf scale` sets the upper limit, annotations set the lifetime.**

- `cf scale -i N` is the route's `vms=N`, the cap of VMs, under every cloned mode
- annotations beside `budget-ms` and `instances` (`store.rs`, set from `cf push`'s manifest `metadata` and
  the v3 API alike; a label is for selecting apps): `wicket.sluice.ik.am/mode`, and how long a VM lives:
  `keep-ms`, `uses`, `lifetime-ms`. With none annotated, 1 stays `shared` and more `pool`
- `cf scale -i 0` starts nothing and serves nothing, as on CF (a cap of 0), not today's VM per connection
  (`vm_sandbox`: "none still serves, as on a wasm wicket"; the user finds it wrong that 0 still starts
  one). Per connection is reached by the annotation, with a cap from 1. Follow what `cf stop` does to the
  wicket's route and what a request then gets, and check it against CF; `cf app` shows `0/0` and `cf push`
  of `instances: 0` waits for no instance. An app already at 0 stops serving on the upgrade: the README says so

To settle:

- the wasi wicket has the same 0 (the README's table: "0 still serves", a request getting an instance of
  its own), where it is how an app has no warm instance. Change it the same way, with its scale to zero
  kept by an annotation, so both wickets mean one thing by 0

- a pool's `warm` defaults to `vms`: a fixed size, no VM beyond `warm`, so `keep-ms` never applies and N is
  a floor as much as a cap. For N to be a limit, what does `warm` become when a lifetime is annotated: an
  annotation of its own, or 0 once a lifetime is set (scaling to zero, a cold request paying the restore)
- `shared` annotated with `instances` above 1: refused, or the VM still one; the instance state and count
  `cf app` shows when VMs come and go below the cap
- an annotation its mode does not take (`uses` and `lifetime-ms` are `reuse`'s, `keep-ms` `reuse`'s and
  `pool`'s): refused, not ignored. A value that is no mode or no number is refused at the push and by the
  v3 API (`check_annotations` in `manifest.rs` holds numbers only)
- a mode a disk cannot take (a disk is `mode=shared` alone) and a wasi wicket's app: what each does with
  the annotations
- a changed annotation applies as `instances` does, or from the next start: `desired` in `controller.rs`
  compares `sandbox.mode`, so check a reload and a VM's start follow it
- the instance index of `cf logs` (todo 212) under each mode
- the test: `vm_sandbox`'s table in `controller.rs` and the firecracker steps of `tests/cli.rs`, seen failing
  first. The cf-api's README table and the wicket's README in step
