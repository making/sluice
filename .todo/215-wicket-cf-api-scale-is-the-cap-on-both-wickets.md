Difficulty: High

# sluice-wicket-cf-api: `cf scale -i N` means N CF instances on both wickets

The user's decision: `cf scale` sets the upper limit on the wasi wicket too. And a limit that serves one request
at a time at the `instances: 1` of most manifests is not acceptable: the spec below is the common-sense one.

Today only a Firecracker app's cloned modes have a limit by `cf scale`. On wasi, `instances` is the route's
`pool`, the limit the annotation `wicket.sluice.ik.am/instances`, unset bounded by `--max-instances` alone
(`Controller::desired`). `-i 1` with `warm: 0` runs any number of instances while `cf app` says `1/1`.

## The spec

**An instance is what CF calls one: it takes many requests at once.** The limit is N instances, each taking
`concurrency` at once, as Cloud Run's maximum instances and concurrency.

| | `-i N` limits | at once |
|---|---|---|
| wasi | `N x concurrency` slots (the route's `instances`); `pool` stays N unless `warm` says | `N x concurrency` requests |
| Firecracker `shared` (N = 1) | 1 VM | any |
| Firecracker `pool` | N VMs (`vms`), `concurrency` the route's `conns` | as the wicket's `conns` has it |
| Firecracker `per-connection`, `reuse` | N VMs (`vms`): the memory is N x `mem`, as CF's quota counts it | N connections |

- the annotation `wicket.sluice.ik.am/concurrency`, from 1, default 16 (the wicket's `conns`): taken by wasi and
  `pool`; `shared` has none, and `per-connection` / `reuse` are 1 by definition, so both refuse it. A wasi app
  wanting one request at a time per instance annotates 1
- `-i 1` with no annotation behaves as a CF app: a wasi app takes 16 requests at once, a Firecracker app is a
  shared VM. No annotation is N instances kept up, as on CF: the limit only differs from the count below
  it where `warm` or `keep-ms` ask for that
- `per-connection` and `reuse` keep N literal: a VM is memory, and whoever annotates them chose one connection
  per VM. Their README example names a real count, and `-i 1` is one connection at a time, so say it where
  it is met: a warning on `cf push` / `cf scale` (`X-Cf-Warnings`, which the CLI prints, if it reads so), and the
  502 of a connection finding no room
- the annotation `wicket.sluice.ik.am/instances` goes, refused saying `cf scale -i` and `concurrency` replace it
- `cf logs`' index stays in `0` to `N - 1`, what `cf app` lists: more units alive than N are instances' work, so
  a wasi unit past N names `index % N` (settle whether the wicket or the cf-api maps it). A pool's and a VM's
  index stay as they are: they are within N already
- a limit above the wicket's own runs, bounded by the wicket: N x 16 past `--max-instances` is no refusal

The user wants the ideal spec and does not hold back a change for what exists today: no compatibility path, no
upgrade notes, and the notes the README has on apps at 0 instances go. If building this shows a better spec,
take it and say so.

## To settle

- `pool`'s `conns` is soft (past it a connection goes to the VM holding the fewest when no VM can start), wasi's
  limit hard (it waits within the budget, then 503). One meaning of `concurrency`, if the two can have one
  without an idle keep-alive connection holding a VM's share; otherwise the README says each
- `wait-ms`, `prime`: annotations or not, by whether anything here needs them

The test: `vm_sandbox`'s table and `desired` in `controller.rs`, the scale steps of `tests/cli.rs` on both wickets:
a wasi app at `-i 1` answering requests at once, and limited at `-i 1` with `concurrency: 1`, seen failing first.
The cf-api's README tables, "How long a VM lives", the upgrade notes and `.kb/live-access-log.md` in step. Mind todo
214 (the scale's check) and todo 216 (`warm`'s default), which touch the same lines.
