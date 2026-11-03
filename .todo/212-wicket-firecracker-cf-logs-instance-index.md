Difficulty: Medium

# sluice-wicket-runtime-firecracker: `cf logs` names instance 0 however many VMs an app has

Reported on a real deployment: after a scale out, `cf logs` still shows every line as instance 0.

The cause, read from the code: a VM's lines reach the flume with the fields `vm` and `line`
(`Printer::line` in `output.rs`), no `instance`. The cf-api takes a line without one as 0 (`instance(&event).unwrap_or(0)`
in `sluice-wicket-cf-api/src/flume.rs`, "a wicket predating the instance index"). The wasi wicket sends it
(`wasm_host.rs`, `.kb/live-access-log.md`), and `cf scale -i 2` is tested on it (`tests/cli.rs`:
`[APP/PROC/WEB/1]`). The cf-api's README states the firecracker gap: "`cf logs` | the process's output,
instance 0". The fix has not been run yet.

- `instance` on a VM's `stdout` / `stderr` as `.kb/live-access-log.md` defines it: the index among the
  route's instances alive, a pool's warm ones `0` to `pool - 1`, one of its own the lowest free from
  `pool` up. `vm` is no such index: it names the VM in the wicket's run
- settle what each mode names: a shared VM (0), a pool, a clone per connection (`instances: 0`), and a
  pool's VM replaced after it ends or on a reload
- what `cf app` / the process stats show for the instances, if they disagree with the logs' index
- the test: the firecracker steps of `sluice-wicket-cf-api/tests/cli.rs` (`[APP/PROC/WEB/0] OUT GET /log`)
  extended with a `cf scale -i 2` and a line of instance 1, as the wasi steps do, seen failing first
- the README's table of the cf-api, the wicket's README and `.kb/live-access-log.md` in step
