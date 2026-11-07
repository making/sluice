Difficulty: Medium

# sluice-wicket-runtime-wasi: a pool growing with the requests and shrinking after `keep-ms`

Start from consideration: measure what it buys, then implement it (or record why it stays as it is).

## Now

- A route's `pool=N` is fixed: N units (`wasm_host.rs`, `Pool`), each a detached task running
  `Store::run_concurrent` once with a job loop, one request at a time, each holding a
  `--max-instances` slot and one of the route's `instances` cap for its lifetime. A request beyond
  them gets a per-request instance, dropped with its request.
- sluice-wicket-cf-api maps annotation `warm` to `pool` (default `cf scale -i`), and refuses a
  component's `keep-ms` (`manifest.rs`: "no VMs"). So a component is either N units for good or,
  at `warm: 0`, cold on every request; a Firecracker `pool` has the middle: grown on demand, a VM
  beyond `warm` serving none stopped after `keep-ms`.

## Wanted

`keep-ms=N` on a route: a unit beyond `pool` (started by a request finding no idle unit, up to the
`instances` cap) stays after its request and is ended once it served none for `keep-ms`. `pool`
becomes the floor. cf-api passes `wicket.sluice.ik.am/keep-ms` for components as for a VM pool.

## To weigh

- Worth: a hello component is ~250us cold vs ~130us pooled (`.kb/wicket-wasi-findings.md`).
  Measure a guest with a costly start (a JS / Python component, a large data section) first; if the
  gap stays near 120us, record that and close this.
- Slots: idle units beyond `pool` hold `--max-instances` slots and the cap nobody reserved. A request
  finding none free must take one from such a unit (end it), as the Firecracker wicket kills a spare
  VM for a connection short of room. The startup / reload check (pools within `--max-instances`)
  stays on `pool` only.
- Turning a per-request instance into a unit is not possible as is (`run_concurrent` consumes the
  store): either start a unit for the request instead of the per-request path, or keep the
  per-request path and start the unit behind it.
- Indices (`cf logs` / `cf app`, the live access log's `instance`): units `0..pool` keep theirs;
  the ones beyond take the lowest free from `pool` up, as per-request instances do now.
- Reload: lowering `keep-ms` or `pool` ends idle units beyond at once (as `pool` down does).
- Docs: the wasi README's instance pool and options, the cf-api README's Instances table
  ("scale to zero" bullet: a component's `keep-ms`) and the annotation table of "How long a VM
  lives" (`keep-ms` taken by a component too).
