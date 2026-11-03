Difficulty: Low

# sluice-wicket-runtime-firecracker: a restore failing is tried every second for as long as it fails

A route below `warm`, and since todo 225 one at `warm: 0`, retries a failing restore every second: a Firecracker started
and a line on stderr each time, however long its cause lasts (a snapshot gone, no room). Over a night that is tens of
thousands of each, per route (`.kb/wicket-firecracker-findings.md#a-scale` records the cost as accepted).

- back off as a shared VM's start does (1s, 2s, 4s up to 60s; [A shared VM going down] in the README), anew after a
  success. Todo 231 keeps that backoff per VM for `mode: boot`: share one implementation
- what that costs: `CRASHED` ends up to 60s after its cause did. Decide what starts a try at once instead of waiting: a
  connection arriving (a per-connection route's tries its own restore anyway), `cf scale`, a reload
- `since` stays when the failure began; only `why` changes, as it does now
- a connection that waits for room within `wait-ms` must not wait on the backoff

The test: the steps of `below_warm` in `tests/vm.rs`, the backoff's values as a unit test with no waiting. Keep the VM test
from taking the backoff's whole 60s. The README's `cf app` bullet says "every second"; correct it, and `.kb`.
