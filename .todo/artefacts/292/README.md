# The CPU shares, measured (todo 292)

What `.kb/wicket-firecracker-findings.md` ("The CPU budget and the shares") was measured with: a
jailed wicket on `--listen`, `oha`, on the 64-core host. Needs `sudo -n`, `oha`, a static `busybox`,
`firecracker` in `~/.local/bin`, the kernel in `~/.local/firecracker`, and the release build and
sample guest of `sluice-wicket-runtime-firecracker` (`cargo build --release`, `scripts/build-guest.sh`).

```text
B=../../../sluice-wicket-runtime-firecracker/target/release/sluice-wicket-runtime-firecracker
./bench.sh $B clones 'mode=per-connection&warm=16' -c 16 --disable-keepalive   # a route's rate
./spin-rootfs.sh                                                               # -> spin.ext4
S=$PWD/spin.ext4
LAYOUT=own ./share.sh $B shares 0,1 "a.local=$S?vcpus=2&cpu=200@0" "b.local=$S?instances=6@0"
LAYOUT=own ./share.sh $B shared 0,1 "a.local=$S?vcpus=2@0" "c.local=$S?mode=per-connection&warm=4@0"
LAYOUT=own UNKNOWN=0 SHOW=". $PWD/threads.sh" ./share.sh $B own 0,1 "a.local=vcpus=1@0" "b.local=$S?instances=8@0"
```

- `bench.sh`: one route's rate, 5s a run. Alternate the builds compared: the spread of runs is the
  noise (a booted VM's keep-alive rate 15-22k).
- `spin-rootfs.sh`: a guest spinning every vCPU, so a VM takes all it is given.
- `share.sh`: the CPUs each VM's cgroup and the wicket's process took, the wicket pinned to a few
  (`taskset`: its VMs with it). `LAYOUT=own` runs it in a delegated scope, the keeper's leaf beside
  the VMs' cgroups; without, `--jail-cgroup`, where the wicket is in the caller's session with `oha`
  (a share of its own that says nothing).
- `threads.sh`: the wicket's threads under a load of its own, how long each waited for a CPU.

Two variants were builds of their own:

- a group of the cpu controller for the clones per connection too: `route.rs` without
  `Snapshot::of_connections` (8% of the clone rate at c=16)
- the shared group's weight written at every spawn and reap: `SHARED_WEIGHT_INTERVAL` zero in
  `keeper.rs` (3%)
