# A VM's limits without the jail, measured (todo 293)

What `.kb/wicket-firecracker-findings.md` ("The forker", and the lines of "The CPU budget and the
shares" on a wicket without the jail) was measured with: a wicket on `--listen`, `oha`, on the
64-core host. Needs `oha`, `firecracker` in `~/.local/bin`, the kernel in `~/.local/firecracker`,
a user manager (`systemd-run --user`), and the release build and sample guest of
`sluice-wicket-runtime-firecracker` (`cargo build --release`, `scripts/build-guest.sh`).

```text
B=../../../sluice-wicket-runtime-firecracker/target/release/sluice-wicket-runtime-firecracker
./bench.sh $B clones 'mode=per-connection&warm=16' -c 16 --disable-keepalive   # a route's rate
./bench.sh $B fresh 'mode=per-connection' -c 1 --disable-keepalive             # a restore per connection
SCOPE=1 ./bench.sh $B clones 'mode=per-connection&warm=16' -c 16 --disable-keepalive
../292/spin-rootfs.sh                                                          # -> spin.ext4
S=$PWD/spin.ext4
SCOPE=1 ./share.sh $B shares 0,1 "a.local=$S?vcpus=4@0" "b.local=$S?vcpus=1@0"
SCOPE=1 ./share.sh $B capped 0,1 "a.local=$S?vcpus=2&cpu=200@0" "b.local=$S?instances=6@0"
SCOPE=1 ./share.sh $B shared 0,1 "a.local=$S?vcpus=2@0" "c.local=$S?mode=per-connection&warm=4@0"
SCOPE=1 UNKNOWN=64 ./share.sh $B own 0,1 "a.local=vcpus=1@0" "b.local=$S?instances=8@0"
```

- `bench.sh`: one route's rate, 5s a run. `SCOPE=1` runs the wicket in a delegated scope of the
  user's manager: its VMs in cgroups of its own; without, where it is started (none). Alternate
  the builds compared: the spread of runs is the noise.
- `share.sh`: the CPUs each VM's process and the wicket's took, the wicket pinned to a few
  (`taskset`: its forker and VMs with it).

The launcher's column was the build before the forker (`git show 559aa74b`). `clone.c`: what a
helper's clone costs with `CLONE_INTO_CGROUP` and without. A move by `cgroup.procs` instead, in a
delegated scope:

```text
systemd-run --user --scope -q -p Delegate=yes -- bash -c '
  d=/sys/fs/cgroup$(sed -n "s/^0:://p" /proc/self/cgroup)
  mkdir $d/leaf && echo $$ > $d/leaf/cgroup.procs && echo "+memory +pids +cpu" > $d/cgroup.subtree_control
  mkdir $d/vm-0
  for i in 1 2 3; do
    sleep 100 & pid=$!
    s=$(date +%s%N); echo $pid > $d/vm-0/cgroup.procs; e=$(date +%s%N); echo "$(( (e - s) / 1000 ))us"
    kill $pid; wait $pid 2>/dev/null; sleep 1
  done'
```
