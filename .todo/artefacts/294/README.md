# The memory budget, measured (todo 294)

What `.kb/wicket-firecracker-findings.md` ("The memory budget") was measured with: a wicket on
`--listen` in a delegated scope of the user's manager (its VMs in cgroups of its own), the state
dir on `$XDG_RUNTIME_DIR`'s tmpfs, on the 64-core host. Needs `oha`, `firecracker` in
`~/.local/bin`, the kernel in `~/.local/firecracker`, a user manager (`systemd-run --user`), and
the release build and sample guest of `sluice-wicket-runtime-firecracker` (`cargo build --release`,
`scripts/build-guest.sh`). Run them in a directory of their own: they write `wicket.log`,
`wicket.pid` and the rootfs images there.

```text
A=$PWD
mkdir /tmp/m && cd /tmp/m
$A/process-rootfs.sh                    # -> process.ext4: its /oom takes all the guest's memory
$A/charge.sh 512                        # a clone's cgroup and its route's snapshot
$A/charge.sh 512 boot                   # a booted VM's cgroup
$A/full-tmpfs.sh                        # the state dir's tmpfs full under a clone (sudo)
$A/own.sh pool 'mode=pool&instances=4' -c 256
$A/own.sh clones 'mode=per-connection&warm=64' -c 64 --disable-keepalive
$A/storm.sh 16                          # the VMs killed and not gone yet, under a storm of clones
```

- `charge.sh`: before the clones' cgroups were their route's own and allowed for the snapshot,
  the clone of 512 MiB ended by the host's OOM killer (`git show 60b0d140`: the build before).
- `own.sh`: what the wicket itself takes, the reserve's reason.
- `storm.sh`: the children in state `D` are `firecracker`s killed, waiting in KVM's teardown.

The balloon (`balloon.patch`: a build of its own, the device added to every VM booted where
`SFC_BALLOON` is set, `reporting` for free page reporting; not in the wicket):

```text
(cd ../../.. && git apply .todo/artefacts/294/balloon.patch) && cargo build --release
export BIN=.../target/release/sluice-wicket-runtime-firecracker
$A/churn-rootfs.sh                      # -> churn.ext4: /cgi-bin/churn?300 takes 300 MiB and frees them
for mode in none balloon reporting; do $A/balloon.sh $mode; done
for mode in none reporting; do $A/balloon-clone.sh $mode; done
$A/rate.sh 'vcpus=2' -c 16
$A/rate.sh 'mode=per-connection&warm=16' -c 16 --disable-keepalive
```
