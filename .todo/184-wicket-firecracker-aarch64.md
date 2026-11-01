Difficulty: Medium

# sluice-wicket-firecracker: aarch64

The wicket, its guest and the CI run on x86_64 only. Firecracker supports aarch64 hosts
(Graviton, Ampere); what changes:

- The guest: build `/init` for `aarch64-unknown-linux-musl` (`scripts/build-guest.sh`,
  `scripts/cnb-rootfs.sh` pick the target by the host or a flag), an aarch64 guest kernel
  from Firecracker's CI builds
- The clones' clock: `clock_realtime` on snapshot load is x86_64 only (it advances kvmclock;
  `src/vm.rs` also boots with `clocksource=kvm-clock`). On aarch64 the restore notice
  (`wire::CONTROL_PORT`) has to carry the host's time and `/init` set the clock, or
  Firecracker's vmclock device (once released) has the guest resync
- VMGenID and the vsock device on aarch64 Firecracker: check a clone's random state and
  the half-close framing as `tests/vm.rs` does
- CI: an aarch64 runner exposing `/dev/kvm`, or the job stays x86_64
