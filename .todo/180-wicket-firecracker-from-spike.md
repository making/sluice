Difficulty: High

# sluice-wicket-firecracker: from spike to a wicket

The spike (`sluice-wicket-firecracker/`, findings in its README) serves routes with
Firecracker microVMs over vsock, shared or one per connection from a snapshot, and runs CNB
images as rootfs. What a usable wicket still needs:

- One tunnel implementation: extract `sluice-wicket-wasi`'s session / supervisor / tunnel
  (ListNodes fan-out, drain, reclaims, stall timeout, connection limit) into a crate both
  wickets use, its handler a trait (hyper serving vs a byte relay). The spike's
  `src/tunnel.rs` is a minimal rewrite to replace, not to grow
- An opaque target: the server dials `host[:port]` of a non-`wasm:` target URL
  (`Router.addressOf`), so the spike keys routes by host; give `firecracker:` targets the
  verbatim treatment `wasm:` gets
- Limits: VMs alive at once, total guest memory, per-route caps; what a connection beyond
  them gets (the wasm wicket's `--max-instances` semantics)
- Snapshot lifecycle: rebuild on a rootfs / kernel / spec change, a config file with reload
  (as `--config` of the wasm wicket)
- Clone safety: user-space RNG state cloned with the memory (VMGenID reseeds the kernel only);
  decide what the guest `/init` does on restore (e.g. a restore hook the process can watch)
- The jailer (chroot, cgroups, seccomp as Firecracker's production guide asks), aarch64
- Automated tests: an integration test booting a VM where `/dev/kvm` is usable (skip
  elsewhere), `scripts/check.sh`'s cases moved into it
- Throughput of a shared VM (40% of the Java client's keep-alive rate): measure where the
  time goes (Firecracker's VMM thread, the guest relay) before optimizing
