Difficulty: Medium

# sluice-wicket-runtime-firecracker: jail mode in a container without seccomp=unconfined

Jail mode (`src/keeper.rs`, `src/jail.rs`) pivots each VM into its jail (`pivot_root`), and
builds the jails' template namespace so. Docker's default seccomp profile refuses
`pivot_root` (with `CAP_SYS_ADMIN` too; `unshare`, `mount`, `umount2` pass), so the README runs
the image jailed with `--security-opt seccomp=unconfined`: the keeper (root in the
container) and the wicket (`nonroot`, which reads what the guests send) run without a
filter; the VMs keep Firecracker's.

- A filter of the keeper's own for the wicket it spawns (and for itself), the default
  profile's denials at least, so that the container's may be unconfined at no loss
- Or a seccomp profile shipped beside the Dockerfile: Docker's default plus `pivot_root`
  (it drifts from Docker's)
- Not `MS_MOVE` + `chroot` instead of `pivot_root`: the mounts under the jail's root stay in
  the VM's namespace (the template's, or the host's)

Check it with the image test jailed (`SLUICE_WICKET_RUNTIME_FIRECRACKER_IMAGE` and `_JAIL`).
