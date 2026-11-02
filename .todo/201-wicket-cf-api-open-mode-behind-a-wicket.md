Difficulty: Medium

# sluice-wicket-cf-api: nothing refuses the open mode where a wicket exposes it

Without `--oidc-issuer` the cf-api takes any credentials (`uaa::Mode::Open`). What guards that on a
host is `bind` in `lib.rs`: `--listen` on a loopback address only. In a VM of the Firecracker wicket
that loopback is the VM's and guards nothing: the wicket relays the route to whoever reaches its
`--listen`, or the sluice server with `--server`, saying the app's host. Whoever does logs in with
any credentials and pushes what runs in the wicket's VMs.

Today its README says so ("On a Firecracker wicket", **who reaches it**) and that setup publishes
the wicket's ports on the host's loopback only. A `-p 8080:8080` or a `--server` gets no refusal and
no warning.

Decision deferred (2026-11-02, as it stands for now): whether to refuse, and how.

- the cf-api finds it is in a VM and refuses the open mode without a flag saying so. Nothing marks
  every VM today: `/sluice/init.json` is only in a rootfs made from an image (the guest's launcher
  mode), the `sluice.*` kernel parameters only with an env, an egress or a disk (`boot_args` in
  `vm.rs`). So a marker from the guest or the boot args, and the cf-api knowing where it is placed
- a flag for the open mode wherever it runs: nothing to detect, but the local setups and the tests
  take it, and it says nothing of who reaches the listener
- on the wicket's side, a route served only to peers on its loopback: what `bind` means, for any
  such app rather than the cf-api alone. A port Docker publishes does not come from the wicket's
  loopback and a tunnel's connection comes from the server, so the README's setup would need another
  way in
- keep the warning

Its test goes in `tests/cli.rs`'s Firecracker test, which runs the cf-api in a VM without a provider
until the upgrade, and `one-container.sh` of `.todo/artefacts/200-wicket-cf-api-on-firecracker/`
runs the README's commands.
