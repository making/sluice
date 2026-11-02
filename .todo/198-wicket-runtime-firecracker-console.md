Difficulty: High

# sluice-wicket-runtime-firecracker: a console deploying rootfs images as routes

sluice-wicket-runtime-wasi has `--console <host:port>` (its README, "Console"): a
page and a JSON API deploying components as routes without a restart, kept in
`--console-dir` across restarts, behind a token or an OpenID Provider
(`sluice-wicket-oidc`), and an apps API a controller drives
(`sluice-wicket-cf-api`). The Firecracker wicket has none: its routes come from
`--vm` and the `--config` file alone (the README's Not yet names the console).

Nothing in the VMs stands in the way: a deploy is a reload of the routes as they
would be with it (`routes.rs`: a target's VMs started before anything changes,
one failure keeps the table), which the wicket already does on a file change.

- what a deploy takes: an ext4 rootfs upload (by digest, as the wasm console's
  `PUT /api/components/<sha256>`: rootfs files run to hundreds of MiB), and the
  route's options (`mode`, `vcpus`, `mem`, `warm`, `vms`, `egress`, ...) as the
  sandbox; an OCI / CNB image converted on the wicket (as `scripts/cnb-rootfs.sh`
  does with docker and sudo today: `mkfs.ext4 -d` builds one without root, the
  layers pulled from a registry) is a step beyond, decide whether it belongs here
- the console's routes beside the config file's and the flags' (read-only), a
  host routed twice refused, the console's kept in `--console-dir`
  (`routes.yaml`, the rootfs files under their digest, never rewritten: a new
  file is a new target, the old one drained)
- what the page shows that the wasm console has no counterpart for: each
  route's VMs (shared, warm, in use, kept, pooled), the budget (`--max-vms`,
  `--max-memory`), the template's snapshot
- auth as the wasm console's: `--console-token`, else loopback only (DNS
  rebinding refused), an edit from another origin refused, sign-in through
  `sluice-wicket-oidc`; jail mode: the console runs in the unprivileged wicket,
  an uploaded rootfs opened as the wicket's user and passed to the keeper as the
  config file's are
- the apps API (`PUT /api/apps/<name>` and its listing) so that
  `sluice-wicket-cf-api` could deploy to this wicket too (a `cf push` built by
  buildpacks is a CNB image: the conversion above): scope it as a follow-up if
  the console alone is large
- share what the two consoles have alike rather than copying it (the page's
  shell, the auth, the persistence layout): a crate both wickets use, as
  `sluice-wicket-oidc` is

Tests: a console test in the VM test's process (`tests/vm.rs`, one more route
deployed through the API, served on the tunnel, kept across a restart), unit
tests for the persistence and the auth checks as the wasm wicket has them.
