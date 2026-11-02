Difficulty: Medium

# sluice-wicket-cf-api: outbound connections through CF's security groups

An app pushed with `cf` on a Firecracker wicket reaches no other host: `vm_sandbox` sets no `egress`.
Seen with `ghcr.io/categolj/blog-frontend-htmx:jvm`: it starts, `/actuator/health` answers 200, `/`
answers 500 (`java.nio.channels.UnresolvedAddressException`, its backend's name).

The wicket has what it takes: a route's `egress` (`README.md` of the wicket, `### Egress`). The CF side
is `cf create-security-group` and its family; the CLI's `/v3/security_groups` calls answer empty today
(`api.rs`).

## The mapping

| CF | the wicket's `egress` |
|---|---|
| `destination`: an address, a CIDR | an address, a range (`Host::Net`) |
| `destination`: `10.0.0.1-10.0.0.9` | the CIDRs covering it |
| `ports`: `80,443` | a rule per port (`Rule` holds one) |
| `ports`: `8000-8100` | a rule per port, within a bound: each is a listener in the guest |
| `protocol`: `tcp` | what a rule is |
| `protocol`: `udp`, `icmp`, `all` | refused (422): the wicket relays TCP alone |
| `bind-security-group ORG --space`, `bind-running-security-group` | the apps of the one space: the same here |
| `--lifecycle staging`, `bind-staging-security-group` | no effect: the wicket pulls the image, no VM stages |
| applies at `cf restart` | `egress` in the `running` snapshot, as `env` is |

## Decided

- **Names.** A guest has no name server: `/etc/resolv.conf` is empty, and `/init` writes the names of
  the rules to `/etc/hosts`. CF's destinations are addresses alone, so `0.0.0.0/0:443` lets an app
  connect to any address and resolve nothing. A `destination` here may also be a name, an extension.
  First check the CLI passes it through unvalidated (`cf create-security-group`, `CF_TRACE`); if it
  does not, this choice is open again. A resolver through the wicket is left out.
- **Who may.** On CF only an admin. Here every user holds `cloud_controller.admin` (`SCOPES` in
  `uaa.rs`), and a rule reaches what the wicket's host reaches: `localhost` is the host's, a range
  covers link-local addresses, and on the Compose network `wicket:9090` is the console. So an option
  of the cf-api names who may edit them, as a claim rule like `--oidc-claim`. Without it, nobody may:
  closed by default, `Mode::Open` (no provider) included.

## Still to settle

- **A range's port** cannot be listened on by the process (`### Egress` in the wicket's README): an app
  whose `PORT` falls in one (`0.0.0.0/0:8080`): refuse at the start, or what `/init` says.
- **The clients of `--client-credentials`**: whether the option can name them.
- **How many rules**: a route's rules reach the guest on the kernel's command line (`boot_args` in
  `vm.rs`, `wire::EGRESS_PARAMETER`), which is bounded: a bound on the rules a port range expands to
  must fit it. Measure before choosing.

## Where it goes

- `store.rs`: the groups in `State` (`#[serde(default)]`), and `egress` in `Running`, the snapshot of a
  start, as `env` (built in `api.rs` near its `Ok(Running {`, and in `controller.rs`'s tests)
- `wicket.rs` `Sandbox`: `egress`; `vm_sandbox` and `same` in `controller.rs` (compare sorted: a wasm
  app has none and takes no effect)
- `api.rs`: `(&Method::GET, ["security_groups"])` is in "what there is none of", and
  `["spaces", _, "running_security_groups"]` and its staging twin answer empty. See which calls the
  CLI makes with `CF_TRACE`
- `uaa.rs`: the session keeps the ID token's claims (`Subject.claims`), the access token does not: the
  check needs them at the request

## Tests

`the_cf_cli_deploys_to_a_firecracker_wicket` in `sluice-wicket-cf-api/tests/cli.rs`, the sample guest's
`POST /egress?to=<host>:<port>` connecting as a process does. Update the `outbound connections` row of
`## On a Firecracker wicket`'s Mapping to VMs.
