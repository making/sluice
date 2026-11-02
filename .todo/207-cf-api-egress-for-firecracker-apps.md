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

## What to decide

- **Names.** A guest has no name server: `/etc/resolv.conf` is empty, and `/init` writes the names of
  the rules to `/etc/hosts`. CF's destinations are addresses alone, so `0.0.0.0/0:443` lets an app
  connect to any address and resolve nothing. Either a `destination` here may also be a name (an
  extension; check the CLI passes it through unvalidated), or the guest gets a resolver through the
  wicket, its answers reachable by the ranges' rules. The first is small and enough for an app naming
  its backends; the second is what makes plain CF rules work.
- **Who may.** On CF only an admin. Here every user holds `cloud_controller.admin` (`uaa.rs`), and a rule
  reaches what the wicket's host reaches: `localhost` is the host's, a range covers link-local
  addresses, and on the Compose network `wicket:9090` is the console. So an option naming who may edit
  them (a claim, as `--oidc-claim`), and refusing the rest; what it means with no provider.
- **A range's port** cannot be listened on by the process (`### Egress`): an app whose `PORT` falls in
  one (`0.0.0.0/0:8080`): refuse at the start, or what `/init` says.

## Tests

`the_cf_cli_deploys_to_a_firecracker_wicket` in `sluice-wicket-cf-api/tests/cli.rs`, the sample guest's
`POST /egress?to=<host>:<port>` connecting as a process does. Update the `outbound connections` row of
`## On a Firecracker wicket`'s Mapping to VMs.
