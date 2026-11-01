Difficulty: High

# sluice-wicket-runtime-firecracker: outbound connections from the guests

A guest has no network device (vsock only): nothing leaves a VM but its answers, so an app
calling a database or another service cannot run. Options:

- Egress through the wicket over vsock: `/init` listens where the route says (loopback
  addresses and ports, or an HTTP CONNECT / SOCKS proxy on 127.0.0.1 named by `HTTP_PROXY`)
  and relays each connection over vsock to the wicket, which dials it if the route's allow
  list (host:port, CIDR) has it. No root, no TAP; the wicket logs and limits the egress
- A TAP device per VM with egress filtering on the host: needs NET_ADMIN (the Docker image,
  `.todo` 182, runs without it) and an address per VM; jail mode's VMs share an empty
  network namespace of the keeper's (`src/keeper.rs`), where their TAP devices would go
- Clones: a connection open in the template's snapshot is every clone's; egress opens after
  the restore only

Decide, measure the relay's cost (a database round trip through the wicket), and give a
route its allow list (`egress=` in `--vm`, the config file).
