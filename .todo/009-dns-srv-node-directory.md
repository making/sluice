Difficulty: Medium

# DNS SRV NodeDirectory

Dynamic cluster membership via DNS SRV (e.g. a k8s headless Service), without k8s API / RBAC:

- `NodeDirectory` implementation resolving `_grpc._tcp.<name>` style records; each node name
  maps to its public URL through a configured template (e.g. `grpcs://{node}.tunnel.example.com`)
- Poll on `sluice.cluster.membership-poll` and bump `version()` on membership change, so the
  `MembershipBroadcaster` pushes updates to connected clients
- Wire the provider selection (static list vs SRV) without ambiguity when both are configured
- Static-list provider and the push path already exist (`server/cluster/NodeDirectoryConfiguration`,
  `server/tunnel/MembershipBroadcaster`)

Part of the former todo 007 item 2 (split out on 2026-10-06; 007 shipped with the static
provider only).
