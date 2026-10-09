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
- On completion, update the "Scale-out" section of `k8s.md` to the dynamic variant
  (drop the static `SLUICE_CLUSTER_NODES` enumeration; `kubectl scale` becomes enough)
- Native-image constraint: Spring AOT freezes `@ConditionalOnProperty` bean conditions at
  build time, so the current `native` image ignores runtime `SLUICE_CLUSTER_NODES`
  (single-node only; the `k8s.md` scale-out section uses the `jvm` image for now).
  Fix tracked in 028 -- the provider selection here must be runtime-decided regardless.

Part of the former todo 007 item 2 (split out on 2026-10-06; 007 shipped with the static
provider only).
