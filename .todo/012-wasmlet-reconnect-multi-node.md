Difficulty: High

# sluice-wasmlet: reconnect and multi-node membership

The client is single-node and dies with the stream. Port the supervisor semantics of
the Java client (`TunnelClient` / `NodeConnection`):

- reconnect with exponential backoff (1s..30s) when the tunnel stream drops
- `ListNodes` bootstrap + one stream per node, keyed by node id (`x-sluice-id` already
  sent); adopt the bootstrap stream when it turns out to be a listed node
- `MembershipUpdate` handling to open/close per-node streams; `AdvertiseAck.node_id`
  rekeying; `Drain` handling (keep the stream, fail over via membership)

The wasm serving path is per-connection and node-agnostic; only the session layer
changes. Careful: the Java client dedups streams per node (a second stream makes the
server drop the first — see `TunnelClient.rekey`).

Acceptance: kill one of two nodes; routes stay served through the survivor and
return when the node rejoins. A two-node setup is in the root README "Cluster".

Current state: `wasmlet/src/main.rs` `run()` — single `connect`, no retry loop, membership
frames only printed.
