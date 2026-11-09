Difficulty: Medium

# sluice-wicket-runtime-wasi / sluice-wicket-cf-api: restrict a wasi app's outbound connections

A wasi app reaching a database needs `inherit-network` (`.kb/shiori-on-wasi-wicket.md`), which allows every
address: the wicket's console, the cf-api, other Compose services, the host's loopback, a cloud's metadata
endpoint. The cf-api's security groups are "accepted and shown, no effect" on a wasi wicket, while on the
Firecracker wicket they are the route's `egress` and an app reaches no other host.

Give the wasi wicket's sandbox an allow list of destinations (CIDR and ports; host names as the Firecracker
wicket's `egress` takes them, if they fit), enforced through wasmtime-wasi's `socket_addr_check` for TCP
and UDP alike, and have the cf-api map the security groups onto it as it does for the Firecracker wicket
(`egress.rs` and the cf-api's security groups mapping are the model). Decide what `inherit-network`
means once an allow list exists. Pin it in the wicket's sandbox tests and the cf-api's integration test;
update both READMEs and the .kb file above.
