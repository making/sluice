Difficulty: Low

# sluice.tunnel.bytes counts chunks, not bytes

`DataProxyServer#relayedBytes` and `TcpPortGateway#relayedBytes` call
`counter.increment()` once per relayed chunk, so `sluice_tunnel_bytes_total` is a chunk
count. The `direction` tag is always `data` although `StreamRelay.Direction` is known at
that point.

- Increment by `count`
- Tag the real direction (`inbound` / `outbound`, matching the console's wording), or
  document why one series is enough
- Failing test first (SimpleMeterRegistry over a relayed request with a known body size)
