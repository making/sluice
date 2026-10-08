Difficulty: Medium

# PROXY protocol (v1 / v2) on the data plane

The data plane has no PROXY protocol support: a proxy-protocol prefix breaks the head
parsers (`ConnectionHeadParser` / `SniHeadParser` answer "unrecognized connection head"),
so an SNAT-hiding LB cannot hand the real client address to sluice today. This todo adds an
opt-in per-listener PROXY protocol header parse ahead of everything else:

- A pre-parser that consumes the 108-byte-max v1 text header or the 16-byte v2 binary
  signature + TLVs, and replays the remainder into the existing pipeline unchanged
- Placement: before the TLS detection / `SniHeadParser` / `ConnectionHeadParser` chain in
  `DataProxyServer.transport` and before the relay in `TcpPortGateway.relay` (tcp routes
  have no head parse, the header bytes must not reach the upstream)
- The source address extracted from the header becomes the peer for `AccessControl` (and
  ideally the access log `remote`); the destination address is available too if ever needed
- Configuration: a single server switch (e.g. `sluice.proxy-protocol`) or per data plane
  listener; malformed headers fail the connection with the existing close path
- Keep it strict: v2 signature check, length and enum validation per the spec; unit tests
  with the spec's example bytes, plus an E2E where a local proxy speaks v2 to the data plane
  and to a tcp route

Depends on / relates to todo 017 (header-based trust): either suffices for SNAT LBs; this
one also covers non-HTTP tcp routes and TLS passthrough where no forwarded header exists.
