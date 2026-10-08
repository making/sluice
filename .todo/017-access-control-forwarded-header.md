Difficulty: Medium

# Access control via X-Forwarded-For behind a proxying load balancer

The data plane IP access control (`sluice.access-control.*`, route `allowed-cidrs`) judges
by the socket peer address only. Behind a SNAT load balancer every connection appears to
come from the LB, so per-client filtering collapses to "the LB's range". This todo adds
opt-in trust of a forwarded header:

- New properties under `sluice.access-control` (e.g. `trusted-proxy-cidrs` and the header /
  hop selection; keys alphabetical, comment above each)
- Only when the connection's peer address matches `trusted-proxy-cidrs`, take the client
  address from the forwarded header (the rightmost entry appended by a trusted proxy wins;
  beware client-side spoofing — the hop count / trusted chain is the trust boundary)
- The resolved address feeds the existing `AccessControl.allowed(route, peer)`; the
  deny-then-allow order is unchanged. The plain peer address remains the default when no
  trusted proxy is configured
- `DataProxyServer` only: the check already runs after head parse, so HTTP/1.1 and h2 heads
  carry the header; tcp routes and TLS passthrough have no header and keep the peer address
- Unit tests in `AccessControlTest` style + an E2E in `sluice-it` where the "LB" is a local
  TCP proxy appending `X-Forwarded-For`

Note the interplay with `ConnectionHeadParser`: the header must be read from the parsed
head, no second parse. If the LB also strips/rewrites the header, document that the operator
must configure it to append.
