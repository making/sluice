Difficulty: Medium

## SNI routing (TLS passthrough)

Route data-plane connections by the SNI hostname in the TLS ClientHello, relaying the
TLS bytes untouched (backend terminates TLS). Complements the existing HTTP Host routing
and the TLS-terminating mode (`sluice.data-tls-bundle`).

### Spike result (2026-10-05, artefacts/005-sni-routing/SniSpike.java — run: `java .todo/artefacts/005-sni-routing/SniSpike.java`)

- ClientHello SNI extraction is ~60 lines of plain JDK: record header (5B) -> handshake
  type 0x01 -> skip version/random/session/ciphers/compression -> walk extensions for
  type 0x0000 (server_name), list len, name type 0, name len, name. Works with real
  `SSLSocket` handshakes.
- Raw TLS passthrough relay (bidirectional copy + half-close propagation) carries the
  full handshake, app data, and close_notify without issue; end-to-end TLS with the
  backend verified.
- Two learnings that apply to the real implementation:
  1. The consumed ClientHello bytes MUST be replayed to the upstream before relaying
     (same pattern as `StreamRelay.builder(..).prefix(head)`), otherwise the upstream
     sees a truncated handshake.
  2. The proxy never touches certificates; each backend presents its own cert. ALPN,
     client certs, and TLS versions pass through untouched.

### Design sketch

- New `SniHeadParser` next to `ConnectionHeadParser` (sluice-server, proxy pkg): peek
  the first record; if it is a ClientHello, extract server_name, return head bytes.
- `DataProxyServer.transport()`: when the first byte is 0x16 AND no SSL bundle is
  configured (passthrough mode), parse SNI instead of HTTP head and relay with the
  head as prefix. When an SSL bundle IS configured, current termination path wins
  (Host header is available after termination; SNI routing is a passthrough-mode
  feature) -- or decide to support both; make this explicit in the implementation.
- Router: reuse `byDomain` lookup as-is (SNI value == host, no port suffix).
- Client side: no change (upstream host entry doubles as the SNI name).
- E2E in sluice-it: two upstreams, one data port, connect with distinct SNI names
  (`SNIHostName`), assert each lands on its own upstream; include half-close.
