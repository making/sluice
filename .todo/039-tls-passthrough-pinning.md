Difficulty: High

# TLS passthrough connection pinning (follow-up to 037/038)

Todos 037 (h1 keep-alive re-routing) and 038 (h2 stream demux) removed the per-connection
route pinning for plaintext HTTP: every h1 request head and every h2 stream is routed
anew, which fixes the browser connection-coalescing symptom when the data plane sees
plaintext (TLS terminated on the data plane, or plain HTTP). This todo is the remaining
gap: **`tls-passthrough` routes**.

## Problem

On a `tls-passthrough` route the data plane relays the TLS records untouched, routed once
by the SNI of the ClientHello (`DataProxyServer#transport` -> `Head.encrypted`). The
connection is pinned to that first SNI's route for its lifetime. When a browser coalesces
several origins onto one TLS connection (same IP, one certificate covering all of them --
exactly the production symptom recorded in the session memory `sluice-conn-pinning-repro`),
every later origin on the coalesced connection is answered by the first origin's upstream.
037/038 cannot see inside the encrypted stream, so they do not apply.

## First step: verify the premise

Before designing, check the production route configuration: are the affected routes
actually `tls-passthrough`? The data plane's access log `transport=` field
(`tls-passthrough` vs `h2`/`h1`) tells this from a live connection, and the client
upstream configs tell it statically. If every affected route terminates TLS already, this
todo is cancellable -- close it with the evidence. The 037/038 fix is deployed and its
per-request `type=req` access lines make coalescing directly visible either way.

## Design space (decide, do not implement all)

1. **Terminate TLS on the affected routes** (drop `tls-passthrough`, present the
   certificate on the data plane): no new code -- 037/038 then apply as-is. Requires the
   certificate to be mountable by the server; changes who sees the client cert / the
   upstream's TLS. Often the operationally cheapest fix.
2. **Demux inside the tunnel**: the data plane terminates TLS locally while still
   forwarding the original handshake? Not possible without re-encrypting; a MITM proxy
   with its own CA changes the trust story and the upstream's view. Only worth it if
   passthrough is a hard requirement (client certificates seen by the upstream, etc).
3. **Keep pinning, answer coalesced-but-unrouted... ** not possible: the SNI is the only
   routing signal inside TLS before the handshake completes.

Recommendation: prefer 1; treat 2 as a separate spike if 1 is blocked.

## Implementation notes (if code changes at all)

- Termination path already exists: `DataProxyServer#transport` (SSL bundle, ALPN
  selector, `forceHttp1`), `TlsDataPlaneE2ETest` covers the plain cases. The work is
  configuration + docs + possibly a route-level default, not new relay machinery.
- The h2 demux then applies through the normal `Head` path (`version=2`, not encrypted);
  no changes in `Http2DemuxRelay` expected.
- If a spike for in-tunnel demux is wanted anyway, the frame parser to reuse lives in
  `Http2DemuxRelay` (feed/onFrame) and `ConnectionHeadParser` (head detection); the
  HPACK constraint notes in its class javadoc apply.

## Acceptance

- The production coalescing symptom no longer misroutes, on every affected route, with
  the transport recorded in the close log line.
- Evidence: an incognito-profile browser walk across the affected origins answering from
  their own upstreams, and/or an E2E in `sluice-it` if code changed.
