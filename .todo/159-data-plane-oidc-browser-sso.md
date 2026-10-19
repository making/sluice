Difficulty: High

# Data plane OIDC: browser sign-in per route

Route auth (156) and OIDC bearer (157) answer a request without credentials `401`. For browsers,
add the authorization code flow (oauth2-proxy style): an unauthenticated request is redirected to
the provider, the callback sets a session cookie, later requests pass with it.

Depends on 157 (scheme SPI, provider config, JWKS). The code exchange is network I/O: do it in a
`bounded()` credential check, which the h2 demux runs off its loop (158).

## Pieces

- The route is an OIDC client (confidential, authorization code + PKCE): per route under
  `sluice.route.auth.[<key>].oidc` -- issuer (discovery), client id, client secret, scopes
  (`openid` at least). Or name a registration of `spring.security.oauth2.client.*`, which the
  console already uses, so one provider config serves both; decide. The ID token from the code
  exchange is validated with 157's JWKS / claim checks (issuer, audience = client id, nonce, `exp`).
- Authorization past sign-in: who may pass (allowed emails / email domains / groups claim) per
  route; a signed-in user outside it gets `403`, not another redirect.
- `Verdict` gains a response other than a refusal: a `302` to the provider (`Location`, state /
  nonce / PKCE cookie). `ErrorResponse` already carries arbitrary headers (`Status#headers`).
- Callback: a reserved path per route host (e.g. `/.sluice/oauth2/callback`) answered by the data
  plane itself, never relayed. `RequestHeaders` needs the request path (today method + headers).
  The provider's registered redirect URI is per host: document it.
- Session: stateless (encrypted, signed cookie with expiry and the claims kept), so any cluster node
  serves it without a shared store; the key shared across nodes like `sluice.token`. Cookie scope per
  route host; `Secure` / `HttpOnly` / `SameSite=Lax`.
- Strip: only the session cookie leaves the `Cookie` header before relaying (partial rewrite, h1 and
  h2), unlike the whole-header strip of 156. Optionally forward identity to the upstream as headers
  (`X-Forwarded-User` / `-Email`), stripping any the client sent.
- Session refresh / expiry: via a new redirect round trip, so no response of the upstream needs a
  `Set-Cookie` added (the relays pass responses through untouched). Decide whether to keep the
  refresh token.
- Sign-out path; CSRF of the callback (state); open redirect of the return URL (same host only).
- Non-browser requests on such a route (no `Accept: text/html`, XHR): `401` instead of `302`, so
  APIs behind the same route get 157's bearer check.
- WebSocket: the session cookie rides the upgrade request, so browsers can open one (unlike bearer).
- Plaintext data plane: refuse to set the cookie without TLS (or a trusted `X-Forwarded-Proto`)?
  Decide.

## Acceptance

- E2E with `MockOidcServer`: redirect, callback, cookie, pass; expired session redirects again;
  tampered cookie refused; callback with a bad state refused; h1 and h2 (TLS-terminated); two nodes
  accept the same cookie.
- Cookie stripped before the upstream, other cookies kept.
- README section; native image checked (crypto of the cookie, the provider client).
