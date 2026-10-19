Difficulty: Medium

# Data plane OIDC: JWT bearer validation per route

Route auth (156) checks static secrets: Basic, Bearer and API tokens matched by `{id}`-prefixed
encoders. Add OIDC access tokens: a route accepts `Authorization: Bearer <JWT>` issued by a
configured provider (resource-server style), for API / machine clients. Browser sign-in is 159.

## Scheme SPI (done)

`CredentialScheme` (header, auth-scheme token, challenge, `credential(value)`) with `BasicScheme`,
`BearerScheme`, `ApiTokenScheme`; a `Credential` owns its check (`id` for the cache, `bounded`,
`matches`). `RouteAuthenticator.Policy#credential` dispatches a header's value by the auth-scheme
token. OIDC is a fourth scheme on `Authorization`; it shares `bearer` with `BearerScheme`, so the
dispatch must tell a JWT from a static token (or the combination is refused, see below).

## OIDC bearer

- Config under `sluice.route.auth.[<key>].oidc`: issuer (discovery), audience, allowed algorithms,
  optional required claims (e.g. groups / email domain), clock skew.
- Verification is CPU only on the request path: the JWKS is fetched at startup and refreshed in the
  background (and on an unknown `kid`, rate limited, off the request thread). The h2 demux verifies
  under its loop lock (see 158), so no network I/O may happen in `verify`.
- Startup with an unreachable issuer: decide (fail the start vs. refuse the route's requests until
  the JWKS loads); never pass unchecked.
- Cache: today a success is remembered a fixed `CACHE_TTL`. A JWT must not outlive its `exp`: make
  the expiry per entry (`min(exp, CACHE_TTL)`).
- Bearer with static tokens and OIDC on one route: a token that parses as a JWT goes to OIDC,
  else to the static list -- or forbid the combination; decide and document.
- Challenge: `Bearer realm="...", error="invalid_token"` (RFC 6750) on a presented bad token.
- Library: spring-security-oauth2-jose (`NimbusJwtDecoder`) vs. own; check the native image
  (reflection, JCA services for RS/ES/PS algorithms).
- Access log: `reason=auth-invalid` plus why (expired, audience, signature) at debug, never the
  token.

## Acceptance

- Unit: valid token passes; expired, wrong audience, wrong issuer, bad signature, unknown `kid`,
  `alg=none` refused; cache entry expires at `exp`; JWKS refresh picks up a rotated key.
- E2E (extend `AccessControlE2ETest`, mock issuer like `MockOidcServer`): h1 and h2c, 401 without,
  200 with, header stripped.
- README "Route authentication" section; native image checked.
