Difficulty: Medium

# Per-route data plane authentication (basic / bearer / x-api-token)

Requests passing the data plane carry no authentication of their own; the only per-route admission
is `allowed-cidrs` (`AccessControl`). Add a per-route credential check, first step: HTTP Basic,
`Authorization: Bearer`, and an API token header (`X-API-Token`).

## Where it applies

Only where the data plane sees plaintext HTTP: plain HTTP and TLS terminated on the data plane
(with an `http://` or `https://` target). Not on `tls-passthrough` routes (only the SNI is visible)
nor on tcp routes (`listen-port`). A WebSocket is checked on its upgrade request only; browsers
cannot put custom headers on it, so bearer / token there is for non-browser clients.

Configuring auth on a route the check cannot reach (passthrough, tcp) must fail loudly (startup
error or a warning per advertise -- decide), never silently pass.

## Check points

The same three places `AccessControl#allowed` runs, after it (403 before 401):

- `DataProxyServer#relay`: the connection's first request (the h2 seed stream included)
- `ReroutingRelay#resolveOrRespond`: every later h1 request head on a keep-alive connection
- `NettyHttp2DemuxRelay#routeLocked`: every later h2 stream

A refusal is `401` with `WWW-Authenticate` (`Basic realm="..."` for basic, `Bearer` for bearer),
added to `ErrorResponse` for h1 (response + close, like the 403) and h2 (`StreamPage`, the connection
goes on).

## Decisions

1. Where the credentials live. Recommended: server side, keyed by the route key the gate uses
   (`sluice.route.auth.[<route key>]...`). Secrets stay off the control plane, candidates of one key
   cannot disagree, and the server deploys via GitOps while clients are updated by hand. The
   alternative is the client `Upstream` (like `allowed-cidrs`, carried in the advertise).
2. Forward or strip the credential header to the upstream. Per-route option; default strip (the
   credential is for the data plane, and an upstream with its own `Authorization` would collide).
   h1: rewrite the head (`ConnectionHeadRewriter`, as `rewrite-host`); h2: remove from the headers.
3. Storage (decided): Spring Security `DelegatingPasswordEncoder`; each configured secret (basic
   password, bearer token, API token) carries its algorithm prefix (`{noop}...`, `{bcrypt}...`,
   ...), chosen per route.
   - Move `ConsoleSecurityConfiguration#passwordEncoder` to a shared definition (e.g. under
     `am.ik.sluice.server.auth`) that both the console and the data plane auth use; the console
     keeps accepting what it accepts today (pin it in `ConsoleAuthE2ETest`).
   - Register the encoders explicitly (bcrypt, noop, pbkdf2, ...). scrypt / argon2 need Bouncy
     Castle: add it, or leave them out and document so. Check each registered encoder in the
     native image.
   - Add a SHA-based encoder (e.g. `{sha256}<hex>`, unsalted, constant-time compare): cheap per
     request and fit for high-entropy tokens; the docs say it is not for human-chosen passwords.
     Spring Security's `MessageDigestPasswordEncoder` is deprecated -- prefer an own small encoder.
   - Validate at startup: a value without a prefix or with an unknown id fails the start, not the
     first request.
   - Cost: bcrypt runs per request (every h1 request head, every h2 stream). Cache successful
     verifications keyed by (route, SHA-256 of the presented credential), bounded size and TTL, so
     a client's repeated requests skip the encoder; never cache failures. Bearer / API token
     values are salted, so a presented token is matched against every configured token of the
     route: the cache keeps that to the first request.
   - Unauthenticated requests with random credentials make the server run the encoder each
     time: bound the concurrent verifications (refuse past the bound) and state it in the docs.
4. CORS preflight: an option to exempt `OPTIONS` (browsers send no credentials on it).
5. Plain HTTP route with auth: warn (credentials in cleartext) or refuse.

Out of scope for the first step: brute-force rate limiting, console display beyond a tag.

## Acceptance

- Unit test of the checker: each scheme accepts the right credential, rejects wrong / missing /
  malformed (`Basic` without colon, bad base64, wrong scheme, empty token), with `{noop}` and
  `{bcrypt}` secrets; a missing / unknown prefix fails the start; a cached success skips the
  encoder (assert the encoder's call count), a failure is not cached; past the verification bound
  a request is refused.
- E2E (extend `AccessControlE2ETest`, do not add a class): per scheme, 401 without and 200 with the
  credential, over h1 and h2c; a keep-alive h1 connection whose second request goes to an
  authenticated route is refused (pins the `ReroutingRelay` check); an h2 connection whose second
  stream is for an authenticated route is refused (pins the demux check); stripping observed at the
  upstream (the header absent / present per option).
- The passthrough / tcp misconfiguration case fails as decided in 1.
- Access log records the refusal; README documents the properties and the scope above.
- Native image: any new console view record registered in `reflect-config.json`; config read at run
  time (no `@ConditionalOn*`).
