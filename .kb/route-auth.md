# Route authentication

Detailed behavior behind README "Route authentication": the challenges, the checks, the
credential cache and the refusal logs.

## Behavior

- The data plane's error answers (`401`, `403`, `502`, `503`) are `application/problem+json`
  documents (RFC 9457); `WWW-Authenticate` and `Retry-After` stay headers. Only a request
  whose `Accept` lists `text/html` gets the HTML page instead.

- A request passes with any one credential of its route. Otherwise it is answered `401` with a
  `WWW-Authenticate` challenge per scheme of the route (`Basic realm="<realm>", charset="UTF-8"`,
  `Bearer realm="<realm>"`, `<api-token-header> realm="<realm>"`) and never reaches the upstream.
  A request carrying a credential header of the route twice is refused whatever the values.
- OIDC tokens (resource-server style, for API and machine clients): a signed JWT passes when its
  signature verifies against the provider's keys with an allowed algorithm, it carries `exp`,
  `exp` / `nbf` hold within `clock-skew`, and `iss`, `aud` and the `claims` match. Unsigned
  (`alg=none`) and HMAC tokens are refused. With static `bearer` tokens on the same route, a
  token in JWS compact form (three base64url parts joined by two dots) goes to OIDC, any other
  to the static tokens: a static token must not have that form.
- The provider's keys (JWKS) are loaded at startup and every 5 minutes in the background; a
  token naming a key not loaded loads them anew, at most once per 10 seconds, its check held off
  the request thread meanwhile. Each document is fetched within 5 seconds and 512KiB. A failed
  load keeps the keys loaded before. When the provider is unreachable at startup the server
  starts all the same (after waiting about 10 seconds for it): the route's JWTs are answered
  `503` with `Retry-After: 10` until the keys load, retried every 10 seconds; they never pass
  unchecked.
- Every HTTP/1.1 request head and every h2 stream is checked, plaintext or TLS-terminated, after
  `allowed-cidrs` (`403` before `401`). On HTTP/1.1 a refused request is answered once the
  responses to the requests before it are relayed, then the connection closes; on h2 the
  connection goes on. A WebSocket is checked on its upgrade request only: browsers cannot add
  headers to it, so bearer / API tokens there serve non-browser clients.
- A request presenting a bearer token (static or OIDC) that is refused gets
  `Bearer realm="<realm>", error="invalid_token"` (RFC 6750).
- Unless `forward-credentials=true`, `Authorization` (Basic, Bearer) and the API token header
  are removed from every request of the route before it is relayed.
- Only a request the data plane reads can be checked: a TLS passthrough or non-HTTP connection
  to such a route is closed unrelayed, and the server warns at the advertise of a TLS passthrough
  upstream under such a key; a `tcp:<port>` key fails the start.
- Each secret carries the `{id}` of its encoder: `bcrypt`, `pbkdf2`,
  `pbkdf2@SpringSecurity_v5_8`, `noop` (the value itself), `sha256` (the hex SHA-256 of the
  UTF-8 value, unsalted: for high-entropy tokens, never for passwords people choose; e.g.
  `printf %s "$TOKEN" | sha256sum`). scrypt and argon2 are not supported (they need Bouncy
  Castle). A secret without a prefix, with an unknown id or malformed for its encoder fails the
  start, as does a route with no credential.
- A credential that matched is remembered per route by its SHA-256 for 10 minutes -- a JWT until
  its `exp` at most -- (up to 10000 of them), so a client's later requests skip the encoder; one that did not match is never
  remembered. At most as many checks against `bcrypt` / `pbkdf2` as CPUs (at least 2) run at
  once; a request whose check would take one more is answered `503` with `Retry-After: 1`, and
  requests presenting the credential of a check under way wait for that check. bcrypt and
  pbkdf2 cost milliseconds per check by design: prefer `sha256` for tokens.
- On h2 such a check holds its own stream only: the connection's other streams go on
  meanwhile. A stream whose check has not ended after `sluice.limits.h2-stream-wait-timeout`
  is refused with `REFUSED_STREAM`. Details: [.kb/h2-demux.md](.kb/h2-demux.md).
- Without `sluice.data-tls-bundle` the server warns at startup that the credentials cross the
  data plane in cleartext unless a front end terminates TLS.
- Refusals are logged as `type=req ... status=401 reason=auth-missing` (no credential),
  `reason=auth-invalid`, `reason=auth-unreadable` (passthrough / non-HTTP), `status=503
  reason=auth-busy` and `reason=auth-unavailable` (the OIDC keys not loaded). Why a credential
  was refused (e.g. `Jwt expired at ...`, `The aud claim is not valid`, `Invalid signature`) is
  logged at debug by `am.ik.sluice.server.auth.RouteAuthenticator`, never the credential. The
  console tags such a route `Auth: <schemes>` (`oidc` for the provider's tokens).
