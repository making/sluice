# Route authentication

Detailed behavior behind README "Route authentication": the challenges, the checks, the
credential cache, the browser sign-in and the refusal logs.

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
  console tags such a route `Auth: <schemes>` (`oidc` for the provider's tokens, `oidc-login`
  for the browser sign-in).

## Browser sign-in

A route with `oidc.client-id` is an OIDC client of its provider (authorization code flow, PKCE
`S256` always; confidential with `client-secret`, public without). Per request, in order:

1. `/.sluice/oauth2/callback` and `/.sluice/oauth2/sign-out` (the path, any query) are answered
   by the data plane and never relayed, whatever the method.
2. A CORS preflight passes as with the other schemes (`allow-preflight`).
3. A session cookie that opens, has not expired and whose claims match `oidc.claims` passes.
4. Otherwise the credential headers decide as without the sign-in (Basic, Bearer, API token).
5. Without any: a session whose claims do not match is `403` (`reason=auth-denied`); a
   browser's navigation is redirected to the provider (`302`, `reason=auth-sign-in`); any other
   request is `401`, its challenges the route's schemes' and `OIDC realm="<realm>"` last.

A navigation is a `GET` or `HEAD` whose `Accept` lists `text/html`, without
`X-Requested-With`, and whose `Sec-Fetch-Mode` is absent or `navigate` (a WebSocket's is
`websocket`, a `fetch` / XHR's `cors` / `same-origin`).

- **Redirect to the provider**: the authorization endpoint of the discovery document, with
  `response_type=code`, `client_id`, `redirect_uri=<origin>/.sluice/oauth2/callback`, `scope`,
  `state`, `nonce` (32 random bytes each, base64url) and the PKCE challenge. The origin is the
  one the client addressed: https when the data plane terminated TLS or a trusted proxy states
  `X-Forwarded-Proto`, and the `Host` / `:authority` (a trusted proxy's `X-Forwarded-Host`, with
  its `X-Forwarded-Port` when not the default). A host that is no DNS name, IPv4 or bracketed
  IPv6 literal (with an optional port) gets no redirect, the `401` instead. Over http the
  navigation is `403` (`reason=auth-insecure`): the cookies are `Secure`. While the discovery
  document has not loaded, `503` with `Retry-After: 10` (`reason=auth-unavailable`).
- **State cookie** `__Host-sluice-state-<first 12 characters of the state>`, `Max-Age=600`: the
  state, nonce, PKCE verifier, origin and the page to return to (the request target when it is an
  origin-form path of visible ASCII not starting `//` or `/\`, else `/`). One per sign-in, so
  sign-ins in several tabs do not overwrite each other.
- **Callback**: a query parameter twice, no `state`, no state cookie of it (its value compared
  in constant time), one past its 10 minutes, the provider's `error`, or no `code` is `403`
  (`reason=auth-failed`, the page offering a new sign-in). Otherwise the code is exchanged at the
  token endpoint off the request thread, counted against the bound of the checks in flight (`503
  reason=auth-busy` past it), within the fetch timeout (5 seconds) and 512KiB. The client
  authenticates with `client_secret_basic` unless the discovery document lists only
  `client_secret_post`. The ID token must verify against the provider's keys (loaded anew once
  for a key not loaded, as for bearer tokens) with an allowed algorithm, carry `exp`, hold
  `exp` / `nbf` within `clock-skew`, its `iss` the issuer, its `aud` the client id, its `azp`
  (when present) the client id, its `nonce` the sign-in's; else `403` (`reason=auth-failed`).
  Success is `302` (`reason=auth-signed-in`) back to `<origin><page>`, setting the session
  cookie and removing the state cookie. Why a callback failed is logged at debug, never the code.
  The access log leaves the callback's query (code, state) out.
- **Session cookie** `__Host-sluice-session`, `Max-Age=<session-max-age>`: `sub` and the ID
  token claims `oidc.claims` and `oidc.headers` name, with the expiry, sealed (below). The claims
  are matched on every request, so a changed `oidc.claims` applies to the sessions issued
  before. Nothing renews a session: past its max age the next navigation signs in anew (silently
  while the provider's own session lasts), any other request is `401`. A session whose cookie
  would exceed 4000 bytes fails the sign-in (`403`) with a WARN naming the subject: keep fewer
  claims.
- **Sealing**: AES-256-GCM, a random 12-byte nonce per cookie, the purpose (`session` /
  `state`) as associated data, under a key derived per route by HKDF-SHA256 from
  `sluice.route.session-secret` (salt `sluice route session`, info the route key). A cookie of one
  route or purpose opens on no other; an altered one is no session (a navigation signs in anew).
  Without the property a single node seals with a random secret per start (WARN); cluster mode
  refuses to start.
- **Relay**: the sign-in's cookies (`__Host-sluice-*`) are removed from `Cookie` before relay,
  the other cookies kept in order (h2: joined into one `cookie` field), unless
  `forward-credentials=true`. The `oidc.headers` are removed from every request of the route that
  passes and set from the session: a list claim's values joined by `,`, the UTF-8 bytes as they
  go on the wire, control characters dropped. An identity header must not name `Host`,
  `Cookie`, `Authorization`, a framing / hop-by-hop header, a forwarding header or the route's
  API token header.
- **Sign-out**: removes the session cookie and redirects (`302 reason=auth-signed-out`) to the
  provider's `end_session_endpoint` with `client_id` and `post_logout_redirect_uri=<origin>/`,
  or to `<origin>/` when the provider names none (its session then lasts, and the next
  navigation signs in silently).
- On HTTP/1.1 every answer of the sign-in ends the connection like a refusal; on h2 the
  connection goes on.
- At startup a route signing browsers in on a data plane without TLS and without
  `trusted-proxy-cidrs` is warned about: its sign-ins are refused over plaintext.
