Difficulty: High

# sluice-wicket-wasi: console login through an OpenID Provider

The console takes `--console-token` only (HTTP Basic / Bearer). With a provider configured
(`--oidc-issuer`, `--oidc-client-id`, `--oidc-client-secret`, `--oidc-claim`, as the cf-api's
177), a browser signs in through it (authorization code flow with PKCE, a session cookie), the
token staying for API clients (the cf-api). The provider client of the cf-api (discovery, the
flow, the claim rules) moves to a crate both use, `sluice-oidc`, with the mock provider of the
cf-api's test for both tests.

- `GET /login` sends the browser to the provider, `GET /oauth2/callback` sets the session
  (signed with a key drawn at startup, 8 hours), `GET /logout` ends it; a page request without
  a session is redirected to `/login`, an API request answered 401
- a user whose ID token fails `--oidc-claim` gets 403
- the page shows who is signed in and a sign-out link
- a console on a non-loopback address needs `--console-token` or `--oidc-issuer`
- the cf-api README's Docker Compose sample: the console signs in through the same Dex

## Acceptance

- the wicket's test drives the flow against the mock: the redirect, the callback, the cookie
  on the API, a user refused, the token still good, the logout
- the cf-api's integration test uses the shared mock
- the Compose sample verified end to end (cf login --sso and the console through Dex)
