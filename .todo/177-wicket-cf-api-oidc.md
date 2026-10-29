Difficulty: High

# sluice-wicket-cf-api: OIDC login behind the UAA endpoints

173 takes any credentials and listens on loopback only. With an OIDC provider configured
(issuer, client id and secret, the users or groups allowed):

- `cf login --sso`: the CLI asks for `Temporary Authentication Code ( Get one at
  <login>/passcode )`, the prompt being `prompts.passcode` of `GET <login>/login` (spike of
  173). `/uaa/passcode` runs the authorization code flow (PKCE) against the provider and shows
  a one-time code (short-lived, single use); the CLI posts it as
  `grant_type=password&passcode=<code>`, and the cf-api answers its own access and refresh
  tokens, `user_name` taken from the provider's `preferred_username` or `email`
- `cf login -u -p` is refused with a hint to `--sso`
- `cf auth <client> <secret> --client-credentials` (CI): clients configured in the cf-api
- refresh tokens rotate; a user no longer allowed is refused at the next refresh
- every request verifies the cf-api's token; an allowed user may do everything (roles later)
- with OIDC configured the listener may bind any address; TLS of its own (`--tls-cert`,
  `--tls-key`) or behind a proxy
- the provider settings follow the sluice server's (todos 157 and 159: discovery, client)
  where they fit

## Acceptance

- a test drives `cf login --sso` (the code on its stdin) against a mock provider, as
  sluice-it's `MockOidcServer`: the passcode page, the code, the CLI's token, a refresh; a
  user not allowed and a code used twice are refused
- README: setting up the provider
