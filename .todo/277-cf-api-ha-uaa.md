Difficulty: Medium

# sluice-wicket-cf-api: a login good on every instance

Needs 275. Part of 275-284.

## What holds it to one instance

- `uaa.rs`: `jwt.key` is made in `--data-dir` when missing: two instances with a directory each
  sign with two keys, a token of one refused (401) by the other
- the sessions (a refresh token good once, rotating) are a `HashMap` in the process, written
  whole to `sessions.json`: a refresh on another instance finds no session, or both take the
  same refresh token
- the passcodes of `cf login --sso` (`Uaa::passcodes`, one use, a TTL) are in the process: the
  browser's `/uaa/callback` and the CLI's `POST /uaa/oauth/token` may reach two instances
- sluice-wicket-oidc's `Provider::pending` (the state and PKCE verifier of a sign-in begun) is
  in the process: `/uaa/passcode` on one instance, `/uaa/callback` on another fails
- `MAX_SESSIONS` / `MAX_PASSCODES` bound a process, not the deployment

## Design (to confirm)

- the key in the store (275), made by the first instance in a transaction; with a key id
  (`kid`) and the one before accepted until its tokens expired, so it can be rotated (none
  today)
- the sessions as rows: a refresh takes its session by a compare-and-swap of the `jti`, so one
  of two concurrent refreshes wins; expiry by a sweep of the leader (278) or at insert
- the passcodes as rows, taken once in a transaction
- the sign-in begun: sealed in a cookie by sluice-wicket-oidc's session sealer (its key next to
  the JWT key), no row; else rows as the passcodes. The wickets' consoles keep `pending` as it
  is
- the file backend keeps `jwt.key` / `sessions.json` readable (an upgrade keeps the logins)

## Tests

Extend the UAA tests to two `Uaa`s on one store: a token of one verified by the other, a refresh
token used on both (one wins), a passcode made on one and spent on the other, once; the mock
provider's sign-in begun on one and finished on the other.
