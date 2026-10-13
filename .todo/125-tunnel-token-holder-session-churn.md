Difficulty: Medium

# A token holder can grow the registered sessions and the server log without bound

Found while bounding the pending tunnel streams (119).

## Problem

119 bounds the streams waiting for their first `Advertise`, but not what an advertised
stream holds: each distinct client id (a request header the token holder picks) is its
own registered session with its routes, its `SessionSender` and its executors, so a
holder of a valid token can register sessions for as many ids as it likes. Each accepted
stream also logs several info lines (`TunnelSession`: "client ... runs", "advertised",
"closing session"), so streams opened and closed quickly flood the log.

## Scope

- Decide a bound on registered sessions (global, per source address, per token if tokens
  become per client) with a refusal status, a meter and README; and throttle or demote the
  per-stream info lines under churn (as 119's `LogThrottle` does for refusals).

## Tests

- Past the bound a new client id's stream is refused while existing sessions go on; a
  session ending frees its place.
- Churning streams logs a bounded number of lines per interval.
