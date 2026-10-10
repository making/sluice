Difficulty: Medium

# sluice-wasmlet: a response body hyper never reads recycles its pool unit

hyper drops a response body it must not send without polling it: every HEAD
response (and presumably 204 / 304). The dropped `Tracked` body reads as a
client departure (`Stop::Gone`), so the guest is stopped and its pooled unit
retired; before the tracked body, the relay's failed send (`Outcome::Uncertain`)
retired it all the same. Measured: pool=1, three `HEAD /` -> `spawned == 4`.
A route serving HEAD (health checks, link checkers) thus pays the cold path
plus a respawn on every request.

Also, a unit retired on `Stop::Gone` logs nothing (`supervise` returns
silently), unlike the "recycling ... uncertain request" line of the relay
path, so this churn is invisible.

Options:

- `respond` knows the request method and the head's status: when the body is
  bodiless by HTTP (HEAD, 1xx, 204, 304), do not count its drop as a
  departure, and let the relay drain the guest's body without forwarding it
  (a closed frame channel is then not `Uncertain`)
- hand the guest no body to produce at all is not possible (the guest owns
  its response), so the drain is needed either way

Acceptance:

- pool=1, repeated `HEAD /` -> `spawned == 1`, every response a correct head
- 204 / 304 from the guest: same, if hyper drops them unread (check first)
- a mid-body departure on GET still retires the unit
  (`a_client_leaving_mid_body_frees_its_unit_before_the_budget`)
- a unit retired on a gone client leaves one log line, like the other
  recycle reasons
