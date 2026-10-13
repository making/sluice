Difficulty: Medium

# Bound the tunnel streams waiting for their first Advertise

Follow-up of 113 (advertise deadline).

## Problem

A tunnel stream is pending until its client's first `Advertise`
(`SessionRegistry#pending`): it holds a `SessionSender` worker, an advertise executor and
a deadline thread until the advertise or `sluice.limits.advertise-timeout` (10s). Nothing
bounds how many are pending at once. Before 113 a new stream replaced the client id's
session at once, so one id had at most one connecting stream; now one id can have many,
and distinct ids are unbounded either way (the id is a request header, the token may be
shared). A holder of a valid token can open streams faster than the deadline ends them.

## Scope

- Decide the bound: a global cap on pending streams, per source address, or both
  (refuse with `RESOURCE_EXHAUSTED`, logged once per source per interval); and whether a
  newer pending stream of a client id should end the older pending one.
- Meter the refusals; README "Connection limits".

## Tests

- Past the cap a new stream is refused at once; a pending stream advertising frees its
  place; the deadline frees it too.
