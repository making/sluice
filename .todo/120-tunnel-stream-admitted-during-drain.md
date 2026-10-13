Difficulty: Low

# A tunnel stream admitted as the drain starts outlives it

Follow-up of 113 (advertise deadline).

## Problem

`TunnelService#connect` rejects streams once `ClusterLifecycle#isDraining`, and the drain
closes the pending streams (`SessionRegistry#pending`) at its start. A stream that passed
the draining check just before the flag was set, but built its session after the drain
took its pending snapshot, is in neither list: it stays until its advertise (then the
final close of the drain catches it only if it registered before that loop) or until
`sluice.limits.advertise-timeout`, and the gRPC shutdown waits for it meanwhile.

## Scope

- Close the gap: e.g. `SessionRegistry` refuses `admit` / `register` once the drain
  started (the stream failed with `UNAVAILABLE` "node is draining"), so no session can
  appear after the snapshot.

## Tests

- A stream admitted after the drain started (hook between the check and the admit) is
  failed at once and the context close stays within its budget.
