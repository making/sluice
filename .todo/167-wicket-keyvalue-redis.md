Difficulty: Medium

# wicket-wasi: `redis` keyvalue store

The `redis` store of the wicket's wasi:keyvalue (166 shipped the capability, the `Backend`
trait in `sluice-wicket-wasi/src/keyvalue.rs` and the `sqlite` store; README "Key-value
stores"): stores shared across hosts.

## Design (from 166)

- cargo feature `redis` (in the default image); a `type: redis` store in the `keyvalue`
  section: `url: redis://host:6379/0`, optional `prefix`
- keys `<prefix><namespace>:<key>`; `INCRBY` for increment (the decimal ASCII i64 rule is
  Redis's own); swap as a Lua script (`SET NX` for an absent expected value); `SCAN MATCH`
  for list-keys (the cursor is SCAN's, opaque to the guest); pipelines (`MGET`, ...) for
  batch
- a namespace quota as the sqlite store's `max-namespace-bytes` is not Redis's to keep
  cheaply: `maxmemory` bounds it; the host-side `max-key-bytes` / `max-value-bytes` apply
- connection loss / timeouts map to `other` (logged); bound each call by an operation
  timeout (a dropped future must not leave the connection in a bad state)

## Acceptance

- the counter example (`examples/counter`) counts on a redis store, two wickets on two hosts
  sharing it count correctly (increment and cas)
- list-keys pages through more keys than one SCAN page; the namespaces stay apart
- tested against a real Redis (a container or a child process) that CI runs too, not a
  test skipped where none is available
