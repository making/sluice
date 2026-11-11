Difficulty: Low

# sluice-wicket-runtime-wasi: a keyvalue store's `list-keys` page size, configurable

A `list-keys` page holds 100 keys at most, one constant for every store (`PAGE_KEYS`,
`src/keyvalue.rs`): sqlite's `LIMIT`, redis's `SCAN ... COUNT` and gemfire's page bound. On gemfire
each page costs a scan of the region's whole key set (`KeySetStream`: the gRPC service lists no
namespace alone), so a listing of N keys scans it N/100 times.

Measured on the production-like cluster of `.kb/wicket-keyvalue-gemfire.md`, from a client on the
same LAN through `sluice-wicket-keyvalue-gemfire` (one connection, each call timed):

- a `ContainsKey` takes about 5ms, a `KeySetStream` 30-40ms whether the region holds 0 or 3000
  keys, a `GetStream` of 100 keys about 30ms (0.25-0.3ms a key)
- listing the 3000 keys of a namespace as a guest does (a page, then its values in one `get-many`):
  2.2-3.1s by pages of 100, of which the scans take 1.1-1.9s; 0.88-0.98s by pages of 1000;
  0.67-0.79s in one page of 3000
- counter-api's `GET /counter` (categolj/counter-api-lisp: every counter, a page at a time) on a
  wicket deployed by the cf-api: 0.45-0.89s over 774 counters, 1.29-1.56s over 1500, 2.89-3.10s
  over 3000; extrapolated, the default 10s budget runs out at about 10000

Make the page size a store's setting, as `max-key-bytes` is (`page-keys`, say; default 100, as
now; bounded, so no page outgrows what a guest can take), each backend taking it where it takes
`PAGE_KEYS`. Whether gemfire's default should be larger is part of the item: a page there is a
scan whatever its size. Cover it by extending an existing paging test (redis's `listed` over 250
keys, say) with a store of another page size, not with a new test. Document the setting with the
store options in the README, and the numbers above in the `.kb` file.

Not urgent: counter-api lists its counters only when they move to another deployment.
