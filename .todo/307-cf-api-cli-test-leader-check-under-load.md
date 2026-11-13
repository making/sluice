Difficulty: Medium

# cf-api CLI test: the previous build was not the leader during the update, once

`tests/cli.rs`, the update of the image an instance at a time: `assert_eq!(api.leader(&shared), 0)`
after `api.update_one(1, ..)` failed once (2026-11-13) with both CLI tests (wasi and Firecracker)
running at once on one machine. Alone, and both one after the other, it passed. The lease code was
unchanged.

To find out: whether instance 0 lost the lease under load (a renewal later than the lease's term: a
real handover the test should allow, or a term too short for a loaded host), or the test reads the
leader before the store settled.

Acceptance: the cause named; the test or the lease fixed accordingly, shown by the two CLI tests run
at once repeatedly.
