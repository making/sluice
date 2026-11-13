Difficulty: Low

# sluice-wicket-runtime-wasi: bound keyvalue's batch calls

`get-many` / `set-many` / `delete-many` (`keyvalue.rs`: `batch::Host`) check each key and value but not
their number, nor repeated keys: `get-many` with one key N times has the host hold N x up to 1 Mi outside
the guest's memory, on every backend (sqlite, redis, gemfire). By reading; first a failing test.

Fix: a ceiling on a call's keys / pairs and on the bytes a `get-many` answers (to decide: an error past
it, as a key too long is). 274 is the list page size, another matter. Extend the existing batch test.
