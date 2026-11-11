Difficulty: Low

# sluice-wicket-cf-api: a second process on a data directory is refused

Needs nothing; 275 leans on it (the file backend is one process, and says so).

Two cf-apis started on one `--data-dir` (a unit started twice, an old container not stopped)
each read `state.json` once and overwrite the other's changes from then on, silently; the
second's `Store::open` also removes the first's uploads under way. Only a Firecracker wicket's
disk has one writer (`disk.rs`).

`check_data_dir` takes an exclusive `flock` on a file of the directory (`.lock`), held for the
process's life, before anything is removed or written: a second start fails saying who holds it
(`--data-dir <dir>: in use by another sluice-wicket-cf-api`). Released by the kernel at the
process's end, a kill included: nothing to clean.

## Tests

Extend a `store::tests` open test: a second `Store::open` (or `check_data_dir`) on the
directory is refused while the first lives, taken once it is dropped.
