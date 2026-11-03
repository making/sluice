Difficulty: Medium

# sluice-wicket-runtime-firecracker: tests for what was only checked by hand

Each of these held on a real wicket, once, by hand (numbers in `.kb/wicket-firecracker-findings.md`),
and no test would see it break. Extend the existing tests rather than add new ones.

- **a restart's wait starting over**: 60s up and it is short again, while 30s up keeps it growing. The
  wait's sum is inside `tend_restarts` (`Restart` in `routes.rs`), and nothing has it: take it out and
  test it as a function, with no 60s waited
- **a sole route's relay of what is no HTTP**: `--vm` alone hands a guest speaking first through, with
  nothing sent; with `--console`, or as the config file's one route, nothing reaches that guest and
  another host answers 404. `tests/vm.rs` has `--vm` alone answering any host over HTTP: give the sample
  process a way to speak first (as `SAMPLE_WAIT_MS` does for its listening) and extend that step
- **an edit taking over from a reload on a disk, the reload made after it**: a touched `--kernel` and
  SIGHUP, or the poll, a PUT of the same app while the swap's VM starts: the reload fails `took over
  from it`, the PUT answers 200, one VM throughout, the reload made again finds no change. Extend the
  disk steps of `tests/vm.rs`, or `never_listening_app`, which already says a change of the files takes
  over

Each test seen failing with what it guards undone.
