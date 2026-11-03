Difficulty: Low

# sluice-wicket-runtime-firecracker: a `Diff` snapshot on real aarch64

A template now boots with `track_dirty_pages` and its snapshot is a `Diff` (`vm.rs`), for every cloned route and so,
through the cf-api, for every app. It ran on x86_64 alone: the CI's aarch64 runner skips the VM test, and an M3 or
later Mac was not at hand. Firecracker's `snapshot-support.md` lists no exception by architecture.

- `scripts/lima.sh` on a Mac: `tests/vm.rs` plain and jailed, then a pool of one at `mem=1024`: the memory file's
  blocks against its size, what a clone serves (`.kb/wicket-firecracker-findings.md`, A snapshot's memory, and its
  aarch64 section, a line)
- restores failing or the guest's memory not as it was: back to a `Full` snapshot there (`cfg!(target_arch)`, as
  `RESTORE_ADVANCES_CLOCK` is), the README's memory sentences saying so, not a `Diff` given up on x86_64
