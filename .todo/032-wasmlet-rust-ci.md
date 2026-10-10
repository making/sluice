Difficulty: Low

# sluice-wasmlet: run the rust tests in CI

`.github/workflows/ci.yaml` builds only the Maven reactor; `sluice-wasmlet` is
never built or tested there.

- `cargo test -p sluice-wasmlet` builds the example guests through the rustup
  `cargo` proxy inside `sluice-wasmlet/examples`, which pins
  `nightly-2026-10-09` + `wasm32-wasip3` (`rust-toolchain.toml`): the job needs
  rustup with that toolchain installable (auto-install from the toml), plus the
  stable host toolchain for the wasmlet itself
- cache `~/.cargo/registry`, `sluice-wasmlet/target`, `sluice-wasmlet/examples/target`
- a separate job (path filter `sluice-wasmlet/**`, `sluice-proto/**`) so pure
  Java changes are unaffected

Acceptance: a PR touching `sluice-wasmlet` runs its 23+ tests in CI.
Related: 025 (E2E in sluice-it) may reuse the built guests as artifacts.
