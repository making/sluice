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
- gate on `cargo fmt --check` and `cargo clippy --all-targets -- -D warnings`
  as well, for the wasmlet and the examples. The examples' toolchain
  (`profile = "minimal"`) lacks rustfmt / clippy: add
  `components = ["rustfmt", "clippy"]`. Clean up the existing
  `collapsible_if` warnings first (`rekey` in `main.rs`,
  `streams_response_frames_as_the_guest_produces_them` in `wasm_host.rs`)

Acceptance: a PR touching `sluice-wasmlet` runs its 34+ tests in CI, and fmt / clippy findings fail it.
Related: 025 (E2E in sluice-it) may reuse the built guests as artifacts.
