Difficulty: Low

# sluice-wicket-wasi: run the rust tests in CI

`.github/workflows/ci.yaml` builds only the Maven reactor; `sluice-wicket-wasi` is
never built or tested there.

- `cargo test -p sluice-wicket-wasi` builds the example guests through the rustup
  `cargo` proxy inside `sluice-wicket-wasi/examples`, which pins
  `nightly-2026-10-18` + `wasm32-wasip3` (`rust-toolchain.toml`): the job needs
  rustup with that toolchain installable (auto-install from the toml), plus the
  stable host toolchain for the wicket itself
- cache `~/.cargo/registry`, `sluice-wicket-wasi/target`, `sluice-wicket-wasi/examples/target`
- a separate job (path filter `sluice-wicket-wasi/**`, `sluice-proto/**`) so pure
  Java changes are unaffected
- gate on `cargo fmt --check` and `cargo clippy --all-targets -- -D warnings`
  as well, for the wicket and the examples. The examples' toolchain
  (`profile = "minimal"`) lacks rustfmt / clippy: add
  `components = ["rustfmt", "clippy"]`. Clean up the existing
  `collapsible_if` warnings first (`rekey` in `main.rs`,
  `streams_response_frames_as_the_guest_produces_them` in `wasm_host.rs`)

Acceptance: a PR touching `sluice-wicket-wasi` runs its 61+ tests in CI, and fmt / clippy findings fail it.
Related: 025 (E2E in sluice-it) may reuse the built guests as artifacts.
