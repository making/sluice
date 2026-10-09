Difficulty: High

# sluice-wasmlet: per-route capabilities + wasmtime-serve compatibility (p3 only)

Goal: run components built for `wasmtime serve`-style invocation on this platform, with
capabilities granted per route (wasmlet) instead of globally. **p2 / proxy-world
support is explicitly out of scope — p3 (`wasi:http/handler@0.3.0`) only.**

## Findings from wasmtime v49 `src/commands/serve.rs` (survey 2026-10-09)

- `-Scli` on `wasmtime serve` is repurposed from `wasmtime run`: present => link the
  full wasi surface (`add_wasmtime_wasi_to_linker` + http); absent => only what the
  serve path needs (p2: `add_to_linker_async`; p3: `wasmtime_wasi_http::p3` +
  p3 `clocks` / `random` / `cli`). I.e. the capability knob is **linker-level**.
- `tcp` / `inherit_network` live on `run.common.wasi.*` and flow into the `WasiCtx`
  (sockets via the host stack; verify the exact p3 gate — `wasi/tcp` /
  `inherit_network` handling in `wasmtime run` common flags — during implementation).
  `inherit-network` implies host-stack bind/connect for guest sockets.
- stdio: separate `inherit_stdin/stdout/stderr` flags (default off in serve).

## Design sketch

- per-route flag: `--caps host=cli,tcp,inherit-network` (repeatable; unknown name =
  startup error), default none = current bare behavior. Alternative: embed in the
  route value (`host=locator#cli,tcp`) — decide at implementation, keep the locator
  grammar clean
- `cli`: inherit stdio into `WasiCtxBuilder` (guest logs reach the wasmlet process
  stderr; today they are null), plus args/env = empty unless a follow-up wants them
- `tcp` / `inherit-network`: gate the socket paths; check whether wasmtime-wasi p3
  enforces anything at `WasiCtx` level or whether gating must be linker/instance
  level; do not fake-enforce
- the p3 host side is already linked in full (`wasmlet/src/wasm_host.rs` `load`);
  caps only configure `Ctx` per route — `Ctx` must become per-route (carries the
  `Caps`), the store is per-request already
- keep `NoHooks` unless a cap needs otherwise; http outgoing stays available as today

## Acceptance

- a p3 handler guest that opens a TCP connection (e.g. fetches an upstream and
  composes the response) works when its route grants `tcp,inherit-network`, and
  fails cleanly (trapped -> 500, tunnel alive) when not granted
- `cli` guest output appears in the wasmlet log
- existing hello route unchanged with no caps
