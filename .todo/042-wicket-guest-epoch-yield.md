Difficulty: Medium

# wicket: a CPU-bound guest must yield its tokio worker

Found during the 041 investigation (by reading, not tested).

## Problem

`wasm_host.rs` enables `epoch_interruption(true)` and arms each store with
`set_epoch_deadline(budget_ticks(..))` (in `respond`, `run_unit` and its job loop). No
`epoch_deadline_callback` / `epoch_deadline_async_yield_and_update` is installed, so the
default applies: the guest runs uninterrupted until the deadline, then traps. A busy-loop
guest therefore holds its tokio worker thread for the whole budget (default 10s,
`DEFAULT_BUDGET_MS`). The runtime is the default multi-thread one (`#[tokio::main]`,
`main.rs:137`), one worker per core: as many busy guests as cores freeze the whole process
-- every tunnel stream's receive loop, every connection, gRPC keepalive -- for up to the
budget. The comment at `wasm_host.rs:79` covers only the ticker thread.

## Fix

Install a deadline callback on every store that yields on each tick and traps only once
the budget is spent: e.g. `store.epoch_deadline_callback(..)` returning
`UpdateDeadline::Yield(1)` while the elapsed ticks are below `budget_ticks(budget)`, and an
error (trap) after. Check the exact API and semantics in the wasmtime 49 docs first
(`Store::epoch_deadline_callback`, `UpdateDeadline`, `epoch_deadline_async_yield_and_update`)
and the project caveat in `CLAUDE.md`: a timer raced inside `run_concurrent` may never
fire, so the budget must stay enforced by the epoch count / the outside watchdog
(`supervise`), not by a timer inside the closure.

## Acceptance

- Test (cargo): with the runtime limited to N workers (`#[tokio::test(flavor =
  "multi_thread", worker_threads = 2)]`), N busy-loop guests running; a concurrent trivial
  task (or a second guest request) completes within a bound far below the budget. Watch
  it fail before the fix.
- Test: a busy guest still traps at its budget (the yield must not extend it); assert the
  elapsed time is within budget + one tick.
- Needs a busy-loop example guest under `sluice-wicket-wasi/examples/` if none exists (the
  hello guest's `/spin` is one).
- The ticker follows a fixed schedule now (tracks the wall clock; a stall beyond 100 ticks is
  forgotten), so the budget + one tick bound holds except after such a stall.
- `a_reset_stream_keeps_its_share_until_its_instance_is_gone` (059) relies on a
  spinning guest outliving its reset stream until the budget: the watchdog shares the
  guest's task and cannot stop it. Once the guest yields, the watchdog stops it at the
  next tick, and that test needs another way to keep an instance past its stream's end
  (or its premise is gone: then reduce it to the share being released with the store).
