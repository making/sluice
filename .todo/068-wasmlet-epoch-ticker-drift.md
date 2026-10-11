Difficulty: Low

# wasmlet: the epoch ticker lags the wall clock under load

Found while working on 059. The engine's ticker thread (`engine()` in `wasm_host.rs`)
increments the epoch after each `std::thread::sleep(EPOCH_TICK)`, so every oversleep
adds up. A busy guest is stopped by the epoch deadline (`budget_ticks`), so its budget
stretches by the accumulated lag. Measured in `cargo test` (16 cores, the suite in
parallel): a `/spin` request with a 1500ms budget held its instance 1.92-2.01s
(`a_reset_stream_keeps_its_share_until_its_instance_is_gone`, whose timing allows for
it). The wall-clock watchdog (`supervise`) does not help a busy guest: it shares the
guest's task (042).

## Direction

Tick on a fixed schedule from a start instant (sleep until `start + n * tick`) and
increment once per elapsed tick when the thread wakes late, so the epoch tracks wall
time. Check `Engine::increment_epoch` cost for catch-up bursts.

## Tests (`cargo test`; watch each fail first)

- With the ticker's thread delayed on purpose (e.g. a test hook that stalls it for 5
  ticks), the epoch count after the stall matches the wall time elapsed / tick within
  one tick.
- A `/spin` request with a 1500ms budget ends within budget + 2 ticks while the suite
  runs in parallel.

Ordering: before or with 042, whose acceptance asserts a busy guest ends within budget +
one tick.
