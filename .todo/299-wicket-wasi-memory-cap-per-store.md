Difficulty: Medium

# sluice-wicket-runtime-wasi: `memory-mib` must bound the store, not each memory

`MemoryCap::memory_growing` (`wasm_host.rs`) refuses `desired > max`, where `desired` is one memory's size;
`used` sums every memory but is not compared. The pooling allocator allows 4 memories per instance, so an
instance may hold 4 x `memory-mib`. By reading, not reproduced: per the bug-fix rule, first a failing test
with a component of several memories.

With it:

- `table_growing` answers `Ok(true)` always: bounded by the pool's 100,000 elements x 16 tables, and by
  nothing on the on-demand fallback (address space reservation refused: no table, memory count or core
  instance limit there)
- whether the GC heap goes through the limiter is unchecked
- `ResourceTable` has no entry limit

Fix: compare `used + growth` with the cap, count tables (and the GC heap if it bypasses the limiter)
against it, the same limits on the fallback. Extend the existing memory cap test. A refusal is told to
the store's meter (`usage::Meter::refused`, 291), which names the trap that follows out of memory and
lists `used` as the instance's memory: keep both on every refusal and growth, the tables' included.
