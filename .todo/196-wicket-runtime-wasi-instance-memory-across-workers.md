Difficulty: Medium

# sluice-wicket-runtime-wasi: instance memory mapping across the runtime's workers

The wasm wicket runs on tokio's default runtime, a worker per core. Measured on the 64-core
test host (its README, Design notes): keep-alive `GET /` to the hello guest, per-request
instances, took 1.4ms of the wicket's CPU a request at 4.0-4.2k req/s with 64 workers,
0.44-0.52ms at 6.8-8.4k with 4. `perf` puts the CPU past a few workers in the kernel's
memory map lock and TLB shootdowns (`rwsem_spin_on_owner`, `smp_call_function_many_cond`):
each instance's memory is mapped and unmapped, and an unmapping interrupts every core
running one of the process's threads.

- Profile where the maps come from (an instance's linear memory, its tables, the fiber
  stacks of `run_concurrent`, the store) at 4 and 64 workers
- Try wasmtime's pooling allocator (`InstanceAllocationStrategy::Pooling`: slots reused,
  memory reset by `madvise` instead of unmapped), sized from `--max-instances` and
  `memory-mib`; check the pool routes (`pool=N`) and the memory a slot reserves
- Then choose the worker count (fixed, `--workers`, or the CPUs) on what remains; the
  firecracker wicket's relay workers (`sluice_wicket_tunnel::workers`) fit a relay, not
  a guest computing on the worker
