# sluice-wicket-runtime-wasi: findings

Measurements of the wicket. What a user needs of them is in
[its README](../sluice-wicket-runtime-wasi/README.md).

## Instance allocation and workers

On a 64-core host, keep-alive `GET /` to the hello guest (per-request instances, `oha` for 5s on
`--listen`; rate, and the wicket's CPU per request):

| workers (`TOKIO_WORKER_THREADS`) | 1 | 4 | 16 | 64 |
|---|---|---|---|---|
| pooled, `-c 16` | 5.3k, 0.19ms | 15k, 0.26ms | 47k, 0.31ms | 42-44k, 0.37-0.41ms |
| pooled, `-c 64` | | | 62k, 0.25ms | 118-125k, 0.36-0.37ms |
| on demand, `-c 16` | 3.2k, 0.30ms | 6.8-8.4k, 0.44-0.52ms | 4.3-4.8k, 1.0-1.4ms | 3.8-4.2k, 1.4-1.5ms |

On demand, each instance's memory and fiber stack were mapped and unmapped, and past a
few workers the CPU went to the kernel's memory map lock and TLB shootdowns
(`rwsem_spin_on_owner`, `smp_call_function_many_cond` in `perf`: an unmapping
interrupts every core running one of the process's threads); pooled, no memory map call
is left per request. A `pool=16` route serves 66k at 0.23ms either way.

## A pooled unit's latency

Hello guest, release profile: ~250us cold vs ~130us pooled per request.

## Transport flow control

A unit test sends 32MiB through a proxy that holds the wicket's answers back one round trip at a
time: 17 round trips at hyper's defaults, at most 3 with the window.
