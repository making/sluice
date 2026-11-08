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
is left per request. A `warm=16` route serves 66k at 0.23ms either way.

## A pooled unit's latency

Hello guest, release profile: ~250us cold vs ~130us pooled per request.

A guest with a costly start against the hello guest (the ignored `load_comparison_reference`,
sequential requests, release profile, mean of 200; the guest built for it, not kept): one with a
16MiB data section (a snapshotted JS / Python runtime's size), per request touching nothing,
reading a byte of each of its pages, writing a byte of each page of an 8MiB heap, or running
~0.5ms of init once per instance. Cold / pooled:

| guest, request | Linux 7.2 (aarch64, a container on OrbStack) | macOS (M-series) |
|---|---|---|
| hello | 34-40us / 14-18us | 51us / 20us |
| 16MiB data | 83-85us / 13-14us | 975-983us / 17us |
| 16MiB data, read | 95-103us / 39-43us | 1.04ms / 30us |
| 8MiB heap, written | 1.62-1.67ms / 28-29us | 1.37-1.40ms / 26us |
| both | 1.24-1.26ms / 52-55us | 1.42-1.49ms / 38-39us |
| init | 470-473us / 14us | 1.44-1.49ms / 18-19us |

On Linux a data section costs a cold instance little: its image is mapped copy-on-write, and a
read faults the shared page in. A written page is the cost: a fresh instance faults every page its
heap writes (past the 4MiB the pooling allocator keeps resident), a unit wrote them on its first
request. Without copy-on-write (macOS) the data section is copied per instance. So the gap grows
with what a guest writes and computes at its start, 1-1.6ms here against 20-30us for the hello
guest: what `keep-ms` keeps beyond `warm`.

## Concurrency

A store is driven by one task. On an M4 Max (16 cores, default workers), keep-alive `GET /`
to the hello guest, `oha` for 5s on `--listen`, release profile; rate, and the wicket's CPU per
request:

| route | `-c 16` | `-c 64` |
|---|---|---|
| `warm=16` / `warm=64`, one request a store | 92k, 62us | 110k, 66us |
| `warm=4&concurrency=4` / `warm=16&concurrency=4` | 89k, 62us | 110k |
| `warm=4&concurrency=16` | | 99k, 64us |
| `warm=1&concurrency=16` / `warm=1&concurrency=64` | 40-41k, 61us | 36k, 63us |
| no pool, one request a store | 22k | |

A request costs the same CPU in a shared store; one store's requests share one core, which caps
it near 40k/s for the hello guest. `/hold/5` (the guest waits 5ms on a host timer) at `-c 16`:
2.1k/s on `warm=1&concurrency=16` and on `warm=16` alike, 145/s on one unit of one request.
So the default stays 1 and CPU-bound routes spread over stores; the cf-api maps a CF instance
to one store (memory, indices and the trap's reach as CF has them), `cf scale -i` adding cores.

The cf-api's default of 200 (2026-11-08, same machine, `warm=1&instances=1`, `oha -c 200` for
5s): `/hold/50` served 3.8k/s at 53ms on average (86ms the slowest) at `concurrency=200`, against
340/s at 596ms (3.1s) at 16, the others waiting for room; `/` 26k/s against 34k/s. The wicket's
RSS stayed 57-59MB, and the pool's fiber stacks (8 an instance of `--max-instances`) sufficed.

What wasmtime 50 lets the host do with several requests in one store:

- no guest task can be cancelled (wasmtime#11833; `wasmtime_wasi_http::handler` lets an expired
  task run on, as here). Dropping a call's future leaves its task in the store: a request past
  its budget is answered from outside, its store drained and dropped once its others are done
- busy wasm, the epoch callback and sync host calls hold the store (wasmtime#11869, #11870):
  nothing else in it runs, the closure passed to `run_concurrent` included, so timers live
  outside it. An epoch callback returning `UpdateDeadline::Yield` suspends the fiber and returns
  `Pending` from the store's poll, which lets the watchdog beside it run; a tokio `Sleep` past
  due may still be unfired then (the worker was held), so the watchdog checks its deadline at
  every poll. Requests handed to a store held so are never taken: answered 503 at its end
- a trap poisons the store: every request in it fails
- the task group hook (feature `task-group-hook`) tells which call's task group runs at each
  thread switch. A guest's write to stdout reaches the host's `StreamConsumer` within the write
  (`Instance::consume`, polled synchronously), in the writer's group, so a line is attributed
  so. Binding at `write-via-stream` would not do: Rust's wasm32-wasip3 std opens stdout's stream
  once per instance (one `async_stream()` for every request a unit served)

## Transport flow control

A unit test sends 32MiB through a proxy that holds the wicket's answers back one round trip at a
time: 17 round trips at hyper's defaults, at most 3 with the window.

## A client gone from `--listen`

A client that closes a socket with nothing unread sends a FIN alone, as one that only half-closed
(RFC 9112 9.6) does. A reset comes only with bytes left unread or from a write to the closed socket.
So watching for a reset alone frees
nothing for the common client, and the wicket probes as the server does ("Half-close probes" in
[connection-limits.md](connection-limits.md)): same `100 Continue`, default interval and limit, a client
gone answers the first probe with a reset.

Treating the FIN as the client leaving (nginx's `proxy_ignore_client_abort off`, hyper without
`half_close`) would be one line, and is refused on both paths: it drops the answer of a client that
half-closed, and over the tunnel it would undo the server's probes, which keep such clients answered.

hyper does not read between a request's end and its response's end, so `listener::client_gone` runs
beside it on the same socket, polled with it and never at once: a probe cannot land within hyper's
writes (no probe while they are unflushed, as a pipelined request's predecessor may still be). The FIN is
tokio's sticky `READ_CLOSED`, read each interval rather than awaited: bytes pipelined behind the request
keep the socket readable, and waiting for it would spin.

## A guest's files, and a response's headers

An app's files reach its guest as a directory preopened read-only at `/` (`wasi:filesystem`,
`FsPerms::ReadOnly`), linked for the route alone without `cli`: the guest opens and reads, and its
reads stream from the host (`read-via-stream` hands back the stream a response body takes as it is,
so a file is never copied into linear memory and a site is not bounded by `memory-mib`). The
directory is unpacked once from a tar uploaded by digest, files and directories only, and keeps the
tar's times: a guest's `ETag` of size and time is the tar's, the same on every wicket and across
restarts. A preopen is opened per store: a request without a pool opens it per request.

In wasi:http 0.3, `response.get-headers` (and the request's) answers an immutable copy: an `append`
on it fails, and with `let _ =` silently. Every header goes into the `fields` before
`Response::new` (sluice-wicket-site's `text` takes them so).
