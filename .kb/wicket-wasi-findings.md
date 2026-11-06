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
