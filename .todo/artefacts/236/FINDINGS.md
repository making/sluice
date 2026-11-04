# Per-route filters: spike findings

Code on the client: `client-filter-spike.patch` (applies to b55c26d with `git apply --binary`; nothing of it is on main). The data plane and wicket alternatives: `route-filter-spike.patch`. Manual check: `llm_upstream.py`, `run-native.sh`; `upstream.py` for the wicket guest.

## A plugin mechanism for the client

Recommendation:

- Extension point for users: proxy-wasm plugins (ABI 0.2.1) interpreted by Endive, `ProxyWasmResponseFilter`.
- Filters the maintainer ships: Java on `ResponseFilter`, the token usage among them.
- An external process over gRPC is not needed: the native image holds.

### Why proxy-wasm, not an ABI of ours

Both were built on the same pipe: `WasmResponseFilter` with an ABI of ours (`sluice-filter` in `src/test/guest/sdk`), and `ProxyWasmResponseFilter`. The usage logic is one crate under both.

- Users get SDKs and documentation that exist (Rust 0.2.5, May 2026; a Go SDK under the same organization), and no ABI to design or keep.
- The SDK's own `http_body` and `http_headers` examples, source unchanged, run in `theSdksOwnExamplesRun`.
- `http_body` removes `content-length` to replace a body, the convention the host uses to decide a body may change.
- It costs no more CPU than our ABI (below), and a VM serves many streams instead of one instance per response.
- Price: a larger host (959 lines against 507 after formatting), and a partial implementation of the ABI.

### Evidence

Native image, macOS arm64, 16 processors. No reflection or other configuration was needed. Endive's docs do not state native image support for the interpreter; this is the evidence.

- `WasmResponseFilterTest`, `ProxyWasmResponseFilterTest`, 13 tests: pass on the JVM and on a native image (`-PnativeTest`).
- The real native client through a real server, with curl and `llm_upstream.py`: the counters on `/actuator/prometheus` are the expected ones for each API, JSON and SSE.
- `WasmFilterE2ETest` (real server and client, JVM): a stream arrives event by event; a client leaving is counted `cut`.
- Whole reactor with the usage plugin on every HTTP/1.1 upstream connection (`SLUICE_SPIKE_PROXY_WASM_FILTER`): `sluice-it` 697 tests, 2 failures, 1 error, 1 skipped, the same `partial-head` parameters as before: a filter holds a response head until it is complete. `sluice-server` and `sluice-client` pass.

That whole run found what the focused tests had not, all fixed: heads rewritten in lower case by any edit, and a broken chunk size lost under a rewriter.

Misbehaving plugins (`misbehave`, `misbehave-proxy`), on a native image too: an endless loop, a trap, a deep recursion, memory exhausted, a framing header edited, CRLF in a value or a name. The response passes unchanged, a failure is counted, the VM starts anew. A plugin that had changed a body gets the response cut.

### Cost

One run each, on one machine: orders of magnitude, not benchmarks. `WasmGuestCostProbe`, native image:

| | ABI of ours | proxy-wasm | Java |
|---|---|---|---|
| Response of a path it ignores | 163us | 132us | - |
| JSON 1KiB | 583us | 654us | - |
| JSON 1MiB | 61ms | 64ms | - |
| SSE, 2000 events | 468ms | 491ms | 1.6ms |

- Java is a small scanner with Jackson, in the probe; it reads less than the guest does. The ratio, about 300, is why the shipped usage filter should be Java.
- The ratio is the guest's scanning in the interpreter: a guest doing nothing costs 15ms for those 2000 events.
- The interpreter on the native image is about 1.5 times slower than on a JVM warmed for a few hundred calls.
- Endive's runtime compiler is documented as possibly unsupported on a native image, and the build time compiler cannot take a module added later. Neither was tried.

The real binaries, 200 streams at once of 152 events each (30400 events), twice each:

| | CPU | RSS during (idle) |
|---|---|---|
| No filter, baseline build | 1.9s | 164MiB (106) |
| ABI of ours | 11.0s | 294MiB (114) |
| proxy-wasm | 11.3s | 249MiB (136) |

So about 300us of CPU per event, five times the relay's own work: usable for one user's LLM traffic, not for a busy gateway. More than 200 streams were not run; a heap figure per proxy-wasm stream came out negative (noise) and is not reported. The image grows by 2MB, both hosts included.

### Isolation

- A guest gets no WASI files, sockets or environment, and every outgoing call answers `UNIMPLEMENTED`. Time, randomness and logs only.
- Memory is capped per VM, and each call's time (the interpreter checks for an interrupt at each branch and call).
- Guests run on platform threads, the connection's virtual thread waiting unmounted: with a guest spinning on every processor, a virtual thread still started at once (probe).
- Metrics are capped; logs are cut and stripped of control characters; head edits are validated, `host` and framing refused. Names were not checked before this spike.
- The SDKs fail on a header that is not UTF-8. A client must not be able to do that, so a plugin reads replacement characters while the line on the wire stays as it came.
- Not bounded: a module declaring a huge table at instantiation. Modules are chosen by the operator.

### Limits of the proxy-wasm host

- A failure discards the VM and ends the observation of its other streams, whose counts are lost. One request can trigger that for others on the same VM.
- A stream stays on its VM, one call at a time: a plugin stuck for its 1s holds the other streams of that VM. Not measured.
- Not offered: pausing a head, the request's body, a local response, outgoing calls, shared data, ticks, gauges and histograms.
- Without the request's body a filter cannot add `stream_options.include_usage`.
- A metric's tags are in its name (`name;key=value`), a convention of ours a plugin would not carry to Envoy.
- A chunked response goes through the rewriting path even if the plugin only reads: same payload, chunks possibly split, `Transfer-Encoding` moved to the end of the head.
- Run nowhere but macOS arm64. Not run: a TLS upstream, the release images.

### LLM usage, as verified

- Anthropic (documentation): `message_delta`'s usage is cumulative, and with a server tool its `input_tokens` is larger than `message_start`'s. `input_tokens` excludes both cache fields.
- OpenAI (documentation of the Responses API): `input_tokens` includes `cached_tokens` and `cache_write_tokens`. The guest assumes the same of `prompt_tokens`, which that page does not state.

## Response rewriting on the client

The goal turned out to be rewriting an upstream's responses, bodies included. It changes `sluice-client` and adds `sluice-it` tests, the server not at all.

`ResponseFilterPipe` wraps the upstream socket's `DuplexPipe` in `NodeConnection.dial`, for connections whose `Connect.application_protocol` is `http/1.1`. The tunnel and the server's relay see an ordinary upstream. Each connection has its own virtual threads, so a filter may block, holding that connection only; the tunnel's flow control holds the upstream meanwhile. The byte accounting that makes body rewriting hard on the h2 demux does not exist here.

### Evidence

`ResponseFilterE2ETest` (6 tests, real server and tunnel client), all passing:

- A pipeline on one connection through Content-Length, chunked, HEAD, 304, 103, an unframed body, a verbatim body and a whole-body replace across chunks: the server's relay keeps following the framing.
- A rewritten chunk reaches the client before the upstream sent the next.
- 8MiB doubled in length arrives whole.
- `/close` and a filter throwing on a head: `502`. A cut body stays cut, no last chunk made up.
- 101 and what follows pass verbatim.
- h2c on a route without `force-http1` passes unfiltered; with it the stream is refused.

Whole `sluice-it` with `SLUICE_SPIKE_RESPONSE_FILTER_ALL=1` (a filter that rewrites nothing on every client, so every HTTP/1.1 upstream connection goes through the pipe): 695 tests, 2 failures and 1 error, 1 skipped. The three are the same `partial-head` parameters as the data plane's run: the head is held until complete. `sluice-server` 438 and `sluice-client` tests all pass.

### Limits

- h2 is not covered. An h2 connection opens an h2 leg, whose responses pass unfiltered: `force-http1` is needed on the route, so browsers fall back through ALPN and the `421`. An h2-only client (h2c with prior knowledge, gRPC) gets the `421`.
- Real h2 needs one of these, neither prototyped: an h2 endpoint on the client, with `proxy.hop` and netty's codec moved to `sluice-proto` and windows per hop; or the server's demux opening HTTP/1.1 legs for such a route.
- `Connect` carries the target address, not the route: routes sharing a target differ only by the `Host` header, and with `rewrite-host` by `X-Forwarded-Host`.
- A filter handles `Content-Encoding`, `ETag`, `Range` (206) and `Digest` itself. The spike removes `accept-encoding` towards the upstream, which an upstream may still ignore.
- A request of HTTP/1.0 is not offered to the filter (no chunked framing).
- A rewrite past the limit of `ResponseFilter.whole` cuts the response: decide by `Content-Length` and type at the head.
- Not done: configuration (a bean needs a custom build; built-ins by name, or a plugin), advertising `force-http1` automatically for a filtered upstream so h2 cannot bypass, and sharing the HTTP/1.1 parsers through `sluice-proto` (the pipe has its own). Not run: TLS upstream.

## Placements

| Place | What it can do | Cost | Verdict |
|---|---|---|---|
| sluice-server data plane | head edits, short circuit, async decision per request, body observation, all per route after the credential check | relay changes, about 250 lines plus the SPI | analysis and head edits |
| wicket guest (`wasi:http`) | all of it plus body rewriting, in a sandbox | no host change, one more hop | body rewriting and arbitrary logic |
| sluice-client | response rewriting on HTTP/1.1 connections, no server change; see "Response rewriting on the client" | Medium; h2 High | response rewriting |
| A gateway behind the client (SCG itself) | everything | another process to run | when one is already there |

## Data plane

Request side: `Verdict` already is a filter's result (`Pass(strip, add)`, or an answer), and both relays take it after the credential check. `RouteFilterChain` runs after that check and returns a `Verdict`. Filters see the head with the credentials hidden.

On h2, an async decision uses the existing parking, with its budget and `h2-stream-wait-timeout`, and holds its own stream only (`h2AnAsyncDecisionHoldsItsOwnStreamOnly`).

Response side:

- h2: the head is decoded at `LegListener.onHeadersRead`; editing it there is a few lines.
- HTTP/1.1: `ResponseTracker` only observed bytes already written. On a route with filters it now holds the head until complete and writes everything itself (`Leg.holding`). Routes without filters take the old path.
- The observer gets body payload decoded of the chunked framing, both ways, on both protocols.
- Framing is read from the head as the upstream sent it, so an edit cannot change it. `RouteFilterChain.validate` refuses framing headers, `host` and control characters (a CRLF would smuggle a header on HTTP/1.1).

A filter that throws, times out (5s) or returns an invalid edit gets the request answered `500` / `504`, never relayed.

### Evidence

- `RouteFilterE2ETest` (6 tests, real server and tunnel client): passes.
- Whole `sluice-it` with `SLUICE_SPIKE_FILTER_ALL=1` (a pass-through filter on every route, so every HTTP/1.1 response goes through the holding path): 695 tests, 3 errors, 1 skipped.
- The 3 errors are the `partial-head` parameters of `HalfCloseProbeE2ETest` (2) and `ProbeResponseRaceE2ETest` (1). They assert a partial head reaches the client before it is complete, which holding changes by design.
- The same two classes without the switch: 18 of 18 pass.
- `sluice-server` tests: 438, 1 failure: `PackageStructureArchTest`, the `auth` / `filter` cycle.

### Limits found

- Observers must not block. By the code: on h2 they run under the `Loop` lock of the whole connection, on HTTP/1.1 under the client stream's lock. No test shows the stall.
- A response head has no async path. On h2 it would need parking of its own, like the request's: high effort.
- An HTTP/1.1 filter answer ends the connection, like a refusal; on h2 the connection goes on.
- `exchange.reset` is also set on a normally ended h2 response (the upstream's stream closing); completion is `responseWritten`.
- Observed bodies are as sent: a gzip body stays gzip.
- Not verified: declarative config, native image, overhead, a real TLS-terminated filter test (the switch run covers `TlsDataPlaneE2ETest` with a no-op filter only).

### Body rewriting on the data plane: not recommended

Not prototyped; from the code and `.kb/h2-demux.md`:

- h2 grants a window back per byte written on the other hop, and the budget charges the same bytes; a rewrite breaks that 1:1 accounting.
- Holding a whole body deadlocks past the 1MiB stream window unless the relay grants more than it can hold.
- HTTP/1.1 would need the framing rewritten (`Content-Length` to chunked, HEAD, 304, Range).

## To close

- Config: `sluice.route.filters.<key>`, built-ins by name; no conditional beans (native).
- Bypass: TLS passthrough, tcp routes and non-HTTP connections are relayed unfiltered. A policy filter must refuse them as `routeAuth.protects` does. Not implemented.
- Output: only refusal lines of the access log carry a status today. Analysis needs a line with the relayed status, then the flume and metrics. Keep the heavy work off the relay: a bounded queue, dropping and counting.
- Package cycle: `Verdict.Pass` names `ExchangeObserver`, and `filter` names `Verdict`.
- `RouteFilterChain` passes the switch `SLUICE_SPIKE_FILTER_ALL`: spike only, remove it.

## Wicket guest

`examples/filter` in the patch: about 140 lines of Rust, no host change. Checked by hand with an existing `sluice-wicket-runtime-wasi` debug build (2026-11-02, so a little older than the source) in local mode, with `upstream.py`:

- HTTP/1.1 and h2c: `x-secret` removed and `x-filtered` added towards the upstream, `x-upstream-private` removed and `x-observed` added towards the client.
- `POST` of 3MiB: the upstream read 3145728 bytes, and the guest's tap counted the same.
- `/stream`: the second chunk arrived 2.0s after the first, as the upstream sent it: not buffered.
- `/blocked`: `451` from the guest.

Limits:

- The upstream must be reachable from the wicket by network, and the wicket becomes the route's tunnel client.
- The WIT has no way to express an upgrade (only the `HTTP-upgrade-failed` error code), so WebSocket passthrough looks impossible. Not tested.
- Not tested: trailers (gRPC), overhead per request, chaining through `wasi:http/middleware`.
- The guest reaches any address the wicket host can (README "Capabilities").
- The upstream address is compiled in here (`FILTER_UPSTREAM`).
