# Filters on the client

Detailed behavior behind README "Filters". What a plugin is offered: [proxy-wasm-plugins.md](../proxy-wasm-plugins.md). The spikes behind this, the data plane and wicket alternatives included: `.todo/artefacts/236`.

## Why on the client

| Place | What it can do | Cost | Verdict |
|---|---|---|---|
| sluice-client | request and response rewriting on HTTP/1.1 connections, no server change but naming the route | this; h2 would be high effort | filters |
| sluice-server data plane | head edits, short circuit, async decision per request, body observation, per route after the credential check | relay changes | analysis and head edits only |
| wicket guest (`wasi:http`) | all of it plus body rewriting, in a sandbox | no host change, one more hop; the wicket becomes the route's tunnel client; no way to express an upgrade | arbitrary logic |
| A gateway behind the client | everything | another process to run | when one is already there |

Body rewriting on the data plane is not worth it: h2 grants a window back per byte written on the other hop and the budget charges the same bytes, so a rewrite breaks that accounting; holding a whole body deadlocks past the stream window unless the relay grants more than it can hold. On the client each connection has its own virtual threads, so a filter may block, holding that connection only, the tunnel's flow control holding the upstream meanwhile, and that accounting does not exist.

## The pipe

`UpstreamFilterPipe` wraps the upstream socket in `NodeConnection.dial`, for the connections of a filtered route. The tunnel and the server's relay see an ordinary upstream. Both directions are followed by the parsers the server's relay uses (`am.ik.sluice.http1`), so the pipe and the relay cannot disagree on framing. The server's relay checked each request with the strict parser before relaying it; a request the pipe's parser refuses all the same fails the connection.

- A response: its head is offered, edited in place (no edit, the same bytes). An observer gets the payload as it goes on, the framing the upstream's. A rewriter's output goes out chunked from the head on, each call's output its own chunk at once; to an HTTP/1.0 client, unframed with `Connection: close`, the connection ending behind the body. A framed body cut stays cut: no last chunk is made up. A response the parser cannot follow, and what follows a `101` or a `CONNECT`'s 2xx, pass verbatim.
- A request: likewise, but a rewriter's request head waits for its first output: at the end of the body (the filter held all of it) it goes out with a `Content-Length`, chunked if a part came before, as the original if the rewriter says the body is unchanged and it had a length. An upstream that takes no chunked request so never sees one from a filter holding the body. A request with `Expect: 100-continue` goes out chunked at once: its client waits for the upstream's `100`.
- Whatever a filter leaves of a head's framing, a body not rewritten goes as it was framed: without it the body would read as the next message, from a public client's bytes. An edit past 64KiB of head fails: the relay would stop following it.
- `h2c` does not reach the upstream: h2 past a `101` could not be read, and RFC 9113 lets the upstream answer an ordinary request. The pipe removes it from `Upgrade`, with `HTTP2-Settings`, on every request of the route before any filter: `paths` cannot exempt it, as h2 switched to on one path would carry every path of that connection. Anything naming h2c goes (`H2C/1`, `h2c x`), not only the exact token, as an upstream may read those as it. Other upgrades, one offered beside `h2c` too, pass past their `101`: a filter whose `paths` match sees the request and may refuse it.
- A filter failing, on either side, fails the relay's call: the server answers `502`, or cuts the response begun.
- A local answer (`Exchange.localResponse`) replaces the upstream's response in the order of responses: once the responses before it ended. The upstream is closed then, or at once when nothing is awaited, which is also what wakes the response side reading it. Nothing more of the connection goes on. A request whose response has begun cannot be answered: the connection is cut.
- `close` may run on the tunnel's receive path, which must not wait on a filter: it closes the upstream, which ends what the pumps wait on, and tells the filters on a thread of its own.

Not interpreted: a filter sees bytes as sent, so `Content-Encoding`, `ETag`, `Range` (206) and `Digest` are its own to handle. The `llm-usage` filter removes `accept-encoding` towards the upstream on its paths, and counts what still comes compressed as `outcome=encoded`.

## A route's filter

`Connect.route` names the route (`Router.Route.routeKey`), as routes may share a target: a leg of the server's relays serves one route, HTTP/1.1 and h2 alike. A server before the field names none: the address decides if the routes at it agree on their filters, the connection is refused otherwise.

A filter reads HTTP/1.1. So a filtered upstream advertises `force-http1`, which makes the server refuse its h2 streams with `421` (.kb/h2-demux.md); `tls-passthrough`, `listen-port` and `tcp://` with filters stop the client's start; a connection of a filtered route with another `application_protocol` (a hostless head on a catch-all, say) is refused with an ERROR before any dial, counted as `sluice.client.filter.refused`.

Real h2 needs either an h2 endpoint on the client, with `proxy.hop` and netty's codec moved to `sluice-proto` and windows per hop, or the server's demux opening HTTP/1.1 legs for such a route. Neither was prototyped: high effort, so `421` for now. Browsers are served over HTTP/1.1 (ALPN, and they retry a `421`); an h2-only client is not.

## Settings

`sluice.client.filters.<name>` defines a filter: a plugin, or the defaults of a built-in or factory of that name. `sluice.client.upstream[n].filters[m]` uses it; what a use sets goes over the definition (`FilterConfig.overriddenBy`: `paths` and the plugin's limits replace, `args` merge key by key). Each use has its own filter: its instances of a plugin, its counters by `route`. The name is resolved at the start: unknown, it stops the client. A factory (`UpstreamFilterFactory`) is how a custom build adds one in Java, as a bean; `llm-usage` is one.

A filter in Java, in a custom build of the client:

```java
@Component
class StampFilterFactory implements UpstreamFilterFactory {

	@Override
	public String name() {
		return "stamp";
	}

	@Override
	public UpstreamFilter create(Context context) {
		String value = context.settings().args().getOrDefault("value", context.route());
		return (request, requestHeaders) -> (status, headers, bodiless) -> {
			headers.set("x-stamp", value);
			return null; // the body passes unseen; a BodyObserver or BodyRewriter sees it
		};
	}

}
```

```properties
sluice.client.upstream[0].filters[0].name=stamp
sluice.client.upstream[0].filters[0].args.value=a
```

`FilterChain` runs a route's filters: first to last on a request, last to first on a response, each body through what the previous left. `paths` keeps a filter from all else before anything of it runs: a plugin costs its call on a response it ignores (below). It is a cost measure, not a policy: compared as sent, undecoded, so `/v1/%6Dessages` is another path. A filter failing is counted and left out of that exchange, unless it fails closed; a rewriter failing fails the message, part of which it may have changed.

## Memory

A filtered connection buffers up to 256KiB on top of its 293KiB (`UpstreamFilterPipe.CONNECTION_BYTES`): a head being read each way, the heads of the requests awaiting responses (a pipeline past 4 of them or 64KiB waits before its filters are called, holding the tunnel's flow control, and the wait ends with the responses), the output waiting for the relay (past 64KiB it charges the tunnel budget as it is written, or fails). What it reads of the upstream at once is below that, so what merely passes never depends on the budget. While it waits, it reads nothing of the tunnel: the client's end reaches the upstream only after, so a client gone is held until the upstream answers, as an unfiltered connection is when the upstream ignores its end. A route's plugin adds up to 256KiB more in the host: what it keeps of each head's fields, of at most 256 fields each and 5 streams. With a filter on any route every connection counts the most of these in the derived limit, and the plugins' `vms x max-memory` come off the bytes first: an instance's memory, its objects and tables with it, is taken as it grows, at worst to that. `max-connections` set, the count alone limits, as before.

What a filter holds of a body charges the tunnel budget, not the limit: most hold nothing, and a body worth holding should not cost every connection its size. A hold that does not fit gives up: `include-usage` passes the body unchanged, a plugin fails. Windows shrink under the same budget, which is what they do under load anyway.

## llm-usage

One scanner of JSON and server-sent events, a dialect per API by exact request path, the same counters out. `JsonFields` streams through a body picking fields by path, holding a few hundred bytes whatever the size or split, so a filtered connection costs nothing more for it, and no body is too large to count. It reads what is well formed and is merely wrong on what is not.

- OpenAI compatible (`/v1/chat/completions`, `/v1/completions`, `/v1/responses`): the JSON's `usage`; on SSE only the chunk `stream_options.include_usage` adds before `[DONE]`. That is a request body option: without it the stream counts as `no_usage`. `args.include-usage` adds it to a streaming request that does not set it, spliced before the closing brace so nothing else of the body changes; off by default, as its client gets one chunk more, with no choices, and an upstream may refuse the option.
- Anthropic (`/v1/messages`, not `/v1/messages/count_tokens` or `/batches`): the JSON's `usage`; on SSE `message_start`'s `message.usage`, then `message_delta`'s `usage`, cumulative. With a server tool the last `message_delta` carries a larger `input_tokens`, so later values overwrite, field by field; never add them. No `[DONE]`; ends in `message_stop`, an `error` event may come mid-stream. `input_tokens` excludes `cache_creation_input_tokens` and `cache_read_input_tokens`.
- OpenAI's `input_tokens` (documentation of the Responses API) includes `cached_tokens` and `cache_write_tokens`; the filter assumes the same of `prompt_tokens`, which that page does not state, and counts `input` without them, as Anthropic reports it.
- A cut stream leaves a partial count, recorded as `outcome=cut`.
- `model` comes from a response: past 256 values per use it counts as `other`. Every series has the same tag keys, as Prometheus needs: what was not read has `model` and `stream` = `unknown`.

## The proxy-wasm host

Why proxy-wasm rather than an ABI of ours: both were built on the same pipe. Users get SDKs and documentation that exist, and no ABI to design or keep; the SDK's examples run unchanged; it costs no more CPU (below), and an instance serves many streams instead of one per response. The price is a larger host, and a partial implementation of the ABI. An external process over gRPC is not needed: the native image holds.

Decided of what the spike left open:

- **A request's body**: offered, read or replaced, as the response's. It is what a plugin needs to add `stream_options.include_usage`, or to inspect a prompt.
- **A local response**: offered from the request's calls. It is the most used call after the heads, and a plugin that could not refuse would fail open on an SDK's panic.
- **Outgoing calls**: fail softly (`InternalFailure`, which the Rust SDK hands back as `Err`), so a plugin can go on. A plugin reaches nothing outside.
- **Pausing a head, ticks, shared data, queues, gauges, histograms**: not offered. A head's pause has nothing to resume it without calls or ticks. Shared data would make a plugin's state outlive an instance's failure; add it when a plugin needs it. `Unimplemented` panics in the SDKs, which fails the plugin visibly, and its first call is logged.
- **A VM failing** discards it and ends the observation of its other streams, whose counts are lost. One request can trigger that for others on the same VM. Accepted: a VM per stream costs what the measurements below rule out. Their later calls fail on their own and leave the VM started anew alone; contexts keep counting, so an old one is never taken for a new one's.
- **A stream stays on its VM**, one call at a time: a plugin stuck for its `call-timeout` holds the other streams of that VM.
- **A metric's tags are in its name** (`name;key=value`), a convention of ours a plugin would not carry to Envoy.
- **The allocator** is exported as `malloc` by the Rust SDK on `wasm32-unknown-unknown` and as `proxy_on_memory_allocate` elsewhere: both are taken, as Envoy's host does. The test plugins cover one each.
- **A plugin's objects** (Wasm GC) are counted, not refused: rontolisp compiles filters to them, on a linear memory for the ABI, and Envoy runs those. Its no-GC backend leaves `defclass`, `handler-case` and `car` behind, too high a price for a filter's author. `PluginHeap` is what makes this safe; see "Objects on the heap".

Guests run on platform threads, the connection's virtual thread waiting unmounted: with a guest spinning on every processor, a virtual thread still started at once. The interpreter checks for an interrupt at each branch and call, which `call-timeout` relies on; a host call's work is bounded by itself, so a guest in a loop of them still meets a branch. What a guest makes the host hold is bounded: a head's fields, a metric's definition, a body by `max-body` and the budget.

The SDKs fail on a header that is not UTF-8. A client must not be able to do that, so a plugin reads replacement characters while the line on the wire stays as it came.

A plugin is read from a Spring resource location, once at the start, under a deadline: most resources know no timeout of their own. The bytes read are the bytes checked and parsed. A location other than a file or the class path needs its `sha256`, as Envoy's remote sources do.

### Objects on the heap

Endive allocates a module's Wasm GC objects as JVM objects (`new long[len]` in `ARRAY_NEW_DEFAULT`) and counts nothing, so `max-memory` alone bounded linear memory, and the connection limit derived from it meant little. A hand-written plugin holding an array per response at `max-memory=128KB` took a 256MiB heap to 250MiB and out of memory, every run. The VM was discarded and the heap came back, but in 2 of 5 runs the error had hit other threads for good: the watchdog, after which `call-timeout` stopped nothing; the tunnel's sender, losing 125 of 901 requests on an unfiltered route; Tomcat's poller, after which `/actuator/health` did not answer.

`PluginHeap` charges each allocation before it happens, from Endive's `ExecutionListener`, and refuses past the limit by failing the plugin. It counts what Endive's objects take on a 64-bit JVM, rounded up. A reference slot pays for the `WasmI31Ref` it may hold, one at a time, so boxing in a loop charges nothing. A census from the globals and tables, between calls when no frame holds anything, replaces what was charged by what is reachable: a bound, not a measure, and a root missed would only loosen it. It runs once what was charged since passes what it found last, or a call might not fit, so its work stays in proportion to what was allocated.

- Why not the JVM's counters: `getThreadAllocatedBytes` reports after the allocation, so it cannot stop an array of 128MiB, and it counts allocated, not held.
- The cost: the listener runs on every instruction, of a module with GC types or a growable table only, so a plugin of the Rust SDK has none. With and without it, in a prototype: within the runs' spread (below). The census took 10ms over 4000 exchanges of `http_headers` in Lisp.
- The limit: the frames of a running call cannot be read (`StackFrame.localRef` is package-private), so one call may allocate only what is free, garbage or not.
- The listener is experimental in Endive. A hook on allocation would be better; that, and the limit above, are for Endive to answer.
- A module's tables count from its instantiation: past `max-memory` it is refused at the client's start.

Reporting out of memory can run out of it too, which ended the watchdog from inside its own `catch`. Now that goes unreported instead.

## Measurements

Orders of magnitude, not benchmarks. The real native client on Linux x86_64 (64 processors, `-Xmx512m`) through a real server, 200 streams at once of 152 SSE events each (30400 events, read as the Anthropic API's), each twice:

| Route's filter | CPU | RSS during (idle) |
|---|---|---|
| none | 10.1s | 189MiB (128) |
| `llm-usage` | 10.5s | 191MiB (128) |
| the same counting as a plugin (`llm-usage-proxy` of the spike's patch) | 35.0s | 208-313MiB (161) |
| that plugin, `paths` leaving these requests out | 10.0s | 207MiB (161) |

So the built-in costs next to nothing on top of the relay's own work, and holds nothing; the plugin about 820us of CPU per event: usable for one user's LLM traffic, not for a busy gateway. `paths` takes a plugin's cost away but for its instances' memory. In-process, on a native image on macOS arm64 (16 processors), one run each:

| | proxy-wasm | Java |
|---|---|---|
| Response of a path it ignores | 132us | - |
| JSON 1KiB | 654us | - |
| JSON 1MiB | 64ms | - |
| SSE, 2000 events | 491ms | 1.6ms |

The ratio, about 300, is the guest's scanning in the interpreter: a guest doing nothing costs 15ms for those 2000 events. The interpreter on a native image is about 1.5 times slower than on a JVM warmed for a few hundred calls. Endive's runtime compiler is documented as possibly unsupported on a native image, and its build time compiler cannot take a module added later: neither was tried. The image grows by about 2MB.

### A plugin in Lisp

rontolisp's filters on its SDK (`--optimize=size`) against the Rust SDK's examples they follow, on the same harness: its release at `2a82751e2`, and `b0bce92e3` built from source, which is not released yet and replaces the SDK's `defmethod` by `define-filter` and `search` by `search-bytes`. In-process at the default limits, a JVM per plugin warmed for 3000 exchanges, on Linux x86_64 under other builds' load, so a spread and not a figure. A plugin doing nothing takes 0.3ms:

| Per exchange | Rust | Release | `b0bce92e3` |
|---|---|---|---|
| `http_body`, 5 bytes of body: instructions | 2,342 | 13,967 | 1,145 |
| the same: p50 | 0.5-0.7ms | 0.9-1.4ms | 0.4-0.6ms |
| `http_headers`, logging each field, the log at `WARN`: instructions | 28,124 | 118,699 | 19,936 |
| the same: p50 | 1.6-2.1ms | 4.7-4.9ms | 1.3-2.2ms |
| `http_body`, 64KiB: instructions | 462,207 | 3,227,624 | 223,013 |
| the same: best of 5 | 32ms | 70ms | 17ms |
| `http_body`, 1MiB: best of 5 | 185ms | 750ms | 97ms |
| `http_body`, 4MiB: best of 5 | 471ms | fails, `call-timeout` | 335ms |
| `http_body`'s module | 92,163 bytes | 59,634 bytes | 6,086 bytes |

So what made Lisp slower was not the collector: GC instructions are 15% of the release's `http_body` and 14% of `b0bce92e3`'s, which runs fewer instructions than Rust's on Wasm GC all the same. What it had too much of was runtime and generated code. What allocating costs on Endive by itself was not isolated, and the time per instruction differs: half of Rust's instructions take about the same time. A start takes 0.2-0.7s, no different from Rust's within the spread.

Each instance of such a plugin takes what its `max-memory` says off the connection limit, like any other: at every default a limit of 61 on a 256MiB heap and 488 on 1GiB, 12 requests at once all passing.

## Verified

- `sluice-it` whole with a filter observing both bodies on every HTTP/1.1 upstream connection (a temporary patch, not kept): 696 tests, 3 fail, all the `partial-head` parameter of `HalfCloseProbeE2ETest` (2) and `ProbeResponseRaceE2ETest` (1), which assert a partial head reaches the client before it is complete. A filtered route holds a response head until complete, by design.
- `ProxyWasmFilterTest` and `LlmUsageFilterTest` on a native image on Linux x86_64, as CI runs them; their predecessors of the spike on macOS arm64. Endive needs no reflection or other configuration; a plugin read over http(s) needs `--enable-url-protocols=http,https`.
- The native client on Linux by hand: the counters of each API, JSON and SSE; `include-usage`; a plugin over http with its `sha256`, a wrong one and none refusing the start; an `https://` upstream, an h2c client getting `421` with no `force-http1` set.
- Misbehaving plugins, on a native image too: an endless loop, a trap, memory exhausted, a framing header edited, CRLF in a value, fields added for good, a malformed map. The response passes unchanged, a failure is counted, the VM starts anew.
- An independent review of this code found what the tests had not, each with a test now: a body's framing lost to a VM failing between its head and its body (request smuggling), a pipeline waiting for good once the responses ended (a slot held), a stale stream's call discarding the VM started anew, WASI's `fd_write` misread.
- rontolisp's filters, from its release's executable JAR and binary, which build the same module: `ProxyWasmFilterTest` with `http_body`, `http_headers` and `test_plugin` in the place of the Rust ones, all passing, of the release above and of `b0bce92e3`, and on a native image with a test of its own (`lisp_plugin`). The real client with them at every default. `test_plugin` built with WASI is a command: it is what showed `_start` running twice.
- Holding past `max-memory` with the limit in place, the real client on a 256MiB heap, 60 responses, 3 runs each of arrays, structs, a growing table and Lisp: the plugin fails at the limit, no out of memory and no thread lost, another route losing nothing, `/actuator/health` answering, `call-timeout` stopping a plugin as before.
- Not run: the release images, a plugin from the Go SDK, a plugin of objects in Envoy from here (rontolisp's `examples/proxy-wasm/check.sh` does that).
