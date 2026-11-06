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

`sluice.client.filters.<name>` defines a filter: a plugin, or the defaults of a built-in or factory of that name. `sluice.client.upstream[n].filters[m]` uses it; what a use sets goes over the definition (`FilterConfig.overriddenBy`: `paths` and the plugin's limits replace, `args` merge key by key). Each use has its own filter: its instances of a plugin, its counters by `route`. The name is resolved at the start: unknown, it stops the client. A factory (`UpstreamFilterFactory`) is how a custom build adds one in Java, as a bean; the built-ins (`llm-usage`, `headers`) are factories too.

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

A filtered connection buffers up to 256KiB on top of its 293KiB (`UpstreamFilterPipe.CONNECTION_BYTES`): a head being read each way, the heads of the requests awaiting responses (a pipeline past 4 of them or 64KiB waits before its filters are called, holding the tunnel's flow control, and the wait ends with the responses), the output waiting for the relay (past 64KiB it charges the tunnel budget as it is written, or fails). What it reads of the upstream at once is below that, so what merely passes never depends on the budget. While it waits, it reads nothing of the tunnel: the client's end reaches the upstream only after, so a client gone is held until the upstream answers, as an unfiltered connection is when the upstream ignores its end. A route's plugin adds up to 256KiB more in the host: what it keeps of each head's fields, of at most 256 fields each and 5 streams. With a filter on any route every connection counts the most of these in the derived limit. The plugins' instances, `vms x max-memory` at most, are native memory and not part of it: the heap it derives from holds none of them (the startup log tells their sum). `max-connections` set, the count alone limits, as before.

What a filter holds of a body charges the tunnel budget, not the limit: most hold nothing, and a body worth holding should not cost every connection its size. A hold that does not fit gives up: `include-usage` passes the body unchanged, a plugin fails. Windows shrink under the same budget, which is what they do under load anyway.

## llm-usage

One scanner of JSON and server-sent events, a dialect per API by exact request path, the same counters out. `JsonFields` streams through a body picking fields by path, holding a few hundred bytes whatever the size or split, so a filtered connection costs nothing more for it, and no body is too large to count. It reads what is well formed and is merely wrong on what is not.

- OpenAI compatible (`/v1/chat/completions`, `/v1/completions`, `/v1/responses`): the JSON's `usage`; on SSE only the chunk `stream_options.include_usage` adds before `[DONE]`. That is a request body option: without it the stream counts as `no_usage`. `args.include-usage` adds it to a streaming request that does not set it, spliced before the closing brace so nothing else of the body changes; off by default, as its client gets one chunk more, with no choices, and an upstream may refuse the option.
- Anthropic (`/v1/messages`, not `/v1/messages/count_tokens` or `/batches`): the JSON's `usage`; on SSE `message_start`'s `message.usage`, then `message_delta`'s `usage`, cumulative. With a server tool the last `message_delta` carries a larger `input_tokens`, so later values overwrite, field by field; never add them. No `[DONE]`; ends in `message_stop`, an `error` event may come mid-stream. `input_tokens` excludes `cache_creation_input_tokens` and `cache_read_input_tokens`.
- OpenAI's `input_tokens` (documentation of the Responses API) includes `cached_tokens` and `cache_write_tokens`; the filter assumes the same of `prompt_tokens`, which that page does not state, and counts `input` without them, as Anthropic reports it.
- A cut stream leaves a partial count, recorded as `outcome=cut`.
- `model` comes from a response: past 256 values per use it counts as `other`. Every series has the same tag keys, as Prometheus needs: what was not read has `model` and `stream` = `unknown`.

## headers

Each arg is one edit, `<request|response>.<set|add|remove>.<header>`, so a use's args go over the definition's edit by edit (`overriddenBy` merges `args` key by key). In properties the key goes in brackets, `args[request.set.x-api-key]`, which keeps its dots and case. An environment variable cannot carry the key: its `_` binds as `.`, and a header name with `.` is refused rather than sent as `x.api.key`. The value can come from one: `${ANTHROPIC_API_KEY}`.

- `set` replaces every value, `add` adds one after the others, `remove` takes `true` (or empty) and `false`, which keeps a header a definition removes. Names are compared without case; the same edit of one name in two cases is refused.
- On each head: removals, then sets, then adds. A request's edits apply on every request of the route, `paths` aside; a response's on every final response, a local answer of another filter excepted.
- Refused at the start: the framing and hop-by-hop headers the pipe protects (`content-length`, `transfer-encoding`, `connection`, `upgrade`, `keep-alive`, `proxy-connection`, `te`, `trailer`), `host`, a name that is not a token, a value with a control character or past Latin-1. What is left cannot fail on an exchange, but a head edited past 64KiB.
- A value is in no message or log: it may be a secret.
- No body is seen: one head edit per message.

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
- **A plugin's objects** (Wasm GC) are run, within the one budget: rontolisp compiles filters to them, on a linear memory for the ABI, and Envoy runs those. Its no-GC backend leaves `defclass`, `handler-case` and `car` behind, too high a price for a filter's author. See "The runtime".

Guests run on platform threads, the connection's virtual thread waiting unmounted: with a guest spinning on every processor, a virtual thread still started at once. Compiled code checks the epoch at each function entry and loop back edge, which `call-timeout` relies on; a host call's work is bounded by itself, so a guest in a loop of them still meets a check. What a guest makes the host hold is bounded: a head's fields, a metric's definition, a body by `max-body` and the budget.

The SDKs fail on a header that is not UTF-8. A client must not be able to do that, so a plugin reads replacement characters while the line on the wire stays as it came.

A plugin is read from a Spring resource location, once at the start, under a deadline: most resources know no timeout of their own. The bytes read are the bytes checked and compiled. A location other than a file or the class path needs its `sha256`, as Envoy's remote sources do.

### The runtime

Plugins ran on Endive, a Wasm interpreter in Java, until they moved to wasmtime: Endive took up to 90 times longer per call (below), its starts 60-100ms, and it counted nothing of a plugin's Wasm GC objects, which lived on the JVM heap. `PluginHeap` charged each allocation from a listener on every instruction, a bound that could not see a running call's frames; a plugin holding objects past it had taken the heap out of memory and other threads with it (the watchdog, the tunnel's sender, Tomcat's poller). wasmtime counts the GC heap with the memories, collects inside a call, and needs neither.

`sluice-client-wasm` is a small library of ours on the `wasmtime` crate, not wasmtime's C API: its ABI is plain scalars and pointers, so the Java side (`Wasmtime`, `PluginInstance`, `PluginModule`) reads none of wasmtime's structs, whose layouts are what a binding gets wrong silently. `wasmtime-c-api` 50's release also ships only the reference counting collector, which leaks cycles until the store goes (rontolisp's garbage: 11 failures in 200 exchanges at 16MiB in the prototype), and a limiter per memory.

- **Compile once**: a module is compiled at the client's start (Cranelift, no thread pool), once per process for the same bytes, its instantiation prepared (`InstancePre`). A VM starts in 3-6ms.
- **One budget** (`ResourceLimiter`): linear memories, the GC heap and tables, at instantiation too, at the size they have grown to and a table element as 8 bytes. A refused memory or table growth is the spec's -1 (Endive's `PluginHeap` had trapped); a refused GC heap growth traps the allocation (`GC heap out of memory`).
- **The copying collector**: semispaces, and wasmtime grows its heap by doubling, the request refused when the doubled size does not fit: the live objects a plugin keeps are a quarter to a half of what its linear memory leaves of the budget. A plugin of objects needs a larger `max-memory` than its objects suggest (measured below).
- **Time**: an epoch deadline per task of a VM (`call-timeout`, 10ms ticks), the epoch ticked by a thread of the library: nothing in the JVM, out of memory say, can stop it.
- **Stack**: a call takes at most 1MiB of stack (`Wasmtime.WASM_STACK`), and a VM's thread has 4MiB. A thread whose stack is not larger than wasmtime's limit lets a deep recursion overflow it, which kills the process; wasmtime traps (`call stack exhausted`) within it.
- **Imports**: one upcall, `PluginInstance.host`, given an import's index, its values and the linear memory's base and size; the instance whose call is in progress is the thread's. A Java exception leaving an upcall ends the JVM: everything is caught, kept, and the call traps; the host rethrows what was kept (`HOST_FAILED`). A call from Java inside an import (the plugin's allocator) runs on the import's caller.
- **No panic past the ABI**: each entry catches it and returns a status and a message; a panic in the callback traps the call rather than unwind through the plugin's frames. A panic unwinding through them had left the slot of the caller in progress pointing at a dead frame, and the next call crashed: its test is `a_call_ends_as_a_status_whatever_happens`.
- **Signals**: traps by signals, as wasmtime does by default; its handlers chain to the JVM's and SubstrateVM's for faults outside wasm. On macOS, Unix signals rather than Mach ports, as on Linux, where it was tried with both runtimes.
- **Freed**: a VM's store when it fails, or when its instance is unreachable (a `Cleaner`); compiled modules never, as plugins are defined for good.

Packaging: sluice-client's build runs `cargo build --release` in `sluice-client-wasm` (`exec-maven-plugin`, `generate-resources`) and copies the library to `META-INF/native/<os>-<arch>/` of its classes (a Maven profile per platform: `linux-x86_64`, `linux-aarch64`, `macos-aarch64`, `macos-x86_64`). The JAR, the JVM image (the classes) and the native image (`reachability-metadata.json` includes it as a resource) all carry it; the first plugin defined extracts it to a temporary file of its own, loads it and removes the file. A build carries the library of the platform it ran on, as the images are built on each arch's runner. FFM is restricted: `Enable-Native-Access` in the JAR's manifest, `--enable-native-access` for the JVM image (`BPE_DEFAULT_JAVA_TOOL_OPTIONS`), the tests and the native image.

On a native image a downcall handle made at run time is interpreted, about 5us a call in the prototype: the calls per call into a plugin (`sw_vm_call`, `sw_vm_memory`, `sw_vm_deadline`) go through `@InvokeCFunctionPointer` to the addresses FFM found (`src/native/java`, compiled for `-Pnative` and `-PnativeTest`, against `org.graalvm.sdk:nativeimage` provided), the others stay FFM's. The upcall is a direct upcall (`reachability-metadata.json` under `sluice-client-netty`). A shared arena's close needs `-H:+SharedArenaSupport`, already set for Netty.

## Measurements

Orders of magnitude, not benchmarks, on Linux x86_64 (64 processors) under other builds' load: a spread, with the load average. The real native client (`-Xmx512m`) through a real server, 200 streams at once of 152 SSE events each (30400 events, read as the Anthropic API's), each twice, a load average of 20-27:

| Route's filter | CPU | RSS during (idle) |
|---|---|---|
| none | 9.3-9.6s | 186-187MiB (124) |
| `llm-usage` | 10.0-10.1s | 183-189MiB (125) |
| the same counting as a plugin (`llm-usage-proxy` of the spike's patch, `.todo/artefacts/236`) | 12.4-12.6s | 219-224MiB (154) |
| that plugin, `paths` leaving these requests out | 9.7-9.8s | 216-217MiB (154) |

So the built-in costs next to nothing on top of the relay's own work, and holds nothing; the plugin about 100us of CPU per event, a third more than the relay's work (on Endive: 820us, 35.0s in all). `paths` takes a plugin's cost away but for its instances' memory: 30MiB here, the library, the compiled module and 4 VMs.

In-process, a plugin per filter and one VM, at the default limits but `vms`, through the pipe (`FilterTests.relayed`, 64KiB segments), a load average of 8-19: the same probe on this host and on Endive 1.2.0's (the commit before the switch). p50, or best of 5 for the bodies of 64KiB and more:

| Per exchange | Endive, JVM | wasmtime, JVM | wasmtime, native image |
|---|---|---|---|
| a VM's start | 61-97ms | 3-6ms | 1-3ms |
| a module's compilation, once per process | - | 30ms (Lisp, 5KiB) to 420ms (Rust, 92-160KiB) | the same |
| Rust `http_body`, 5 bytes | 0.69ms | 0.49ms | 0.30ms |
| the same, 64KiB / 1MiB / 4MiB held and replaced | 24 / 123 / 493ms | 2.3 / 24 / 88ms | 1.6 / 28 / 82ms |
| the usage plugin, a path it ignores | 0.55ms | 0.41ms | 0.23ms |
| the same, a JSON response / 1MiB of JSON | 1.6ms / fails, `call-timeout` | 0.32 / 34ms | 0.34 / 40ms |
| the same, SSE of 2000 events | 721ms | 7.8ms | 12ms |
| Lisp `json_plugin` parsing (`json-parse`), 1KiB / 64KiB of JSON | 61ms / fails, `call-timeout` | 1.2 / 46ms | 0.9 / 50ms |

So a call costs about the pipe's own work and the copies, where Endive interpreted: 1.4 to 90 times faster, the more so the more a plugin computes. On a native image the calls per call through C function pointers keep the JVM's speed. The Lisp plugin parsing JSON is bounded by its memory, not its time: 1MiB of JSON fails, `GC heap out of memory`, in 150ms. The largest body it took, of 5 tries, by `max-memory`:

| `max-memory` | 16MB | 32MB | 64MB |
|---|---|---|---|
| JSON body parsed, a field added | 64KiB | 64KiB | 192KiB |

A body of 128KiB at 16MB grew the GC heap from 64KiB to 7MiB by doubling, and the 14MiB after it was refused: the live objects took over half of its 7MiB, 27 times the body. Built by rontolisp's release of 2026-11-06, which allocates less when parsing, the same plugin took 384KiB at 16MB (33ms) and failed on 512KiB.

`json_plugin` as it is reads the body where the host put it (`rontolisp:json-span` finds the object's closing brace; the field goes in before it), so only the body's copies count against `max-memory`. At the default limits, JVM, a load average of 10-13, best of 3 after the first:

| JSON body | 1KiB | 64KiB | 1MiB | 4MiB | 15MiB |
|---|---|---|---|---|---|
| a field added | 3ms | 6ms | 49ms | 138ms | 314ms |

15MiB, the largest tried, is just under `max-body` (16MiB). The real client's Lisp plugin at 16MB, `/hold` keeping an array of 1MiB a response: each VM failed at the 5th (on Endive, which counted 8 bytes an element: at the 2nd).

The native image carries the 12MiB library (131MB in all).

## Verified

- `sluice-it` whole with a filter observing both bodies on every HTTP/1.1 upstream connection (a temporary patch, not kept): 696 tests, 3 fail, all the `partial-head` parameter of `HalfCloseProbeE2ETest` (2) and `ProbeResponseRaceE2ETest` (1), which assert a partial head reaches the client before it is complete. A filtered route holds a response head until complete, by design.
- `ProxyWasmFilterTest`, `LlmUsageFilterTest` and `DirectBufferReleaseTest` on a native image on Linux x86_64, as CI runs them. A plugin read over http(s) needs `--enable-url-protocols=http,https`; the library, its downcalls and upcall need `reachability-metadata.json` (under `sluice-client-netty`).
- `sluice-client-wasm`'s tests on Linux aarch64 too, cross-compiled and run under qemu-user. Not run here: the Java side on aarch64 and on macOS, and the library's build on macOS (no macOS toolchain); CI's `plugins` job runs the former, `rust.yaml` the latter.
- The native client on Linux by hand: the counters of each API, JSON and SSE; `include-usage`; a plugin over http with its `sha256`, a wrong one and none refusing the start; an `https://` upstream, an h2c client getting `421` with no `force-http1` set.
- Misbehaving plugins, on a native image too: an endless loop, a trap, memory exhausted, a recursion without end, a framing header edited, CRLF in a value, fields added for good, a malformed map. The response passes unchanged, a failure is counted, the VM starts anew.
- An independent review of this code found what the tests had not, each with a test now: a body's framing lost to a VM failing between its head and its body (request smuggling), a pipeline waiting for good once the responses ended (a slot held), a stale stream's call discarding the VM started anew, WASI's `fd_write` misread.
- rontolisp's filters, from its release's executable JAR and binary, which build the same module: on Endive, `ProxyWasmFilterTest` with `http_body`, `http_headers` and `test_plugin` in the place of the Rust ones, all passing, of its releases at `58f221883` and `2a82751e2`; on wasmtime, `lisp_plugin` and `json_plugin`, on a native image too, and the real client with `lisp_plugin` at every default. `test_plugin` built with WASI is a command: it is what showed `_start` running twice.
- Holding past `max-memory`, the real client on a 256MiB heap, on the JVM and as a native image: 4 plugins at every default on 4 routes, Lisp's `/hold` for 60 responses (12 failures, each VM at its 5th), 900 requests meanwhile on an unfiltered route (all `200`), a Rust plugin spinning past `call-timeout` 3 times (failed at 1.01-1.03s, the response passing): no out of memory, no thread lost, `/actuator/health` answering. With Endive the same had held for arrays, structs and a growing table too, on `PluginHeap`.
- Not run: the release images (the JVM image's `--enable-native-access` among them), a plugin from the Go SDK, a plugin of objects in Envoy from here (rontolisp's `examples/proxy-wasm/check.sh` does that).
