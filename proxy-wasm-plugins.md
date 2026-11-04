# Proxy-Wasm plugins

A filter of your own on a route of `sluice-client`, without building the client: a [proxy-wasm](https://github.com/proxy-wasm/spec) module (ABI 0.2.1), the kind you would write for Envoy, interpreted inside the client, the released native image included. Use it for one user's traffic or a small gateway's. It is interpreted: for what must keep up with a busy route, a Java filter in a custom build costs far less ([.kb/client-filters.md](.kb/client-filters.md) has the numbers). Filters in general: README "Filters".

## Write one

With the [Rust SDK](https://github.com/proxy-wasm/proxy-wasm-rust-sdk):

```toml
# Cargo.toml
[lib]
crate-type = ["cdylib"]

[dependencies]
proxy-wasm = "0.2.5"
```

```rust
use proxy_wasm::traits::{Context, HttpContext, RootContext};
use proxy_wasm::types::{Action, ContextType, LogLevel};

proxy_wasm::main! {{
    proxy_wasm::set_log_level(LogLevel::Info);
    proxy_wasm::set_root_context(|_| -> Box<dyn RootContext> { Box::new(Root) });
}}

struct Root;

impl Context for Root {}

impl RootContext for Root {
    fn get_type(&self) -> Option<ContextType> {
        Some(ContextType::HttpContext)
    }

    fn create_http_context(&self, _: u32) -> Option<Box<dyn HttpContext>> {
        Some(Box::new(Stream))
    }
}

struct Stream;

impl Context for Stream {}

impl HttpContext for Stream {
    fn on_http_response_headers(&mut self, _: usize, _: bool) -> Action {
        self.add_http_response_header("x-filtered", "yes");
        Action::Continue
    }
}
```

```sh
rustup target add wasm32-wasip1
cargo build --release --target wasm32-wasip1
sha256sum target/wasm32-wasip1/release/my_plugin.wasm
```

`wasm32-unknown-unknown` works as well. The SDK's own `http_body` and `http_headers` examples run unchanged (`sluice-client/src/test/guest/sdk-examples`).

## Define and use it

Define a plugin once by name, use it on routes by that name. Each use on a route has its own instances, settings and counters.

```properties
sluice.client.filters.my-plugin.plugin=https://plugins.example.com/my_plugin.wasm
sluice.client.filters.my-plugin.sha256=9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08
sluice.client.filters.my-plugin.configuration={"mask":"****"}
sluice.client.filters.my-plugin.vms=2

sluice.client.upstream[0].host=a.example.com
sluice.client.upstream[0].target=http://127.0.0.1:3000
sluice.client.upstream[0].filters[0].name=my-plugin

sluice.client.upstream[1].host=b.example.com
sluice.client.upstream[1].target=http://127.0.0.1:3000
sluice.client.upstream[1].filters[0].name=my-plugin
sluice.client.upstream[1].filters[0].configuration={"mask":"xxxx"}
sluice.client.upstream[1].filters[0].paths=/api/*
sluice.client.upstream[1].filters[0].fail-closed=true
```

`plugin` is a Spring resource location: `file:/etc/sluice/my_plugin.wasm`, `classpath:my_plugin.wasm`, `https://...`, or a file path without a scheme. It is read once, at the client's start, within 60s and up to 64MiB; the client does not start if it cannot be read, or is no plugin.

`sha256` is the module's SHA-256 in hex. The client does not start on a mismatch. It is required for anything but a file or the class path: a plugin is code the client runs, and a location does not vouch for what it serves. A startup line tells the SHA-256 of what was read. A client restarting while the location is down does not start: where that matters, keep the plugin in a file.

The settings: README "Configuration (client)", "Filters". `configuration` is what the plugin reads with `get_plugin_configuration`, after which `on_configure` returning `false` stops the client's start.

## What a plugin is called with

Per request, on the HTTP/1.1 connections of its route, for the `paths` it is set to:

| Call | When | It may |
|---|---|---|
| `on_http_request_headers` | the request head, before anything reaches the upstream; `end_of_stream` when no body follows | read and edit the head, answer in the upstream's place |
| `on_http_request_body` | each piece of the body, then `end_of_stream` | read it; replace it, or answer, as below |
| `on_http_response_headers` | the final response head; not for 1xx, and not past a `101` | read and edit the head |
| `on_http_response_body` | each piece of the body, then `end_of_stream` | read it; replace it, as below |
| `on_done`, `on_log`, `on_delete` | the response ended, was cut, or none came | last counts |

A connection that upgraded (a WebSocket's `101`, a `CONNECT`'s 2xx) is not followed past that response: refuse the request if what follows matters. An upgrade to `h2c` does not reach the upstream: it answers an ordinary request.

Heads read as UTF-8: a byte that is not valid reads as a replacement character, and the line itself stays as it came unless edited. Header names read in lower case. A head of more than 256 fields is not offered: the plugin fails on it, as below. A plugin's edits cannot take a head past 256 fields or 64KiB. `:method`, `:path`, `:scheme`, `:authority` and `:status` are there to read.

A plugin cannot edit: pseudo headers, `Host` (the server routed by it), and what frames a message: `Transfer-Encoding`, `Connection`, `Upgrade`, `Keep-Alive`, `Proxy-Connection`, `TE`, `Trailer`, `Content-Length` but for its removal. A name or value that would make a line of its own is refused like them. The SDKs panic on a refused edit, which fails the plugin.

Properties (`get_property`): `request.path`, `request.url_path`, `request.method`, `request.host`, `request.scheme`, `request.protocol`, `response.code` (8 bytes, little endian), `route_name` (the route's key), `plugin_name`. Other paths are not found.

## Replacing a body

As on Envoy, a body can change only if its length is not fixed: it came chunked or with no length, or the plugin removed `content-length` in its head's call. Otherwise the plugin only reads it, and `set_http_*_body` fails.

Returning `Action::Pause` from a body's call holds the body: the next call offers everything held. Once it returns `Action::Continue`, what is in the buffer goes on.

```rust
impl HttpContext for Stream {
    fn on_http_response_headers(&mut self, _: usize, _: bool) -> Action {
        self.set_http_response_header("content-length", None);
        Action::Continue
    }

    fn on_http_response_body(&mut self, size: usize, end_of_stream: bool) -> Action {
        if !end_of_stream {
            return Action::Pause;
        }
        if let Some(body) = self.get_http_response_body(0, size) {
            self.set_http_response_body(0, size, &body.to_ascii_uppercase());
        }
        Action::Continue
    }
}
```

- A replaced response goes out chunked from its head on (to an HTTP/1.0 client: until the connection's end).
- A replaced request goes out with a `Content-Length` if the plugin held it to its end, chunked if a part went on before. A request with `Expect: 100-continue` goes out chunked, not held. An HTTP/1.0 request takes no chunks: replacing it in pieces fails it.
- A chunked body the plugin only reads goes the same way: the payload is the same, the chunks may be split otherwise, and `Transfer-Encoding` moves to the end of the head.
- What is held is bounded by `max-body` and by what the tunnel budget has left. Past it the plugin has failed: see below.
- A compressed body arrives compressed. Remove `accept-encoding` in the request's call, or decompress it yourself.

## Answering in the upstream's place

`send_http_response` from a request's call answers the request; return `Action::Pause` after it.

```rust
fn on_http_request_headers(&mut self, _: usize, _: bool) -> Action {
    if self.get_http_request_header("x-api-key").is_none() {
        self.send_http_response(401, vec![("www-authenticate", "ApiKey")], Some(b"no key\n"));
        return Action::Pause;
    }
    Action::Continue
}
```

The answer follows the responses to the requests before it, with `Content-Length` and `Connection: close`, and the connection ends behind it, as after a refusal on the server. It reaches the client as it is: no filter sees it, and those before the plugin are told the exchange was abandoned. From a body's call, held to its end if nothing of the request should reach the upstream. It fails from a response's call, and answering a request whose response has begun cuts that response.

## Counters and logs

`define_metric(MetricType::Counter, "name;key=value;...")` and `increment_metric` make `sluice.client.filter.<name>` with the tags in the definition, and `route` and `filter` (the name of its definition) added. Micrometer's names: lower case, digits, `.` and `_`, at most 64 characters. All counters of a name need the same tag keys, or Prometheus leaves out the others. Tag values are cut at 128 characters. Past `max-meters` definitions, updates are dropped and counted (`sluice.client.filter.plugin.meters.dropped`). The tags in the name are a convention of this host: they do not carry over to Envoy.

`log` lines and what WASI's `fd_write` gets go to the client's log as `filter <name>: ...`, cut at 1024 characters, control characters replaced.

## When it fails

A plugin fails when it traps (an SDK's panic among them), runs past `call-timeout`, or runs out of `max-memory`. The instance is discarded and started anew for the next stream; the streams it was serving lose their plugin from there. Counted as `sluice.client.filter.failures`, by `phase`.

| The failure | `fail-closed=false` (default) | `fail-closed=true` |
|---|---|---|
| on a request head | the request goes on, the plugin left out of it | the client gets `502` |
| on a response head, or one read | it goes on | `502`, or the response is cut |
| on a body it changed | the message is cut | the message is cut |
| on a body it held but did not change, or past `max-body` | what it held goes on | the message is cut |

One call at a time runs on an instance, and a stream stays on its instance: a plugin stuck for its `call-timeout` holds the other streams there for that long. A stream's state in the plugin lasts until its response ended; a client pipelining waits past 4 requests awaiting responses, so it cannot pile streams up.

## What it reaches

Nothing but what the host gives it. It gets no files, sockets or environment from WASI: time, randomness and output only.

| Not offered | So | Why |
|---|---|---|
| `dispatch_http_call`, gRPC calls | return an error (`InternalFailure`): the plugin can go on | a plugin reaches nothing outside |
| pausing a head | a head goes on when its call returns, whatever it returned | nothing could resume it, with no outgoing calls or ticks |
| ticks, shared data, queues | `Unimplemented`: the SDKs panic | no state beyond an instance, nothing running but a stream's calls |
| gauges, histograms | `define_metric` refuses them: the SDKs panic | counters only |
| trailers | not called, and pass unchanged | |
| editing `:path` or `:method` | refused | the server's route and access checks saw them |

The first call of something not offered is logged, once.
