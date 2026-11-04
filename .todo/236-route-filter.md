Difficulty: Medium

# Filter an upstream's responses on sluice-client (spikes done)

The goal is rewriting responses, bodies included, on the tunnel client. Findings: `artefacts/236/FINDINGS.md`. Code, applicable to b55c26d: `artefacts/236/client-filter-spike.patch` (`git apply --binary`); the data plane and wicket alternatives: `artefacts/236/route-filter-spike.patch`.

Decided (2026-11-04): a filtered route is `force-http1` for now. Browsers are served over HTTP/1.1, h2-only clients are refused with `421`. Serving them is High difficulty and out of scope.

Use case: token usage of an OpenAI compatible API and of the Anthropic API, read from their responses. Read-only: the pipe needs an observer beside `BodyRewriter`, the framing stays the upstream's. One filter: a shared SSE / JSON scanner, a dialect per API chosen by exact request path, the same counters out.

- OpenAI compatible (`/v1/chat/completions`, `/v1/completions`, `/v1/responses`): the JSON's `usage`; on SSE only the chunk `stream_options.include_usage` adds before `[DONE]`. That is a request body option, so without it the stream reports nothing: count such responses, or rewrite the request (not in the spike).
- Anthropic (`/v1/messages`, not `/v1/messages/count_tokens` or `/batches`): the JSON's `usage`; on SSE `message_start`'s `message.usage`, then `message_delta`'s `usage`, cumulative. With a server tool the last `message_delta` carries a larger `input_tokens`, so later values overwrite, field by field; never add them. No `[DONE]`; ends in `message_stop`, an `error` event may come mid-stream. `input_tokens` excludes `cache_creation_input_tokens` and `cache_read_input_tokens`: keep them as separate token types.
- Both: a compressed body cannot be read, so `accept-encoding` stays removed towards the upstream, or the filter decompresses. A cut stream leaves a partial count: record it as incomplete.

Who writes filters: mostly the maintainer, as built-ins on the `ResponseFilter` SPI, in Java; the token usage is about 300 times cheaper there than interpreted. Users get an extension point: proxy-wasm plugins on Endive's interpreter (`ProxyWasmResponseFilter`). It holds on the native image (`FINDINGS.md`), so no external process. The ABI of ours (`WasmResponseFilter`, `src/test/guest/sdk`, `llm-usage`) was the comparison: not to be kept.

Left for an implementation (`FINDINGS.md` "Limits"):

- `sluice.client.upstream[n]` configuration of the filters: built-ins by name, plugins by path with their configuration; remove the spike's `SLUICE_SPIKE_*` switches and `WasmGuestCostProbe`
- The usage filter in Java on the observer, tested with the cases of `usageCases()`, its counters as in the spike
- A filter that matches by path before anything else: a plugin costs 130us on a response it ignores
- Count a plugin's memory in the connection limit: 0.6MiB of RSS per stream measured, against 0.3MiB without
- Decide the rest of a plugin's limits (`FINDINGS.md` "Limits of the proxy-wasm host"), the request's body first: it is what `stream_options.include_usage` needs
- Advertise `force-http1` for a filtered upstream, so h2 cannot bypass the filter
- Share the HTTP/1.1 parsers through `sluice-proto`
- Hold a filter's route: `Connect` carries the address only
- README and a `.kb` topic, a TLS upstream run, a native run on Linux
