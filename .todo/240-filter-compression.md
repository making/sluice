Difficulty: Medium

# Built-in filter: compression

Decode a compressed response for the filters after it, and compress what goes into the tunnel by the client's `Accept-Encoding`.

## Why

- Body filters cannot read an encoded body. `llm-usage` and the test plugin `json_plugin` remove `accept-encoding` towards the upstream, so the whole response then crosses the tunnel uncompressed. Decoding at the client and encoding again after the body filters lets them read it and keeps the tunnel's bytes small.
- An upstream that never compresses (a dev server, a small service) costs tunnel bandwidth and flow-control window for text that compresses several times over.

## Scope

- Response only at first. Placed first in a route's list, so it sees the response last: it decodes before the other filters and encodes after them. Decide whether one filter does both or the pipe offers decoding to any filter.
- gzip and deflate with the JDK. Brotli and zstd need a native library: measure first whether they are worth that on the JVM and the native image.
- Skip what does not pay: small bodies, types already compressed (images, archives), `Cache-Control: no-transform`, ranges (`206`). `ETag` becomes weak when the body changes.
- Streaming: flush per upstream chunk, so SSE stays live.
- What decoding holds charges the tunnel budget, as other holds do. Bound what one body may expand to.

## Done when

`llm-usage` and `json_plugin` read a gzip response without removing `accept-encoding`, an uncompressed upstream's text reaches the client gzip, SSE events arrive one by one, and `.kb/client-filters.md` has the measured bytes and CPU cost.
