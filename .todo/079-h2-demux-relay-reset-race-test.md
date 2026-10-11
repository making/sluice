Difficulty: Medium

# h2 demux: no test pins the relay's own stream reset against concurrent writes

Left open by 065. `Http2DemuxRelay#resetStream` marks the stream reset and writes its
RST_STREAM to the client under the sink lock, and every path writing a response frame
(`relayData`, the response writer, `relayHead`, `onUpstreamReset`, the default branch of
`relayStreamFrame`) checks the stream under that lock before writing, so no frame of the
stream should follow the reset. The tests (`H2DemuxBudgetE2ETest`) reset with the
response either waiting for the client's window or not yet sent; none has a response
frame being written while the reset runs. Nor does any test cover the client gone as the
relay writes its RST_STREAM (the write throws out of the outbound parse, which aborts the
relay): what should be released then is only argued from the generic outbound failure path.

## Tests (watch each fail first)

- A response DATA frame of the stream relayed directly by the leg's inbound side while
  the outbound parse resets the stream (test hook holding one side between its check and
  its write): the client sees the frame before the RST_STREAM or not at all, never after;
  moving the RST_STREAM write out of the sink lock must make it fail.
- The client closing its socket right after sending the trailers that get reset: the
  relay ends, the route's queued bytes, the budget and the tracked streams go back to 0.
