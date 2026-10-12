Difficulty: Low

# h2 demux: a stream depending on itself is not a stream error

Found while working on 082 (by reading, not tested). RFC 9113 5.3.1: a stream cannot
depend on itself; a HEADERS with the PRIORITY flag or a PRIORITY frame naming its own
stream as the dependency is a stream error `PROTOCOL_ERROR`. The relay strips the
priority fields of a HEADERS (`Http2DemuxRelay#appendFragmentTo`) and drops PRIORITY
frames (`Http2DemuxRelay#onPriority`) without looking at the dependency.

## Direction

As `onPriority` does for a PRIORITY of a wrong length: a tracked stream is reset
(`resetStream`, `PROTOCOL_ERROR` to the client, `CANCEL` to the upstream); a PRIORITY on
an idle or closed stream ends the connection (RFC 9113 5.4.1). A HEADERS opening a stream
that depends on itself gets RST_STREAM `PROTOCOL_ERROR` without being routed (its block
still decoded, to keep the decoder in step); trailers doing so reset their stream.

## Tests (`sluice-it`; watch each fail first)

- PRIORITY depending on its own open stream: RST_STREAM `PROTOCOL_ERROR` on it, `CANCEL`
  to the upstream, the other stream completes.
- HEADERS opening a stream that depends on itself: RST_STREAM `PROTOCOL_ERROR`, nothing
  reaches the upstream, the next stream completes.
- A dependency on another stream keeps both.
