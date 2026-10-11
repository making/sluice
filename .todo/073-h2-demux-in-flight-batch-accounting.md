Difficulty: Low

# h2 demux: a batch in flight is accounted as the buffer it was taken from

Found while working on 060: `H2DemuxFlowControlE2ETest.bulkUploadOnASlowLegLetsAnotherStreamOfTheLegComplete`
failed on main (4 runs of 4, before any 060 change) with 132114 bytes queued against its
bound of `STREAM_BUDGET + 1024` = 132095. 060 widened the test's bound to what the
accounting allows; the accounting itself is left as found.

`Http2DemuxRelay.Leg#dataBatchLocked` takes a stream's payload out of its
`PendingPayload` and keeps `released` -- the capacity the pending buffer lost -- accounted
until the batch is written. That is the rounded size of the buffer dropped, not the
`length` bytes the batch holds: the taken payload plus the rest's new buffer can be
accounted up to two granules (2KiB) above the payload, where the class comment, the
derivation of `MAX_QUEUED_BYTES` and `UNBUDGETED_BYTES`, and README's bound say "in a
buffer less than a granule larger". It can also undercount by up to a granule (a take
that leaves the capacity unchanged accounts nothing for the bytes in flight). One batch is
in flight per leg, so the error is at most a granule per leg, not per stream.

## Direction

At the take, move the accounting from the dropped capacity to the payload in flight
(`addQueuedLocked(stream, length - released)`), and release `length` (+ the entry header)
once written; the stream's net accounting is unchanged. Then the queue is at most the
payload plus less than a granule per stream again, and the test bound can go back to
one granule.

## Tests (watch each fail first)

- A unit test on the leg's accounting (or an E2E reading `sluice.h2demux.queued.bytes`):
  1025 bytes pending (a 2KiB buffer), all of them taken into a batch still in flight, then
  one more byte appended: the payload in flight is accounted as 1025 bytes, not as the
  dropped 2KiB buffer.
