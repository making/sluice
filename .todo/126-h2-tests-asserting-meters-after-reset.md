Difficulty: Low

# h2 demux tests that assert meters right after the client sees a reset may race

Found while fixing a CI failure after 121 (0a62925, run
https://github.com/making/sluice/actions/runs/38391042773).

## Problem

Since resets are flushed inside the reset call (116), a client can read the relay's
RST_STREAM before the relay action that wrote it has finished. 0a62925 counted the
refusal / reset meters before the write, and made one test await `sluice_h2demux_queued_bytes`
(updated after the action). Other tests may still assert, right after seeing a reset or a
GOAWAY, a meter or gauge the relay updates after writing it (queued bytes, budget used,
legs failed, admitted connections, session traffic). Not audited.

## Scope

- Go through the sluice-it h2 demux tests: for each immediate assertion of a meter or gauge
  after a reset / GOAWAY / response is observed, check when the relay updates it relative
  to the write; either update it before the write (product) or await it (test).
- Widen the window to find them (a short sleep after the reset write, as 0a62925 did by
  hand), and record which tests failed that way.
