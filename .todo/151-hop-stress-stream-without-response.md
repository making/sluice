Difficulty: Medium

# HopStressTest: a stream gets no response (h2 hop stall?)

CI run 39013697232 (2026-10-17, at 47711ab; the commits before it touched only native-image
JSON) failed `HopStressTest.concurrentUploadsAndDownloadsOverTwoLegsArriveIntact` with
`stream 65 got no response`. The failure dump shows the client wrote 10762645 bytes while
the relay's client side read 10725107: the hop stopped reading with bytes outstanding.
Recent changes in that area: the hop reader now pauses on control bytes only
(`Hop` / `FrameSplitter`, `HOP_CONTROL_BYTES`), relay pages admitted to the budget.

## Work

- Dump the timeline of a failing run (reader paused / resumed and why, control vs DATA
  bytes queued, budget, windows) before probing; reproduce by running the class repeatedly,
  also under parallel load.
- Decide: production stall (fix it, and make the stress test or a smaller existing test
  fail without the fix) or a test artefact (fix the test). Not a flake to prune: this is the
  only stress test of two legs.
