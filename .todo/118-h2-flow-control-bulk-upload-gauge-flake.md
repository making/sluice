Difficulty: Low

# H2DemuxFlowControlE2ETest bulk upload: queue gauge sampled as 0 on CI

`bulkUploadOnASlowLegLetsAnotherStreamOfTheLegComplete` failed on CI (Linux) at
`maxQueued > 0` ("queued for the bulk stream, plus the other stream's head": actual 0)
after 0.996s: https://github.com/making/sluice/actions/runs/38374902326

The commit under test (795138c, upstream header block admission) does not touch the
request DATA path the gauge measures; the class passes locally. Likely cause: the
route gauge is sampled only at Awaitility's poll points (100ms), and on a fast runner
the throttled upstream let the whole run finish with every sample at 0.

## To do

- Dump the gauge timeline once on a failing run (sample every 1-5ms, record
  `streamSent(1)` alongside) to confirm the hypothesis before changing anything.
- Make the precondition deterministic: e.g. sample the gauge on a dedicated thread at a
  short interval, or assert the bulk stream was held (the throttled upstream's received
  bytes lag `streamSent(1)`) instead of `maxQueued > 0`.
