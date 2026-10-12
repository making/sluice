Difficulty: Medium

# H2DemuxClientWindowE2ETest: two tests timed out in one CI run

CI run 38294919888 (head 6bbc980, a workflow-only change) failed in sluice-it with two
errors in the same class:

- `clientGoneWhileAResponseWaitsForItsWindowEndsTheRelay:671` ConditionTimeout
- `nettyClientWithDefaultFlowControlGetsTwoLargeResponses:217` Timeout

Two tests of one class at once suggests something the class shares (its server or client
app, an upstream stub, ordering between its tests) or a starved fork under the 6 parallel
forks (108), more than two independent races.

## Direction

Read the log of that run first (timeline of both tests, what else the fork ran), reproduce
under load (Linux container, parallel forks, CPU stress), then fix the cause or, where a
test is not really needed (its behaviour pinned elsewhere, verified by a mutation), delete
it. Rerun-green is not a fix.
