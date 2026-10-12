Difficulty: Low

# CI: overlapping runs on main race on the `images` branch

`.github/workflows/ci.yaml` has no `concurrency` setting. Pushes to main in quick
succession run the whole pipeline side by side, and each run's `save-image` jobs push to
the `images` branch (the deploy source). On 2026-10-12 run 38253004963 failed in
`save-image (sluice-server, native)` with "fatal error in commit_refs" while another
run was saving; an older run finishing last could also leave an older image as the
deployed one.

## Direction

A `concurrency` group for push runs on main so that a run waits for the previous one,
and a newer pending run replaces an older pending one (`cancel-in-progress: false`; a run
being cancelled midway through `save-image` is worse than waiting). Pull request runs
can cancel in progress per PR. Check how the reusable `categolj/workflows` save-images
workflow behaves, and whether the deployed image can go backwards today.

## Acceptance

- Two pushes a minute apart: the second run starts after the first finishes (or replaces
  a pending one), and `images` ends at the newer commit.
