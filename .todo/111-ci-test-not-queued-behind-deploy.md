Difficulty: Medium

# CI: a push's tests wait for the previous push's whole pipeline

Since 109 the whole `ci.yaml` run is in one concurrency group on main: a second push's
`test` job (about 4 min) waits until the first run has finished its image builds and
`save-image` (about 25 min). Test feedback on back-to-back pushes went from 5 to up to 30
minutes, which undoes much of 108.

## Direction

Split testing from deploying:
- `ci.yaml`: `test` only, no concurrency group on main (PRs may still cancel in progress),
  so every push gets its result in about 5 minutes.
- A deploy workflow triggered by `workflow_run` (CI completed with success on main) that
  builds the images, pushes the manifests and saves them, in one concurrency group
  (`cancel-in-progress: false`), and skips (succeeds without deploying) when its
  `head_sha` is no longer the head of main, so an older run never deploys after a newer
  one. Check out `github.event.workflow_run.head_sha`, not the default ref.
- Keep the path filter (only the CI workflow needs it; the deploy follows CI).
- README / CLAUDE.md / memory notes that describe "push -> CI -> deploy" stay true.

## Acceptance

- Two pushes a minute apart: both `test` jobs start at once; the deploy runs once per
  green head in order, and `images` ends at the newer commit (observe on GitHub, run ids
  in the report).
- A deploy for a commit that is no longer the head of main skips without pushing images.
