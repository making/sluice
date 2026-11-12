Difficulty: Medium

# sluice-wicket-cf-api: a previous release's binary beside the current one in the CLI test

From 283 and 284. `tests/cli.rs` runs two instances of the build under test, and updates their
options one instance at a time (a `--wicket` named, the rules of who logs in). What it cannot
run yet: two builds on one store, as an update of the image makes them. `instances::tests` and
`store::tests::two_instances_change_one_state` cover the rules (the oldest build leads, a row's
fields a build does not read kept, the layout's move) with two builds made in the test.

Needs a release to take: none is published yet (the images are `latest` / `<sha>`).

## Plan

- once there is a release: the test takes its binary (downloaded, or `SLUICE_CF_API_PREVIOUS`),
  starts one instance of it beside one of the build under test on the store, and runs the flow's
  first part through the relay: the previous one reconciles the wickets, the current one waits;
  the previous one stopped, the current one takes over and moves the layout; the previous one
  started again is refused
- skipped where no previous binary is given, as the Firecracker flow is without `/dev/kvm`
