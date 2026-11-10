Difficulty: Medium

# sluice-wicket-cf-api: a run image other than the builder's (tiny)

A build runs on the first run image its builder's metadata names (`stage.rs`, the wicket's:
`images[0]`, else `stack.runImage`). Paketo has no tiny builder for resolute
(`ubuntu-resolute-builder` names `ubuntu-resolute-run` alone), yet publishes
`paketobuildpacks/ubuntu-resolute-run-tiny` (and `-static`): CNB lets a platform pick another run
image of the same target (`pack build --run-image`).

1. Name the run image: a cf-api option beside `--cnb-builder` (`--cnb-run-image`), through the
   build request to the wicket, which pins it to its digest as it does the builder's. Decide
   whether a stack per run image (an app choosing tiny or not, `cf push -s`) is worth it over one
   for the cf-api; check what the lifecycle checks of a run image against the builder (target
   os / arch / distribution) and refuse a mismatch before the build.
2. Required on tiny: Go and Java build on `ubuntu-resolute-builder` and serve on
   `ubuntu-resolute-run-tiny`, measured as `.kb/cnb-java-on-firecracker.md` does (rootfs size
   against `ubuntu-resolute-run`'s). A command (`-c`) there fails staging already (`needs`).
3. Low priority: the community Rust buildpack (`paketo-community/rust`) is in no Paketo
   builder, and `-b` names only the builder's buildpacks: building with it takes a buildpack
   given by image reference (`-b docker://...` or the like), its layers pulled into the build
   VM beside the builder's. Then a Rust app on tiny.
