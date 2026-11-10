Difficulty: Medium

# sluice-wicket-cf-api: a buildpack given by image reference (Rust on tiny)

`cf push -b` names only the builder's buildpacks (`stage.rs`, the wicket's: `order`). The
community Rust buildpack (`paketo-community/rust`) is in no Paketo builder: building a Rust app
takes a buildpack given by image reference (`-b docker://...` or the like, as `pack build
--buildpack docker://...`), its layers pulled into the build VM beside the builder's
(`/cnb/buildpacks/<id>/<version>`), its id and version from its labels
(`io.buildpacks.buildpackage.metadata`), its `[[targets]]` checked by the detector as the
builder's are.

Then a Rust app on `ubuntu-resolute-run-tiny` (`--cnb-run-image`), measured as
`.kb/cnb-java-on-firecracker.md#a-run-image-of-the-platforms` does Go's.
