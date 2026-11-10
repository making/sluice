Difficulty: Medium

# sluice-wicket-cf-api: what the buildpack staging (`--cnb-builder`) left out

`cf push` of source is staged by a builder's buildpacks in a VM of a Firecracker wicket
(`stage.rs` of the wicket and its guest, `Buildpack::Cnb` of the cf-api). Left out:

- a cache between builds: a per-app disk kept on the wicket as `<cache>`, the lifecycle's
  `restorer` before the build, the cache layers written back after it. Every build is cold today
  (Java 65s, `.kb/cnb-java-on-firecracker.md`)
- `cf push -b <buildpack id>` as the build's order (`-b cnb` alone is taken): an `order.toml`
  of the ids given, in the build's request
- `cf push -c <command>` on a buildpack droplet: the launch's entrypoint and arguments over the
  default process's (the launcher's `--` form); today the command must name a file of the package
- the build VM's size from the cf-api (`memory-mib`, `vcpus`, `disk-mib`, `timeout-ms` of
  `POST /api/builds`): the wicket's defaults stand (2 vCPUs, 2048 MiB, 8 GiB, 15 minutes)
- the builder's credentials are the cf-api's flags alone: without them a private run image is not
  placed again. Keep them with the droplet, as an image's are
- a build in jail mode was not run (the VM test's jailed run covers the guest's changes, not a
  build's scratch disk through the keeper)
- an egress by proxy: a build reaches only the hosts `--cnb-egress` names, each redirect's too
  (`.kb/cnb-java-on-firecracker.md` lists those of Java, Node and Go); `HTTPS_PROXY` in the
  build's env with the proxy as the one rule was not tried
- a buildpack's stderr reaches the staging log as `OUT`: the lifecycle writes it to its own
  stdout. Nothing to fix here unless the lifecycle tells them apart
- a failed build's last lines are not in `cf push`'s output: the CLI stops following the log once
  the build is failed, the lines still on their way (Maven's `[ERROR]` lines of a compile
  failing, seen with `BP_JVM_VERSION: 17` on a Java 21 pom). The cf-api has them (its stdout,
  the app's log); only "the builder failed (exit status: 51)" is shown
