Difficulty: Medium

# sluice-wicket-cf-api: a builder whose run image has no shell (Paketo's tiny)

The buildpack staging (`--cnb-builder`) was run with `ubuntu-resolute-builder` and
`builder-jammy-base` alone. A tiny builder (`paketobuildpacks/builder-noble-java-tiny`: its run
image has no shell) was not tried.

1. Build and serve a Java app with a tiny builder, no command: Paketo's default processes are
   direct (exec'd by the launcher, no shell), so it should work; what does not is the finding.
   Measure as `.kb/cnb-java-on-firecracker.md` does.
2. `cf push -c <command>` on such a run image: the droplet's entrypoint is
   `/cnb/lifecycle/launcher <command>`, which execs `/bin/bash` (lifecycle 0.21.28,
   `launch/bash.go`): the app fails at its start. At least, fail staging instead, saying the run
   image has no shell: the wicket has the run image's files when it makes the rootfs
   (`PUT /api/images` with a `launch`), so it can tell whether `/bin/bash` is there.

A tiny run image without `-c` is acceptable (the maintainer's call). Making `-c` work there is
optional, and only in part: the launcher's direct form (`launcher -- <argv>`) neither expands a
variable nor reads shell syntax (`launchDirect`). The command would be split into words, shell
syntax (`;`, `|`, `&&`, redirections, `$(...)`) refused, and `$VAR` / `${VAR}` expanded by the
guest's `/init` at the start from the process's env (the app's, `PORT`, `VCAP_*`; the rootfs is
shared, so not when it is made). Variables the layers set through the launcher (`$JAVA_HOME`)
would stay unexpanded. Shipping a static shell in the droplet (busybox) is not the way: a GPL
binary distributed, and not bash.
