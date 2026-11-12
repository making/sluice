Difficulty: Low

# sluice-wicket-cf-api: a diskless instance's push of a large zip, measured in a VM

From 289. An upload's zip is written once as it comes and a package unpacked from the bucket
by ranges: the peak of `--data-dir` at a push of a 135 MiB site went from 270 MiB to 135 MiB,
measured on the host only (`.kb/wicket-cf-api-findings.md`: An instance's scratch).

Not measured again where it failed: an instance in a VM with no `disk` (`ha.md`: As apps of
Firecracker wickets), `memory-mib` 512, `/tmp` half of it, which refused that zip's upload
(`No space left on device`).

- push the 135 MiB zip to an instance of `memory-mib` 512, and find the smallest `memory-mib`
  taking it; the numbers to the findings, `ha.md`'s Sizing and backup stated by them
- a tmpfs counts as the VM's memory beside the process's own (the 8 MiB piece of a package
  read from the bucket, the zip's inflate): what is left of `/tmp`'s half is the question
