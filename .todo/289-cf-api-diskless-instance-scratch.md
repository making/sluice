Difficulty: Medium

# sluice-wicket-cf-api: an instance without a disk: its scratch and its flume node

From 284. An instance in a VM with no `disk` (`ha.md`: As apps of Firecracker wickets) has its
`--data-dir` on `/tmp`, a tmpfs of half the VM's memory, gone with the VM. Two things follow
that a disk hid:

- an upload is in `--data-dir` twice while it comes (the body spooled, then its part written
  out: `api.rs` `spool`, `multipart.rs`), a package then unpacked from a copy: `memory-mib` 512
  refused a 135 MiB zip at the upload (`No space left on device`), 1024 pushed it
  (`.kb/wicket-cf-api-findings.md`: The deployment of several instances). Write the part as the
  body comes, or put it to the bucket as a stream: the scratch is then one copy, or none
- the instance's id among the flume's nodes is a file of `--data-dir` (`flume-node`): each boot
  is a new node, the ones before listed as disconnected until the flume's restart. An id that
  outlives the VM (of `--instance-url`, where given: two instances never share one; but an old
  and a new VM do for a moment during an update: what the flume does with one id twice first),
  or the flume forgetting a node gone for long

Measure first: the peak of `/tmp` during an upload and a staging, with the bucket and without.
