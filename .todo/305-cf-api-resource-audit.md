Difficulty: Medium

# sluice-wicket-cf-api: audit how an app's CPU / memory / disk reach the wickets

The audit that filed 291-304 covered the two wickets; the cf-api's part did not complete. Read and
report, then file what is missing:

- `memory`, `disk_quota`, instances and any CPU setting: parsed, stored, passed to a wicket, or ignored
  (`manifest.rs`, `staging.rs`, `config.rs`, `controller.rs`, `wicket.rs`); where `vcpus` come from
- `placement.rs`: what room is (memory only? the wicket's reported budget or the cf-api's own count?),
  CPU and disk in it, build VMs, what a push sees where no wicket has room
- org / space quotas (`orgs.rs`): enforced or shown only
- process stats (`views.rs`): `cpu`, `mem` and `disk` are the wickets' measurements now (291: an
  instance's `usage` in their listings, those of one index summed); `cpu_entitlement` and `log_rate`
  are still 0
- packages' and droplets' size limits and the blob store's growth (`blobs.rs`, `s3.rs`, `Store::sweep`)

Its README says disk, security groups and log rate are "accepted and shown, no effect" on a wasi wicket.
