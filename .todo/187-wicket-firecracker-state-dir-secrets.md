Difficulty: Low

# sluice-wicket-firecracker: what the state dir and the command line expose

- A per-connection route's snapshot (`<state-dir>/route-<n>/snapshot/vm.mem`) is the
  template's whole memory, on disk, unencrypted: what the process read before the snapshot
  (an environment's secrets, a key it loaded) is in it, and stays on disk after a crash (the
  wicket removes it at its end). Default the state dir to a tmpfs (`$XDG_RUNTIME_DIR`,
  `/run`), create it 0700, and say so in the README and for the image (`.todo` 182)
- A secret the process generates before the snapshot (a key pair, a session ticket key) is
  every clone's: the restore notice reseeds the random generators, not what they produced.
  README: generate such secrets in the restore hook, or not before the snapshot
- `--token` on the command line is in `/proc/<pid>/cmdline`, readable by every user of the
  host: take it from the environment or the config file alone, or a `--token-file` as the
  server's `sluice.token-file`
