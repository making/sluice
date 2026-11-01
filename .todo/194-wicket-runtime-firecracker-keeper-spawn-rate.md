Difficulty: High

# sluice-wicket-runtime-firecracker: the keeper's single thread bounds the jailed clone rate

In jail mode the keeper (`src/keeper.rs`) forks every VM's jail (`clone3`, `src/jail.rs`)
from its one thread. A fork costs 130us on the 64-core 6.8 test host (the new memory map's
per-CPU counters), with the copy-on-write faults after it about 0.6ms of the keeper's
thread per VM: jailed, the sample guest's clone rate at c=16 is 85% of the unjailed one
(1,295-1,320 against 1,455-1,544 new connections/s, the README's Findings); c=1 alike.

- Spawner threads forking in parallel: the child then runs after a fork of a
  multi-threaded process (no allocation, its errors written without one; `setresuid`,
  `setresgid`, `setgroups` as raw system calls, glibc's broadcast them to threads the child
  lacks); the template's attach / detach (`setns`) needs a single-threaded caller (a vfork
  child for them); dup_mmap still serializes on the memory map lock
- Or spawner processes forked at startup, each single-threaded, owning a share of the VMs
  (the routes' fds and template sent to each; their reaps reported to the keeper)
- Not vfork: the jail's setup in the child takes 1-4ms (the kernel's mount namespace lock),
  which would hold the keeper

Keep the privileged half small and auditable; measure as the Findings do (direct and jailed
alternating, `--listen`).
