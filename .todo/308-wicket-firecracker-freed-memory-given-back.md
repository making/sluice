Difficulty: Medium

# sluice-wicket-runtime-firecracker: give the host back what a guest freed

A VM holds every page its guest ever touched: a guest of 512 MiB that took 300 MiB and freed them
stays at 304 MiB of the host's (62 booted). The memory budget counts the worst case (294), so this
is not admission: it is what the host has free, and what an overcommitted budget
(`--max-memory-mib 1.5x`) bets on.

Measured (findings' "The memory budget"; `.todo/artefacts/294`: `balloon.patch`, `balloon.sh`,
`balloon-clone.sh`, `rate.sh`): a balloon device with `free_page_reporting` gives a booted VM's
back within 2s by itself (304 to 66 MiB), and a clone's own (250 to 14 MiB), at no measurable cost
(a booted VM 15.2-16.3k against 15.1-20.1k keep-alive req/s, a restore 15.3-15.8ms against
15.2-15.5). Inflating the balloon does the same, and takes someone to say by how much.

Not given back by it: what a clone was first to touch of its route's snapshot, the file's holes
filled with zero pages (61 to 303 MiB for that clone's 300; 234 with reporting), which stay as long
as the route, charged to the clone's cgroup. They are the file's alone once the clone wrote its own
copy: punched out again (`fallocate`, the holes the snapshot had as taken), they are freed.

To decide: the device on every VM (Firecracker's state of free page reporting at the version
pinned; a guest kernel without the driver; aarch64), what a template's snapshot holds with it, the
punch (when: a clone's end, the file's growth; what a clone mapping such a page pays), JVM and Go
guests measured (a heap its collector keeps is not free), a clone's cgroup limit with it
(`cgroup.rs`: `Limits::of` allows a clone its route's `memory-mib` once more for those pages).

Acceptance: the VM test shows a VM's memory back after its guest freed it and a snapshot back at
what it was taken with; findings; README ("What a VM uses", "Not yet").
