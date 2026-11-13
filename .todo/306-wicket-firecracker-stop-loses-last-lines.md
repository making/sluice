Difficulty: Medium

# Firecracker wicket: a route's stop may lose its process's last lines

A stop syncs a VM's output (`Output::sync`, `route.rs`) only when it holds the last reference to the
VM. The watch (`watched` / `accepting_within`'s look) upgrades its weak reference briefly once the guest
says the process ended; a stop running then does not hold the last one, and the process's last lines
(what it prints as it is told to stop) are not read.

Seen once in the cf-api's CLI test with Firecracker (2026-11-13, `tests/cli.rs`: "never the old VM's
process told to stop"); the rerun passed. The race predates the usage measurement.

To decide: sync through a shared reference (the output owned apart from the VM's count), or end the
watch before the stop.

Acceptance: a test that fails on the race (a stop while the watch looks), then the fix.
