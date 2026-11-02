# The cf-api on the Firecracker wicket (todo 200)

`./spike.sh`: the cf-api's image on a wicket built from the working tree, on a disk: `cf push`
through the VM, a restart, an upgrade and a failed start of the VM, the outages they make. Fails
if a step behaves otherwise.

Ports 18399 and 19399 (`LISTEN_PORT`, `CONSOLE_PORT`) are fixed: the cf-api's egress names the
console's.

`./one-container.sh [jail]`: the cf-api's README, "On a Firecracker wicket", its commands taken from the
page and run in order, but for images built from the work tree, names and free ports of its own;
then a kill right after a push, 1 GiB written in a VM of 256 MiB.

`./disk-measure.sh [dir]` (`disk-init.c` its VMs' /init): what a writable drive does on Firecracker
alone: the numbers of the wicket's README, Findings. `sg kvm -c`.

`./jail-test.sh <test binary>`: the VM test jailed, as the CI's `Test the jail`, where the host has
no `sudo -n`: in a container of the capabilities the README's jail mode lists, with sudo inside.
