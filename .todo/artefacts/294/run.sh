#!/usr/bin/env bash
# The wicket on --listen with one route, a.local, its pid in wicket.pid and its output in
# wicket.log (both in the current directory): in a delegated scope of the user's manager, its VMs
# in cgroups of its own (SCOPE=0: where it is started, none).
# usage: run.sh <rootfs> <route query>
# BIN=<wicket binary> (the release build), PORT=<port> (18294), EXTRA="<wicket arguments>".
# Needs firecracker in ~/.local/bin and the kernel in ~/.local/firecracker.
set -euo pipefail
wicket=$(cd "$(dirname "$0")/../../../sluice-wicket-runtime-firecracker" && pwd)
bin=${BIN:-$wicket/target/release/sluice-wicket-runtime-firecracker}
rootfs=$1 query=$2
echo $$ > wicket.pid
scope=(systemd-run --user --scope --quiet --collect -p Delegate=yes --)
[ "${SCOPE:-1}" = 0 ] && scope=()
# systemd-run execs the wicket: the pid written is its own
exec "${scope[@]}" "$bin" \
  --firecracker "$HOME/.local/bin/firecracker" --kernel "$HOME/.local/firecracker/vmlinux" \
  --listen "127.0.0.1:${PORT:-18294}" --vm "a.local=$rootfs?$query" ${EXTRA:-} > wicket.log 2>&1
