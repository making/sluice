#!/usr/bin/env bash
# Runs the debug build of the wicket in local mode (--listen) with the kernel of the README's Setup,
# the rootfs on a.local; the arguments after the port go to the wicket (--vm b.local=<rootfs>: a
# second route, curl -H 'Host: b.local').
# Usage: scripts/try.sh <rootfs.ext4> <route query> [port] [wicket arguments]...
set -euo pipefail
cd "$(dirname "$0")/.."
rootfs=$1 query=$2 port=${3:-18080}
shift $(($# < 3 ? $# : 3))
extra=
if (($#)); then extra=$(printf ' %q' "$@"); fi
exec sg kvm -c "target/debug/sluice-wicket-runtime-firecracker --firecracker $HOME/.local/bin/firecracker \
  --kernel $HOME/.local/firecracker/vmlinux --state-dir target/st-$port --listen 127.0.0.1:$port \
  --vm 'a.local=$rootfs?$query'$extra"
