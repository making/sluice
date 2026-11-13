#!/usr/bin/env bash
# What the wicket itself takes under a load: its leaf of its cgroup (the forker's too) and its
# process. usage: own.sh <label> <route query> <oha arguments>...
#   own.sh pool 'mode=pool&instances=4' -c 256
#   own.sh clones 'mode=per-connection&warm=64' -c 64 --disable-keepalive
set -euo pipefail
. "$(dirname "$0")/lib.sh"
label=$1 query=$2; shift 2
EXTRA="--max-instances 128" start "$sample" "$query"
sleep 1
own() {
  local cg; cg=$(cgroups)/wicket
  echo "$label $1: its cgroup $(mib "$(cat "$cg/memory.current")") MiB now, $(mib "$(cat "$cg/memory.peak")") at most" \
    "(anon $(mib "$(of "$cg" anon)"), kernel $(mib "$(of "$cg" kernel)"): stacks $(mib "$(of "$cg" kernel_stack)"), slab $(mib "$(of "$cg" slab)"));" \
    "its process $(awk '/VmRSS/{print int($2/1024)}' "/proc/$pid/status") MiB now, $(awk '/VmHWM/{print int($2/1024)}' "/proc/$pid/status") at most;" \
    "$(pgrep -x firecracker -P "$pid" | wc -l) VM(s)"
}
own idle
out=$(oha -z "${SECS:-10}s" --no-tui "$@" -H 'Host: a.local' "$url/" 2>&1)
echo "$label: $(echo "$out" | awk '/Requests\/sec:/ {print $2}') req/s, $(echo "$out" | awk '/\[200\]/ {print $2}') answered 200"
own "after the load"
