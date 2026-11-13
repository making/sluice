#!/usr/bin/env bash
# A pool of one clone of <memory-mib> (boot: a booted VM) in cgroups of the wicket's own: what
# its route's and its VM's cgroups are charged as it starts, as the VM's process takes all the
# guest's memory (/oom), and again for the VM in its place; how each ended.
# usage: charge.sh <memory-mib> [boot]   (process.ext4: process-rootfs.sh)
set -euo pipefail
. "$(dirname "$0")/lib.sh"
case "${2:-pool}" in boot) mode="" ;; *) mode="mode=pool&instances=1&" ;; esac
start "$PWD/process.ext4" "${mode}memory-mib=$1"
echo "== started"; show
for round in 1 2; do
  mark=$(wc -c < wicket.log)
  curl -s -m 20 -H 'Host: a.local' "$url/oom" >/dev/null || true
  for _ in $(seq 300); do tail -c +"$mark" wicket.log | grep -q "starts again" && break; sleep 0.1; done
  sleep 1.5
  echo "== /oom $round: $(tail -c +"$mark" wicket.log | grep -o 'VM 0: .*' | head -1)"; show
done
