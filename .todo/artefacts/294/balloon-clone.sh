#!/usr/bin/env bash
# The same for a pool's clone: what its guest took and freed, of its own and of its route's
# snapshot. BIN: a wicket built with balloon.patch.
# usage: balloon-clone.sh <none|reporting> [memory-mib] [churned MiB]
set -euo pipefail
. "$(dirname "$0")/lib.sh"
mode=$1 mem=${2:-512} churn=${3:-300}
case "$mode" in none) unset SFC_BALLOON ;; *) export SFC_BALLOON=$mode ;; esac
start "$PWD/churn.ext4" "mode=pool&instances=1&memory-mib=$mem"
vm=$(pgrep -x firecracker -P "$pid" | head -1)
say() {
  echo "$mode $1: the clone's process $(awk '/RssAnon/{printf "%d", $2/1024}' "/proc/$vm/status") MiB of its own," \
    "$(awk '/RssShmem/{printf "%d", $2/1024}' "/proc/$vm/status") of the snapshot;" \
    "the snapshot $(du -m "/run/user/$(id -u)/sluice-wicket-runtime-firecracker-$pid/route-0/snapshot/vm.mem" | cut -f1) MiB"
}
sleep 1; say "restored"
for again in "" " again"; do
  curl -s -H 'Host: a.local' "$url/cgi-bin/churn?$churn" >/dev/null; say "after its guest took $churn MiB$again and freed them"
  sleep 5; say "5s later"
done
