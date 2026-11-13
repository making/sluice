#!/usr/bin/env bash
# What a balloon gives back of a booted VM's memory: its guest takes <MiB> and frees them
# (churn.ext4: churn-rootfs.sh); without a balloon (none), with one inflated by as much and
# deflated (balloon), with free page reporting (reporting). BIN: a wicket built with balloon.patch.
# usage: balloon.sh <none|balloon|reporting> [memory-mib] [churned MiB]
set -euo pipefail
. "$(dirname "$0")/lib.sh"
mode=$1 mem=${2:-512} churn=${3:-300}
case "$mode" in none) unset SFC_BALLOON ;; *) export SFC_BALLOON=$mode ;; esac
start "$PWD/churn.ext4" "memory-mib=$mem"
vm=$(pgrep -x firecracker -P "$pid" | head -1)
api=$(ls "/run/user/$(id -u)/sluice-wicket-runtime-firecracker-$pid"/route-0/vms/vm-*/api.sock)
say() {
  echo "$mode $1: the VM's process $(awk '/VmRSS/{printf "%d", $2/1024}' "/proc/$vm/status") MiB;" \
    "its guest: $(curl -s -H 'Host: a.local' "$url/cgi-bin/mem" | awk '/MemFree|MemAvailable/{printf "%s %d MiB ", $1, $2/1024}')"
}
balloon() { curl -s --unix-socket "$api" -X PATCH http://localhost/balloon -H 'Content-Type: application/json' -d "{\"amount_mib\": $1}"; }
sleep 1; say "booted"
curl -s -H 'Host: a.local' "$url/cgi-bin/churn?$churn" >/dev/null; say "after its guest took $churn MiB and freed them"
case "$mode" in
  balloon)
    balloon "$churn"; sleep 3; say "the balloon inflated to $churn MiB"
    balloon 0; sleep 3; say "deflated" ;;
  *) sleep 5; say "5s later" ;;
esac
