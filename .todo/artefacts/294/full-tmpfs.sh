#!/usr/bin/env bash
# The state dir's tmpfs full under a clone: a pool of one (128 MiB) on a tmpfs of 1 GiB, shrunk
# to 100 MiB once the route started; its clone's process then takes all the guest's memory.
# Needs sudo (the mount). usage: full-tmpfs.sh   (process.ext4: process-rootfs.sh)
set -euo pipefail
. "$(dirname "$0")/lib.sh"
state=/tmp/sfc294
sudo -n mkdir -p $state
sudo -n mount -t tmpfs -o "size=1g,uid=$(id -u),gid=$(id -g),mode=0700" tmpfs $state
trap 'stop; sudo -n umount $state; sudo -n rmdir $state' EXIT
EXTRA="--state-dir $state" "$here/run.sh" "$PWD/process.ext4" "mode=pool&instances=1" &
for _ in $(seq 300); do grep -q "listening on" wicket.log 2>/dev/null && break; sleep 0.1; done
pid=$(cat wicket.pid)
sudo -n mount -o remount,size=100m $state
mark=$(wc -c < wicket.log)
curl -s -m 10 -H 'Host: a.local' "$url/oom" >/dev/null || true
sleep 12
tail -c +"$mark" wicket.log | grep -v '^stdout'
df -m $state | tail -1
STATE=$state show
