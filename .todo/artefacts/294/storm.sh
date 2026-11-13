#!/usr/bin/env bash
# A storm of clones (a VM per connection, warm=64, c=64): the wicket's children by state (D: a
# VM killed and not gone yet), the tasks and the memory of its scope, every 2s; then what one
# killed and not gone waits in (sudo: its kernel stack).
# usage: storm.sh [seconds]
set -euo pipefail
. "$(dirname "$0")/lib.sh"
secs=${1:-16}
EXTRA="--max-instances 128" start "$sample" 'mode=per-connection&warm=64'
sleep 1
scope=$(cgroups)
(oha -z "${secs}s" --no-tui -c 64 --disable-keepalive -H 'Host: a.local' "$url/" 2>&1 | grep "Requests/sec") &
for i in $(seq $(( secs / 2 + 2 ))); do
  sleep 2
  echo "t=$((i * 2))s: children by state: $(ps --ppid "$pid" -o stat= | cut -c1 | sort | uniq -c | tr -s ' \n' ' ');" \
    "the scope: $(cat "$scope/pids.current") tasks, $(mib "$(cat "$scope/memory.current")") MiB, the wicket's leaf $(mib "$(cat "$scope/wicket/memory.current")") MiB"
  if [ "$i" = 2 ] && sudo -n true 2>/dev/null; then
    dying=$(ps --ppid "$pid" -o pid=,stat= | awk '$2 ~ /^D/ && n++ < 200 {printf "%s ", $1}')
    sudo -n sh -c "for p in $dying; do cat /proc/\$p/task/*/stack 2>/dev/null | head -4 | tr '\n' ' '; echo; done" \
      | sed 's/+0x[0-9a-f/x]*//g; s/\[<0>\] //g' | grep -v '^ *$' | sort | uniq -c | sort -rn | head -1 || true
  fi
done
wait
