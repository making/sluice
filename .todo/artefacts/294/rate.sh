#!/usr/bin/env bash
# A route's rate without the balloon device and with it (free page reporting), alternating.
# BIN: a wicket built with balloon.patch.
# usage: rate.sh <route query> <oha arguments>...
#   rate.sh 'vcpus=2' -c 16
#   rate.sh 'mode=per-connection&warm=16' -c 16 --disable-keepalive
set -euo pipefail
. "$(dirname "$0")/lib.sh"
query=$1; shift
for run in 1 2 3; do
  for mode in none reporting; do
    case "$mode" in none) unset SFC_BALLOON ;; *) export SFC_BALLOON=$mode ;; esac
    EXTRA="--max-instances 128" start "$sample" "$query"
    sleep 1
    out=$(oha -z 5s --no-tui "$@" -H 'Host: a.local' "$url/" 2>&1)
    echo "$mode run $run: $(echo "$out" | awk '/Requests\/sec:/ {print $2}') req/s, p50 $(echo "$out" | awk '/50.00% in/ {print $3, $4}')"
    stop
  done
done
