#!/bin/bash
# The series of todo 135 (results-135.jsonl): the h2credit suite at a 20ms tunnel RTT for the
# builds $1 (base) and $2 (after), each a dir holding sluice-server/target and
# sluice-client/target, alternated three times on fresh stacks; rows tagged base-<n> / after-<n>.
cd "$(dirname "$0")"
uptime > "out/uptime-135-before.txt"
for n in 5 6 7; do
  python3 bench.py "$1" h2credit --delay 10 --tag "base-$n" > "out/run-135-base-$n.log" 2>&1
  python3 bench.py "$2" h2credit --delay 10 --tag "after-$n" > "out/run-135-after-$n.log" 2>&1
done
uptime > "out/uptime-135-after.txt"
echo SERIES DONE
