#!/bin/bash
# The series of todo 140 (results-140.jsonl): for the builds $1 (base) and $2 (after), each
# a dir holding sluice-server/target and sluice-client/target, alternated twice on fresh
# stacks: suites one + h2credit + budget at a 20ms tunnel RTT, ovh on loopback; rows tagged
# base-<n> / after-<n>.
cd "$(dirname "$0")"
uptime > "out/uptime-140-before.txt"
for n in 1 2; do
  for b in base after; do
    dir=$1; [ "$b" = after ] && dir=$2
    python3 bench.py "$dir" one,h2credit,budget --delay 10 --tag "$b-$n" > "out/run-140-$b-$n-d.log" 2>&1
    python3 bench.py "$dir" ovh --tag "$b-$n" > "out/run-140-$b-$n-l.log" 2>&1
  done
done
uptime > "out/uptime-140-after.txt"
echo SERIES DONE
