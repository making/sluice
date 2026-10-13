#!/bin/bash
# The series of todo 134 (results-134.jsonl): one measured series for build $1 (a dir holding
# sluice-server/target and sluice-client/target) tagged $2. Rows: the budget suite at a 20ms
# tunnel RTT (one and many connections, the largest heap / budget use / buffered payload per
# window) and the held connections' live heap.
cd "$(dirname "$0")"
B=$1
T=$2
uptime > "out/uptime-$T-before.txt"
python3 bench.py "$B" budget --delay 10 --tag "$T-d10" > "out/run-$T-d10.log" 2>&1
python3 bench.py "$B" hold --tag "$T-hold" > "out/run-$T-hold.log" 2>&1
uptime > "out/uptime-$T-after.txt"
echo SERIES DONE "$T"
