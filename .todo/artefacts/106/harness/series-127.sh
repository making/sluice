#!/bin/bash
# The series of todo 127 (results-127.jsonl). One measured series for build $1 (a dir holding sluice-server/target and sluice-client/target) tagged $2.
# Rows: loopback and 20ms tunnel RTT download/upload (winexp), the latter also with a 1MiB upstream window,
# and the client-RTT suite (cwan).
cd "$(dirname "$0")"
B=$1
T=$2
uptime > "out/uptime-$T-before.txt"
python3 bench.py "$B" winexp --tag "$T-d0" > "out/run-$T-d0.log" 2>&1
python3 bench.py "$B" winexp --delay 10 --tag "$T-d10" > "out/run-$T-d10.log" 2>&1
python3 bench.py "$B" winexp --delay 10 --upstream-window 1048576 --tag "$T-up1m-d10" > "out/run-$T-up1m-d10.log" 2>&1
python3 bench.py "$B" cwan --tag "$T-cwan10" > "out/run-$T-cwan10.log" 2>&1
uptime > "out/uptime-$T-after.txt"
echo SERIES DONE "$T"
