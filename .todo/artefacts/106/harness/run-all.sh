#!/bin/bash
# The recorded series: every suite on a fresh stack, one after the other (nothing else
# of the harness runs meanwhile). Results: out/results.jsonl, logs: out/run-*.log.
cd "$(dirname "$0")"
REPO=${REPO:?set REPO to the sluice checkout}
W="--server-opt=-Drelay.streamWindow=1048576 --server-opt=-Dhop.connWindow=16777216"
uptime > out/uptime-before.txt
python3 bench.py "$REPO" direct,throughput,small --tag base > out/run-base.log 2>&1
python3 bench.py "$REPO" wan --delay 10 --tag wan10 > out/run-wan10.log 2>&1
python3 bench.py "$REPO" cwan --tag cwan10 > out/run-cwan10.log 2>&1
python3 bench.py "$REPO" lock --patched --tag lock > out/run-lock.log 2>&1
python3 bench.py "$REPO" hold --tag hold > out/run-hold.log 2>&1
python3 bench.py "$REPO" winexp --tag winbase-d0 > out/run-winbase-d0.log 2>&1
python3 bench.py "$REPO" winexp --window-patch --tag win1m-d0 $W > out/run-win1m-d0.log 2>&1
python3 bench.py "$REPO" winexp --delay 10 --tag winbase-d10 > out/run-winbase-d10.log 2>&1
python3 bench.py "$REPO" winexp --window-patch --delay 10 --tag win1m-d10 $W > out/run-win1m-d10.log 2>&1
python3 bench.py "$REPO" winexp --window-patch --delay 10 --upstream-window 1048576 --tag win1m-up1m-d10 $W > out/run-win1m-up1m-d10.log 2>&1
uptime > out/uptime-after.txt
echo ALL DONE
