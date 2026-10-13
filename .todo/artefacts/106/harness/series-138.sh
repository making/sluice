#!/bin/bash
# The series of todo 138 (results-138.jsonl): builds $1 (before) and $2 (after), each a dir
# holding sluice-server/target and sluice-client/target, alternated on fresh stacks: the
# budget suite three times each, then the budgetone suite (three single h1 downloads and
# uploads per stack) twice each, all at a 20ms tunnel RTT. Each stack waits until no
# surefire JVM runs.
cd "$(dirname "$0")"
A=$1
B=$2
quiet() {
	while pgrep -f surefire > /dev/null; do sleep 10; done
	uptime >> out/uptime-138.txt
}
for i in 1 2 3; do
	quiet; python3 bench.py "$A" budget --delay 10 --tag "base-r$i" > "out/run-base-r$i.log" 2>&1
	quiet; python3 bench.py "$B" budget --delay 10 --tag "after-r$i" > "out/run-after-r$i.log" 2>&1
done
for i in 1 2; do
	quiet; python3 bench.py "$A" budgetone --delay 10 --tag "base-one$i" > "out/run-base-one$i.log" 2>&1
	quiet; python3 bench.py "$B" budgetone --delay 10 --tag "after-one$i" > "out/run-after-one$i.log" 2>&1
done
echo SERIES DONE
