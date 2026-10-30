#!/usr/bin/env bash
# Stops the demo stack. --reset also deletes its state: the apps, the services, the keyvalue
# data and the CLI's login.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
work=$(cd "$here/.." && pwd)/target/demo

for name in cf-api wicket flume; do
	pid_file=$work/run/$name.pid
	[ -f "$pid_file" ] || continue
	pid=$(cat "$pid_file")
	rm -f "$pid_file"
	# a pid its process no longer holds may be another program's by now
	ps -p "$pid" -o command= 2> /dev/null | grep -q sluice- || continue
	kill "$pid" 2> /dev/null || continue
	for _ in $(seq 1 50); do
		kill -0 "$pid" 2> /dev/null || break
		sleep 0.1
	done
	kill -9 "$pid" 2> /dev/null || true
done
docker rm -f sluice-cf-demo-dex > /dev/null 2>&1 || true
if [ "${1:-}" = --reset ]; then
	rm -rf "$work/run" "$work/logs" "$work/cfhome"
fi
