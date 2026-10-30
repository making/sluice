#!/usr/bin/env bash
# Starts the demo stack on the loopback from the release builds: sluice-flume (8002), the wicket
# in local mode (the apps on 8080, its console on 9090), Dex as the OpenID Provider (5556, with
# Docker; dex.yaml: the user demo@example.com, password demo) and the cf-api (9091) logging in
# through it. Its state, logs and the packages cf push takes (apps/hello, apps/counter) are in
# ../target/demo.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$here/../.." && pwd)
work=$repo/sluice-wicket-cf-api/target/demo
run=$work/run

flume=$repo/sluice-flume/target/release/sluice-flume
wicket=$repo/sluice-wicket-wasi/target/release/sluice-wicket-wasi
cf_api=$repo/sluice-wicket-cf-api/target/release/sluice-wicket-cf-api
guests=$repo/sluice-wicket-wasi/examples/target/wasm32-wasip3/release
for file in "$flume" "$wicket" "$cf_api" "$guests/hello.wasm" "$guests/counter.wasm"; do
	if [ ! -f "$file" ]; then
		echo "$file is missing: build it (cargo build --release), or run record.sh" >&2
		exit 1
	fi
done
if ! command -v docker > /dev/null; then
	echo "docker is missing: Dex, the demo's OpenID Provider, runs as a container" >&2
	exit 1
fi
for port in 5556 8002 8080 9090 9091; do
	if nc -z 127.0.0.1 "$port" 2> /dev/null; then
		echo "port $port is in use (the demo stack still running? stop.sh)" >&2
		exit 1
	fi
done

mkdir -p "$run/wicket" "$run/cf-api" "$work/logs" "$work/apps/hello" "$work/apps/counter"
cp "$guests/hello.wasm" "$work/apps/hello/"
cp "$guests/counter.wasm" "$work/apps/counter/"

start() { # start <name> <dir> <command...>
	local name=$1 dir=$2
	shift 2
	# only the command goes to the background, so $! is its pid, not a subshell's
	(cd "$dir" || exit 1; nohup "$@" > "$work/logs/$name.log" 2>&1 < /dev/null & echo $! > "$run/$name.pid")
}
wait_port() {
	for _ in $(seq 1 100); do
		nc -z 127.0.0.1 "$1" 2> /dev/null && return 0
		sleep 0.1
	done
	echo "port $1 did not open (see $work/logs)" >&2
	return 1
}

start flume "$run" "$flume" --host 127.0.0.1 --port 8002 --token flume-secret
wait_port 8002
# the wicket's working directory holds its keyvalue store (keyvalue.db)
start wicket "$run/wicket" "$wicket" \
	--listen 127.0.0.1:8080 --console 127.0.0.1:9090 --console-token wicket-secret \
	--console-dir "$run/wicket/deployed" \
	--flume grpc://127.0.0.1:8002 --flume-token flume-secret
wait_port 9090
# Dex on the host's network: the browser, the cf-api and this script reach it at 127.0.0.1:5556
docker run -d --rm --name sluice-cf-demo-dex --network host \
	-v "$here/dex.yaml:/etc/dex/dex.yaml:ro" ghcr.io/dexidp/dex:v2.46.1 dex serve /etc/dex/dex.yaml > /dev/null
wait_port 5556
start cf-api "$run/cf-api" "$cf_api" \
	--listen 127.0.0.1:9091 --wicket http://127.0.0.1:9090 --wicket-token wicket-secret \
	--data-dir "$run/cf-api" \
	--flume grpc://127.0.0.1:8002 --flume-token flume-secret \
	--oidc-issuer http://127.0.0.1:5556/dex --oidc-client-id cf --oidc-client-secret demo-secret \
	--oidc-claim 'email=*@example.com'
wait_port 9091
echo "cf API http://127.0.0.1:9091 (cf login --sso: demo@example.com / demo at Dex, or passcode.sh)," \
	"apps http://<app>.localhost:8080, wicket console http://127.0.0.1:9090 (password wicket-secret)"
