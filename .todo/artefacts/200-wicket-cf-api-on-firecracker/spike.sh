#!/usr/bin/env bash
# The cf-api as an app of the Firecracker wicket: its image from a local registry, the rootfs
# made by the wicket, `cf push` through the VM, then a restart and an upgrade of the VM, its
# disk keeping its state. Needs docker, the kvm group, firecracker,
# ~/.local/firecracker/vmlinux, cf8 and registry:3. Exits non-zero when what it shows changes.
# Files stay in the directory it prints.
set -euo pipefail
repo=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)
fc=$repo/sluice-wicket-runtime-firecracker
work=$(mktemp -d)
echo "work: $work"
cd "$work"
token=s3cret listen=${LISTEN_PORT:-18399} console=${CONSOLE_PORT:-19399}
registry_name=sluice-cf-api-spike-registry
registry=
wicket=
cleanup() {
  [ -n "$wicket" ] && pkill -TERM -f "console-dir $work/deployed" || true
  docker rm -f "$registry_name" >/dev/null 2>&1 || true
  [ -n "$registry" ] && docker rmi "$registry/test/cf-api:1" "$registry/test/tiny:1" >/dev/null 2>&1 || true
}
trap cleanup EXIT
expect() { # <what> <text> <pattern>
  grep -q -- "$3" <<<"$2" || { echo "UNEXPECTED $1: $2" >&2; exit 1; }
  echo "ok: $1"
}

docker run -d --name "$registry_name" -p 127.0.0.1:0:5000 registry:3 >/dev/null
registry=127.0.0.1:$(docker port "$registry_name" 5000/tcp | cut -d: -f2)
docker build -q -f "$repo/sluice-wicket-cf-api/Dockerfile" -t "$registry/test/cf-api:1" "$repo" >/dev/null
docker push -q "$registry/test/cf-api:1" >/dev/null
guest=$fc/$("$fc/scripts/guest.sh")
mkdir ctx
cp "$(dirname "$guest")/sample-process" ctx/serve
printf 'FROM scratch\nCOPY serve /app/serve\nENTRYPOINT ["/app/serve", "/app/serve"]\n' > ctx/Dockerfile
docker build -q -t "$registry/test/tiny:1" ctx >/dev/null
docker push -q "$registry/test/tiny:1" >/dev/null
cargo build --locked --manifest-path "$fc/Cargo.toml" 2>/dev/null
sg kvm -c "$fc/target/debug/sluice-wicket-runtime-firecracker --kernel $HOME/.local/firecracker/vmlinux \
  --firecracker $(command -v firecracker) --listen 127.0.0.1:$listen --console 127.0.0.1:$console \
  --console-token $token --console-dir $work/deployed --guest $guest --watch-interval 0" > wicket.log 2>&1 &
wicket=$!
sleep 2
auth="Authorization: Bearer $token" api=http://127.0.0.1:$console

made=$(curl -sS -X PUT --max-time 300 -H "$auth" -d "{\"image\":\"$registry/test/cf-api:1\"}" "$api/api/images")
sha=$(python3 -I -c 'import json,sys;print(json.loads(sys.argv[1])["sha256"])' "$made")
put_cf_api() { # <env, JSON>
  curl -sS -X PUT --max-time 60 -H "$auth" -d "{\"owner\":\"ops\",\"hosts\":[\"cf.localhost\"],\
\"component\":{\"sha256\":\"${2:-$sha}\",\"name\":\"cf-api.ext4\"},\"sandbox\":{\"mem\":${MEM:-256},${DISK:-}\
\"egress\":[\"localhost:$console\"],\"env\":$1}}" "$api/api/apps/cf-api"
}
listed() {
  curl -sS -H "$auth" "$api/api/apps" |
    python3 -I -c 'import json,sys;print(sorted(a["owner"] for a in json.load(sys.stdin)["apps"]))'
}
common="\"SLUICE_WICKET_CF_API_ANY_CREDENTIALS\":\"true\",\"SLUICE_WICKET_CF_API_WICKET\":\"http://localhost:$console\",\"SLUICE_WICKET_CF_API_WICKET_TOKEN\":\"$token\""

echo "1. the default data directory: the rootfs is read-only"
expect "refused" "$(put_cf_api "{$common}")" "guest not listening"
expect "its reason" "$(cat wicket.log)" "Read-only file system"

echo "2. a disk at the image's working directory: the default data directory is on it"
DISK='"disk":"/data","disk-mib":1024,'
expect "started" "$(put_cf_api "{$common}")" "added cf.localhost"
expect "the disk" "$(curl -sS -H "$auth" "$api/api/disks")" '"owner":"ops","name":"cf-api","mib":1024,"app":true'

echo "3. cf push through the VM"
export CF_HOME=$work/cfhome CF_COLOR=false
mkdir -p "$CF_HOME"
timeout 60 cf8 api "http://cf.localhost:$listen" >/dev/null
timeout 60 cf8 login -u me -p x >/dev/null
timeout 200 cf8 push hello --docker-image "$registry/test/tiny:1" >/dev/null
expect "the app, served" "$(curl -sS --max-time 10 -H 'Host: hello.localhost' "http://127.0.0.1:$listen/")" "^uid 0"
expect "both apps listed" "$(listed)" "\['cf', 'ops'\]"

# how long the API does not answer as `put_cf_api "$@"` runs: its VM stopped, the next booted
outage() {
  local first=0 last=0 now status
  while :; do
    now=$(date +%s%N)
    status=$(curl -s -o /dev/null -w '%{http_code}' --max-time 2 -H 'Host: cf.localhost' "http://127.0.0.1:$listen/v3/info" || true)
    if [ "$status" != 200 ]; then
      [ "$first" = 0 ] && first=$now
      last=$(date +%s%N)
    elif [ "$first" != 0 ]; then
      break
    fi
    [ -e "$work/outage.end" ] && [ "$first" = 0 ] && break
    sleep 0.005
  done
  echo "$(( (last - first) / 1000000 ))ms" >"$work/outage.ms"
}

echo "4. a restart of its VM: the state kept, the app with it, the CLI's token good"
rm -f "$work/outage.end"
outage &
watcher=$!
sleep 0.2
started=$(date +%s%N)
expect "restarted" "$(put_cf_api "{$common,\"RESTART\":\"1\"}")" "restarted cf.localhost"
echo "   the PUT took $(( ($(date +%s%N) - started) / 1000000 ))ms"
touch "$work/outage.end"
wait "$watcher"
echo "   the API did not answer for $(cat "$work/outage.ms")"
sleep 2
expect "the app kept" "$(listed)" "\['cf', 'ops'\]"
expect "the CLI's token" "$(timeout 60 cf8 apps 2>&1)" "hello"
expect "the app, served" "$(curl -sS --max-time 10 -H 'Host: hello.localhost' "http://127.0.0.1:$listen/")" "^uid 0"
expect "no app left alone" "$(grep -c 'left in the wicket' wicket.log || true)" "^0$"

echo "5. an upgrade: another image of the cf-api, the disk kept"
cp -f "$(ls "$work"/deployed/rootfs/"$sha"/*.ext4)" upgraded.ext4
truncate -s +4096 upgraded.ext4
upgraded=$(sha256sum upgraded.ext4 | cut -d' ' -f1)
curl -sS -X PUT --max-time 120 -H "$auth" --data-binary @upgraded.ext4 "$api/api/components/$upgraded" >/dev/null
rm -f "$work/outage.end"
outage &
watcher=$!
sleep 0.2
expect "restarted" "$(put_cf_api "{$common}" "$upgraded")" "restarted cf.localhost"
touch "$work/outage.end"
wait "$watcher"
echo "   the API did not answer for $(cat "$work/outage.ms")"
expect "the app kept" "$(timeout 60 cf8 apps 2>&1)" "hello"

echo "6. a start failing, a cf-api that never listens: the old VM started again on the disk"
started=$(date +%s%N)
expect "refused" "$(put_cf_api "{$common,\"SLUICE_WICKET_CF_API_LISTEN\":\"127.0.0.1:1\"}" "$upgraded")" "guest not listening"
echo "   the PUT took $(( ($(date +%s%N) - started) / 1000000 ))ms"
expect "served again" "$(timeout 60 cf8 apps 2>&1)" "hello"

echo "as measured"
