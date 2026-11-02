#!/usr/bin/env bash
# The cf-api as an app of the Firecracker wicket: its image from a local registry, the rootfs
# made by the wicket, `cf push` through the VM, then a restart of the VM. Needs docker, the kvm
# group, firecracker, ~/.local/firecracker/vmlinux, cf8 and registry:3. Exits non-zero when what
# it shows changes; once todo 200 is done, step 4 must keep the app. Files stay in the directory
# it prints.
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
\"component\":{\"sha256\":\"$sha\",\"name\":\"cf-api.ext4\"},\"sandbox\":{\"mem\":256,\
\"egress\":[\"localhost:$console\"],\"env\":$1}}" "$api/api/apps/cf-api"
}
listed() {
  curl -sS -H "$auth" "$api/api/apps" |
    python3 -I -c 'import json,sys;print(sorted(a["owner"] for a in json.load(sys.stdin)["apps"]))'
}
common="\"SLUICE_WICKET_CF_API_WICKET\":\"http://localhost:$console\",\"SLUICE_WICKET_CF_API_WICKET_TOKEN\":\"$token\""

echo "1. the default data directory: the rootfs is read-only"
expect "refused" "$(put_cf_api "{$common}")" "guest not listening"
expect "its reason" "$(cat wicket.log)" "Read-only file system"

echo "2. the data directory on the guest's tmpfs"
expect "started" "$(put_cf_api "{$common,\"SLUICE_WICKET_CF_API_DATA_DIR\":\"/tmp/cf\"}")" "added cf.localhost"

echo "3. cf push through the VM"
export CF_HOME=$work/cfhome CF_COLOR=false
mkdir -p "$CF_HOME"
timeout 60 cf8 api "http://cf.localhost:$listen" >/dev/null
timeout 60 cf8 login -u me -p x >/dev/null
timeout 200 cf8 push hello --docker-image "$registry/test/tiny:1" >/dev/null
expect "the app, served" "$(curl -sS --max-time 10 -H 'Host: hello.localhost' "http://127.0.0.1:$listen/")" "^uid 0"
expect "both apps listed" "$(listed)" "\['cf', 'ops'\]"

echo "4. a restart of its VM: the state is lost, and the app with it (the bug of todo 200)"
expect "restarted" "$(put_cf_api "{$common,\"SLUICE_WICKET_CF_API_DATA_DIR\":\"/tmp/cf\",\"RESTART\":\"1\"}")" "restarted cf.localhost"
sleep 6
expect "the app gone" "$(listed)" "^\['ops'\]$"
expect "why" "$(cat wicket.log)" "removed from the wicket: no such app"
# no check by host here: --listen serves a sole route whatever the host, so hello.localhost
# answers from the cf-api now
expect "the CLI's token" "$(timeout 60 cf8 apps 2>&1 || true)" "bad signature"
echo "as measured"
