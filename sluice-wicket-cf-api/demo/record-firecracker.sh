#!/usr/bin/env bash
# Records firecracker.tape into sluice-cf-firecracker-demo.mp4 and .gif next to this script (see
# README.md), on an Apple silicon Mac: the wicket, the flume and the cf-api built from the work tree
# and Dex in the wicket's Lima VM (scripts/lima.sh), the cf CLI and the recording on the Mac. Needs
# vhs (with ttyd and ffmpeg), jq, curl, nc, limactl and the OSS cf CLI v8: SLUICE_CF_CLI, else cf8 or
# cf on the PATH.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$here/../.." && pwd)
work=$repo/sluice-wicket-cf-api/target/demo-firecracker
lima=$repo/sluice-wicket-runtime-firecracker/scripts/lima.sh
vm=${SLUICE_LIMA_VM:-sluice-firecracker}
token=wicket-secret
# the registry the wicket pulls the cf-api's image from: on its loopback, so over plain HTTP
registry=127.0.0.1:15000

for tool in vhs ffmpeg jq curl nc limactl; do
	if ! command -v "$tool" > /dev/null; then
		echo "$tool is missing" >&2
		exit 1
	fi
done
cli=${SLUICE_CF_CLI:-$(command -v cf8 || command -v cf || true)}
version=$([ -n "$cli" ] && "$cli" version 2> /dev/null || true)
# the CLI names itself as invoked: "cf8 version 8.19.0+..."
case $version in
*" version 8."*) ;;
*)
	echo "needs the OSS cf CLI v8 (SLUICE_CF_CLI, else cf8 or cf on the PATH), found: ${version:-none}" >&2
	exit 1
	;;
esac
cli=$(cd "$(dirname "$cli")" && pwd)/$(basename "$cli")

remove='docker rm -f wicket flume dex registry; docker volume rm wicket; docker network rm sluice'
"$lima" bash -c "{ $remove; } > /dev/null 2>&1; true"
# a stack just removed: its forwards take a moment to go
for port in 5556 8080 9090; do
	for _ in $(seq 1 20); do
		nc -z 127.0.0.1 "$port" 2> /dev/null || continue 2
		sleep 0.5
	done
	echo "port $port is in use on the Mac" >&2
	exit 1
done
trap '"$lima" bash -c "{ $remove; } > /dev/null 2>&1; true"' EXIT

# the registry's container holds the network and the published ports, and the wicket joins it
"$lima" bash -c "set -e
cd ~/sluice
commit=$(git -C "$repo" rev-parse HEAD)
docker build -q -f sluice-wicket-runtime-firecracker/Dockerfile --build-arg GIT_COMMIT=\$commit -t sluice-wicket-runtime-firecracker:demo .
docker build -q -f sluice-flume/Dockerfile --build-arg GIT_COMMIT=\$commit -t sluice-flume:demo .
docker build -q -f sluice-wicket-cf-api/Dockerfile --build-arg GIT_COMMIT=\$commit -t $registry/demo/cf-api:1 .
docker network create sluice
docker run -d --name registry --network sluice -e REGISTRY_HTTP_ADDR=0.0.0.0:15000 \
    -p $registry:15000 -p 127.0.0.1:8080:8080 -p 127.0.0.1:9090:9090 registry:3
docker run -d --name flume --network sluice -e SLUICE_FLUME_TOKEN=flume-secret sluice-flume:demo
docker run -d --name dex --network sluice --network-alias dex.localhost -p 127.0.0.1:5556:5556 \
    -v ~/sluice/sluice-wicket-cf-api/demo/dex-firecracker.yaml:/etc/dex/dex.yaml:ro \
    ghcr.io/dexidp/dex:v2.46.1 dex serve /etc/dex/dex.yaml
until docker push -q $registry/demo/cf-api:1; do sleep 1; done
docker run -d --name wicket --network container:registry --device /dev/kvm --group-add \$(stat -c %g /dev/kvm) \
    --tmpfs /run/sluice --stop-timeout 40 -v wicket:/data \
    -e SLUICE_WICKET_RUNTIME_FIRECRACKER_CONSOLE_TOKEN=$token \
    -e SLUICE_WICKET_RUNTIME_FIRECRACKER_FLUME_TOKEN=flume-secret \
    sluice-wicket-runtime-firecracker:demo \
    --listen 0.0.0.0:8080 --console 0.0.0.0:9090 --flume grpc://flume:8002
rm -rf /tmp/sluice-demo && mkdir /tmp/sluice-demo
docker cp wicket:/usr/local/share/sluice-wicket-runtime-firecracker/guest.ext4 /tmp/sluice-demo/hello.ext4"

rm -rf "$work"
mkdir -p "$work/bin" "$work/out" "$work/cfhome" "$work/apps/hello"
limactl copy "$vm:/tmp/sluice-demo/hello.ext4" "$work/apps/hello/"

# the console and Dex, once their forwards are up on the Mac
auth="Authorization: Bearer $token"
for url in http://127.0.0.1:9090/api/apps http://dex.localhost:5556/dex/.well-known/openid-configuration; do
	for _ in $(seq 1 120); do
		curl -fsS -o /dev/null -H "$auth" "$url" 2> /dev/null && break
		sleep 0.5
	done
done
sha256=$(curl -fsS -X PUT -H "$auth" http://127.0.0.1:9090/api/images \
	-d "{\"image\": \"$registry/demo/cf-api:1\"}" | jq -r .sha256)
curl -fsS -X PUT -H "$auth" http://127.0.0.1:9090/api/apps/cf-api -d @- > /dev/null << JSON
{
  "owner": "ops",
  "hosts": ["cf.localhost"],
  "component": {"sha256": "$sha256", "name": "sluice-wicket-cf-api.ext4"},
  "sandbox": {
    "memory-mib": 256,
    "disk": "/data",
    "disk-mib": 4096,
    "egress": ["localhost:9090", "flume:8002", "dex.localhost:5556"],
    "env": {
      "SLUICE_WICKET_CF_API_WICKET": "http://localhost:9090",
      "SLUICE_WICKET_CF_API_WICKET_TOKEN": "$token",
      "SLUICE_WICKET_CF_API_FLUME": "grpc://flume:8002",
      "SLUICE_WICKET_CF_API_FLUME_TOKEN": "flume-secret",
      "SLUICE_WICKET_CF_API_OIDC_ISSUER": "http://dex.localhost:5556/dex",
      "SLUICE_WICKET_CF_API_OIDC_CLIENT_ID": "cf",
      "SLUICE_WICKET_CF_API_OIDC_CLIENT_SECRET": "demo-secret",
      "SLUICE_WICKET_CF_API_OIDC_CLAIM": "email=*@example.com",
      "SLUICE_WICKET_CF_API_ADMIN": "email=demo@example.com"
    }
  }
}
JSON

# the tape types cf: this one, whichever cf the PATH holds
ln -sf "$cli" "$work/bin/cf"
# the browser's part of cf login --sso, done here: the tape types the code (good for 5 minutes)
passcode=$("$here/passcode.sh" http://cf.localhost:8080)
sed "s/{{PASSCODE}}/$passcode/" "$here/firecracker.tape" > "$work/demo.tape"
cd "$work"
CF_HOME=$work/cfhome CF_COLOR=false PATH=$work/bin:$PATH vhs demo.tape

"$lima" bash -c "docker logs wicket > /tmp/sluice-demo/wicket.log 2>&1; true"
limactl copy "$vm:/tmp/sluice-demo/wicket.log" "$work/wicket.log"
cp out/sluice-cf-firecracker-demo.mp4 "$here/"
# the README shows the GIF: GitHub plays no video committed to the repository
ffmpeg -hide_banner -loglevel error -y -i out/sluice-cf-firecracker-demo.mp4 -vf \
	"fps=10,scale=1280:-1:flags=lanczos,split[a][b];[a]palettegen=max_colors=64:stats_mode=full[p];[b][p]paletteuse=dither=none:diff_mode=rectangle" \
	"$here/sluice-cf-firecracker-demo.gif"
ls -l "$here/sluice-cf-firecracker-demo.mp4" "$here/sluice-cf-firecracker-demo.gif"
