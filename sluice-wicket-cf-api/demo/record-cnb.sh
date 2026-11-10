#!/usr/bin/env bash
# Records cnb.tape into sluice-cf-cnb-demo.mp4 and .gif next to this script (see README.md), on
# Linux with a usable /dev/kvm: a flume, a Firecracker wicket and the cf-api built from the work
# tree and run here, source pushed with the cf CLI and staged by --cnb-builder's buildpacks. Needs
# vhs (with ttyd and ffmpeg), curl, nc, firecracker, a guest kernel and the OSS cf CLI v8:
# SLUICE_CF_CLI, else cf8 or cf on the PATH.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$here/../.." && pwd)
work=$repo/sluice-wicket-cf-api/target/demo-cnb
# short: the VMs' sockets are below it
state=/tmp/sluice-demo-cnb
token=wicket-secret
builder=${SLUICE_CNB_BUILDER:-paketobuildpacks/ubuntu-resolute-builder}
kernel=${SLUICE_WICKET_RUNTIME_FIRECRACKER_KERNEL:-$HOME/.local/firecracker/vmlinux}
firecracker=${SLUICE_WICKET_RUNTIME_FIRECRACKER_FIRECRACKER:-$(command -v firecracker || true)}

for tool in vhs ffmpeg curl nc; do
	if ! command -v "$tool" > /dev/null; then
		echo "$tool is missing" >&2
		exit 1
	fi
done
if [ ! -w /dev/kvm ] || [ ! -f "$kernel" ] || [ -z "$firecracker" ]; then
	echo "needs a usable /dev/kvm, firecracker and a guest kernel (the wicket's README, Setup)" >&2
	exit 1
fi
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
for port in 8002 8080 9090 9091; do
	if nc -z 127.0.0.1 "$port" 2> /dev/null; then
		echo "port $port is in use" >&2
		exit 1
	fi
done

(cd "$repo/sluice-flume" && cargo build --release --locked)
(cd "$repo/sluice-wicket-runtime-firecracker" && cargo build --release --locked)
guest=$repo/sluice-wicket-runtime-firecracker/$("$repo/sluice-wicket-runtime-firecracker/scripts/guest.sh")
(cd "$repo/sluice-wicket-cf-api" && cargo build --release --locked)

pids=()
stop() {
	for pid in "${pids[@]}"; do
		kill "$pid" 2> /dev/null || true
	done
	wait 2> /dev/null || true
	rm -rf "$state"
}
trap stop EXIT
rm -rf "$work" "$state"
mkdir -p "$work/bin" "$work/out" "$work/cfhome" "$work/logs" "$work/apps"
cp -r "$here/cnb/hello-node" "$here/cnb/hello-java" "$work/apps/"

"$repo/sluice-flume/target/release/sluice-flume" --host 127.0.0.1 --port 8002 --token flume-secret \
	> "$work/logs/flume.log" 2>&1 &
pids+=($!)
"$repo/sluice-wicket-runtime-firecracker/target/release/sluice-wicket-runtime-firecracker" \
	--kernel "$kernel" --firecracker "$firecracker" --state-dir "$state" --guest "$guest" \
	--listen 127.0.0.1:8080 --console 127.0.0.1:9090 --console-token "$token" \
	--console-dir "$work/wicket" --flume grpc://127.0.0.1:8002 --flume-token flume-secret \
	> "$work/logs/wicket.log" 2>&1 &
pids+=($!)
# the hosts the builds of the tape download from (.kb/cnb-java-on-firecracker.md)
egress=github.com:443,release-assets.githubusercontent.com:443,repo.maven.apache.org:443
egress=$egress,repo1.maven.org:443,nodejs.org:443,registry.npmjs.org:443
"$repo/sluice-wicket-cf-api/target/release/sluice-wicket-cf-api" --listen 127.0.0.1:9091 \
	--wicket http://127.0.0.1:9090 --wicket-token "$token" --data-dir "$work/cf" \
	--flume grpc://127.0.0.1:8002 --flume-token flume-secret --any-credentials \
	--cnb-builder "$builder" --cnb-egress "$egress" > "$work/logs/cf-api.log" 2>&1 &
pids+=($!)
for _ in $(seq 1 120); do
	curl -fsS -o /dev/null http://127.0.0.1:9091/ 2> /dev/null && break
	sleep 0.5
done

# the tape types cf: this one, whichever cf the PATH holds
ln -sf "$cli" "$work/bin/cf"
export CF_HOME=$work/cfhome CF_COLOR=false PATH=$work/bin:$PATH
# the builder's rootfs, made by the first build with it (minutes): before the recording
cf api http://127.0.0.1:9091 > /dev/null
cf login -u demo -p demo > /dev/null
(cd "$work/apps" && cf push warm -p hello-node/ -m 512M > "$work/logs/warm.log" 2>&1)
cf delete warm -r -f > /dev/null
cf logout > /dev/null
rm -rf "$work/cfhome"

cp "$here/cnb.tape" "$work/demo.tape"
cd "$work"
vhs demo.tape

cp out/sluice-cf-cnb-demo.mp4 "$here/"
# the README shows the GIF: GitHub plays no video committed to the repository. Fewer frames,
# pixels and colors than the other demos': a build's output scrolls all the way
ffmpeg -hide_banner -loglevel error -y -i out/sluice-cf-cnb-demo.mp4 -vf \
	"fps=4,scale=1000:-1:flags=lanczos,split[a][b];[a]palettegen=max_colors=16:stats_mode=full[p];[b][p]paletteuse=dither=none:diff_mode=rectangle" \
	"$here/sluice-cf-cnb-demo.gif"
ls -l "$here/sluice-cf-cnb-demo.mp4" "$here/sluice-cf-cnb-demo.gif"
