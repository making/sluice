#!/usr/bin/env bash
# Records demo.tape on a fresh demo stack into sluice-cf-demo.mp4 and sluice-cf-demo.gif next to
# this script (see README.md). Needs vhs (with ttyd and ffmpeg), jq, curl, docker (Dex) and the
# OSS cf CLI v8: SLUICE_CF_CLI, else cf8 or cf on the PATH.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$here/../.." && pwd)
work=$repo/sluice-wicket-cf-api/target/demo

for tool in vhs ffmpeg jq curl nc docker; do
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

(cd "$repo/sluice-flume" && cargo build --release --locked)
(cd "$repo/sluice-wicket-wasi" && cargo build --release --locked)
(cd "$repo/sluice-wicket-wasi/examples" && cargo build --release --locked -p hello -p counter)
(cd "$repo/sluice-wicket-cf-api" && cargo build --release --locked)

"$here/stop.sh" --reset
trap '"$here/stop.sh"' EXIT
"$here/start.sh"

# the tape types cf: this one, whichever cf the PATH holds
mkdir -p "$work/bin" "$work/out"
ln -sf "$cli" "$work/bin/cf"
# the browser's part of cf login --sso, done here: the tape types the code (good for 5 minutes)
passcode=$("$here/passcode.sh")
sed "s/{{PASSCODE}}/$passcode/" "$here/demo.tape" > "$work/demo.tape"
cd "$work"
CF_HOME=$work/cfhome PATH=$work/bin:$PATH vhs demo.tape

cp out/sluice-cf-demo.mp4 "$here/"
# the README shows the GIF: GitHub plays no video committed to the repository
ffmpeg -hide_banner -loglevel error -y -i out/sluice-cf-demo.mp4 -vf \
	"fps=10,scale=1280:-1:flags=lanczos,split[a][b];[a]palettegen=max_colors=64:stats_mode=full[p];[b][p]paletteuse=dither=none:diff_mode=rectangle" \
	"$here/sluice-cf-demo.gif"
ls -l "$here/sluice-cf-demo.mp4" "$here/sluice-cf-demo.gif"
