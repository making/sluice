#!/bin/sh
# ./compare.sh <rev>: the transcripts of the pages at <rev> (kit.js and console.js) and of the
# working tree, diffed. REPO=<repository> when run from a scratch copy. FIX_IMPORT=0: <rev>'s wasm
# script as it is (see the README: before the shared flow it misses an import, so every deploy
# form fails there). The diffs of the wasm page and of the Firecracker page without apps must
# be empty; with apps, only the api rows differ.
set -u
here=$(cd "$(dirname "$0")" && pwd)
repo=${REPO:-$here/../../..}
rev=$1
out=$(mktemp -d)
mkdir -p "$out/old"
show() { git -C "$repo" show "$rev:$1"; }
show sluice-wicket-console/src/assets/kit.js > "$out/old/kit.js"
show sluice-wicket-runtime-firecracker/src/console/console.js > "$out/old/fc.js"
show sluice-wicket-runtime-wasi/src/console/console.js > "$out/old/wasm.js"
if [ "${FIX_IMPORT:-1}" = 1 ] && ! grep -q '[ ,]joined,' "$out/old/wasm.js" && ! grep -q consolePage "$out/old/wasm.js"; then
  sed -i 's/humanBytes, normalizeHost,/humanBytes, joined, normalizeHost,/' "$out/old/wasm.js"
fi
K=$repo/sluice-wicket-console/src/assets/kit.js
W=$repo/sluice-wicket-runtime-wasi/src/console
F=$repo/sluice-wicket-runtime-firecracker/src/console
N="node --experimental-vm-modules --no-warnings"
cd "$here" || exit 2
$N harness.js "$W/index.html" wasm-listing.json "$out/old/kit.js" "$out/old/wasm.js" > "$out/wasm-old.txt"
$N harness.js "$W/index.html" wasm-listing.json "$K" "$W/console.js" > "$out/wasm-new.txt"
export TRANSCRIPT=1
$N fc-harness.js "$F/index.html" fc-listing.json "$out/old/kit.js" "$out/old/fc.js" > "$out/fc-old.txt"
$N fc-harness.js "$F/index.html" fc-listing.json "$K" "$F/console.js" > "$out/fc-new.txt"
NO_API=1 $N fc-harness.js "$F/index.html" fc-listing.json "$out/old/kit.js" "$out/old/fc.js" > "$out/fc-noapi-old.txt"
NO_API=1 $N fc-harness.js "$F/index.html" fc-listing.json "$K" "$F/console.js" > "$out/fc-noapi-new.txt"
unset TRANSCRIPT
echo "transcripts in $out"
for page in wasm fc-noapi; do
  echo "$page: $(wc -l < "$out/$page-new.txt") lines, $(grep -c '^=== ' "$out/$page-new.txt") steps, $(grep -c '^! ' "$out/$page-new.txt") page errors, $(diff "$out/$page-old.txt" "$out/$page-new.txt" | wc -l) diff lines"
done
echo "fc (with an app): $(wc -l < "$out/fc-new.txt") lines, $(grep -c '^! ' "$out/fc-new.txt") page errors; what differs, by count:"
diff "$out/fc-old.txt" "$out/fc-new.txt" | grep '^[<>]' | sed 's/^\([<>]\) */\1 /' | sort | uniq -c | sort -rn
