#!/usr/bin/env bash
# many.sh "<cf args>" ...: run.sh for each command line
cd "$(dirname "$0")"
for c in "$@"; do echo "=========== cf $c"; eval "./run.sh $c" 2>&1 | grep -v "KiB /"; done
