#!/usr/bin/env bash
# Rebuilds the plugins src/test/resources/wasm holds: run it after changing them.
# The test plugin on WASI, the SDK's examples without: an allocator export of each name,
# and WASI's imports.
# The plugin on Wasm GC is written by hand, the plugins in Lisp built by rontolisp's release.
# Needs: rustup target add wasm32-unknown-unknown wasm32-wasip1, wasm-tools, curl and a JDK
set -euo pipefail
cd "$(dirname "$0")"
cargo build --release --target wasm32-wasip1 -p test-plugin
cargo build --release --target wasm32-unknown-unknown -p http_body -p http_headers
cp target/wasm32-wasip1/release/test_plugin.wasm ../resources/wasm/
cp target/wasm32-unknown-unknown/release/http_body.wasm target/wasm32-unknown-unknown/release/http_headers.wasm ../resources/wasm/
wasm-tools parse gc-plugin/gc_plugin.wat -o ../resources/wasm/gc_plugin.wasm
mkdir -p target
curl -fsSL -o target/rontolisp.jar https://github.com/making/rontolisp/releases/download/0.1.0-SNAPSHOT/rontolisp-0.1.0-SNAPSHOT-exec.jar
curl -fsSL -o rontolisp/proxy-wasm.lisp https://raw.githubusercontent.com/making/rontolisp/0.1.0-SNAPSHOT/examples/proxy-wasm/proxy-wasm.lisp
for plugin in lisp_plugin json_plugin; do
  java -jar target/rontolisp.jar rontolisp/$plugin.lisp -o ../resources/wasm/$plugin.wasm --no-wasi --optimize=size
done
