#!/usr/bin/env bash
# Rebuilds the plugins src/test/resources/wasm holds: run it after changing them.
# The test plugin on WASI, the SDK's examples without: an allocator export of each name,
# and WASI's imports.
# Needs: rustup target add wasm32-unknown-unknown wasm32-wasip1
set -euo pipefail
cd "$(dirname "$0")"
cargo build --release --target wasm32-wasip1 -p test-plugin
cargo build --release --target wasm32-unknown-unknown -p http_body -p http_headers
cp target/wasm32-wasip1/release/test_plugin.wasm ../resources/wasm/
cp target/wasm32-unknown-unknown/release/http_body.wasm target/wasm32-unknown-unknown/release/http_headers.wasm ../resources/wasm/
