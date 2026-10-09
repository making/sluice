Difficulty: Medium

# Component locators: oci / s3 resolvers and integrity

`wasmlet/src/wasm_host.rs` `Locator` has `File` and `Http` variants; the `wasm:<url>`
address form was designed for more sources:

- `oci://registry/repo:tag` (or `wasm:oci://...`) — pull a component artifact from an
  OCI registry; consider `oci-wasm` / wasm pack manifest type, or shelling to
  `wasm-tools`? decide and document
- `s3://bucket/key` — presigned-GET style; credentials via env/flags
- integrity: sha256 pinning (`--wasm host=...#sha256:...`) verified after fetch,
  surfaced in the advertise log; matters once components come from the network
- cache: on-disk cache keyed by locator+hash so restarts do not re-fetch

The server side needs no changes (`Router.addressOf` passes `wasm:` through
verbatim); only the client resolver grows.

Acceptance: `--wasm demo.local=oci://...` and `s3://...` routes serve; hash mismatch
fails the startup with a clear error.
