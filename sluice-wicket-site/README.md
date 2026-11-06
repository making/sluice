# sluice-wicket-site

The static site's wasi:http component: serves the files of the directory its route preopens read-only at `/`
(`files=`, see [the wicket's README](../sluice-wicket-runtime-wasi/README.md#local-mode)), nothing else. It imports
`wasi:filesystem` and no other capability; without files it does not load.
`sluice-wicket-cf-api` embeds it and stages a directory of no component with an `index.html`
with it (`cf push site -p public/`). It builds on the nightly `rust-toolchain.toml` pins
(wasm32-wasip3), the WIT being the wicket's (`../sluice-wicket-runtime-wasi/wit`).

```text
cargo build --release
sluice-wicket-runtime-wasi --listen 127.0.0.1:8080 \
    --wasm 'site.localhost=target/wasm32-wasip3/release/sluice_wicket_site.wasm?files=public'
curl http://site.localhost:8080/
```

| request | answer |
|---|---|
| `GET` / `HEAD` of a file | the file, streamed from the host; `Content-Type` by its extension, `Content-Length`, `ETag` (weak: its size and time), `Last-Modified`, `Cache-Control: no-cache`, `X-Content-Type-Options: nosniff` |
| `If-None-Match` naming the file's tag | 304 |
| a directory, `/docs/` | its `index.html`; `/docs` redirects (301) to `/docs/` (its query kept) |
| what is not there, a dot file or directory (but `/.well-known/`) | 404: the site's `404.html`, else text |
| a `.` or `..` segment, a bad escape, a NUL, a backslash | 400 |
| another method | 405 (`Allow: GET, HEAD`) |

No directory is listed and no symbolic link followed; the files are the host's, read-only:
the guest writes nothing.
