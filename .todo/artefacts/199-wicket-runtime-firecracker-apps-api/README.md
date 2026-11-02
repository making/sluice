# The console pages in jsdom (todo 198, for 199)

No browser where todo 198 was done: these check the wickets' console pages in jsdom.
`DUMP=1 harness.js` and `fc-harness.js`'s checks concatenate `kit.js` and a page's `console.js`
as one classic script (imports and exports dropped) and stub `fetch` with the listing given.

```text
npm install                       # jsdom 24.1.3, here or in a scratch copy
K=../../../sluice-wicket-console/src/assets/kit.js
W=../../../sluice-wicket-runtime-wasi/src/console
F=../../../sluice-wicket-runtime-firecracker/src/console
DUMP=1 node harness.js $W/index.html wasm-listing.json $K $W/console.js > after.html
node fc-harness.js $F/index.html fc-listing.json $K $F/console.js   # "all checks passed"
python3 undef.py $F/console.js $K # calls and imports of names not there (rough)
./compare.sh 8a15a17              # REPO=<repository> in a scratch copy
node --experimental-vm-modules harness.js $W/index.html wasm-listing.json $K $W/console.js  # a transcript
TRANSCRIPT=1 node --experimental-vm-modules fc-harness.js $F/index.html fc-listing.json $K $F/console.js
```

- `harness.js`: `DUMP=1` renders a listing and prints the body; a body-only comparison misses
  the edit, upload and error paths (a split that commented out `ApiError` passed it). Without
  it, the wasm page's transcript.
- `transcript.js`: the operations of both pages, each section on a fresh page: rendering, the
  link, the intake and refused files, the deploy form, replace, the settings editor, rename,
  remove (each with every kind of server error), the upload's progress, drag and drop. It prints
  each request and prompt and the DOM after each step. Clock and timers are virtual, the
  scripts real modules (a missing import fails: `! page error`). `COVERAGE=1`: the lines no step ran,
  on stderr.
- `compare.sh <rev>`: both pages' transcripts at `<rev>` and in the working tree, diffed
  (`8a15a17` is the last before the shared flow). The wasm page and the Firecracker page without
  apps must print no difference; with apps, only the api rows. At `8a15a17` the wasm script does
  not import `joined`, so opening the deploy form throws (`FIX_IMPORT=0` shows it): the
  concatenating harnesses and `undef.py` hid that; the baseline gets the import.
- `browser.js`: the operations once in a real Chromium, with the wicket's CSP and a fake API
  (`npm install --no-save playwright-core`; `CHROME=<binary>` or the Playwright cache).
- `fc-harness.js`: the Firecracker page: rendering (the budget, the VMs lines, a
  wicket's string kept as text), the deploy form for a file carrying the ext4 magic, the
  mode switching the fields, the PUT and PATCH URLs, the 5 s refresh, a file refused.
  `XMLHttpRequest` stubbed, `Blob.arrayBuffer` polyfilled (jsdom's Blob lacks it).
- The listings are fakes in the shape of each console's `GET /api/routes`.

`real-image.sh`: `PUT /api/images` on a real image (a Paketo run image with a binary on top) from a
local registry, served in a VM: the image's user, a setuid file, the env, the same digest again,
e2fsck. Needs docker and KVM, so it is not with the jsdom checks above.
