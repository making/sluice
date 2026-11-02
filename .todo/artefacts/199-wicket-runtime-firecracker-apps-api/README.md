# The console pages in jsdom (todo 198, for 199)

No browser where todo 198 was done: these check the wickets' console pages in jsdom.
Each harness concatenates `kit.js` and a page's `console.js` as one classic script
(imports and exports dropped) and stubs `fetch` with the listing given.

```text
npm install                       # jsdom 24.1.3, here or in a scratch copy
K=../../../sluice-wicket-console/src/assets/kit.js
W=../../../sluice-wicket-runtime-wasi/src/console
F=../../../sluice-wicket-runtime-firecracker/src/console
DUMP=1 node harness.js $W/index.html wasm-listing.json $K $W/console.js > after.html
node fc-harness.js $F/index.html fc-listing.json $K $F/console.js   # "all checks passed"
python3 undef.py $F/console.js    # calls of names neither defined nor imported (rough)
```

- `harness.js`: renders a listing and prints the body (`DUMP=1`) and the requests made.
  Comparing the wasm page's body before and after a change (the old `console.js` from
  `git show <rev>:...`) caught no regression of todo 198's split, but it does not run the
  edit or error paths: a split that commented out `ApiError` and `errorMessage` in
  `kit.js` passed it. Run the edit, upload and error paths too.
- `fc-harness.js`: the Firecracker page: rendering (the budget, the VMs lines, a
  wicket's string kept as text), the deploy form for a file carrying the ext4 magic, the
  mode switching the fields, the PUT and PATCH URLs, the 5 s refresh, a file refused.
  `XMLHttpRequest` stubbed, `Blob.arrayBuffer` polyfilled (jsdom's Blob lacks it).
- The listings are fakes in the shape of each console's `GET /api/routes`.
