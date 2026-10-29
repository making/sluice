Difficulty: Low

# wicket-wasi console: name a console component without the console directory's absolute path

A deploy the routes refuse is answered 422 with the reload's error, and the page shows it
verbatim. It names the component by its absolute file URL:

```text
wasm route 'counter.localhost': component file:///data/deployed/components/0954…e8/counter.wasm imports wasi:keyvalue/store@0.2.0-draft2: the route needs wasi=keyvalue
```

The path is the wicket host's layout, nothing the console user acts on, and the file is
already gone: the refused component is collected right after. `components/<sha256>/<name>`
(relative to `--console-dir`) says the same.

- the name comes from `link()` in `wasm_host.rs` (the target URL minus `wasm:`, also in
  `compile {target}: ...`); `plan()` in `routes.rs` prefixes `wasm route '<host>': `. A
  console route's name can travel with its `Spec` (`Origin::Console`), so the 422, the
  reload-failure banner and the log say the same (the log's startup line names the
  directory)
- the target URL itself stays as it is: it is the route's identity (lingering targets and
  their acks, the compile cache key, the advertised route); only the message changes
- the config file's and `--wasm` routes keep their absolute paths: there the operator needs
  the file the error is about
- the listing's `target` field (`GET /api/routes`) carries the same absolute URL; the page
  does not show it. Decide whether the API keeps it

## Acceptance

- a refused console deploy names its component without the absolute path of
  `--console-dir`; `the_consoles_edits_are_kept_only_when_they_load` (`routes.rs`) pins
  the message
- the errors of the config file's routes are unchanged
