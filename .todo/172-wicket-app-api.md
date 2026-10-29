Difficulty: Medium

# wicket-wasi: an app API for a controller (several hosts, components by digest, keyvalue maps)

`sluice-wicket-cf-api` (todo 173) keeps the desired state of its apps and has a wicket serve
it, as the Cloud Controller drives Diego. The console API (`/api/routes/<host>`) does not
fit that client:

- one edit per host: an app with N routes takes N uploads and N reloads, and a refusal half
  way leaves it half moved
- a console route takes the default keyvalue map (the host as the namespace); a CF app's
  data belongs to a service instance, not to a host (todo 174)
- the page can edit or remove what the controller deployed, and the controller cannot tell
  its routes from the page's

Add to the console's listener, behind the same `--console-token`:

```text
PUT    /api/components/<sha256>   the body, a component (128MiB at most), kept under its digest; 400 when it has another
GET    /api/apps                  the apps: name, owner, hosts, component (sha256, file name), sandbox, deployed
PUT    /api/apps/<name>           JSON: owner, hosts, component (sha256, file name), sandbox (the --wasm options, a keyvalue map)
DELETE /api/apps/<name>
```

- an app is hosts served by one component in one sandbox. A PUT is one reload, all its
  hosts or none, kept only when the routes load (as a console deploy); the same content
  again changes nothing
- an app's routes never share a handler with another app's (its target names the app): its
  pool, its `instances` cap and its guest output (todo 176) stay its own
- answers as the console's edits; 409 also for a missing component, naming the digests; a
  422 names the host whose route failed, so the controller blames the right app
- keyvalue maps as in the config file (`identifier -> store/namespace`); the namespace is the
  controller's (`cf/<instance guid>`, which no host is)
- `owner`: a PUT or DELETE of another owner's app is refused (409); a controller reconciles
  only its own
- an uploaded component is kept while an app or a route names it, and for some minutes after
  the upload, so the PUT naming it does not race the garbage collection
- `GET /api/routes` lists an app's routes with `origin: "api"`, the app and its owner, and
  gains `keyvalue_stores` (name, type, default) for the controller's service plans
- the page lists them read-only (as the config file's); the per-host endpoints refuse an
  app's host (409)
- kept in `--console-dir` with the console's routes and served again after a restart. One
  model may serve both: a console route is an app of one host owned by the page
- errors name a component as todo 170 has the console's do

## As built

- `DELETE /api/apps/<name>?owner=<owner>`: the owner in the query (400 without one)
- the sandbox in JSON has the listing's keys (`wasi` a list, `budget_ms`, `memory_mib`,
  `pool`, `instances`, `keyvalue` a map), not the `--wasm` option names; with `keyvalue`
  granted the map is required, each place `store/namespace` in full
- `GET /api/apps` takes `?after=<revision>` as `/api/routes`; an app as listed is a body its
  PUT takes (less `name` and `deployed`)
- not one model: the page's routes and the apps are two lists in `routes.yaml` (a route takes
  the default map, an app names its own); an upload waits in `uploads/` for 10 minutes

## Acceptance

- `the_consoles_edits_are_kept_only_when_they_load` (`routes.rs`), extended or a sibling: a
  two-host app deploys, changes its component and drops a host, each in one generation; a
  refused PUT changes nothing and names the host; an explicit keyvalue map reaches its
  namespace; two apps on one component get two handlers
- the console test in `tests/local.rs`: upload by digest, PUT an app of two hosts, both answer
  on `--listen`, the page's endpoints refuse its hosts, DELETE
- README Console section: the API
