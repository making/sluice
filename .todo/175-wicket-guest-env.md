Difficulty: Medium

# Guest environment variables: the wicket's sandbox `env`; `cf set-env` and VCAP_* through the cf-api

The guest's `wasi:cli/environment` returns nothing today; a CF app reads its settings and its
bindings from the environment.

- wicket: a route's sandbox takes `env` (name -> value) from the config file
  (`sandbox.env`) and the app API (172); not from `--wasm` or the console page
- env may hold secrets: never in the advertised target, the logs or the listing (names
  only). The target carries a digest of it, so a changed env is a new target (drained as a
  changed sandbox) and routes with other envs never share a handler. A file holding it is
  written 0600
- cf-api: lifts 173's refusal of `env:`, `set-env` and `unset-env`; from the next start on,
  as on CF, the guest gets the user's variables, `VCAP_APPLICATION` and `VCAP_SERVICES`
  (JSON)

## Acceptance

- wicket: a guest answering a variable (an example, or `hello` extended) answers it; a config
  reload with another value retargets the route; the advertised target never holds the value
- cf-api: `cf set-env hello K v` and a restart: the guest answers `v`, and sees its
  `VCAP_APPLICATION.application_name`
