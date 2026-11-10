Difficulty: High

# sluice-wicket-cf-api: wickets of both runtimes, an app placed by its droplet's stack

One cf-api over wasi and Firecracker wickets at once, what is pushed deciding where it runs: as
Diego places an LRP on the cells of its stack (Linux and Windows cells in one foundation).

## Now

`--wicket`s of one runtime only: the first listed decides (`Controller::runtimes`), one listing
another is `odd` (logged, nothing placed or put on it). Introduced with the wickets' placement
(7072695) as a simplification; what runs is per droplet already:

- `Controller::desired` picks each share's sandbox (`Vm` / `Wasm`) by `running.file.ends_with(ROOTFS)`
- a start checks annotations and health checks against the droplet's kind (`api.rs` `snapshot`)
- each listing is typed by its wicket's runtime (`Listing::typed`), each budget per wicket
- the app API (put, `held-ms`, confirmed, evacuated), the sluice server and the flume are the same
  for both

What assumes one runtime:

- `Controller::runtime()`: staging (`staging::run`: a site allowed or not, an image pulled by the
  cf-api or made by a wicket), the edits' checks (`api.rs`: create / patch app, metadata, health
  checks, scale; `manifest::apply`), a new app's stack, `/v3/info`, `/v3/stacks` (one stack)
- `Controller::odd` (its runtime part), `with_offers` / `offered` (the marketplace,
  `wickets.stores`), `make_image` (any wicket not odd)
- `placement.rs`: `Wickets::odd`, the one filter of `placed`, `pass`, `evacuate`, `respread`
  (`takes_more`)
- `purge_on` / `unmapped` go through every wicket: a Firecracker wicket answers 404 (dropped, logged)

## Design

- **The stack is the droplet's**: `wasip3` (a component, a static site), `firecracker` (a rootfs),
  decided at staging from what is pushed, as buildpack detection:
  - a zip: as now (`.wasm`, `.ext4`, else `index.html` a site), the site no longer gated on the
    runtime
  - an image: the cf-api reads its manifest first (`registry.rs`, the package's credentials): an
    `application/wasm` layer, or an index's `wasm` entry, pulled as now; anything else made into a
    rootfs by a Firecracker wicket (`make_image` on those alone). An index of a wasm entry and
    others goes wasm; its other platform's manifest by digest picks a rootfs
  - a stack none of the wickets listed runs fails staging, naming the stacks they run
- **`cf push -s` / a manifest's `stack:` pins it** (buildpack lifecycle): staging fails a package of
  another stack. A stack the wickets do not run is refused (422) naming theirs, as CC refuses one it
  does not have; a state's stack of none (written before: `cflinuxfs4` from a CF manifest) is no
  pin. Keep the pin apart from "not given" (detection). The stack alone cannot route: the CLI
  refuses `-s` with `--docker-image`
- **Placement by stack**: each wicket's runtime noted from its listing and kept in the state beside
  `wicket_ids` (a wicket down since the start keeps its runtime). An index is on a wicket of the
  stack of the snapshot its share serves (`App::running_on`); a new index, a pass, an evacuation and
  a respread go to wickets of the app's stack (`running`'s). An index on a wicket of another stack
  is placed again: a start of a droplet of another stack moves the app (a stop and a start). A
  wicket never listed takes no new index and keeps what it has. `odd` is a second name alone
- **Services**: the marketplace and `wickets.stores` are the common offer of the wasm wickets.
  keyvalue / wasi instances bound to an app of a rootfs: its start refused (422) naming them, `cf
  bind-service` refused once the current droplet is one. User-provided and broker instances on
  both. Purges and `unmapped` through the wickets listing the store only
- **Checks** (annotations, readiness / liveness, scale): against the app's stack (its current
  droplet's, else its pin, else none: the start checks) in place of `Controller::runtime()`; the
  messages name the stack, not "this wicket"
- **Views**: `/v3/stacks` lists the wickets' stacks (the default the first wicket's), `/v3/info`
  names what they run, `lifecycle.data.stack` the pin, else the current droplet's
- **A rolling / canary deployment of a droplet of another stack**: refused (422) until 268 lifts it,
  naming the way round (`cf push` without `--strategy`)
- **Local mode**: each runtime's apps answer on their wicket's `--listen`; one entry point for both
  takes a sluice server

Overlaps 243: an explicit kind / stack on `Droplet` and `Running` (inferred from `file` for a state
written before) replaces the `ends_with(ROOTFS)` inferences this needs in more places. Settle it
here and update 243, or do 243 first.

## Docs

- cf-api README: the intro table (what `cf push` deploys depends on what is pushed), Several
  wickets' "What they share", Mapping (the stack), Local mode's two entry points; the root
  `CLAUDE.md`'s line of the cf-api. A Compose sample of both runtimes only if run (real images)
- `.kb/wicket-cf-api-findings.md`, what the CLI (8.19.0) does, seen 2026-11-10: `-s` with
  `--docker-image` refused by the CLI ("cannot be used together"); a manifest's `docker:` with
  `stack:` passes it; `-s` with a zip reaches `lifecycle.data.stack`; `cf app`'s stack is empty
  before staging (the current droplet's)

## Tests

Extend, not add: `placement.rs`'s tables (a runtime per wicket, apps of each stack, an index on a
wicket of another stack placed again, an evacuation with no wicket of its stack), the staging and
registry tests (detection, the pin refusing), and `the_cf_cli_deploys_to_a_firecracker_wicket`
(`tests/cli.rs`, KVM) with a wasi wicket beside the Firecracker one: a component and an image
pushed to one cf-api each on its wicket, `cf stacks`, a keyvalue binding refused for the image's
app, a push of another stack moving an app, a rolling one refused.
