Difficulty: Medium

# sluice-wicket-cf-api: consider abstracting staging's ways of making a droplet (buildpack-like)

Start from consideration: decide whether to abstract and in what shape, then implement what is decided (or record why
it stays as it is).

## Now

`staging.rs` hard-codes each kind of droplet as a branch:

- `run`: by package kind (zip / docker) and the wicket's `Runtime` (Wasm / Firecracker): a zip's `unpack`, an image's
  `pull` (wasm) or the wicket's `make_image` (rootfs). The `STG` log line describing the lifecycle is per branch.
- `unpack`: a static site first (`site && command.is_none() && is_site`, then `stage_site` / `pack_site`), else `stage`
  (the command's entry, else the only `.wasm` / `.ext4`).
- The droplet's stack is kept since the wickets of both runtimes (`Droplet::stack`, `Running::stack`: `wasip3` /
  `firecracker`, told by the file's name for a state written before), detected in `run` (`Takes`: the stacks the
  wickets run, the app's pin). What is still told back by fields, outside staging:
  - `views.rs` `droplet`: `site` / `wasm` as the buildpack name, by `droplet.files`

## To weigh

- A trait (detect + stage, like a buildpack's detect/build) vs. an enum of droplet kinds stored on `Droplet` beside
  its stack (so views stops inferring a site from `files`) vs. leaving it. Three kinds exist (component,
  rootfs, site); weigh what a fourth would cost each way.
- Ordering and selection: CF's buildpack detection order, `cf push -b <name>` choosing one (now `app.buildpacks` is
  kept and shown back, never read by staging), the `buildpack` names `views.rs` reports, and whether
  `GET /v3/buildpacks` (now always empty) should list them.
- `Store`'s droplet record (`file`, `files`, `image`): what a kind field would replace, and migrating existing
  `state` files.
- Keep the blocking work on `spawn_blocking` (one vCPU VM) and the per-kind limits (`MAX_DROPLET_BYTES`,
  `MAX_SITE_BYTES`, `MAX_SITE_FILES`).

Tests: `staging::tests::picks_the_component_or_packs_a_site`, `views` / `controller` tests and `tests/cli.rs` cover
the kinds; extend them rather than adding new ones.
