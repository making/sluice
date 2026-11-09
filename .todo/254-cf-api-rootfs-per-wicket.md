Difficulty: Medium

# sluice-wicket-cf-api: an image's rootfs made by each Firecracker wicket

After 252. A Firecracker wicket makes an image's rootfs itself (`PUT /api/images`), and another wicket's guest
makes another digest (its README, Apps). The cf-api keeps one digest per droplet: `Controller::provide`, the
wicket missing it, has that wicket make it again and rewrites `Droplet.sha256` and every `Running.sha256` with
the digest it answers.

Two wickets of different guests -- every rolling upgrade of the wickets whose guest changed -- make that flip:
the other wicket's app no longer matches `same()`, is put, misses the digest, makes its own and flips it back.
The apps restart in a loop.

## To do

- A droplet of an image keeps the reference pinned to its manifest's digest and, per wicket, the digest that
  wicket made; a wicket's body names its own, `same()` compares with it. Staging makes it on one wicket (one
  with room), the others at their first put (`missing`), as now
- A zip's rootfs and a component: one digest everywhere, unchanged
- Migrate the state's digests (the one wicket's); `cf app`'s `docker image:` and the droplet views stay the
  reference

## Tests

`the_cf_cli_deploys_to_a_firecracker_wicket` (where KVM is) with a second wicket given a guest of other bytes:
both serve the image and nothing is put again once both listed it. The per-wicket digest in `desired` /
`same()` as a unit test.
