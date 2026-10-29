Difficulty: Medium

# sluice-wicket-cf-api: `cf push --docker-image` for components in an OCI registry

A component built by CI and pushed to a registry (`wkg oci push`, `oras push`) is deployed by
its reference:

```text
cf push hello --docker-image ghcr.io/me/hello:1.2.0
```

- the spike of 173 drove the CLI's path: apply_manifest carries `docker.image` and the app's
  lifecycle must turn `docker`; then `POST /v3/packages {type: docker, data: {image}}`, a
  build, and a droplet whose `image` `cf app` shows
- staging pulls the image's manifest and its wasm layer (the wasm OCI artifact: config
  `application/vnd.wasm.config.v0+json`, layer `application/wasm`). The tag resolves to a
  digest, so a restart never pulls a moved tag; `cf restage` pulls again. Another kind of
  image fails the build, saying what it found
- credentials: `CF_DOCKER_USERNAME` / `CF_DOCKER_PASSWORD` arrive in the package
  (`data.username`, `data.password`), are kept with the app (never shown) and used for the
  pull only; anonymous without them
- the wicket is not involved: the component reaches it as a bits droplet's does
- lifts 173's refusal of docker images

## Acceptance

- a test registry (an OCI distribution stub in the test, or `zot` as a child process)
  serving a pushed component: a push by tag serves it; the tag moved and a restart keep the
  old digest; a restage takes the new one; a private image needs the credentials
