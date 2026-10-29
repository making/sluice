# 173 spike: the Cloud Foundry API the cf CLI needs

A stub of the API, driven by the OSS cf CLI to see which requests each command makes and
which answers it accepts. No wicket behind it: start and stop flip states only.

- `stub.py`: Cloud Controller v3, UAA (`/uaa`) and log-cache (`/api/v1`) in memory
  (Python 3 + PyYAML). Staging picks the component from the uploaded zip (the process
  command names it, else the only `.wasm`). An app whose name starts with `crash` reports
  a CRASHED instance with a wicket-style reason. Every request goes to stderr
- `run.sh <cf args>`: runs the CLI (`$CF`, default `cf8`) with `CF_HOME=.cfhome`, then
  prints the requests the command made
- `many.sh "<cf args>" ...`: `run.sh` per command line
- `requests.md`: the requests of every command below (cf8 8.19.0, 2026-10-30)
- `multi/manifest.yml`: two routes, a service with binding parameters, an annotation, and
  `command:` choosing one of two components

```text
# the OSS CLI: the Tanzu distribution (cf 10.x) refuses any other platform
gh release download v8.19.0 -R cloudfoundry/cli -p 'cf8-cli_8.19.0_macosarm.tgz'
tar xzf cf8-cli_8.19.0_macosarm.tgz && export CF=$PWD/cf8

mkdir -p app && cp ../../../sluice-wicket-wasi/examples/target/wasm32-wasip3/release/hello.wasm app/
python3 stub.py 18080 2> stub.log &
./many.sh "api http://localhost:18080" "login -u alice -p x" "push hello -p app" "app hello"
```

| check | result |
|---|---|
| `cf api`, `login` (password), `auth --client-credentials`, `target` | ok; a path in `links.login` / `links.uaa` (`/uaa`) is kept |
| a path in `links.log_cache` | dropped: the CLI reads `/api/v1/read/<app guid>` at the root of the host |
| `cf push -p <dir>` | apply_manifest, resource_matches, package + upload (zip), build (polled), PATCH current_droplet, restart, process stats (polled) |
| `cf push -p hello.wasm` | fails in the CLI: `zip: not a valid zip file` |
| `cf push` with a manifest file | `manifest_diff` first, then as above; `command:` reaches the process before staging |
| `--strategy rolling` (push, restart) | `POST /v3/deployments`, polled until `FINALIZED` / `DEPLOYED` |
| `--docker-image` | apply_manifest carries `docker.image`; the app's lifecycle must turn `docker`; a docker package, a build, a droplet with `image` |
| `cf start` | needs `GET /v3/packages/:guid/droplets?states=STAGED` |
| `cf scale -i` / `-m` | process scale; for `-m` the CLI also stops and starts the app |
| map-route, unmap-route, routes, domains | ok |
| marketplace, create-service, bind-service `-c`, services, service, delete-service | ok; `service` needs `relationships/shared_spaces` |
| set-env, env | ok |
| `logs --recent`, `logs` | log-cache `read`; streaming polls it about once a second |
| a CRASHED instance with `details` | `Error starting instances: '<details>'`, then `Start unsuccessful` |
| Tanzu `cf` 10.4.2 | reads `/v3/info`, then `This distribution of the cf CLI is only compatible with Tanzu Platform` |
