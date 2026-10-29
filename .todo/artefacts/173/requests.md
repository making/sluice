# Requests of the OSS cf CLI 8.19.0, per command

Recorded by `many.sh` against `stub.py` (see README.md). `:guid` stands for a GUID;
query values are kept as sent (URL-encoded brackets decoded). Bodies are cut at 120
characters.

## `cf api http://localhost:18080`

```text
200 GET /
```

## `cf login -u alice -p secret`

```text
200 GET /
200 GET /uaa/login
200 POST /uaa/oauth/token  grant_type=password&password=secret&username=alice
200 GET /v3/organizations?order_by=name
200 GET /v3/spaces?order_by=name&organization_guids=:guid
```

## `cf push hello -p app`

```text
202 POST /v3/spaces/:guid/actions/apply_manifest  applications:\n- name: hello\n  path: <local path>\n  default-route: true\n
200 GET /v3/jobs/:guid
200 GET /v3/apps?names=hello&space_guids=:guid
201 POST /v3/resource_matches  {"resources":[{"path":"hello.wasm","mode":"644","checksum":{"value":"d321fcc4502f704a41b8248805fc4970237c57d5"},"size...
201 POST /v3/packages  {"relationships":{"app":{"data":{"guid":":guid"}}},"type":"bits"}
200 POST /v3/packages/:guid/upload  <59426 bytes>
201 POST /v3/builds  {"package":{"guid":":guid"}}
200 GET /v3/apps?names=hello&space_guids=:guid
200 GET /v3/builds/:guid
200 GET /api/v1/info
200 GET /v3/droplets/:guid
200 GET /api/v1/read/:guid?descending=true&limit=1&start_time=<ns>
200 PATCH /v3/apps/:guid/relationships/current_droplet  {"data":{"guid":":guid"}}
200 POST /v3/apps/:guid/actions/restart
200 GET /v3/apps/:guid/processes
200 GET /v3/processes/:guid/stats
200 GET /v3/apps?names=hello&space_guids=:guid
200 GET /v3/apps/:guid/processes
200 GET /v3/processes/:guid
200 GET /v3/processes/:guid/sidecars
200 GET /v3/processes/:guid/stats
200 GET /v3/apps/:guid/routes
200 GET /v3/apps/:guid/droplets/current
200 GET /v3/deployments?app_guids=:guid&order_by=-created_at&page=1&per_page=1&status_values=ACTIVE
```

## `cf push hello2 -p app/hello.wasm` (exit 1)

CLI: `zip: not a valid zip file` / `FAILED`

```text
202 POST /v3/spaces/:guid/actions/apply_manifest  applications:\n- name: hello2\n  path: <local path>\n  default-route: true\n
200 GET /v3/jobs/:guid
200 GET /v3/apps?names=hello2&space_guids=:guid
```

## `cf apps`

```text
200 GET /v3/apps?order_by=name&per_page=5000&space_guids=:guid
200 GET /v3/processes?app_guids=:guid,:guid
200 GET /v3/processes/:guid/stats
200 GET /v3/processes/:guid/stats
200 GET /v3/routes?app_guids=:guid,:guid
```

## `cf app hello`

```text
200 GET /v3/apps?names=hello&space_guids=:guid
200 GET /v3/apps/:guid/processes
200 GET /v3/processes/:guid/sidecars
200 GET /v3/processes/:guid/stats
200 GET /v3/apps/:guid/routes
200 GET /v3/apps/:guid/droplets/current
200 GET /v3/deployments?app_guids=:guid&order_by=-created_at&page=1&per_page=1&status_values=ACTIVE
```

## `cf domains`

```text
200 GET /v3/organizations/:guid/domains
```

## `cf routes`

```text
200 GET /v3/routes?per_page=5000&space_guids=:guid
200 GET /v3/spaces?guids=:guid
200 GET /v3/apps?guids=:guid,:guid
200 GET /v3/service_route_bindings?include=service_instance&route_guids=:guid,:guid
```

## `cf map-route hello localhost --hostname foo`

```text
200 GET /v3/domains?names=localhost&page=1&per_page=1
200 GET /v3/apps?names=hello&space_guids=:guid
200 GET /v3/routes?domain_guids=:guid&hosts=foo&page=1&paths=&per_page=1
200 GET /v3/domains?names=localhost&page=1&per_page=1
201 POST /v3/routes  {"host":"foo","relationships":{"space":{"data":{"guid":":guid"}},"domain":{"data":{"guid":":guid"}}}}
200 POST /v3/routes/:guid/destinations  {"destinations":[{"app":{"guid":":guid"}}]}
```

## `cf unmap-route hello localhost --hostname foo`

```text
200 GET /v3/domains?names=localhost&page=1&per_page=1
200 GET /v3/apps?names=hello&space_guids=:guid
200 GET /v3/routes?domain_guids=:guid&hosts=foo&page=1&paths=&per_page=1
204 DELETE /v3/routes/:guid/destinations/:guid
```

## `cf scale hello -i 3`

```text
200 GET /v3/apps?names=hello&space_guids=:guid
202 POST /v3/apps/:guid/processes/web/actions/scale  {"instances":3}
200 GET /v3/apps/:guid/processes
200 GET /v3/processes/:guid/stats
200 GET /v3/apps?names=hello&space_guids=:guid
200 GET /v3/apps/:guid/processes
200 GET /v3/processes/:guid/sidecars
200 GET /v3/processes/:guid/stats
200 GET /v3/apps/:guid/routes
200 GET /v3/apps/:guid/droplets/current
200 GET /v3/deployments?app_guids=:guid&order_by=-created_at&page=1&per_page=1&status_values=ACTIVE
```

## `cf scale hello -m 64M -f`

```text
200 GET /v3/apps?names=hello&space_guids=:guid
202 POST /v3/apps/:guid/processes/web/actions/scale  {"memory_in_mb":64}
200 POST /v3/apps/:guid/actions/stop
200 POST /v3/apps/:guid/actions/start
200 GET /v3/apps/:guid/processes
200 GET /v3/processes/:guid/stats
200 GET /v3/apps?names=hello&space_guids=:guid
200 GET /v3/apps/:guid/processes
200 GET /v3/processes/:guid/sidecars
200 GET /v3/processes/:guid/stats
200 GET /v3/apps/:guid/routes
200 GET /v3/apps/:guid/droplets/current
200 GET /v3/deployments?app_guids=:guid&order_by=-created_at&page=1&per_page=1&status_values=ACTIVE
```

## `cf set-env hello GREETING hi`

```text
200 GET /v3/apps?names=hello&space_guids=:guid
200 PATCH /v3/apps/:guid/environment_variables  {"var":{"GREETING":"hi"}}
```

## `cf env hello`

```text
200 GET /v3/apps?names=hello&space_guids=:guid
200 GET /v3/apps/:guid/env
```

## `cf marketplace`

```text
200 GET /v3/service_plans?available=true&fields[service_offering.service_broker]=name,guid&include=service_offering&per_page=5000&space_guids=:guid
```

## `cf create-service keyvalue local kv`

```text
200 GET /v3/service_plans?names=local&service_offering_names=keyvalue
202 POST /v3/service_instances  {"name":"kv","relationships":{"service_plan":{"data":{"guid":":guid"}},"space":{"data":{"guid":":guid"}}},"type":"man...
200 GET /v3/jobs/:guid
```

## `cf create-service wasi-network inherit-network net`

```text
200 GET /v3/service_plans?names=inherit-network&service_offering_names=wasi-network
202 POST /v3/service_instances  {"name":"net","relationships":{"service_plan":{"data":{"guid":":guid"}},"space":{"data":{"guid":":guid"}}},"type":"ma...
200 GET /v3/jobs/:guid
```

## `cf bind-service hello net -c '{"identifier":"x"}'`

```text
200 GET /v3/service_instances?names=net&page=1&per_page=1&space_guids=:guid
200 GET /v3/apps?names=hello&page=1&per_page=1&space_guids=:guid
202 POST /v3/service_credential_bindings  {"parameters":{"identifier":"x"},"relationships":{"app":{"data":{"guid":":guid"}},"service_instance":{"data":{"guid":...
200 GET /v3/jobs/:guid
```

## `cf services`

```text
200 GET /v3/service_instances?fields[service_plan.service_offering.service_broker]=guid,name&fields[service_plan.service_offering]=guid,name,relationships.service_broker&fields[service_plan]=guid,name,relationships.service_offering&order_by=name&per_page=5000&space_guids=:guid
200 GET /v3/service_credential_bindings?include=app&service_instance_guids=:guid,:guid
```

## `cf service kv`

```text
200 GET /v3/service_instances?fields[service_plan.service_offering.service_broker]=name,guid&fields[service_plan.service_offering]=name,guid,description,tags,documentation_url&fields[service_plan]=name,guid&fields[space.organization]=name,guid&fields[space]=name,guid&names=kv&page=1&per_page=1&space_guids=:guid
200 GET /v3/feature_flags/service_instance_sharing
200 GET /v3/service_offerings/:guid
200 GET /v3/service_instances/:guid/relationships/shared_spaces?fields[space.organization]=guid,name&fields[space]=guid,name,relationships.organization
200 GET /v3/service_credential_bindings?include=app&service_instance_guids=:guid&type=app
```

## `cf unbind-service hello net`

```text
200 GET /v3/service_instances?names=net&page=1&per_page=1&space_guids=:guid
200 GET /v3/apps?names=hello&page=1&per_page=1&space_guids=:guid
200 GET /v3/service_credential_bindings?app_guids=:guid&service_instance_guids=:guid&type=app
202 DELETE /v3/service_credential_bindings/:guid
200 GET /v3/jobs/:guid
```

## `cf delete-service net -f`

```text
200 GET /v3/service_instances?names=net&page=1&per_page=1&space_guids=:guid
202 DELETE /v3/service_instances/:guid
200 GET /v3/jobs/:guid
```

## `cf restage hello`

```text
200 GET /v3/apps?names=hello&space_guids=:guid
200 GET /v3/packages?app_guids=:guid&order_by=-created_at&page=1&per_page=1&states=READY
200 GET /v3/apps?names=hello&space_guids=:guid
200 GET /v3/apps?names=hello&space_guids=:guid
200 GET /v3/packages?app_guids=:guid&guids=:guid&page=1&per_page=1
200 GET /api/v1/info
201 POST /v3/builds  {"package":{"guid":":guid"}}
200 GET /api/v1/read/:guid?descending=true&limit=1&start_time=<ns>
200 GET /api/v1/read/:guid?envelope_types=LOG&start_time=<ns>
200 GET /api/v1/read/:guid?envelope_types=LOG&start_time=<ns>
200 GET /v3/builds/:guid
200 POST /v3/apps/:guid/actions/stop
200 PATCH /v3/apps/:guid/relationships/current_droplet  {"data":{"guid":":guid"}}
200 POST /v3/apps/:guid/actions/start
200 GET /v3/apps/:guid/processes
200 GET /v3/processes/:guid/stats
200 GET /v3/apps?names=hello&space_guids=:guid
200 GET /v3/apps/:guid/processes
200 GET /v3/processes/:guid/sidecars
200 GET /v3/processes/:guid/stats
200 GET /v3/apps/:guid/routes
200 GET /v3/apps/:guid/droplets/current
200 GET /v3/deployments?app_guids=:guid&order_by=-created_at&page=1&per_page=1&status_values=ACTIVE
```

## `cf stop hello`

```text
200 GET /v3/apps?names=hello&space_guids=:guid
200 POST /v3/apps/:guid/actions/stop
```

## `cf start hello`

```text
200 GET /v3/apps?names=hello&space_guids=:guid
200 GET /v3/packages?app_guids=:guid&order_by=-created_at&page=1&per_page=1
200 GET /v3/packages/:guid/droplets?page=1&per_page=1&states=STAGED
200 POST /v3/apps/:guid/actions/start
200 GET /v3/apps/:guid/processes
200 GET /v3/processes/:guid/stats
200 GET /v3/apps?names=hello&space_guids=:guid
200 GET /v3/apps/:guid/processes
200 GET /v3/processes/:guid/sidecars
200 GET /v3/processes/:guid/stats
200 GET /v3/apps/:guid/routes
200 GET /v3/apps/:guid/droplets/current
200 GET /v3/deployments?app_guids=:guid&order_by=-created_at&page=1&per_page=1&status_values=ACTIVE
```

## `cf restart hello`

```text
200 GET /v3/apps?names=hello&space_guids=:guid
200 GET /v3/packages?app_guids=:guid&order_by=-created_at&page=1&per_page=1
200 GET /v3/packages/:guid/droplets?page=1&per_page=1&states=STAGED
200 POST /v3/apps/:guid/actions/stop
200 POST /v3/apps/:guid/actions/start
200 GET /v3/apps/:guid/processes
200 GET /v3/processes/:guid/stats
200 GET /v3/apps?names=hello&space_guids=:guid
200 GET /v3/apps/:guid/processes
200 GET /v3/processes/:guid/sidecars
200 GET /v3/processes/:guid/stats
200 GET /v3/apps/:guid/routes
200 GET /v3/apps/:guid/droplets/current
200 GET /v3/deployments?app_guids=:guid&order_by=-created_at&page=1&per_page=1&status_values=ACTIVE
```

## `cf restart hello --strategy rolling`

```text
200 GET /v3/apps?names=hello&space_guids=:guid
200 GET /v3/packages?app_guids=:guid&order_by=-created_at&page=1&per_page=1
200 GET /v3/packages/:guid/droplets?page=1&per_page=1&states=STAGED
201 POST /v3/deployments  {"strategy":"rolling","relationships":{"app":{"data":{"guid":":guid"}}}}
200 GET /v3/deployments/:guid
200 GET /v3/apps/:guid/processes
200 GET /v3/processes/:guid/stats
200 GET /v3/apps?names=hello&space_guids=:guid
200 GET /v3/apps/:guid/processes
200 GET /v3/processes/:guid/sidecars
200 GET /v3/processes/:guid/stats
200 GET /v3/apps/:guid/routes
200 GET /v3/apps/:guid/droplets/current
200 GET /v3/deployments?app_guids=:guid&order_by=-created_at&page=1&per_page=1&status_values=ACTIVE
```

## `cf push hello -p app --strategy rolling`

```text
202 POST /v3/spaces/:guid/actions/apply_manifest  applications:\n- name: hello\n  path: <local path>\n  default-route: true\n
200 GET /v3/jobs/:guid
200 GET /v3/apps?names=hello&space_guids=:guid
201 POST /v3/resource_matches  {"resources":[{"path":"hello.wasm","mode":"644","checksum":{"value":"d321fcc4502f704a41b8248805fc4970237c57d5"},"size...
201 POST /v3/packages  {"relationships":{"app":{"data":{"guid":":guid"}}},"type":"bits"}
200 POST /v3/packages/:guid/upload  <59426 bytes>
201 POST /v3/builds  {"package":{"guid":":guid"}}
200 GET /v3/apps?names=hello&space_guids=:guid
200 GET /api/v1/info
200 GET /v3/builds/:guid
200 GET /api/v1/read/:guid?descending=true&limit=1&start_time=<ns>
200 GET /v3/droplets/:guid
201 POST /v3/deployments  {"droplet":{"guid":":guid"},"strategy":"rolling","relationships":{"app":{"data":{"guid":":guid"}}}}
200 GET /v3/deployments/:guid
200 GET /v3/apps/:guid/processes
200 GET /v3/processes/:guid/stats
200 GET /v3/apps?names=hello&space_guids=:guid
200 GET /v3/apps/:guid/processes
200 GET /v3/processes/:guid
200 GET /v3/processes/:guid/sidecars
200 GET /v3/processes/:guid/stats
200 GET /v3/apps/:guid/routes
200 GET /v3/apps/:guid/droplets/current
200 GET /v3/deployments?app_guids=:guid&order_by=-created_at&page=1&per_page=1&status_values=ACTIVE
```

## `cf logs hello --recent`

```text
200 GET /v3/apps?names=hello&space_guids=:guid
200 GET /api/v1/info
200 GET /api/v1/read/:guid?descending=true&envelope_types=LOG&limit=1000&start_time=<ns>
```

## `cf push img --docker-image ghcr.io/example/hello:wasm`

```text
202 POST /v3/spaces/:guid/actions/apply_manifest  applications:\n- name: img\n  docker:\n    image: ghcr.io/example/hello:wasm\n  default-route: true\n
200 GET /v3/jobs/:guid
200 GET /v3/apps?names=img&space_guids=:guid
201 POST /v3/packages  {"relationships":{"app":{"data":{"guid":":guid"}}},"type":"docker","data":{"image":"ghcr.io/example/hello:wasm"}}
201 POST /v3/builds  {"package":{"guid":":guid"}}
200 GET /v3/apps?names=img&space_guids=:guid
200 GET /api/v1/info
200 GET /api/v1/read/:guid?descending=true&limit=1&start_time=<ns>
200 GET /api/v1/read/:guid?envelope_types=LOG&start_time=<ns>
200 GET /v3/builds/:guid
200 GET /v3/droplets/:guid
200 PATCH /v3/apps/:guid/relationships/current_droplet  {"data":{"guid":":guid"}}
200 POST /v3/apps/:guid/actions/restart
200 GET /v3/apps/:guid/processes
200 GET /v3/processes/:guid/stats
200 GET /v3/apps?names=img&space_guids=:guid
200 GET /v3/apps/:guid/processes
200 GET /v3/processes/:guid
200 GET /v3/processes/:guid/sidecars
200 GET /v3/processes/:guid/stats
200 GET /v3/apps/:guid/routes
200 GET /v3/apps/:guid/droplets/current
200 GET /v3/deployments?app_guids=:guid&order_by=-created_at&page=1&per_page=1&status_values=ACTIVE
```

## `cf push crash -p app` (exit 1)

CLI: `Error starting instances: 'wasm route 'crash.localhost': component crash.wasm imports wasi:keyvalue/store@0.2.0-draft2: the route needs wasi=keyvalue'` / `FAILED`

```text
202 POST /v3/spaces/:guid/actions/apply_manifest  applications:\n- name: crash\n  path: <local path>\n  default-route: true\n
200 GET /v3/jobs/:guid
200 GET /v3/apps?names=crash&space_guids=:guid
201 POST /v3/resource_matches  {"resources":[{"path":"hello.wasm","mode":"644","checksum":{"value":"d321fcc4502f704a41b8248805fc4970237c57d5"},"size...
201 POST /v3/packages  {"relationships":{"app":{"data":{"guid":":guid"}}},"type":"bits"}
200 POST /v3/packages/:guid/upload  <59426 bytes>
201 POST /v3/builds  {"package":{"guid":":guid"}}
200 GET /v3/apps?names=crash&space_guids=:guid
200 GET /v3/builds/:guid
200 GET /api/v1/info
200 GET /api/v1/read/:guid?descending=true&limit=1&start_time=<ns>
200 GET /v3/droplets/:guid
200 PATCH /v3/apps/:guid/relationships/current_droplet  {"data":{"guid":":guid"}}
200 POST /v3/apps/:guid/actions/restart
200 GET /v3/apps/:guid/processes
200 GET /v3/processes/:guid/stats
200 GET /v3/apps?names=crash&space_guids=:guid
200 GET /v3/apps/:guid/processes
200 GET /v3/processes/:guid
200 GET /v3/processes/:guid/sidecars
200 GET /v3/processes/:guid/stats
200 GET /v3/apps/:guid/routes
200 GET /v3/apps/:guid/droplets/current
200 GET /v3/deployments?app_guids=:guid&order_by=-created_at&page=1&per_page=1&status_values=ACTIVE
```

## `cf delete hello -r -f`

```text
200 GET /v3/apps?names=hello&space_guids=:guid
200 GET /v3/apps/:guid/routes
202 DELETE /v3/apps/:guid
200 GET /v3/jobs/:guid
202 DELETE /v3/routes/:guid
200 GET /v3/jobs/:guid
```

## `cf push (in multi/, with its manifest.yml)`

```text
201 POST /v3/spaces/:guid/manifest_diff  applications:\n- name: counter\n  instances: 2\n  memory: 64M\n  default-route: true\n  command: counter.wasm\n  meta...
202 POST /v3/spaces/:guid/actions/apply_manifest  applications:\n- name: counter\n  instances: 2\n  memory: 64M\n  default-route: true\n  command: counter.wasm\n  meta...
200 GET /v3/jobs/:guid
200 GET /v3/apps?names=counter&space_guids=:guid
201 POST /v3/resource_matches  {"resources":[{"path":"counter.wasm","mode":"644","checksum":{"value":"e3f1ba106a850e77a718a8ce2538db6648d4c2d6"},"si...
201 POST /v3/packages  {"relationships":{"app":{"data":{"guid":":guid"}}},"type":"bits"}
200 POST /v3/packages/:guid/upload  <120102 bytes>
200 GET /v3/apps?names=counter&space_guids=:guid
201 POST /v3/builds  {"package":{"guid":":guid"}}
200 GET /api/v1/info
200 GET /api/v1/read/:guid?descending=true&limit=1&start_time=<ns>
200 GET /v3/builds/:guid
200 GET /api/v1/read/:guid?envelope_types=LOG&start_time=<ns>
200 GET /v3/droplets/:guid
200 PATCH /v3/apps/:guid/relationships/current_droplet  {"data":{"guid":":guid"}}
200 POST /v3/apps/:guid/actions/restart
200 GET /v3/apps/:guid/processes
200 GET /v3/processes/:guid/stats
200 GET /v3/apps?names=counter&space_guids=:guid
200 GET /v3/apps/:guid/processes
200 GET /v3/processes/:guid
200 GET /v3/processes/:guid/sidecars
200 GET /v3/processes/:guid/stats
200 GET /v3/apps/:guid/routes
200 GET /v3/apps/:guid/droplets/current
200 GET /v3/deployments?app_guids=:guid&order_by=-created_at&page=1&per_page=1&status_values=ACTIVE
```

