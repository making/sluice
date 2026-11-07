//! The resources in Cloud Controller v3's shapes, their lists filtered,
//! ordered and paginated as the CLI asks.

use std::collections::BTreeMap;

use serde_json::{Value, json};

use crate::controller::Controller;
use crate::store::{
    App, Build, Deployment, Droplet, Job, Lifecycle, Named, Outcome, Package, ROOTFS_STACK, Route,
    STACK, SecurityGroup, State, epoch_secs, now,
};
use crate::wicket::{ListedApp, Runtime, Sandbox};

/// The Cloud Controller version announced.
pub const CC_VERSION: &str = "3.200.0";

pub fn root(base: &str) -> Value {
    json!({"links": {
        "self": {"href": base},
        "bits_service": null,
        "cloud_controller_v2": null,
        "cloud_controller_v3": {"href": format!("{base}/v3"), "meta": {"version": CC_VERSION}},
        "network_policy_v0": null,
        "network_policy_v1": null,
        "login": {"href": format!("{base}/uaa")},
        "uaa": {"href": format!("{base}/uaa")},
        "credhub": null,
        "routing": null,
        "logging": null,
        "log_cache": {"href": base},
        "log_stream": null,
        "app_ssh": null,
    }})
}

pub fn v3(base: &str) -> Value {
    let names = [
        "apps",
        "builds",
        "deployments",
        "domains",
        "droplets",
        "jobs",
        "organizations",
        "packages",
        "processes",
        "routes",
        "service_brokers",
        "service_credential_bindings",
        "service_instances",
        "service_offerings",
        "service_plans",
        "spaces",
        "stacks",
    ];
    let links: serde_json::Map<String, Value> = names
        .iter()
        .map(|name| {
            (
                name.to_string(),
                json!({"href": format!("{base}/v3/{name}")}),
            )
        })
        .collect();
    json!({"links": links})
}

/// `/v3/info`: the description names what the wicket runs, when it answers.
pub fn info(base: &str, build: &str, runtime: Option<Runtime>) -> Value {
    let description = match runtime {
        Some(Runtime::Wasm) => {
            "sluice-wicket-cf-api: deploys wasm components to a sluice-wicket-runtime-wasi with the cf CLI"
        }
        Some(Runtime::Firecracker) => {
            "sluice-wicket-cf-api: deploys container images and rootfs images to a sluice-wicket-runtime-firecracker with the cf CLI"
        }
        None => "sluice-wicket-cf-api: deploys apps to a sluice wicket with the cf CLI",
    };
    json!({
        "build": build,
        "cli_version": {"minimum": "", "recommended": ""},
        "custom": {},
        "description": description,
        "name": "sluice-wicket-cf-api",
        "version": 0,
        "osbapi_version": "2.15",
        "links": {"self": {"href": format!("{base}/v3/info")}, "support": {"href": ""}},
    })
}

fn metadata(app: Option<&App>) -> Value {
    match app {
        Some(app) => json!({"labels": app.labels, "annotations": app.annotations}),
        None => json!({"labels": {}, "annotations": {}}),
    }
}

/// A lifecycle as CC shows it: a docker one holds no data.
fn lifecycle(lifecycle: Lifecycle, buildpacks: &[String], stack: &str) -> Value {
    match lifecycle {
        Lifecycle::Buildpack => {
            json!({"type": "buildpack", "data": {"buildpacks": buildpacks, "stack": stack}})
        }
        Lifecycle::Docker => json!({"type": "docker", "data": {}}),
    }
}

fn rel(guid: Option<&str>) -> Value {
    match guid {
        Some(guid) => json!({"data": {"guid": guid}}),
        None => json!({"data": null}),
    }
}

pub fn org(base: &str, state: &State) -> Value {
    let org = &state.org;
    json!({
        "guid": org.guid, "created_at": org.created_at, "updated_at": org.created_at,
        "name": org.name, "suspended": false,
        "relationships": {"quota": {"data": {"guid": "default"}}},
        "metadata": metadata(None),
        "links": {
            "self": {"href": format!("{base}/v3/organizations/{}", org.guid)},
            "domains": {"href": format!("{base}/v3/organizations/{}/domains", org.guid)},
            "default_domain": {"href": format!("{base}/v3/organizations/{}/domains/default", org.guid)},
            "quota": {"href": format!("{base}/v3/organization_quotas/default")},
        },
    })
}

pub fn space(base: &str, state: &State) -> Value {
    let space = &state.space;
    json!({
        "guid": space.guid, "created_at": space.created_at, "updated_at": space.created_at,
        "name": space.name,
        "relationships": {"organization": rel(Some(&state.org.guid)), "quota": rel(None)},
        "metadata": metadata(None),
        "links": {
            "self": {"href": format!("{base}/v3/spaces/{}", space.guid)},
            "organization": {"href": format!("{base}/v3/organizations/{}", state.org.guid)},
            "features": {"href": format!("{base}/v3/spaces/{}/features", space.guid)},
            "apply_manifest": {"href": format!("{base}/v3/spaces/{}/actions/apply_manifest", space.guid), "method": "POST"},
        },
    })
}

pub fn domain(base: &str, domain: &Named) -> Value {
    json!({
        "guid": domain.guid, "created_at": domain.created_at, "updated_at": domain.created_at,
        "name": domain.name, "internal": false, "router_group": null,
        "supported_protocols": ["http"],
        "metadata": metadata(None),
        "relationships": {"organization": rel(None), "shared_organizations": {"data": []}},
        "links": {
            "self": {"href": format!("{base}/v3/domains/{}", domain.guid)},
            "route_reservations": {"href": format!("{base}/v3/domains/{}/route_reservations", domain.guid)},
        },
    })
}

pub fn security_group(base: &str, group: &SecurityGroup) -> Value {
    let spaces = |guids: &[String]| json!({"data": guids.iter().map(|guid| json!({"guid": guid})).collect::<Vec<_>>()});
    json!({
        "guid": group.guid, "created_at": group.created_at, "updated_at": group.updated_at,
        "name": group.name,
        "globally_enabled": {"running": group.running, "staging": group.staging},
        "rules": group.rules,
        "relationships": {
            "running_spaces": spaces(&group.running_spaces),
            "staging_spaces": spaces(&group.staging_spaces),
        },
        "links": {"self": {"href": format!("{base}/v3/security_groups/{}", group.guid)}},
    })
}

/// The one stack, of what the wicket runs (a wasm wicket's until it answers).
pub fn stack(base: &str, runtime: Option<Runtime>) -> Value {
    let (name, description) = match runtime {
        Some(Runtime::Firecracker) => (
            ROOTFS_STACK,
            "root filesystems in microVMs, served by sluice-wicket-runtime-firecracker",
        ),
        _ => (
            STACK,
            "WASI 0.3 components, served by sluice-wicket-runtime-wasi",
        ),
    };
    json!({
        "guid": name, "created_at": "2026-01-01T00:00:00Z", "updated_at": "2026-01-01T00:00:00Z",
        "name": name, "description": description,
        "build_rootfs_image": name, "run_rootfs_image": name, "default": true,
        "metadata": metadata(None),
        "links": {"self": {"href": format!("{base}/v3/stacks/{name}")}},
    })
}

pub fn app(base: &str, state: &State, app: &App) -> Value {
    let guid = &app.guid;
    json!({
        "guid": guid, "created_at": app.created_at, "updated_at": app.updated_at,
        "name": app.name,
        "state": if app.started { "STARTED" } else { "STOPPED" },
        "lifecycle": lifecycle(app.lifecycle, &app.buildpacks, &app.stack),
        "relationships": {"space": rel(Some(&state.space.guid))},
        "metadata": metadata(Some(app)),
        "links": {
            "self": {"href": format!("{base}/v3/apps/{guid}")},
            "space": {"href": format!("{base}/v3/spaces/{}", state.space.guid)},
            "processes": {"href": format!("{base}/v3/apps/{guid}/processes")},
            "packages": {"href": format!("{base}/v3/apps/{guid}/packages")},
            "environment_variables": {"href": format!("{base}/v3/apps/{guid}/environment_variables")},
            "current_droplet": {"href": format!("{base}/v3/apps/{guid}/droplets/current")},
            "droplets": {"href": format!("{base}/v3/apps/{guid}/droplets")},
            "tasks": {"href": format!("{base}/v3/apps/{guid}/tasks")},
            "start": {"href": format!("{base}/v3/apps/{guid}/actions/start"), "method": "POST"},
            "stop": {"href": format!("{base}/v3/apps/{guid}/actions/stop"), "method": "POST"},
            "revisions": {"href": format!("{base}/v3/apps/{guid}/revisions")},
            "deployed_revisions": {"href": format!("{base}/v3/apps/{guid}/revisions/deployed")},
            "features": {"href": format!("{base}/v3/apps/{guid}/features")},
        },
    })
}

pub fn process(base: &str, state: &State, app: &App) -> Value {
    let process = &app.process;
    let check = &process.health_check;
    let mut data =
        json!({"timeout": check.timeout, "invocation_timeout": check.invocation_timeout});
    if check.kind == "http" {
        data["endpoint"] = json!(check.endpoint.clone().unwrap_or_else(|| "/".into()));
    }
    json!({
        "guid": process.guid, "created_at": process.created_at, "updated_at": process.updated_at,
        "type": "web",
        "command": process.command,
        "instances": process.instances,
        "memory_in_mb": process.memory_mb,
        "disk_in_mb": process.disk_mb,
        "log_rate_limit_in_bytes_per_second": process.log_rate_limit,
        "health_check": {"type": check.kind, "data": data},
        "readiness_health_check": {"type": "process", "data": {"invocation_timeout": null}},
        "relationships": {"app": rel(Some(&app.guid)), "revision": rel(None)},
        "metadata": metadata(None),
        "version": process.guid,
        "links": {
            "self": {"href": format!("{base}/v3/processes/{}", process.guid)},
            "scale": {"href": format!("{base}/v3/processes/{}/actions/scale", process.guid), "method": "POST"},
            "app": {"href": format!("{base}/v3/apps/{}", app.guid)},
            "space": {"href": format!("{base}/v3/spaces/{}", state.space.guid)},
            "stats": {"href": format!("{base}/v3/processes/{}/stats", process.guid)},
        },
    })
}

/// What `cf app` lists, as the CLI reads it: it counts the entries and, at a start, waits for an
/// entry `RUNNING` or for none, failing once all are `CRASHED`.
///
/// `DOWN` for a stopped app and, of `instances` entries too, `CRASHED` with the wicket's refusal
/// as `details` and `STARTING` until it serves the app (`listed`, as it answers now). Serving it,
/// an entry `RUNNING` per instance alive, by the index `cf logs` names, up since its own start,
/// `CRASHED` for what it says is down, since then, and `STARTING` for a VM it keeps and has not up
/// yet: none is a limit not reached, no failure, as is an app the wicket has no reason to serve
/// (at 0, without a route).
pub fn stats(state: &State, app: &App, owner: &str, listed: Option<&ListedApp>) -> Value {
    let all = |since, state, details: Option<&String>| -> Vec<Entry> {
        (0..app.process.instances)
            .map(|index| Entry {
                index,
                state,
                details: details.cloned(),
                since,
            })
            .collect()
    };
    let entries = match (&app.running, app.started) {
        (Some(running), true) => {
            let all = |state, details| all(running.since, state, details);
            match state.wicket.get(&app.guid) {
                _ if Controller::desired(state, app, owner).is_none() => Vec::new(),
                Some(Outcome::Refused(detail)) => all("CRASHED", Some(detail)),
                Some(Outcome::Running) => match listed {
                    Some(listed) => alive(listed, app.process.instances),
                    None => all("STARTING", None),
                },
                Some(Outcome::Unreachable) | None => all("STARTING", None),
            }
        }
        _ => all(epoch_secs(), "DOWN", None),
    };
    let resources: Vec<Value> = entries
        .into_iter()
        .map(|entry| {
            json!({
                "type": "web", "index": entry.index, "state": entry.state,
                "routable": entry.state == "RUNNING",
                "usage": {"time": now(), "cpu": 0.0, "cpu_entitlement": 0.0, "mem": 0, "disk": 0, "log_rate": 0},
                "host": "wicket", "instance_internal_ip": "127.0.0.1", "instance_ports": [],
                "uptime": epoch_secs().saturating_sub(entry.since),
                "mem_quota": app.process.memory_mb << 20,
                "disk_quota": app.process.disk_mb << 20,
                "log_rate_limit": app.process.log_rate_limit,
                "fds_quota": 16384, "isolation_segment": null,
                "details": entry.details, "port": null,
            })
        })
        .collect();
    json!({"resources": resources})
}

/// An entry of [`stats`], up `since` a time in seconds since the epoch.
struct Entry {
    index: u64,
    state: &'static str,
    details: Option<String>,
    since: u64,
}

/// The entries of what the wicket lists alive of an app of `instances`, by index, those it names
/// by one index as one, up since the start of the lowest of them, the one staying as the others
/// finish; each VM it lists as down at its own index, since that goes on; and what else it is short
/// of, at the first index below `instances` no other holds. Nothing down, the VMs it keeps and has
/// not up yet are starting, at the lowest indices free: one down, an entry `STARTING` beside it
/// would keep the CLI's wait going where all that it lists has failed.
fn alive(listed: &ListedApp, instances: u64) -> Vec<Entry> {
    let mut indices = BTreeMap::new();
    for instance in &listed.alive {
        indices
            .entry(crate::flume::listed(instance.index, instances))
            .or_insert(instance.since);
    }
    let mut crashed = BTreeMap::new();
    for down in &listed.crashed {
        let index = crate::flume::listed(down.index, instances);
        if !indices.contains_key(&index) {
            crashed.entry(index).or_insert(down);
        }
    }
    let mut down: Vec<Entry> = crashed
        .into_iter()
        .map(|(index, down)| Entry {
            index,
            state: "CRASHED",
            details: Some(down.why.clone()),
            since: down.since,
        })
        .collect();
    if down.is_empty()
        && let Some(short) = &listed.down
        && let Some(index) = (0..instances).find(|index| !indices.contains_key(index))
    {
        down.push(Entry {
            index,
            state: "CRASHED",
            details: Some(short.why.clone()),
            since: short.since,
        });
    }
    if down.is_empty() {
        let short = kept(&listed.app.sandbox).saturating_sub(indices.len() as u64);
        let free = (0..instances).filter(|index| !indices.contains_key(index));
        let starting: Vec<u64> = free.take(short as usize).collect();
        down.extend(starting.into_iter().map(|index| Entry {
            index,
            state: "STARTING",
            details: None,
            since: epoch_secs(),
        }));
    }
    let mut entries: Vec<_> = indices
        .into_iter()
        .map(|(index, since)| Entry {
            index,
            state: "RUNNING",
            details: None,
            since,
        })
        .chain(down)
        .collect();
    entries.sort_by_key(|entry| entry.index);
    entries
}

/// The VMs a wicket keeps up of a route as listed, whatever its requests: of a pool `warm`, its
/// default `instances`, booted `instances`, 1 going unlisted. None of the other modes, a clone
/// coming and going with its connection, nor of a component, whose units the wicket lists as it
/// has them.
fn kept(sandbox: &Sandbox) -> u64 {
    let Sandbox::Vm(vm) = sandbox else {
        return 0;
    };
    match vm.mode.as_deref() {
        Some("pool") => vm.warm.or(vm.instances).unwrap_or(0),
        Some("boot") => vm.instances.unwrap_or(1),
        _ => 0,
    }
}

pub fn route(base: &str, state: &State, route: &Route) -> Value {
    json!({
        "guid": route.guid, "created_at": route.created_at, "updated_at": route.updated_at,
        "protocol": "http", "host": route.host, "path": "", "port": null,
        "url": state.url(route),
        "destinations": destinations(route),
        "options": {},
        "metadata": metadata(None),
        "relationships": {"space": rel(Some(&state.space.guid)), "domain": rel(Some(&route.domain))},
        "links": {
            "self": {"href": format!("{base}/v3/routes/{}", route.guid)},
            "space": {"href": format!("{base}/v3/spaces/{}", state.space.guid)},
            "domain": {"href": format!("{base}/v3/domains/{}", route.domain)},
            "destinations": {"href": format!("{base}/v3/routes/{}/destinations", route.guid)},
        },
    })
}

pub fn destinations(route: &Route) -> Vec<Value> {
    match (&route.app, &route.destination) {
        (Some(app), Some(destination)) => vec![json!({
            "guid": destination,
            "app": {"guid": app, "process": {"type": "web"}},
            "weight": null, "port": 8080, "protocol": "http1",
        })],
        _ => Vec::new(),
    }
}

/// A package; a docker one's password as `***`, as CC shows it.
pub fn package(base: &str, package: &Package) -> Value {
    let mut links = json!({
        "self": {"href": format!("{base}/v3/packages/{}", package.guid)},
        "app": {"href": format!("{base}/v3/apps/{}", package.app)},
    });
    let (kind, data) = match &package.docker {
        Some(docker) => (
            "docker",
            json!({
                "image": docker.image,
                "username": docker.username,
                "password": docker.password.as_ref().map(|_| "***"),
            }),
        ),
        None => {
            links["upload"] = json!({"href": format!("{base}/v3/packages/{}/upload", package.guid), "method": "POST"});
            links["download"] =
                json!({"href": format!("{base}/v3/packages/{}/download", package.guid)});
            (
                "bits",
                json!({"error": null, "checksum": {"type": "sha256", "value": package.sha256}}),
            )
        }
    };
    json!({
        "guid": package.guid, "created_at": package.created_at, "updated_at": package.updated_at,
        "type": kind,
        "data": data,
        "state": package.state,
        "relationships": {"app": rel(Some(&package.app))},
        "metadata": metadata(None),
        "links": links,
    })
}

pub fn build(base: &str, state: &State, build: &Build) -> Value {
    let app = state.app(&build.app);
    json!({
        "guid": build.guid, "created_at": build.created_at, "updated_at": build.updated_at,
        "created_by": {"guid": "", "name": "", "email": ""},
        "state": build.state,
        "staging_memory_in_mb": 1024, "staging_disk_in_mb": 1024,
        "staging_log_rate_limit_bytes_per_second": -1,
        "error": build.error,
        "lifecycle": lifecycle(
            build.lifecycle,
            app.map_or(&[][..], |app| &app.buildpacks),
            app.map_or(STACK, |app| &app.stack),
        ),
        "package": {"guid": build.package},
        "droplet": build.droplet.as_ref().map(|guid| json!({"guid": guid})),
        "relationships": {"app": rel(Some(&build.app))},
        "metadata": metadata(None),
        "links": {
            "self": {"href": format!("{base}/v3/builds/{}", build.guid)},
            "app": {"href": format!("{base}/v3/apps/{}", build.app)},
        },
    })
}

/// A droplet: an image's shows the image, pinned to its digest (what `cf
/// app` shows of a docker app), and no buildpack.
pub fn droplet(base: &str, droplet: &Droplet) -> Value {
    let (buildpack, stack) = match droplet.file.ends_with(crate::staging::ROOTFS) {
        true => ("rootfs", ROOTFS_STACK),
        false if droplet.files.is_some() => ("site", STACK),
        false => ("wasm", STACK),
    };
    let (kind, buildpacks, stack) = match droplet.image {
        Some(_) => ("docker", json!([]), Value::Null),
        None => (
            "buildpack",
            json!([{"name": buildpack, "detect_output": buildpack, "buildpack_name": buildpack, "version": "0.0.1"}]),
            json!(stack),
        ),
    };
    json!({
        "guid": droplet.guid, "created_at": droplet.created_at, "updated_at": droplet.updated_at,
        "state": "STAGED", "error": null,
        "lifecycle": {"type": kind, "data": {}},
        "execution_metadata": "",
        "process_types": {"web": droplet.file},
        "checksum": {"type": "sha256", "value": droplet.sha256},
        "buildpacks": buildpacks,
        "stack": stack, "image": droplet.image,
        "relationships": {"app": rel(Some(&droplet.app))},
        "metadata": metadata(None),
        "links": {
            "self": {"href": format!("{base}/v3/droplets/{}", droplet.guid)},
            "package": {"href": format!("{base}/v3/packages/{}", droplet.package)},
            "app": {"href": format!("{base}/v3/apps/{}", droplet.app)},
            "assign_current_droplet": {"href": format!("{base}/v3/apps/{}/relationships/current_droplet", droplet.app), "method": "PATCH"},
        },
    })
}

pub fn deployment(base: &str, deployment: &Deployment) -> Value {
    json!({
        "guid": deployment.guid, "created_at": deployment.created_at, "updated_at": deployment.updated_at,
        "state": "DEPLOYED",
        "status": {"value": "FINALIZED", "reason": "DEPLOYED", "details": {"last_successful_healthcheck": deployment.updated_at}},
        "strategy": deployment.strategy,
        "options": {"max_in_flight": 1},
        "droplet": {"guid": deployment.droplet},
        "previous_droplet": {"guid": deployment.previous_droplet},
        "new_processes": [],
        "revision": null,
        "relationships": {"app": rel(Some(&deployment.app))},
        "metadata": metadata(None),
        "links": {
            "self": {"href": format!("{base}/v3/deployments/{}", deployment.guid)},
            "app": {"href": format!("{base}/v3/apps/{}", deployment.app)},
        },
    })
}

pub fn job(base: &str, job: &Job) -> Value {
    let mut links = json!({"self": {"href": format!("{base}/v3/jobs/{}", job.guid)}});
    if let Some((name, path)) = &job.resource {
        links[name] = json!({"href": format!("{base}{path}")});
    }
    json!({
        "guid": job.guid, "created_at": job.created_at, "updated_at": job.updated_at,
        "operation": job.operation,
        "state": if job.errors.is_empty() { "COMPLETE" } else { "FAILED" },
        "errors": job.errors,
        "warnings": [],
        "links": links,
    })
}

/// The app's environment as `cf env` shows it: the user's variables, and
/// the `VCAP_*` its next start gives the guest.
pub fn env(base: &str, state: &State, app: &App) -> Value {
    json!({
        "staging_env_json": {},
        "running_env_json": {},
        "environment_variables": app.env,
        "system_env_json": {"VCAP_SERVICES": crate::services::vcap_services(state, &app.guid)},
        "application_env_json": {
            "VCAP_APPLICATION": crate::env::vcap_application(base, state, app),
        },
    })
}

/// The user's variables, as `cf set-env` gets them back.
pub fn environment_variables(base: &str, app: &App) -> Value {
    json!({
        "var": app.env,
        "links": {
            "self": {"href": format!("{base}/v3/apps/{}/environment_variables", app.guid)},
            "app": {"href": format!("{base}/v3/apps/{}", app.guid)},
        },
    })
}

/// The app's manifest, as `cf create-app-manifest` writes it: a docker
/// app's names the image of its newest package (and its username), and no
/// stack.
pub fn manifest(state: &State, app: &App) -> String {
    // a JSON string is a YAML scalar
    let quoted = |text: &str| serde_json::to_string(text).expect("a string");
    let mut text = format!("---\napplications:\n- name: {}\n", app.name);
    text.push_str(&format!(
        "  instances: {}\n  memory: {}M\n  disk_quota: {}M\n",
        app.process.instances, app.process.memory_mb, app.process.disk_mb
    ));
    let image = state
        .packages
        .iter()
        .rev()
        .filter(|package| package.app == app.guid)
        .find_map(|package| package.docker.as_ref());
    match (app.lifecycle, image) {
        (Lifecycle::Docker, Some(docker)) => {
            text.push_str(&format!(
                "  docker:\n    image: {}\n",
                quoted(&docker.image)
            ));
            if let Some(username) = &docker.username {
                text.push_str(&format!("    username: {}\n", quoted(username)));
            }
        }
        _ => text.push_str(&format!("  stack: {}\n", app.stack)),
    }
    if let Some(command) = &app.process.command {
        text.push_str(&format!("  command: {command}\n"));
    }
    let routes: Vec<String> = state
        .routes_of(&app.guid)
        .map(|route| format!("  - route: {}\n", state.url(route)))
        .collect();
    if routes.is_empty() {
        text.push_str("  no-route: true\n");
    } else {
        text.push_str("  routes:\n");
        text.extend(routes);
    }
    let services: Vec<String> = state
        .bindings_of(&app.guid)
        .filter_map(|binding| {
            let instance = state.instance(&binding.instance)?;
            let mut entry = format!("  - name: {}\n", quoted(&instance.name));
            if let Some(name) = &binding.name {
                entry.push_str(&format!("    binding_name: {}\n", quoted(name)));
            }
            if !binding.identifier.is_empty() {
                entry.push_str(&format!(
                    "    parameters:\n      identifier: {}\n",
                    quoted(&binding.identifier)
                ));
            }
            Some(entry)
        })
        .collect();
    if !services.is_empty() {
        text.push_str("  services:\n");
        text.extend(services);
    }
    if !app.env.is_empty() {
        text.push_str("  env:\n");
        for (name, value) in &app.env {
            text.push_str(&format!("    {}: {}\n", quoted(name), quoted(value)));
        }
    }
    if !app.annotations.is_empty() || !app.labels.is_empty() {
        text.push_str("  metadata:\n");
        for (what, map) in [("labels", &app.labels), ("annotations", &app.annotations)] {
            if !map.is_empty() {
                text.push_str(&format!("    {what}:\n"));
                for (key, value) in map {
                    text.push_str(&format!("      {key}: \"{value}\"\n"));
                }
            }
        }
    }
    text
}

/// The filters the CLI sends, as JSON pointers into a resource's view;
/// `app_guids` of a route reads its destinations.
const FILTERS: [(&str, &str); 24] = [
    ("names", "/name"),
    ("guids", "/guid"),
    ("space_guids", "/relationships/space/data/guid"),
    (
        "organization_guids",
        "/relationships/organization/data/guid",
    ),
    ("app_guids", "/relationships/app/data/guid"),
    ("hosts", "/host"),
    ("paths", "/path"),
    ("ports", "/port"),
    ("domain_guids", "/relationships/domain/data/guid"),
    ("types", "/type"),
    ("states", "/state"),
    ("status_values", "/status/value"),
    ("stacks", "/lifecycle/data/stack"),
    ("lifecycle_type", "/lifecycle/type"),
    ("package_guids", "/package/guid"),
    ("droplet_guids", "/droplet/guid"),
    ("type", "/type"),
    ("available", "/available"),
    ("globally_enabled_running", "/globally_enabled/running"),
    ("globally_enabled_staging", "/globally_enabled/staging"),
    (
        "service_instance_guids",
        "/relationships/service_instance/data/guid",
    ),
    (
        "service_plan_guids",
        "/relationships/service_plan/data/guid",
    ),
    (
        "service_offering_guids",
        "/relationships/service_offering/data/guid",
    ),
    (
        "service_broker_guids",
        "/relationships/service_broker/data/guid",
    ),
];

/// `items` filtered and ordered as `query` asks (`names=a,b`, `order_by=-name`).
pub fn select(items: Vec<Value>, query: &[(String, String)]) -> Vec<Value> {
    let mut items = items;
    for (key, values) in query {
        let Some((_, pointer)) = FILTERS.iter().find(|(filter, _)| filter == key) else {
            continue;
        };
        let wanted: Vec<&str> = {
            let split: Vec<&str> = values
                .split(',')
                .filter(|value| !value.is_empty())
                .collect();
            if split.is_empty() { vec![""] } else { split }
        };
        items.retain(|item| {
            let mut found: Vec<String> = Vec::new();
            if let Some(value) = item.pointer(pointer) {
                found.extend(text(value));
            }
            if key == "app_guids" {
                for destination in item["destinations"].as_array().into_iter().flatten() {
                    found.extend(text(&destination["app"]["guid"]));
                }
            }
            found.iter().any(|value| wanted.contains(&value.as_str()))
        });
    }
    if let Some((_, order)) = query.iter().find(|(key, _)| key == "order_by") {
        let (descending, field) = match order.strip_prefix('-') {
            Some(field) => (true, field),
            None => (false, order.trim_start_matches('+')),
        };
        let field = field.to_string();
        items.sort_by(|a, b| {
            let key = |item: &Value| text(&item[&field]).unwrap_or_default();
            let ordered = key(a).cmp(&key(b));
            if descending {
                ordered.reverse()
            } else {
                ordered
            }
        });
    }
    items
}

fn text(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        Value::Bool(flag) => Some(flag.to_string()),
        _ => None,
    }
}

/// The page of `items` the query asks (`page`, `per_page`), with the
/// pagination links and the resources `included`.
pub fn page(
    base: &str,
    path: &str,
    query: &[(String, String)],
    items: Vec<Value>,
    included: Option<Value>,
) -> Value {
    let number = |key: &str, default: usize| {
        query
            .iter()
            .find(|(k, _)| k == key)
            .and_then(|(_, value)| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(default)
    };
    let per_page = number("per_page", 50).min(5000);
    let current = number("page", 1);
    let total = items.len();
    let pages = total.div_ceil(per_page).max(1);
    let href = |page: usize| {
        let mut pairs: Vec<(String, String)> = query
            .iter()
            .filter(|(key, _)| key != "page" && key != "per_page")
            .cloned()
            .collect();
        pairs.push(("page".into(), page.to_string()));
        pairs.push(("per_page".into(), per_page.to_string()));
        let encoded: String = form_urlencoded::Serializer::new(String::new())
            .extend_pairs(pairs)
            .finish();
        json!({"href": format!("{base}{path}?{encoded}")})
    };
    let resources: Vec<Value> = items
        .into_iter()
        .skip((current - 1) * per_page)
        .take(per_page)
        .collect();
    let mut body = json!({
        "pagination": {
            "total_results": total,
            "total_pages": pages,
            "first": href(1),
            "last": href(pages),
            "next": if current < pages { href(current + 1) } else { Value::Null },
            "previous": if current > 1 { href(current - 1) } else { Value::Null },
        },
        "resources": resources,
    });
    if let Some(included) = included {
        body["included"] = included;
    }
    body
}

#[cfg(test)]
mod tests {
    use super::*;

    fn query(text: &str) -> Vec<(String, String)> {
        form_urlencoded::parse(text.as_bytes())
            .into_owned()
            .collect()
    }

    /// What the stats give of an app of 2 instances, as the CLI reads them.
    #[test]
    fn the_stats_list_what_is_alive() {
        let mut state = State::new("o", "s", &["localhost".into()]);
        let guid = state.create_app("a", crate::store::STACK).guid.clone();
        let domain = state.domains[0].guid.clone();
        let app = state.app_mut(&guid).unwrap();
        app.process.instances = 2;
        // the instances of `alive`'s indices, each up since the epoch's first second
        let listed = |alive: Value, down: Value| -> ListedApp {
            let alive: Vec<Value> = alive
                .as_array()
                .unwrap()
                .iter()
                .map(|index| json!({"index": index, "since": 1}))
                .collect();
            let listed: ListedApp<Value> = serde_json::from_value(json!({
                "name": "a", "owner": "cf", "hosts": [], "deployed": "now",
                "component": {"sha256": "", "name": ""}, "sandbox": {},
                "alive": alive, "down": down,
            }))
            .unwrap();
            listed.typed(Runtime::Wasm).unwrap()
        };
        let entries = |state: &State, listed: Option<&ListedApp>| {
            let app = state.app(&guid).unwrap();
            let stats = stats(state, app, "cf", listed);
            let resources = stats["resources"].as_array().unwrap().clone();
            let entry = |entry: &Value| {
                let details = entry["details"].as_str().map(String::from);
                let index = entry["index"].as_u64().unwrap();
                (index, entry["state"].as_str().unwrap().to_string(), details)
            };
            resources.iter().map(entry).collect::<Vec<_>>()
        };
        let some = |index, instance_state: &str| (index, instance_state.to_string(), None);
        let one = listed(json!([0]), Value::Null);

        // stopped, never started: down
        let down = vec![some(0, "DOWN"), some(1, "DOWN")];
        assert_eq!(entries(&state, Some(&one)), down);
        let app = state.app_mut(&guid).unwrap();
        app.started = true;
        app.running = Some(crate::store::Running {
            droplet: "d".into(),
            sha256: "ab".repeat(32),
            file: "a.wasm".into(),
            files: None,
            memory_mib: 64,
            budget_ms: None,
            lifetime: crate::store::Lifetime::default(),
            wasi: Vec::new(),
            keyvalue: Default::default(),
            env: Default::default(),
            egress: Vec::new(),
            tag: None,
            since: 0,
        });

        // without a route: nothing for the wicket to serve
        assert_eq!(entries(&state, None), vec![]);
        let route = state.create_route("a", &domain).guid.clone();
        state.map(&route, &guid);

        // the wicket's answer awaited, away, refused
        let starting = vec![some(0, "STARTING"), some(1, "STARTING")];
        assert_eq!(entries(&state, None), starting);
        state.wicket.insert(guid.clone(), Outcome::Unreachable);
        assert_eq!(entries(&state, None), starting);
        let refused = Outcome::Refused("no".into());
        state.wicket.insert(guid.clone(), refused);
        let crashed = |index| (index, "CRASHED".to_string(), Some("no".to_string()));
        assert_eq!(entries(&state, None), [crashed(0), crashed(1)]);

        // taken, and gone from its listing or the listing not read
        state.wicket.insert(guid.clone(), Outcome::Running);
        assert_eq!(entries(&state, None), starting);

        // what is alive, by the index `cf logs` names, one index one entry; none is no failure
        for (alive, listed_as) in [
            (json!([]), vec![]),
            (json!([0]), vec![some(0, "RUNNING")]),
            (json!([1]), vec![some(1, "RUNNING")]),
            (json!([2]), vec![some(0, "RUNNING")]),
            (json!([0, 2, 4]), vec![some(0, "RUNNING")]),
            (json!([1, 2]), vec![some(0, "RUNNING"), some(1, "RUNNING")]),
        ] {
            assert_eq!(
                entries(&state, Some(&listed(alive.clone(), Value::Null))),
                listed_as,
                "{alive}"
            );
        }

        // a VM it keeps gone down, at an index no other holds: a failure of all at none alive
        let why = |index| (index, "CRASHED".to_string(), Some("exit 83".to_string()));
        let exit_83 = json!({"why": "exit 83", "since": 1});
        for (alive, listed_as) in [
            (json!([]), vec![why(0)]),
            (json!([0]), vec![some(0, "RUNNING"), why(1)]),
            (json!([1]), vec![why(0), some(1, "RUNNING")]),
            (json!([0, 1]), vec![some(0, "RUNNING"), some(1, "RUNNING")]),
        ] {
            assert_eq!(
                entries(&state, Some(&listed(alive.clone(), exit_83.clone()))),
                listed_as,
                "{alive}"
            );
        }

        // the VMs it keeps and has not up yet are starting, the rest of a limit is no entry, and
        // one down leaves what the CLI takes for a failure of all
        let running = |index| some(index, "RUNNING");
        let starting = |index| some(index, "STARTING");
        for (sandbox, alive, down, listed_as) in [
            (
                json!({"mode": "pool", "instances": 2}),
                json!([0]),
                Value::Null,
                vec![running(0), starting(1)],
            ),
            (
                json!({"mode": "pool", "instances": 2}),
                json!([1]),
                Value::Null,
                vec![starting(0), running(1)],
            ),
            (
                json!({"mode": "pool", "instances": 2}),
                json!([]),
                Value::Null,
                vec![starting(0), starting(1)],
            ),
            (
                json!({"mode": "pool", "instances": 2}),
                json!([0, 1]),
                Value::Null,
                vec![running(0), running(1)],
            ),
            (
                json!({"mode": "pool", "instances": 2}),
                json!([]),
                exit_83.clone(),
                vec![why(0)],
            ),
            (
                json!({"mode": "pool", "instances": 2, "warm": 1}),
                json!([]),
                Value::Null,
                vec![starting(0)],
            ),
            (
                json!({"mode": "pool", "instances": 2, "warm": 1}),
                json!([1]),
                Value::Null,
                vec![running(1)],
            ),
            (
                json!({"mode": "pool", "instances": 2, "warm": 0}),
                json!([]),
                Value::Null,
                vec![],
            ),
            (
                json!({"mode": "boot", "instances": 2}),
                json!([0]),
                Value::Null,
                vec![running(0), starting(1)],
            ),
            (
                json!({"mode": "boot"}),
                json!([]),
                Value::Null,
                vec![starting(0)],
            ),
            (
                json!({"mode": "reuse", "instances": 2, "warm": 1}),
                json!([]),
                Value::Null,
                vec![],
            ),
        ] {
            let mut kept = listed(alive.clone(), down);
            kept.app.sandbox = Sandbox::Vm(serde_json::from_value(sandbox.clone()).unwrap());
            assert_eq!(entries(&state, Some(&kept)), listed_as, "{sandbox} {alive}");
        }

        // a VM of 3 down at its own index alone, the others serving on or starting unlisted
        state.app_mut(&guid).unwrap().process.instances = 3;
        let mut two = listed(json!([0]), exit_83.clone());
        two.crashed =
            serde_json::from_value(json!([{"index": 2, "why": "exit 83", "since": 1}])).unwrap();
        assert_eq!(
            entries(&state, Some(&two)),
            [some(0, "RUNNING"), why(2)],
            "index 1 still booting"
        );
        two.crashed = serde_json::from_value(json!([
            {"index": 1, "why": "exit 83", "since": 1},
            {"index": 2, "why": "exit 83", "since": 1},
        ]))
        .unwrap();
        assert_eq!(
            entries(&state, Some(&two)),
            [some(0, "RUNNING"), why(1), why(2)]
        );
        state.app_mut(&guid).unwrap().process.instances = 2;

        // each entry up since its own start: the lowest index's of those it holds; what is short,
        // since it went down
        let started = epoch_secs();
        let down = json!({"why": "exit 83", "since": started - 50});
        let mut one = listed(json!([0]), down);
        one.alive = serde_json::from_value(json!([
            {"index": 1, "since": started - 100},
            {"index": 3, "since": started - 200},
        ]))
        .unwrap();
        let uptimes = stats(&state, state.app(&guid).unwrap(), "cf", Some(&one));
        let uptime = |at: usize| uptimes["resources"][at]["uptime"].as_u64().unwrap();
        assert_eq!(uptimes["resources"][1]["state"], "RUNNING");
        assert_eq!(uptimes["resources"][1]["routable"], true);
        assert_eq!(uptimes["resources"][0]["routable"], false, "CRASHED");
        assert!((100..110).contains(&uptime(1)), "{uptimes}");
        assert_eq!(uptimes["resources"][0]["state"], "CRASHED");
        assert!((50..60).contains(&uptime(0)), "{uptimes}");
    }

    #[test]
    fn selects_orders_and_paginates_as_the_cli_asks() {
        let items = vec![
            json!({"guid": "1", "name": "b", "destinations": [{"app": {"guid": "x"}}]}),
            json!({"guid": "2", "name": "a", "destinations": []}),
            json!({"guid": "3", "name": "c", "relationships": {"app": {"data": {"guid": "x"}}}}),
        ];
        let named = select(items.clone(), &query("names=a,c&order_by=-name"));
        assert_eq!(named[0]["guid"], "3");
        assert_eq!(named[1]["guid"], "2");
        let of_x = select(items.clone(), &query("app_guids=x"));
        assert_eq!(of_x.len(), 2);
        assert!(select(items.clone(), &query("paths=")).is_empty());
        assert_eq!(select(items.clone(), &query("unknown=1")).len(), 3);
        let paged = page(
            "http://h",
            "/v3/apps",
            &query("names=a,c&page=2&per_page=1"),
            named,
            None,
        );
        assert_eq!(paged["pagination"]["total_results"], 2);
        assert_eq!(paged["pagination"]["total_pages"], 2);
        assert_eq!(paged["resources"][0]["guid"], "2");
        assert_eq!(paged["pagination"]["next"], Value::Null);
        assert_eq!(
            paged["pagination"]["previous"]["href"],
            "http://h/v3/apps?names=a%2Cc&page=1&per_page=1"
        );
    }
}
