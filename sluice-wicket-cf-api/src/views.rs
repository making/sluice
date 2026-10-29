//! The resources in Cloud Controller v3's shapes, their lists filtered,
//! ordered and paginated as the CLI asks.

use serde_json::{Value, json};

use crate::controller::Controller;
use crate::store::{
    App, Build, Deployment, Droplet, Job, Named, Outcome, Package, Route, STACK, State, epoch_secs,
    now,
};

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

pub fn info(base: &str, build: &str) -> Value {
    json!({
        "build": build,
        "cli_version": {"minimum": "", "recommended": ""},
        "custom": {},
        "description": "sluice-wicket-cf-api: deploys wasm components to a sluice-wicket-wasi with the cf CLI",
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

pub fn stack(base: &str) -> Value {
    json!({
        "guid": STACK, "created_at": "2026-01-01T00:00:00Z", "updated_at": "2026-01-01T00:00:00Z",
        "name": STACK, "description": "WASI 0.3 components, served by sluice-wicket-wasi",
        "build_rootfs_image": STACK, "run_rootfs_image": STACK, "default": true,
        "metadata": metadata(None),
        "links": {"self": {"href": format!("{base}/v3/stacks/{STACK}")}},
    })
}

pub fn app(base: &str, state: &State, app: &App) -> Value {
    let guid = &app.guid;
    json!({
        "guid": guid, "created_at": app.created_at, "updated_at": app.updated_at,
        "name": app.name,
        "state": if app.started { "STARTED" } else { "STOPPED" },
        "lifecycle": {"type": "buildpack", "data": {"buildpacks": app.buildpacks, "stack": app.stack}},
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

/// The process stats: `instances` entries, `RUNNING` once the wicket took
/// the app, `CRASHED` with its refusal as `details`, `STARTING` while it
/// is unreachable, `DOWN` for a stopped app.
pub fn stats(state: &State, app: &App, owner: &str) -> Value {
    let (instance_state, details, since) = match (&app.running, app.started) {
        (Some(running), true) => match state.wicket.get(&app.guid) {
            Some(Outcome::Running) => ("RUNNING", None, running.since),
            Some(Outcome::Refused(detail)) => ("CRASHED", Some(detail.clone()), running.since),
            Some(Outcome::Unreachable) => ("STARTING", None, running.since),
            // no answer yet: an app without a route is nothing the wicket serves
            None if Controller::desired(state, app, owner).is_none() => {
                ("RUNNING", None, running.since)
            }
            None => ("STARTING", None, running.since),
        },
        _ => ("DOWN", None, epoch_secs()),
    };
    let uptime = epoch_secs().saturating_sub(since);
    let resources: Vec<Value> = (0..app.process.instances)
        .map(|index| {
            json!({
                "type": "web", "index": index, "state": instance_state, "routable": true,
                "usage": {"time": now(), "cpu": 0.0, "cpu_entitlement": 0.0, "mem": 0, "disk": 0, "log_rate": 0},
                "host": "wicket", "instance_internal_ip": "127.0.0.1", "instance_ports": [],
                "uptime": uptime,
                "mem_quota": app.process.memory_mb << 20,
                "disk_quota": app.process.disk_mb << 20,
                "log_rate_limit": app.process.log_rate_limit,
                "fds_quota": 16384, "isolation_segment": null,
                "details": details, "port": null,
            })
        })
        .collect();
    json!({"resources": resources})
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

pub fn package(base: &str, package: &Package) -> Value {
    json!({
        "guid": package.guid, "created_at": package.created_at, "updated_at": package.updated_at,
        "type": "bits",
        "data": {"error": null, "checksum": {"type": "sha256", "value": package.sha256}},
        "state": package.state,
        "relationships": {"app": rel(Some(&package.app))},
        "metadata": metadata(None),
        "links": {
            "self": {"href": format!("{base}/v3/packages/{}", package.guid)},
            "upload": {"href": format!("{base}/v3/packages/{}/upload", package.guid), "method": "POST"},
            "download": {"href": format!("{base}/v3/packages/{}/download", package.guid)},
            "app": {"href": format!("{base}/v3/apps/{}", package.app)},
        },
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
        "lifecycle": {"type": "buildpack", "data": {
            "buildpacks": app.map_or(Vec::new(), |app| app.buildpacks.clone()),
            "stack": app.map_or(STACK.to_string(), |app| app.stack.clone()),
        }},
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

pub fn droplet(base: &str, droplet: &Droplet) -> Value {
    json!({
        "guid": droplet.guid, "created_at": droplet.created_at, "updated_at": droplet.updated_at,
        "state": "STAGED", "error": null,
        "lifecycle": {"type": "buildpack", "data": {}},
        "execution_metadata": "",
        "process_types": {"web": droplet.file},
        "checksum": {"type": "sha256", "value": droplet.sha256},
        "buildpacks": [{"name": "wasm", "detect_output": "wasm", "buildpack_name": "wasm", "version": "0.0.1"}],
        "stack": STACK, "image": null,
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

/// The app's manifest, as `cf create-app-manifest` writes it.
pub fn manifest(state: &State, app: &App) -> String {
    let mut text = format!("---\napplications:\n- name: {}\n", app.name);
    text.push_str(&format!(
        "  instances: {}\n  memory: {}M\n  disk_quota: {}M\n  stack: {}\n",
        app.process.instances, app.process.memory_mb, app.process.disk_mb, app.stack
    ));
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
    // a JSON string is a YAML scalar
    let quoted = |text: &str| serde_json::to_string(text).expect("a string");
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
const FILTERS: [(&str, &str); 22] = [
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
