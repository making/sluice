//! The manifest `cf push` applies (`POST /v3/spaces/:guid/actions/apply_manifest`,
//! and `manifest_diff` before it with a manifest file): the apps, their web
//! process, routes and metadata are applied server side, as on CF. What the
//! wicket cannot serve is refused, naming it.

use std::collections::BTreeMap;

use serde::Deserialize;
use serde::de::IgnoredAny;

use crate::store::{BUDGET_ANNOTATION, INSTANCES_ANNOTATION, State, now};

#[derive(Deserialize, Debug, Default)]
pub struct Manifest {
    #[serde(default)]
    pub applications: Vec<AppManifest>,
}

#[derive(Deserialize, Debug, Default)]
#[serde(rename_all = "kebab-case")]
pub struct AppManifest {
    pub name: String,
    pub default_route: Option<bool>,
    pub random_route: Option<bool>,
    pub no_route: Option<bool>,
    pub routes: Option<Vec<RouteManifest>>,
    pub instances: Option<u64>,
    pub memory: Option<Scalar>,
    #[serde(rename = "disk_quota", alias = "disk-quota")]
    pub disk_quota: Option<Scalar>,
    pub command: Option<Scalar>,
    pub buildpacks: Option<Vec<String>>,
    pub buildpack: Option<String>,
    pub stack: Option<String>,
    pub health_check_type: Option<String>,
    pub health_check_http_endpoint: Option<String>,
    pub health_check_invocation_timeout: Option<u64>,
    pub timeout: Option<u64>,
    pub env: Option<BTreeMap<String, Option<Scalar>>>,
    pub services: Option<Vec<IgnoredAny>>,
    pub docker: Option<IgnoredAny>,
    pub sidecars: Option<IgnoredAny>,
    pub processes: Option<Vec<ProcessManifest>>,
    pub metadata: Option<Metadata>,
}

#[derive(Deserialize, Debug)]
pub struct RouteManifest {
    pub route: String,
    pub protocol: Option<String>,
}

#[derive(Deserialize, Debug, Default)]
#[serde(rename_all = "kebab-case")]
pub struct ProcessManifest {
    #[serde(rename = "type")]
    pub kind: String,
    pub instances: Option<u64>,
    pub memory: Option<Scalar>,
    #[serde(rename = "disk_quota", alias = "disk-quota")]
    pub disk_quota: Option<Scalar>,
    pub command: Option<Scalar>,
    pub health_check_type: Option<String>,
    pub health_check_http_endpoint: Option<String>,
    pub health_check_invocation_timeout: Option<u64>,
    pub timeout: Option<u64>,
}

#[derive(Deserialize, Debug, Default)]
pub struct Metadata {
    pub labels: Option<BTreeMap<String, Option<Scalar>>>,
    pub annotations: Option<BTreeMap<String, Option<Scalar>>>,
}

/// A YAML scalar, whatever it was typed as.
#[derive(Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum Scalar {
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
}

impl Scalar {
    pub fn text(&self) -> String {
        match self {
            Scalar::Bool(value) => value.to_string(),
            Scalar::Int(value) => value.to_string(),
            Scalar::Float(value) => value.to_string(),
            Scalar::Text(value) => value.clone(),
        }
    }
}

pub fn parse(text: &str) -> Result<Manifest, String> {
    serde_saphyr::from_str(text).map_err(|e| format!("the manifest: {e}"))
}

/// Applies `manifest` to `state`: the guids of the apps it names.
pub fn apply(state: &mut State, manifest: &Manifest) -> Result<Vec<String>, String> {
    let mut guids = Vec::new();
    for spec in &manifest.applications {
        guids.push(apply_app(state, spec)?);
    }
    Ok(guids)
}

fn apply_app(state: &mut State, spec: &AppManifest) -> Result<String, String> {
    let name = spec.name.trim();
    if name.is_empty() {
        return Err("an application needs a name".into());
    }
    if spec.docker.is_some() {
        return Err(format!(
            "{name}: docker images are not supported: push a directory or a zip holding the component"
        ));
    }
    if spec.env.as_ref().is_some_and(|env| !env.is_empty()) {
        return Err(format!(
            "{name}: environment variables are not supported yet"
        ));
    }
    if spec
        .services
        .as_ref()
        .is_some_and(|services| !services.is_empty())
    {
        return Err(format!("{name}: services are not supported yet"));
    }
    if spec.sidecars.is_some() {
        return Err(format!("{name}: sidecars are not supported"));
    }
    let mut web: Option<&ProcessManifest> = None;
    for process in spec.processes.iter().flatten() {
        if process.kind != "web" {
            return Err(format!(
                "{name}: process types but web are not supported (found '{}')",
                process.kind
            ));
        }
        web = Some(process);
    }
    let guid = match state.app_named(name) {
        Some(app) => app.guid.clone(),
        None => state.create_app(name).guid.clone(),
    };
    let app = state.app_mut(&guid).expect("just found or created");
    if let Some(buildpacks) = &spec.buildpacks {
        app.buildpacks = buildpacks.clone();
    } else if let Some(buildpack) = &spec.buildpack {
        app.buildpacks = vec![buildpack.clone()];
    }
    if let Some(stack) = &spec.stack {
        app.stack = stack.clone();
    }
    if let Some(metadata) = &spec.metadata {
        merge(&mut app.labels, metadata.labels.as_ref());
        merge(&mut app.annotations, metadata.annotations.as_ref());
        check_annotations(&app.annotations).map_err(|e| format!("{name}: {e}"))?;
    }
    let process = &mut app.process;
    let fields = [(
        spec.instances,
        spec.memory.as_ref(),
        spec.disk_quota.as_ref(),
        spec.command.as_ref(),
        spec.health_check_type.as_deref(),
        spec.health_check_http_endpoint.as_deref(),
        spec.health_check_invocation_timeout,
        spec.timeout,
    )]
    .into_iter()
    .chain(web.map(|web| {
        (
            web.instances,
            web.memory.as_ref(),
            web.disk_quota.as_ref(),
            web.command.as_ref(),
            web.health_check_type.as_deref(),
            web.health_check_http_endpoint.as_deref(),
            web.health_check_invocation_timeout,
            web.timeout,
        )
    }));
    for (instances, memory, disk, command, check, endpoint, invocation, timeout) in fields {
        if let Some(instances) = instances {
            process.instances = instances;
        }
        if let Some(memory) = memory {
            process.memory_mb = megabytes(memory).map_err(|e| format!("{name}: memory: {e}"))?;
        }
        if let Some(disk) = disk {
            process.disk_mb = megabytes(disk).map_err(|e| format!("{name}: disk_quota: {e}"))?;
        }
        if let Some(command) = command {
            process.command = Some(command.text()).filter(|command| !command.is_empty());
        }
        if let Some(check) = check {
            process.health_check.kind = check.to_string();
        }
        if let Some(endpoint) = endpoint {
            process.health_check.endpoint = Some(endpoint.to_string());
        }
        if let Some(invocation) = invocation {
            process.health_check.invocation_timeout = Some(invocation);
        }
        if let Some(timeout) = timeout {
            process.health_check.timeout = Some(timeout);
        }
    }
    process.updated_at = now();
    app.updated_at = now();
    apply_routes(state, &guid, spec)?;
    Ok(guid)
}

/// `routes:` adds routes; `no-route` unmaps them all; `default-route` /
/// `random-route` map one when the app has none.
fn apply_routes(state: &mut State, guid: &str, spec: &AppManifest) -> Result<(), String> {
    let name = spec.name.trim().to_string();
    if spec.no_route == Some(true) {
        let mapped: Vec<String> = state
            .routes_of(guid)
            .map(|route| route.guid.clone())
            .collect();
        for route in mapped {
            state.unmap(&route);
        }
        return Ok(());
    }
    if let Some(routes) = &spec.routes {
        for entry in routes {
            if entry
                .protocol
                .as_deref()
                .is_some_and(|protocol| protocol != "http1")
            {
                return Err(format!(
                    "{name}: route {}: only http1 routes are served",
                    entry.route
                ));
            }
            let (host, domain) = parse_route(state, &entry.route)?;
            map_route(state, guid, &host, &domain)?;
        }
    }
    if state.routes_of(guid).next().is_none() {
        let domain = state
            .domains
            .first()
            .ok_or("no shared domain: --domain names one at least")?
            .guid
            .clone();
        if spec.random_route == Some(true) {
            let host = random_host(&name);
            map_route(state, guid, &host, &domain)?;
        } else if spec.default_route == Some(true) {
            let host = check_host(&name).map_err(|e| format!("{name}: default route: {e}"))?;
            map_route(state, guid, &host, &domain)?;
        }
    }
    Ok(())
}

/// Maps the route (made unless there) to the app: a route serves one app.
pub fn map_route(state: &mut State, guid: &str, host: &str, domain: &str) -> Result<(), String> {
    let route = match state.route_at(host, domain) {
        Some(route) => route.clone(),
        None => state.create_route(host, domain).clone(),
    };
    if let Some(other) = route.app.as_deref().filter(|other| *other != guid) {
        let other = state
            .app(other)
            .map_or("another app", |app| app.name.as_str());
        return Err(format!(
            "route {} is mapped to {other}: a route serves one app",
            state.url(&route)
        ));
    }
    state.map(&route.guid, guid);
    Ok(())
}

/// `<host>.<domain>` against the known domains, the longest first: the host
/// and the domain's guid. A path, a port, a wildcard or a bare domain is
/// refused.
pub fn parse_route(state: &State, url: &str) -> Result<(String, String), String> {
    let url = url.trim().to_ascii_lowercase();
    let url = url
        .strip_prefix("http://")
        .or_else(|| url.strip_prefix("https://"))
        .unwrap_or(&url);
    if let Some((_, path)) = url.split_once('/') {
        return Err(format!(
            "route {url}: a route with a path (/{path}) is not served"
        ));
    }
    if let Some((_, port)) = url.split_once(':') {
        return Err(format!(
            "route {url}: a route with a port (:{port}) is not served"
        ));
    }
    let mut domains: Vec<&crate::store::Named> = state.domains.iter().collect();
    domains.sort_by_key(|domain| std::cmp::Reverse(domain.name.len()));
    for domain in domains {
        if url == domain.name {
            return Err(format!(
                "route {url}: a route is <host>.{}: the bare domain is not served",
                domain.name
            ));
        }
        if let Some(host) = url.strip_suffix(&format!(".{}", domain.name)) {
            let host = check_host(host).map_err(|e| format!("route {url}: {e}"))?;
            return Ok((host, domain.guid.clone()));
        }
    }
    Err(format!(
        "route {url}: no shared domain of it ({}): a route is <host>.<domain>",
        state
            .domains
            .iter()
            .map(|domain| domain.name.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    ))
}

/// The host of a route: labels of `a-z`, `0-9` and `-` (so the wicket takes
/// it); no wildcard.
pub fn check_host(host: &str) -> Result<String, String> {
    let host = host.to_ascii_lowercase();
    if host == "*" || host.starts_with("*.") {
        return Err("a wildcard route is not served".into());
    }
    let label = |label: &str| {
        (1..=63).contains(&label.len())
            && !label.starts_with('-')
            && !label.ends_with('-')
            && label
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    };
    if !host.is_empty() && host.len() <= 253 && host.split('.').all(label) {
        Ok(host)
    } else {
        Err(format!(
            "'{host}' is no host: labels of a-z, 0-9 and '-' (an app named otherwise takes a route of its own)"
        ))
    }
}

/// `<name>-<adjective>-<animal>-<two letters>`, as the CLI's random routes.
fn random_host(name: &str) -> String {
    const ADJECTIVES: [&str; 8] = [
        "brave", "quiet", "shiny", "lucky", "wise", "calm", "eager", "fancy",
    ];
    const ANIMALS: [&str; 8] = [
        "otter", "heron", "lynx", "fox", "elk", "crane", "ibex", "seal",
    ];
    let mut random = [0u8; 4];
    let _ = getrandom::fill(&mut random);
    let letter = |byte: u8| (b'a' + byte % 26) as char;
    let base = check_host(name).unwrap_or_else(|_| "app".into());
    format!(
        "{base}-{}-{}-{}{}",
        ADJECTIVES[usize::from(random[0]) % ADJECTIVES.len()],
        ANIMALS[usize::from(random[1]) % ANIMALS.len()],
        letter(random[2]),
        letter(random[3])
    )
}

/// `64M`, `1G`, `256MB`, `1GB` or a number of megabytes.
pub fn megabytes(size: &Scalar) -> Result<u64, String> {
    let text = size.text().trim().to_ascii_uppercase();
    let (digits, unit) = text
        .find(|c: char| !c.is_ascii_digit())
        .map_or((text.as_str(), ""), |at| text.split_at(at));
    let number: u64 = digits
        .parse()
        .map_err(|_| format!("'{}' is no size (e.g. 64M, 1G)", size.text()))?;
    let factor = match unit.trim() {
        "" | "M" | "MB" => 1,
        "G" | "GB" => 1024,
        _ => return Err(format!("'{}' is no size (e.g. 64M, 1G)", size.text())),
    };
    let megabytes = number * factor;
    if megabytes == 0 {
        return Err("must be at least 1M".into());
    }
    Ok(megabytes)
}

/// A null value removes the key, as on CF.
fn merge(into: &mut BTreeMap<String, String>, from: Option<&BTreeMap<String, Option<Scalar>>>) {
    for (key, value) in from.into_iter().flatten() {
        match value {
            Some(value) => {
                into.insert(key.clone(), value.text());
            }
            None => {
                into.remove(key);
            }
        }
    }
}

/// The wicket's annotations hold numbers.
pub fn check_annotations(annotations: &BTreeMap<String, String>) -> Result<(), String> {
    for key in [BUDGET_ANNOTATION, INSTANCES_ANNOTATION] {
        if let Some(value) = annotations.get(key)
            && value.parse::<u64>().ok().is_none_or(|number| number == 0)
        {
            return Err(format!("annotation {key}: '{value}' is no number from 1"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state() -> State {
        State::new("o", "s", &["localhost".into(), "example.com".into()])
    }

    #[test]
    fn applies_the_process_the_routes_and_the_metadata() {
        let mut state = state();
        let manifest = parse(
            "applications:\n- name: counter\n  path: /tmp/x\n  command: counter.wasm\n  instances: 2\n  memory: 64M\n  default-route: true\n  routes:\n  - route: counter.localhost\n  - route: count.example.com\n  metadata:\n    annotations:\n      wicket.sluice.ik.am/budget-ms: 5000\n  unknown-key: ignored\n",
        )
        .unwrap();
        let guids = apply(&mut state, &manifest).unwrap();
        let app = state.app(&guids[0]).unwrap();
        assert_eq!(app.name, "counter");
        assert_eq!(app.process.instances, 2);
        assert_eq!(app.process.memory_mb, 64);
        assert_eq!(app.process.command.as_deref(), Some("counter.wasm"));
        assert_eq!(app.annotations[BUDGET_ANNOTATION], "5000");
        let mut urls: Vec<String> = state
            .routes_of(&guids[0])
            .map(|route| state.url(route))
            .collect();
        urls.sort();
        assert_eq!(urls, ["count.example.com", "counter.localhost"]);

        // again: the same app, the routes kept; a default route only without any
        let again = parse("applications:\n- name: counter\n  default-route: true\n  memory: 128\n")
            .unwrap();
        assert_eq!(apply(&mut state, &again).unwrap(), guids);
        assert_eq!(state.app(&guids[0]).unwrap().process.memory_mb, 128);
        assert_eq!(state.routes_of(&guids[0]).count(), 2);

        // a second app takes its default route; a route serves one app
        let other = parse("applications:\n- name: other\n  default-route: true\n").unwrap();
        let other = apply(&mut state, &other).unwrap()[0].clone();
        assert_eq!(
            state.url(state.routes_of(&other).next().unwrap()),
            "other.localhost"
        );
        let taken =
            parse("applications:\n- name: other\n  routes:\n  - route: counter.localhost\n")
                .unwrap();
        assert!(
            apply(&mut state, &taken)
                .unwrap_err()
                .contains("mapped to counter")
        );

        // no-route unmaps; random-route names one
        let none = parse("applications:\n- name: other\n  no-route: true\n").unwrap();
        apply(&mut state, &none).unwrap();
        assert_eq!(state.routes_of(&other).count(), 0);
        let random = parse("applications:\n- name: other\n  random-route: true\n").unwrap();
        apply(&mut state, &random).unwrap();
        let url = state.url(state.routes_of(&other).next().unwrap());
        assert!(
            url.starts_with("other-") && url.ends_with(".localhost"),
            "{url}"
        );
    }

    #[test]
    fn refuses_what_the_wicket_cannot_serve() {
        let mut state = state();
        let mut refused = |text: &str| {
            let manifest = parse(text).unwrap();
            apply(&mut state, &manifest).unwrap_err()
        };
        assert!(refused("applications:\n- name: a\n  docker:\n    image: x\n").contains("docker"));
        assert!(refused("applications:\n- name: a\n  env:\n    K: v\n").contains("environment"));
        assert!(refused("applications:\n- name: a\n  services:\n  - kv\n").contains("services"));
        assert!(
            refused("applications:\n- name: a\n  processes:\n  - type: worker\n")
                .contains("worker")
        );
        assert!(
            refused("applications:\n- name: a\n  routes:\n  - route: a.localhost/path\n")
                .contains("path")
        );
        assert!(
            refused("applications:\n- name: a\n  routes:\n  - route: a.localhost:8080\n")
                .contains("port")
        );
        assert!(
            refused("applications:\n- name: a\n  routes:\n  - route: '*.localhost'\n")
                .contains("wildcard")
        );
        assert!(
            refused("applications:\n- name: a\n  routes:\n  - route: localhost\n")
                .contains("bare domain")
        );
        assert!(
            refused("applications:\n- name: a\n  routes:\n  - route: a.other.io\n")
                .contains("no shared domain")
        );
        assert!(refused("applications:\n- name: a_b\n  default-route: true\n").contains("no host"));
        assert!(refused("applications:\n- name: a\n  memory: 2 pounds\n").contains("no size"));
        assert!(refused("applications:\n- name: a\n  metadata:\n    annotations:\n      wicket.sluice.ik.am/instances: many\n").contains("no number"));
        assert!(parse("applications: [").is_err());
    }
}
