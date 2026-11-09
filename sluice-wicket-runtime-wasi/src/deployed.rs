//! The routes and apps the console deployed (`--console`): hosts served by
//! uploaded components, kept in the console's directory (`--console-dir`) so a
//! restart serves them again. A route is the page's: one host, its component
//! uploaded with it, the default keyvalue map. An app is the API's (a
//! controller's): hosts served by one component in one sandbox, its component
//! uploaded by digest first, its keyvalue map named, changed by its owner
//! only; its files too, if any (`files`).
//!
//! ```text
//! <dir>/routes.yaml                      the routes and the apps, in the --config file's route shape
//! <dir>/components/<sha256>/<name>.wasm  the components they name, never rewritten
//! <dir>/uploads/<sha256>.wasm            the components uploaded by digest, for UPLOAD_GRACE
//! <dir>/files/<sha256>/                  the apps' files, unpacked from a tar of that digest
//! ```
//!
//! A component's path names its digest, so other content is another target:
//! its route is retargeted, and the old target drains as a removed one does.
//! The routes and apps are held here, `routes.yaml` being their copy: an edit
//! is reloaded first (`routes::Reloader::apply`) and written once the reload
//! published it, so the file never names routes that did not load. It holds
//! the apps' env, so only its owner reads it (0600). A component file
//! nothing names is removed after every edit and at startup, an upload once
//! `UPLOAD_GRACE` passed: an app naming it links it below `components/`
//! before. An app put with `held-ms` is held at each start until its owner
//! confirms it (`sluice_wicket_console::held`, [`Deployed::held`]).

use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use serde::Deserialize;
use sluice_wicket_console::held::check_held_ms;
use sluice_wicket_console::store::{
    Blobs, UPLOADS, check_digest, check_file, check_host, check_hosts, check_kept, check_name,
    check_tag, collect_uploads, digest, file_name, rfc3339, write_by_rename,
};

use crate::config::{SandboxFile, quote, sandbox_yaml};
use crate::files::{self, FILES};
use crate::keyvalue::{BucketMap, Place, Stores};
use crate::routes::{Origin, Refused, Spec};
use crate::wasm_host::{Caps, Locator, Sandbox};

/// The routes' file in the directory.
const MANIFEST: &str = "routes.yaml";

/// The components' directory in the directory.
const COMPONENTS: &str = "components";

/// One of the console's routes.
#[derive(Clone, PartialEq, Debug)]
pub struct Route {
    pub host: String,
    /// The component file relative to the directory,
    /// `components/<sha256>/<name>`.
    pub component: String,
    /// Without a keyvalue map: with `wasi=keyvalue` the default one applies
    /// (the default store, the host as the namespace). Without an env.
    pub sandbox: Sandbox,
    /// When it took this component and sandbox, RFC 3339.
    pub deployed: String,
}

/// One app of the console's API: hosts served by one component in one
/// sandbox, put and removed by its owner only.
#[derive(Clone, PartialEq, Debug)]
pub struct App {
    pub name: String,
    pub owner: String,
    /// Sorted, each once.
    pub hosts: Vec<String>,
    /// The component file relative to the directory,
    /// `components/<sha256>/<name>`.
    pub component: String,
    /// Its files' directory relative to the directory, `files/<sha256>`.
    pub files: Option<String>,
    /// With its keyvalue map in full: an app has no one host to default a
    /// namespace to; without its files (`files`).
    pub sandbox: Sandbox,
    /// The owner's, kept and listed as given ([`check_tag`]).
    pub tag: Option<String>,
    /// How long a start of the wicket holds it at most, until its owner confirms it.
    pub held_ms: Option<u64>,
    /// When it took this component and sandbox, RFC 3339.
    pub deployed: String,
}

/// An app as its PUT has it.
#[derive(Clone, PartialEq, Debug)]
pub struct AppSpec {
    pub owner: String,
    pub hosts: Vec<String>,
    /// The component's digest.
    pub sha256: String,
    /// The file name the component is kept as.
    pub file: String,
    /// The digest of its files' tar.
    pub files: Option<String>,
    /// With its keyvalue map ([`app_map`]).
    pub sandbox: Sandbox,
    pub tag: Option<String>,
    pub held_ms: Option<u64>,
}

/// The console's routes in the order deployed, its apps in the order put,
/// and their directory.
#[derive(Clone, PartialEq, Debug)]
pub struct Deployed {
    dir: PathBuf,
    routes: Vec<Route>,
    apps: Vec<App>,
}

impl Deployed {
    /// The routes and apps kept in `dir`, created when missing; removes the
    /// component files nothing names (an upload a crash cut short included).
    pub fn open(dir: &Path) -> Result<Deployed, String> {
        let failed = |e: std::io::Error| format!("--console-dir {}: {e}", dir.display());
        std::fs::create_dir_all(dir.join(COMPONENTS)).map_err(failed)?;
        let dir = dir.canonicalize().map_err(failed)?;
        let manifest = dir.join(MANIFEST);
        let (routes, apps) = match std::fs::read_to_string(&manifest) {
            Ok(text) => read(&text).map_err(|e| format!("{}: {e}", manifest.display()))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Default::default(),
            Err(e) => return Err(format!("{}: {e}", manifest.display())),
        };
        let deployed = Deployed { dir, routes, apps };
        deployed.collect(true);
        Ok(deployed)
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// The apps a start holds until their owners confirm them: each one's owner, name and
    /// `held-ms`.
    pub fn held(&self) -> impl Iterator<Item = (String, String, Duration)> + '_ {
        self.apps.iter().filter_map(|app| {
            let held = Duration::from_millis(app.held_ms?);
            Some((app.owner.clone(), app.name.clone(), held))
        })
    }

    /// The routes, then the apps' (one per host), as specs; a keyvalue route
    /// gets the default map of `stores`. Errors name a component by its path
    /// in the directory.
    pub fn specs(&self, stores: &Stores) -> Vec<Spec> {
        let routes = self.routes.iter().map(|route| {
            let mut sandbox = route.sandbox.clone();
            if sandbox.caps.keyvalue {
                sandbox.keyvalue = stores.default_map(&route.host);
            }
            Spec {
                host: route.host.clone(),
                component: Locator::File(self.dir.join(&route.component)),
                shown: Some(route.component.clone()),
                sandbox,
                origin: Origin::Console {
                    deployed: route.deployed.clone(),
                },
            }
        });
        let apps = self.apps.iter().flat_map(|app| {
            let sandbox = Sandbox {
                files: app.files.as_ref().map(|files| self.dir.join(files)),
                ..app.sandbox.clone()
            };
            app.hosts.iter().map(move |host| Spec {
                host: host.clone(),
                component: Locator::File(self.dir.join(&app.component)),
                shown: Some(app.component.clone()),
                sandbox: sandbox.clone(),
                origin: Origin::Api {
                    name: app.name.clone(),
                    owner: app.owner.clone(),
                    tag: app.tag.clone(),
                    held_ms: app.held_ms,
                    deployed: app.deployed.clone(),
                },
            })
        });
        routes.chain(apps).collect()
    }

    /// The routes with `host` served by `component`, uploaded as `name`, in
    /// `sandbox` (without a keyvalue map) since `now`; the component's file
    /// written. The same component and sandbox again change nothing.
    pub fn deploy(
        &self,
        host: &str,
        name: &str,
        component: &[u8],
        sandbox: Sandbox,
        now: SystemTime,
    ) -> Result<Deployed, Refused> {
        debug_assert!(sandbox.keyvalue.is_empty(), "the default map applies");
        debug_assert!(sandbox.env.is_empty(), "the page sets no env");
        let host = check_host(host).map_err(Refused::Invalid)?;
        let component = self
            .store(&file_name(name, &host, ".wasm"), component)
            .map_err(Refused::Failed)?;
        Ok(self.with(Route {
            host,
            component,
            sandbox,
            deployed: rfc3339(now),
        }))
    }

    /// The routes with the route of `host` in `sandbox` since `now`.
    pub fn with_sandbox(
        &self,
        host: &str,
        sandbox: Sandbox,
        now: SystemTime,
    ) -> Result<Deployed, Refused> {
        debug_assert!(sandbox.keyvalue.is_empty(), "the default map applies");
        debug_assert!(sandbox.env.is_empty(), "the page sets no env");
        let route = self.route(host)?;
        Ok(self.with(Route {
            sandbox,
            deployed: rfc3339(now),
            ..route.clone()
        }))
    }

    /// The routes without the route of `host`.
    pub fn without(&self, host: &str) -> Result<Deployed, Refused> {
        let host = self.route(host)?.host.clone();
        let mut deployed = self.clone();
        deployed.routes.retain(|route| route.host != host);
        Ok(deployed)
    }

    /// The routes with the route of `host` moved to `to` as it is: its
    /// component, sandbox and time, in its place. A move replaces no route of
    /// the console.
    pub fn moved(&self, host: &str, to: &str) -> Result<Deployed, Refused> {
        let host = self.route(host)?.host.clone();
        let to = check_host(to).map_err(Refused::Invalid)?;
        if to != host && self.routes.iter().any(|route| route.host == to) {
            return Err(Refused::Taken(format!(
                "{to} is a route of the console already"
            )));
        }
        let mut deployed = self.clone();
        for route in &mut deployed.routes {
            if route.host == host {
                route.host = to.clone();
            }
        }
        Ok(deployed)
    }

    fn route(&self, host: &str) -> Result<&Route, Refused> {
        let host = host.to_ascii_lowercase();
        self.routes
            .iter()
            .find(|route| route.host == host)
            .ok_or_else(|| Refused::Unknown(format!("the console routes no host '{host}'")))
    }

    /// The routes with `route` in place of the one of its host -- which stays
    /// as it was, its time included, when the component and the sandbox are
    /// the same -- or added last.
    fn with(&self, route: Route) -> Deployed {
        let mut deployed = self.clone();
        match deployed.routes.iter_mut().find(|r| r.host == route.host) {
            Some(r) if r.component == route.component && r.sandbox == route.sandbox => {}
            Some(r) => *r = route,
            None => deployed.routes.push(route),
        }
        deployed
    }

    /// Keeps `component` as the upload of `sha256`, its digest, by a rename:
    /// the file's time is the upload's, so an upload again keeps it longer.
    pub fn upload(&self, sha256: &str, component: &[u8]) -> Result<(), Refused> {
        let given = check_digest(sha256).map_err(Refused::Invalid)?;
        let actual = digest(component);
        if actual != given {
            return Err(Refused::Invalid(format!(
                "the body's sha256 is {actual}, not {given}"
            )));
        }
        let path = self.dir.join(UPLOADS).join(format!("{given}.wasm"));
        write_by_rename(&path, component, false).map_err(Refused::Failed)
    }

    /// The apps with the app `name` as `app` has it since `now`: its
    /// component made from an upload or another file of its digest, in place
    /// of the app of that name -- which keeps its time when its component,
    /// files and sandbox stay, its tag and `held-ms` changing or not -- or
    /// added last.
    /// Another owner's app is refused, and so are a component and files of a
    /// digest not kept (all of them named).
    pub fn with_app(&self, name: &str, app: AppSpec, now: SystemTime) -> Result<Deployed, Refused> {
        check_name("app", name).map_err(Refused::Invalid)?;
        check_name("owner", &app.owner).map_err(Refused::Invalid)?;
        let hosts = check_hosts(&app.hosts).map_err(Refused::Invalid)?;
        let sha256 = check_digest(&app.sha256)
            .map_err(|e| Refused::Invalid(format!("component.sha256: {e}")))?;
        check_file(&app.file, ".wasm", "a component")
            .map_err(|e| Refused::Invalid(format!("component.name: {e}")))?;
        if let Some(tag) = &app.tag {
            check_tag(tag).map_err(Refused::Invalid)?;
        }
        debug_assert!(
            app_map(app.sandbox.caps, map_specs(&app.sandbox.keyvalue)).is_ok(),
            "an app names its map with the capability"
        );
        if let Some(kept) = self.app(name)
            && kept.owner != app.owner
        {
            return Err(Refused::Taken(format!(
                "the app '{name}' is {}'s, not {}'s",
                kept.owner, app.owner
            )));
        }
        debug_assert!(app.sandbox.files.is_none(), "an app's files are its own");
        let files = app
            .files
            .as_deref()
            .map(|files| {
                check_digest(files).map_err(|e| Refused::Invalid(format!("files.sha256: {e}")))
            })
            .transpose()?;
        let component = self.materialize(&sha256, &app.file);
        let kept_files = files.as_ref().map(|files| {
            let relative = format!("{FILES}/{files}");
            match files::is_dir(&self.dir.join(&relative)) {
                true => Ok(relative),
                false => Err(files.clone()),
            }
        });
        let (component, files) = match (component, kept_files.transpose()) {
            (Ok(component), Ok(files)) => (component, files),
            (Err(Refused::Missing(mut missing)), Err(files)) => {
                missing.push(files);
                return Err(Refused::Missing(missing));
            }
            (Ok(_), Err(files)) => return Err(Refused::Missing(vec![files])),
            (Err(refused), _) => return Err(refused),
        };
        let app = App {
            name: name.to_string(),
            owner: app.owner,
            hosts,
            component,
            files,
            sandbox: app.sandbox,
            tag: app.tag,
            held_ms: app.held_ms,
            deployed: rfc3339(now),
        };
        let mut deployed = self.clone();
        match deployed.apps.iter_mut().find(|kept| kept.name == app.name) {
            Some(kept)
                if kept.component == app.component
                    && kept.files == app.files
                    && kept.sandbox == app.sandbox =>
            {
                kept.hosts = app.hosts;
                kept.tag = app.tag;
                kept.held_ms = app.held_ms;
            }
            Some(kept) => *kept = app,
            None => deployed.apps.push(app),
        }
        Ok(deployed)
    }

    /// The apps without the app `name`, which is `owner`'s.
    pub fn without_app(&self, name: &str, owner: &str) -> Result<Deployed, Refused> {
        let kept = self
            .app(name)
            .ok_or_else(|| Refused::Unknown(format!("the console has no app '{name}'")))?;
        if kept.owner != owner {
            return Err(Refused::Taken(format!(
                "the app '{name}' is {}'s, not {owner}'s",
                kept.owner
            )));
        }
        let mut deployed = self.clone();
        deployed.apps.retain(|app| app.name != name);
        Ok(deployed)
    }

    fn app(&self, name: &str) -> Option<&App> {
        self.apps.iter().find(|app| app.name == name)
    }

    /// `components/<sha256>/<file>`, made unless there from the upload of
    /// `sha256` or another file of that digest: linked (the same content,
    /// never rewritten), else copied, by a rename. `Missing` with neither.
    fn materialize(&self, sha256: &str, file: &str) -> Result<String, Refused> {
        let upload = self.dir.join(UPLOADS).join(format!("{sha256}.wasm"));
        self.components()
            .materialize(sha256, file, &upload)
            .map_err(Refused::Failed)?
            .ok_or_else(|| Refused::Missing(vec![sha256.to_string()]))
    }

    /// Writes `component` as `components/<sha256>/<name>` unless it is there
    /// as it is, by a rename (no reader sees it half written); its path
    /// relative to the directory.
    fn store(&self, name: &str, component: &[u8]) -> Result<String, String> {
        self.components().store(name, component)
    }

    fn components(&self) -> Blobs<'_> {
        Blobs {
            dir: &self.dir,
            kind: COMPONENTS,
        }
    }

    /// Writes the routes and apps to `routes.yaml`, by a rename; for its
    /// owner's eyes only, as it holds the apps' env.
    pub fn save(&self) -> Result<(), String> {
        write_by_rename(
            &self.dir.join(MANIFEST),
            write(&self.routes, &self.apps).as_bytes(),
            true,
        )
    }

    /// Removes the files below `components/` nothing names, and the
    /// directories they leave empty, the uploads kept for `UPLOAD_GRACE`,
    /// and the apps' files nothing names kept for as long. Links are removed,
    /// never followed.
    pub fn collect_garbage(&self) {
        self.collect(false);
    }

    /// [`Deployed::collect_garbage`]; at `startup` the files an unpack cut
    /// short too (else they may be being unpacked).
    fn collect(&self, startup: bool) {
        let named = self
            .routes
            .iter()
            .map(|route| &route.component)
            .chain(self.apps.iter().map(|app| &app.component));
        // the components are written on the reloader's task alone: none is being written
        self.components().collect_garbage(named, true);
        collect_uploads(&self.dir);
        let named: HashSet<&str> = self
            .apps
            .iter()
            .filter_map(|app| app.files.as_deref())
            .filter_map(|files| files.strip_prefix(&format!("{FILES}/")))
            .collect();
        files::collect_garbage(&self.dir.join(FILES), &named, startup);
    }
}

/// An app's keyvalue map: each identifier to `store/namespace`, the
/// namespace named (an app has no one host to default it to); a map with the
/// keyvalue capability and only with it. The stores are not checked.
pub fn app_map(caps: Caps, map: BTreeMap<String, String>) -> Result<BucketMap, String> {
    match (map.is_empty(), caps.keyvalue) {
        (true, false) => Ok(BucketMap::new()),
        (false, false) => Err("sandbox.keyvalue: needs the keyvalue capability".into()),
        (true, true) => Err(
            "sandbox.keyvalue: an app with the keyvalue capability maps its identifiers \
             (identifier -> store/namespace)"
                .into(),
        ),
        (false, true) => map
            .into_iter()
            .map(|(identifier, spec)| match spec.split_once('/') {
                Some((store, namespace)) if !store.is_empty() && !namespace.is_empty() => {
                    let place = Place {
                        store: store.to_string(),
                        namespace: namespace.to_string(),
                    };
                    Ok((identifier, place))
                }
                _ => Err(format!(
                    "sandbox.keyvalue.\"{identifier}\": '{spec}' is no store/namespace"
                )),
            })
            .collect(),
    }
}

/// `map` as [`app_map`] reads it: `identifier -> store/namespace`.
pub fn map_specs(map: &BucketMap) -> BTreeMap<String, String> {
    map.iter()
        .map(|(identifier, place)| {
            (
                identifier.clone(),
                format!("{}/{}", place.store, place.namespace),
            )
        })
        .collect()
}

/// `routes.yaml`: the routes and the apps in the shape of the `--config`
/// file's routes, each with the time it was deployed.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestFile {
    #[serde(default)]
    routes: Vec<ManifestRoute>,
    #[serde(default)]
    apps: Vec<ManifestApp>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestRoute {
    host: String,
    component: String,
    deployed: String,
    #[serde(default)]
    sandbox: SandboxFile,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestApp {
    name: String,
    owner: String,
    hosts: Vec<String>,
    component: String,
    files: Option<String>,
    deployed: String,
    tag: Option<String>,
    #[serde(rename = "held-ms")]
    held_ms: Option<u64>,
    #[serde(default)]
    sandbox: SandboxFile,
}

/// The routes and apps of `routes.yaml`; errors name the route or app (and a
/// line, never the file's text: it holds the apps' env). A host is routed
/// once over both.
fn read(text: &str) -> Result<(Vec<Route>, Vec<App>), String> {
    let mut options = serde_saphyr::Options::default();
    options.with_snippet = false;
    let file: ManifestFile = serde_saphyr::from_str_with_options(text, options).map_err(|e| {
        let e = e.to_string();
        e.strip_prefix("error: ").unwrap_or(&e).to_string()
    })?;
    let mut routed = HashSet::new();
    let mut routes: Vec<Route> = Vec::with_capacity(file.routes.len());
    for (n, route) in file.routes.into_iter().enumerate() {
        let failed = |e: String| format!("routes[{n}]: {e}");
        let host = check_host(&route.host).map_err(failed)?;
        if !routed.insert(host.clone()) {
            return Err(failed(format!("host '{host}' twice")));
        }
        let component = check_component(&route.component).map_err(failed)?;
        let (sandbox, map) = route.sandbox.split().map_err(failed)?;
        if map.is_some() {
            return Err(failed(
                "sandbox.keyvalue: the console's routes take the default map".into(),
            ));
        }
        if !sandbox.env.is_empty() {
            return Err(failed("sandbox.env: the console's routes take none".into()));
        }
        routes.push(Route {
            host,
            component,
            sandbox,
            deployed: route.deployed,
        });
    }
    let mut apps: Vec<App> = Vec::with_capacity(file.apps.len());
    for (n, app) in file.apps.into_iter().enumerate() {
        let failed = |e: String| format!("apps[{n}]: {e}");
        check_name("name", &app.name).map_err(failed)?;
        if apps.iter().any(|known| known.name == app.name) {
            return Err(failed(format!("app '{}' twice", app.name)));
        }
        check_name("owner", &app.owner).map_err(failed)?;
        let hosts = check_hosts(&app.hosts).map_err(failed)?;
        if let Some(host) = hosts.iter().find(|host| !routed.insert((*host).clone())) {
            return Err(failed(format!("host '{host}' twice")));
        }
        let component = check_component(&app.component).map_err(failed)?;
        let files = app
            .files
            .map(|files| check_files(&files).map(|()| files))
            .transpose()
            .map_err(failed)?;
        if let Some(tag) = &app.tag {
            check_tag(tag).map_err(failed)?;
        }
        if let Some(held_ms) = app.held_ms {
            check_held_ms(held_ms).map_err(failed)?;
        }
        let (mut sandbox, map) = app.sandbox.split().map_err(failed)?;
        sandbox.keyvalue = app_map(sandbox.caps, map.unwrap_or_default()).map_err(failed)?;
        apps.push(App {
            name: app.name,
            owner: app.owner,
            hosts,
            component,
            files,
            sandbox,
            tag: app.tag,
            held_ms: app.held_ms,
            deployed: app.deployed,
        });
    }
    Ok((routes, apps))
}

/// `files` when it names an app's files as [`Deployed::with_app`] keeps them:
/// `files/<sha256>`.
fn check_files(files: &str) -> Result<(), String> {
    files
        .strip_prefix(&format!("{FILES}/"))
        .and_then(|sha256| check_digest(sha256).ok())
        .filter(|sha256| files.ends_with(sha256.as_str()))
        .map(|_| ())
        .ok_or_else(|| format!("files: '{files}' is no {FILES}/<sha256>"))
}

/// `component` when it names a file as [`Deployed::store`] writes it.
fn check_component(component: &str) -> Result<String, String> {
    check_kept(component, COMPONENTS, ".wasm")
        .map(|_| component.to_string())
        .map_err(|e| format!("component: {e}"))
}

/// `routes` and `apps` as `routes.yaml`; every string double-quoted, so
/// never expanded. The apps' section only with apps.
fn write(routes: &[Route], apps: &[App]) -> String {
    let mut yaml = String::from(
        "# The routes and apps the console of sluice-wicket-runtime-wasi (--console) deployed, written \
         by it:\n# read at its startup, overwritten by its next change.\n",
    );
    if routes.is_empty() {
        yaml.push_str("routes: []\n");
    } else {
        yaml.push_str("routes:\n");
    }
    for route in routes {
        yaml.push_str(&format!("  - host: {}\n", quote(&route.host)));
        yaml.push_str(&format!("    component: {}\n", quote(&route.component)));
        yaml.push_str(&format!("    deployed: {}\n", quote(&route.deployed)));
        if let Some(sandbox) = sandbox_yaml(&route.sandbox, false) {
            yaml.push_str(&format!("    sandbox: {sandbox}\n"));
        }
    }
    if !apps.is_empty() {
        yaml.push_str("apps:\n");
    }
    for app in apps {
        let hosts: Vec<String> = app.hosts.iter().map(|host| quote(host)).collect();
        yaml.push_str(&format!("  - name: {}\n", quote(&app.name)));
        yaml.push_str(&format!("    owner: {}\n", quote(&app.owner)));
        yaml.push_str(&format!("    hosts: [{}]\n", hosts.join(", ")));
        yaml.push_str(&format!("    component: {}\n", quote(&app.component)));
        if let Some(files) = &app.files {
            yaml.push_str(&format!("    files: {}\n", quote(files)));
        }
        yaml.push_str(&format!("    deployed: {}\n", quote(&app.deployed)));
        if let Some(tag) = &app.tag {
            yaml.push_str(&format!("    tag: {}\n", quote(tag)));
        }
        if let Some(held_ms) = app.held_ms {
            yaml.push_str(&format!("    held-ms: {held_ms}\n"));
        }
        if let Some(sandbox) = sandbox_yaml(&app.sandbox, true) {
            yaml.push_str(&format!("    sandbox: {sandbox}\n"));
        }
    }
    yaml
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, UNIX_EPOCH};

    use super::*;

    /// The routes file reads back as written, routes and apps, an app's env,
    /// tag and `held-ms` included, the apps a start holds those of `held-ms`;
    /// what the console never writes -- a component not
    /// its own, a keyvalue map or an env of a route, an app's map without a
    /// namespace, a host or an app twice -- fails, naming the route or the
    /// app, never a value. A move keeps a route as it is, in its place, and
    /// replaces none.
    #[test]
    fn the_routes_file_reads_back_as_written() {
        let routes = vec![
            Route {
                host: "a.local".into(),
                component: format!("components/{}/a.wasm", "0".repeat(64)),
                sandbox: Sandbox::default(),
                deployed: rfc3339(UNIX_EPOCH),
            },
            Route {
                host: "b.local".into(),
                component: format!("components/{}/b.wasm", "f".repeat(64)),
                sandbox: Sandbox::parse(
                    "budget-ms=250&concurrency=3&instances=4&keep-ms=500&memory-mib=64&warm=2&wasi=cli,\
                     inherit-network,keyvalue",
                )
                .unwrap(),
                deployed: rfc3339(UNIX_EPOCH + Duration::from_secs(1_793_276_096)),
            },
        ];
        let mut sandbox = Sandbox::parse("warm=1&wasi=keyvalue").unwrap();
        sandbox.keyvalue = app_map(
            sandbox.caps,
            BTreeMap::from([
                (String::new(), "local/cf/0b9e2c6a".into()),
                ("odd \"id\"".into(), "shared/a\\b: c".into()),
            ]),
        )
        .unwrap();
        sandbox.env = crate::env::Env::new(BTreeMap::from([
            ("GREETING".into(), "hello".into()),
            (
                "PEM".into(),
                "-----BEGIN-----\n\"AA==\"\\\n-----END-----".into(),
            ),
            ("PORT".into(), "8080".into()),
            ("odd: name".into(), "${HOME}".into()),
        ]))
        .unwrap();
        let apps = vec![
            App {
                name: "0b9e2c6a".into(),
                owner: "cf-api".into(),
                hosts: vec!["c.local".into(), "d.local".into()],
                component: format!("components/{}/c.wasm", "0".repeat(64)),
                files: Some(format!("files/{}", "f".repeat(64))),
                sandbox,
                tag: Some("0123456789abcdef:\"#{}".into()),
                held_ms: Some(15_000),
                deployed: rfc3339(UNIX_EPOCH),
            },
            App {
                name: "plain".into(),
                owner: "cf-api".into(),
                hosts: vec!["e.local".into()],
                component: format!("components/{}/e.wasm", "e".repeat(64)),
                files: None,
                sandbox: Sandbox::default(),
                tag: None,
                held_ms: None,
                deployed: rfc3339(UNIX_EPOCH),
            },
        ];
        let deployed = Deployed {
            dir: PathBuf::new(),
            routes: routes.clone(),
            apps: Vec::new(),
        };
        let moved = deployed.moved("a.local", "C.local").unwrap();
        assert_eq!(moved.routes[0].host, "c.local");
        assert_eq!(
            (&moved.routes[0].component, &moved.routes[0].deployed),
            (&routes[0].component, &routes[0].deployed)
        );
        assert_eq!(moved.routes[1], routes[1]);
        assert_eq!(deployed.moved("a.local", "a.local"), Ok(deployed.clone()));
        for (to, refused) in [
            (
                "b.local",
                Refused::Taken("b.local is a route of the console already".into()),
            ),
            (
                "a_b",
                Refused::Invalid(
                    "'a_b' is no host name (dot-separated labels of a-z, 0-9 and '-')".into(),
                ),
            ),
        ] {
            assert_eq!(deployed.moved("a.local", to), Err(refused));
        }
        assert!(matches!(
            deployed.moved("x.local", "c.local"),
            Err(Refused::Unknown(_))
        ));

        let text = write(&routes, &apps);
        let held: Vec<_> = Deployed {
            dir: PathBuf::new(),
            routes: Vec::new(),
            apps: apps.clone(),
        }
        .held()
        .collect();
        assert_eq!(
            held,
            [(
                "cf-api".to_string(),
                "0b9e2c6a".to_string(),
                Duration::from_secs(15)
            )]
        );
        assert_eq!(read(&text), Ok((routes.clone(), apps)));
        assert!(
            text.contains("deployed: \"1970-01-01T00:00:00Z\""),
            "{text}"
        );
        assert!(
            text.contains("deployed: \"2026-10-29T12:14:56Z\""),
            "{text}"
        );
        assert_eq!(read(&write(&[], &[])), Ok(Default::default()));
        assert!(
            !write(&routes, &[]).contains("\napps:"),
            "no apps' section without apps"
        );

        let component = format!("components/{}/a.wasm", "0".repeat(64));
        let route = |rest: &str| format!("  - {{host: a.local, deployed: x, {rest}}}\n");
        let routes = |rest: &str| format!("routes:\n{}", route(rest));
        let app = |rest: &str| {
            format!(
                "  - {{name: a, owner: o, hosts: [b.local], component: {component}, \
                 deployed: x{rest}}}\n"
            )
        };
        let apps = |rest: &str| format!("apps:\n{}", app(rest));
        let cases = [
            (
                routes("component: ../a.wasm"),
                "routes[0]: component: '../a.wasm' is not one of the console's".to_string(),
            ),
            (
                routes(&format!("component: components/{}/a.wasm", "A".repeat(64))),
                "routes[0]: component: ".into(),
            ),
            (
                routes(&format!("component: {component}/x.wasm")),
                "routes[0]: component: ".into(),
            ),
            (
                routes(&format!(
                    "component: {component}, sandbox: {{wasi: [keyvalue], keyvalue: {{'': local}}}}"
                )),
                "routes[0]: sandbox.keyvalue: the console's routes take the default map".into(),
            ),
            (
                routes(&format!(
                    "component: {component}, sandbox: {{warm: 2, instances: 1}}"
                )),
                "routes[0]: sandbox: instances=1 is below warm=2".into(),
            ),
            (
                format!(
                    "routes:\n{}",
                    route(&format!("component: {component}")).repeat(2)
                ),
                "routes[1]: host 'a.local' twice".into(),
            ),
            (
                routes(&format!("component: {component}, fuel: 1")),
                "unknown field `fuel`".into(),
            ),
            (
                apps(", sandbox: {wasi: [keyvalue], keyvalue: {'': local}}"),
                "apps[0]: sandbox.keyvalue.\"\": 'local' is no store/namespace".into(),
            ),
            (
                apps(", sandbox: {wasi: [keyvalue]}"),
                "apps[0]: sandbox.keyvalue: an app with the keyvalue capability maps".into(),
            ),
            (
                apps(", sandbox: {keyvalue: {'': local/x}}"),
                "apps[0]: sandbox.keyvalue: needs the keyvalue capability".into(),
            ),
            (
                format!(
                    "{}{}",
                    routes(&format!("component: {component}")),
                    apps("").replace("b.local", "a.local")
                ),
                "apps[0]: host 'a.local' twice".into(),
            ),
            (
                format!(
                    "apps:\n{}{}",
                    app(""),
                    app("").replace("b.local", "c.local")
                ),
                "apps[1]: app 'a' twice".into(),
            ),
            (
                apps("").replace("owner: o", "owner: ''"),
                "apps[0]: owner: '' is no name".into(),
            ),
            (
                apps("").replace("[b.local]", "[]"),
                "apps[0]: hosts: an app serves one host at least".into(),
            ),
            (
                routes(&format!(
                    "component: {component}, sandbox: {{env: {{K: s3cr3t}}}}"
                )),
                "routes[0]: sandbox.env: the console's routes take none".into(),
            ),
            (apps(", tag: 'a b'"), "apps[0]: tag: 'a b' is no tag".into()),
            (
                apps(", held-ms: 0"),
                "apps[0]: held-ms: must be at least 1".into(),
            ),
            (
                apps(", files: files/../etc"),
                "apps[0]: files: 'files/../etc' is no files/<sha256>".into(),
            ),
            // a line, never the file's text: it holds the apps' env
            (
                apps(", sandbox: {env: {K: s3cr3t}}, fuel: 1"),
                "unknown field `fuel`".into(),
            ),
        ];
        for (yaml, error) in cases {
            let failed = read(&yaml);
            assert!(
                matches!(&failed, Err(e) if e.contains(&error) && !e.contains("s3cr3t")),
                "{yaml}: {failed:?}"
            );
        }
    }
}
