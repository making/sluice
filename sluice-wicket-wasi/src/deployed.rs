//! The routes and apps the console deployed (`--console`): hosts served by
//! uploaded components, kept in the console's directory (`--console-dir`) so a
//! restart serves them again. A route is the page's: one host, its component
//! uploaded with it, the default keyvalue map. An app is the API's (a
//! controller's): hosts served by one component in one sandbox, its component
//! uploaded by digest first, its keyvalue map named, changed by its owner
//! only.
//!
//! ```text
//! <dir>/routes.yaml                      the routes and the apps, in the --config file's route shape
//! <dir>/components/<sha256>/<name>.wasm  the components they name, never rewritten
//! <dir>/uploads/<sha256>.wasm            the components uploaded by digest, for UPLOAD_GRACE
//! ```
//!
//! A component's path names its digest, so other content is another target:
//! its route is retargeted, and the old target drains as a removed one does.
//! The routes and apps are held here, `routes.yaml` being their copy: an edit
//! is reloaded first (`routes::Reloader::apply`) and written once the reload
//! published it, so the file never names routes that did not load. A
//! component file nothing names is removed after every edit and at startup,
//! an upload once [`UPLOAD_GRACE`] passed: an app naming it links it below
//! `components/` before.

use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::Deserialize;

use crate::config::{SandboxFile, quote, sandbox_yaml};
use crate::keyvalue::{BucketMap, Place, Stores};
use crate::routes::{Origin, Refused, Spec};
use crate::wasm_host::{Caps, Locator, Sandbox};

/// The routes' file in the directory.
const MANIFEST: &str = "routes.yaml";

/// The components' directory in the directory.
const COMPONENTS: &str = "components";

/// The uploads' directory in the directory.
const UPLOADS: &str = "uploads";

/// Longest file name a component is kept as, and longest name of an app or
/// an owner, in bytes.
const MAX_NAME: usize = 100;

/// How long an upload is kept for an app to name it: minutes, so the PUT
/// naming it after the upload never races its removal.
const UPLOAD_GRACE: Duration = Duration::from_secs(10 * 60);

/// One of the console's routes.
#[derive(Clone, PartialEq, Debug)]
pub struct Route {
    pub host: String,
    /// The component file relative to the directory,
    /// `components/<sha256>/<name>`.
    pub component: String,
    /// Without a keyvalue map: with `wasi=keyvalue` the default one applies
    /// (the default store, the host as the namespace).
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
    /// With its keyvalue map in full: an app has no one host to default a
    /// namespace to.
    pub sandbox: Sandbox,
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
    /// With its keyvalue map ([`app_map`]).
    pub sandbox: Sandbox,
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
        deployed.collect_garbage();
        Ok(deployed)
    }

    pub fn dir(&self) -> &Path {
        &self.dir
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
            app.hosts.iter().map(|host| Spec {
                host: host.clone(),
                component: Locator::File(self.dir.join(&app.component)),
                shown: Some(app.component.clone()),
                sandbox: app.sandbox.clone(),
                origin: Origin::Api {
                    name: app.name.clone(),
                    owner: app.owner.clone(),
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
        let host = check_host(host).map_err(Refused::Invalid)?;
        let component = self
            .store(&file_name(name, &host), component)
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
        write_by_rename(&path, component).map_err(Refused::Failed)
    }

    /// The apps with the app `name` as `app` has it since `now`: its
    /// component made from an upload or another file of its digest, in place
    /// of the app of that name -- which keeps its time when its component and
    /// sandbox stay -- or added last. Another owner's app is refused, and so
    /// is a component of a digest not kept.
    pub fn with_app(&self, name: &str, app: AppSpec, now: SystemTime) -> Result<Deployed, Refused> {
        check_name("app", name).map_err(Refused::Invalid)?;
        check_name("owner", &app.owner).map_err(Refused::Invalid)?;
        let hosts = check_hosts(&app.hosts).map_err(Refused::Invalid)?;
        let sha256 = check_digest(&app.sha256)
            .map_err(|e| Refused::Invalid(format!("component.sha256: {e}")))?;
        check_file(&app.file).map_err(|e| Refused::Invalid(format!("component.name: {e}")))?;
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
        let app = App {
            name: name.to_string(),
            owner: app.owner,
            hosts,
            component: self.materialize(&sha256, &app.file)?,
            sandbox: app.sandbox,
            deployed: rfc3339(now),
        };
        let mut deployed = self.clone();
        match deployed.apps.iter_mut().find(|kept| kept.name == app.name) {
            Some(kept) if kept.component == app.component && kept.sandbox == app.sandbox => {
                kept.hosts = app.hosts;
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
        let relative = format!("{COMPONENTS}/{sha256}/{file}");
        let path = self.dir.join(&relative);
        if regular(&path) {
            return Ok(relative);
        }
        let parent = path.parent().expect("below the components");
        let upload = self.dir.join(UPLOADS).join(format!("{sha256}.wasm"));
        let others = std::fs::read_dir(parent)
            .into_iter()
            .flatten()
            .flatten()
            .map(|entry| entry.path())
            .filter(|other| !hidden(other));
        let Some(source) = std::iter::once(upload)
            .chain(others)
            .find(|source| regular(source))
        else {
            return Err(Refused::Missing(sha256.to_string()));
        };
        let failed = |e: std::io::Error| Refused::Failed(format!("write {}: {e}", path.display()));
        std::fs::create_dir_all(parent).map_err(failed)?;
        let partial = parent.join(format!(".{file}.partial"));
        let _ = std::fs::remove_file(&partial);
        if std::fs::hard_link(&source, &partial).is_err() {
            std::fs::copy(&source, &partial).map_err(failed)?;
        }
        std::fs::rename(&partial, &path).map_err(failed)?;
        Ok(relative)
    }

    /// Writes `component` as `components/<sha256>/<name>` unless it is there
    /// as it is, by a rename (no reader sees it half written); its path
    /// relative to the directory.
    fn store(&self, name: &str, component: &[u8]) -> Result<String, String> {
        let relative = format!("{COMPONENTS}/{}/{name}", digest(component));
        let path = self.dir.join(&relative);
        if std::fs::read(&path).is_ok_and(|kept| kept == component) {
            return Ok(relative);
        }
        write_by_rename(&path, component)?;
        Ok(relative)
    }

    /// Writes the routes and apps to `routes.yaml`, by a rename.
    pub fn save(&self) -> Result<(), String> {
        write_by_rename(
            &self.dir.join(MANIFEST),
            write(&self.routes, &self.apps).as_bytes(),
        )
    }

    /// Removes the files below `components/` nothing names, and the
    /// directories they leave empty, and the uploads kept for
    /// [`UPLOAD_GRACE`]. Links are removed, never followed.
    pub fn collect_garbage(&self) {
        let named: HashSet<PathBuf> = self
            .routes
            .iter()
            .map(|route| &route.component)
            .chain(self.apps.iter().map(|app| &app.component))
            .map(|component| self.dir.join(component))
            .collect();
        let remove = |path: &Path| {
            if let Err(e) = std::fs::remove_file(path) {
                eprintln!("--console-dir: remove {}: {e}", path.display());
            }
        };
        if let Ok(digests) = std::fs::read_dir(self.dir.join(COMPONENTS)) {
            for digest in digests.flatten() {
                let path = digest.path();
                if !digest.file_type().is_ok_and(|kind| kind.is_dir()) {
                    remove(&path);
                    continue;
                }
                for file in std::fs::read_dir(&path).into_iter().flatten().flatten() {
                    let kind = file.file_type();
                    if kind.is_ok_and(|kind| !kind.is_dir()) && !named.contains(&file.path()) {
                        remove(&file.path());
                    }
                }
                // only once empty
                let _ = std::fs::remove_dir(&path);
            }
        }
        for upload in std::fs::read_dir(self.dir.join(UPLOADS))
            .into_iter()
            .flatten()
            .flatten()
        {
            // a partial one a crash left is hidden; a time ahead of the clock is fresh
            let fresh = upload.metadata().is_ok_and(|metadata| {
                metadata.is_file()
                    && metadata
                        .modified()
                        .is_ok_and(|at| at.elapsed().is_ok_and(|age| age <= UPLOAD_GRACE))
            });
            if !fresh || hidden(&upload.path()) {
                remove(&upload.path());
            }
        }
    }
}

/// Whether `path` is a regular file, not a link.
fn regular(path: &Path) -> bool {
    std::fs::symlink_metadata(path).is_ok_and(|metadata| metadata.is_file())
}

/// Whether the file name of `path` starts with a dot: a partial write.
fn hidden(path: &Path) -> bool {
    path.file_name()
        .is_some_and(|name| name.to_string_lossy().starts_with('.'))
}

/// Writes `bytes` to `path` by a rename (no reader sees it half written),
/// its directory made when missing.
fn write_by_rename(path: &Path, bytes: &[u8]) -> Result<(), String> {
    let failed = |e: std::io::Error| format!("write {}: {e}", path.display());
    let parent = path.parent().expect("a file in a directory");
    std::fs::create_dir_all(parent).map_err(failed)?;
    let name = path.file_name().expect("a file").to_string_lossy();
    let partial = parent.join(format!(".{name}.partial"));
    std::fs::write(&partial, bytes).map_err(failed)?;
    std::fs::rename(&partial, path).map_err(failed)
}

/// The sha256 of `bytes`, lower case hex.
fn digest(bytes: &[u8]) -> String {
    use sha2::Digest as _;
    sha2::Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// `host` in lower case when it is a host name: dot-separated labels of
/// `a-z 0-9 -`, each 1 to 63 bytes and neither starting nor ending with `-`,
/// 253 bytes at most.
pub fn check_host(host: &str) -> Result<String, String> {
    let host = host.to_ascii_lowercase();
    let label = |label: &str| {
        (1..=63).contains(&label.len())
            && !label.starts_with('-')
            && !label.ends_with('-')
            && label
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    };
    if host.len() <= 253 && host.split('.').all(label) {
        Ok(host)
    } else {
        Err(format!(
            "'{host}' is no host name (dot-separated labels of a-z, 0-9 and '-')"
        ))
    }
}

/// An app's hosts, sorted: at least one, each a host name ([`check_host`])
/// once.
pub fn check_hosts(hosts: &[String]) -> Result<Vec<String>, String> {
    if hosts.is_empty() {
        return Err("hosts: an app serves one host at least".into());
    }
    let mut checked = hosts
        .iter()
        .map(|host| check_host(host))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("hosts: {e}"))?;
    checked.sort();
    match checked.windows(2).find(|pair| pair[0] == pair[1]) {
        Some(twice) => Err(format!("hosts: '{}' twice", twice[0])),
        None => Ok(checked),
    }
}

/// Whether `name` names an app or an owner (`what`): 1 to [`MAX_NAME`] bytes
/// of `A-Z a-z 0-9 . _ -`, starting with a letter or a digit.
pub fn check_name(what: &str, name: &str) -> Result<(), String> {
    let named = (1..=MAX_NAME).contains(&name.len())
        && name.starts_with(|c: char| c.is_ascii_alphanumeric())
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte));
    if named {
        Ok(())
    } else {
        Err(format!(
            "{what}: '{name}' is no name (1 to {MAX_NAME} of A-Z, a-z, 0-9, '.', '_' and '-', \
             starting with a letter or a digit)"
        ))
    }
}

/// `sha256` in lower case when it is a sha256 digest: 64 hex digits.
pub fn check_digest(sha256: &str) -> Result<String, String> {
    if sha256.len() == 64 && sha256.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Ok(sha256.to_ascii_lowercase())
    } else {
        Err(format!("'{sha256}' is no sha256 digest (64 hex digits)"))
    }
}

/// Whether `file` is a file name a component is kept as ([`file_name`]).
fn check_file(file: &str) -> Result<(), String> {
    if file_name(file, "") == file {
        Ok(())
    } else {
        Err(format!(
            "'{file}' is no file name of a component (A-Z, a-z, 0-9, '.', '_' and '-', \
             ending in .wasm, {MAX_NAME} bytes at most)"
        ))
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

/// The file name an upload named `name` is kept as: its last path segment,
/// every char but `A-Z a-z 0-9 . _ -` replaced by `-` and leading dots
/// dropped, ending in `.wasm`, [`MAX_NAME`] bytes at most; `<host>.wasm` for
/// a name left empty.
fn file_name(name: &str, host: &str) -> String {
    let last = name.rsplit(['/', '\\']).next().unwrap_or_default();
    let clean: String = last
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                c
            } else {
                '-'
            }
        })
        .collect();
    let clean = clean.trim_start_matches('.');
    let stem = clean.strip_suffix(".wasm").unwrap_or(clean);
    let stem = if stem.is_empty() { host } else { stem };
    // ASCII only: any byte is a char boundary
    let stem = &stem[..stem.len().min(MAX_NAME - ".wasm".len())];
    format!("{stem}.wasm")
}

/// `time` as an RFC 3339 UTC instant to the second (`2026-10-29T12:34:56Z`).
pub fn rfc3339(time: SystemTime) -> String {
    let secs = time
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_secs());
    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    // civil-from-days (H. Hinnant), valid for every date after the epoch
    let z = days + 719_468;
    let era = z / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        rem / 3_600,
        rem % 3_600 / 60,
        rem % 60
    )
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
    deployed: String,
    #[serde(default)]
    sandbox: SandboxFile,
}

/// The routes and apps of `routes.yaml`; errors name the route or app. A
/// host is routed once over both.
fn read(text: &str) -> Result<(Vec<Route>, Vec<App>), String> {
    let file: ManifestFile = serde_saphyr::from_str(text).map_err(|e| {
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
        let (mut sandbox, map) = app.sandbox.split().map_err(failed)?;
        sandbox.keyvalue = app_map(sandbox.caps, map.unwrap_or_default()).map_err(failed)?;
        apps.push(App {
            name: app.name,
            owner: app.owner,
            hosts,
            component,
            sandbox,
            deployed: app.deployed,
        });
    }
    Ok((routes, apps))
}

/// `component` when it names a file as [`Deployed::store`] writes it.
fn check_component(component: &str) -> Result<String, String> {
    let parts: Vec<&str> = component.split('/').collect();
    match parts.as_slice() {
        [COMPONENTS, digest, name]
            if digest.len() == 64
                && digest
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                && file_name(name, "") == *name =>
        {
            Ok(component.to_string())
        }
        _ => Err(format!(
            "component: '{component}' is not one of the console's \
             ({COMPONENTS}/<sha256>/<name>.wasm)"
        )),
    }
}

/// `routes` and `apps` as `routes.yaml`; every string double-quoted, so
/// never expanded. The apps' section only with apps.
fn write(routes: &[Route], apps: &[App]) -> String {
    let mut yaml = String::from(
        "# The routes and apps the console of sluice-wicket-wasi (--console) deployed, written \
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
        yaml.push_str(&format!("    deployed: {}\n", quote(&app.deployed)));
        if let Some(sandbox) = sandbox_yaml(&app.sandbox, true) {
            yaml.push_str(&format!("    sandbox: {sandbox}\n"));
        }
    }
    yaml
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn hosts_names_and_file_names_are_checked() {
        assert_eq!(check_host("Demo.LocalHost"), Ok("demo.localhost".into()));
        let longest = [
            "a".repeat(63),
            "b".repeat(63),
            "c".repeat(63),
            "d".repeat(61),
        ]
        .join(".");
        assert_eq!(check_host(&longest), Ok(longest.clone()));
        let too_long = format!("{longest}d");
        for bad in [
            "",
            "a..b",
            ".a",
            "a.",
            "-a.b",
            "a-.b",
            "a_b",
            "a b",
            "ä.b",
            &"a".repeat(64),
            &too_long,
        ] {
            assert!(check_host(bad).is_err(), "'{bad}'");
        }
        let long = format!("{}.wasm", "x".repeat(200));
        for (name, kept) in [
            ("hello.wasm", "hello.wasm".to_string()),
            ("hello", "hello.wasm".into()),
            ("../../etc/passwd", "passwd.wasm".into()),
            ("C:\\guests\\my app.wasm", "my-app.wasm".into()),
            (".hidden.wasm", "hidden.wasm".into()),
            (".wasm", "wasm.wasm".into()),
            ("", "demo.local.wasm".into()),
            (&long, format!("{}.wasm", "x".repeat(MAX_NAME - 5))),
        ] {
            assert_eq!(file_name(name, "demo.local"), kept, "'{name}'");
            assert_eq!(file_name(&kept, "other"), kept, "'{kept}' is kept as it is");
            assert!(check_file(&kept).is_ok(), "'{kept}'");
        }
        for bad in ["hello", "my app.wasm", ".hello.wasm", "a/b.wasm", &long] {
            assert!(check_file(bad).is_err(), "'{bad}'");
        }

        let guid = "0b9e2c6a-6c5e-4c1e-9d3f-2a1b0c9d8e7f";
        for name in [guid, "a", "cf-api", "Hello_World.2", &"x".repeat(MAX_NAME)] {
            assert_eq!(check_name("app", name), Ok(()), "'{name}'");
        }
        for bad in ["", "-a", ".a", "a b", "a/b", "ä", &"x".repeat(MAX_NAME + 1)] {
            assert!(check_name("app", bad).is_err(), "'{bad}'");
        }
        assert_eq!(
            check_hosts(&["B.local".into(), "a.local".into()]),
            Ok(vec!["a.local".into(), "b.local".into()]),
            "sorted, in lower case"
        );
        for (hosts, error) in [
            (vec![], "hosts: an app serves one host at least"),
            (
                vec!["a.local".into(), "A.local".into()],
                "hosts: 'a.local' twice",
            ),
            (vec!["a_b".into()], "hosts: 'a_b' is no host name"),
        ] {
            let checked = check_hosts(&hosts);
            assert!(
                matches!(&checked, Err(e) if e.starts_with(error)),
                "{hosts:?}: {checked:?}"
            );
        }
        let sha256 = "0123456789abcdef".repeat(4);
        assert_eq!(check_digest(&sha256.to_uppercase()), Ok(sha256.clone()));
        for bad in [&sha256[1..], &format!("{}g", &sha256[1..]), ""] {
            assert!(check_digest(bad).is_err(), "'{bad}'");
        }
    }

    /// The routes file reads back as written, routes and apps; what the
    /// console never writes -- a component not its own, a keyvalue map of a
    /// route, an app's map without a namespace, a host or an app twice --
    /// fails, naming the route or the app. A move keeps a route as it is, in
    /// its place, and replaces none.
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
                    "budget-ms=250&instances=4&memory-mib=64&pool=2&wasi=cli,inherit-network,keyvalue",
                )
                .unwrap(),
                deployed: rfc3339(UNIX_EPOCH + Duration::from_secs(1_793_276_096)),
            },
        ];
        let mut sandbox = Sandbox::parse("pool=1&wasi=keyvalue").unwrap();
        sandbox.keyvalue = app_map(
            sandbox.caps,
            BTreeMap::from([
                (String::new(), "local/cf/0b9e2c6a".into()),
                ("odd \"id\"".into(), "shared/a\\b: c".into()),
            ]),
        )
        .unwrap();
        let apps = vec![
            App {
                name: "0b9e2c6a".into(),
                owner: "cf-api".into(),
                hosts: vec!["c.local".into(), "d.local".into()],
                component: format!("components/{}/c.wasm", "0".repeat(64)),
                sandbox,
                deployed: rfc3339(UNIX_EPOCH),
            },
            App {
                name: "plain".into(),
                owner: "cf-api".into(),
                hosts: vec!["e.local".into()],
                component: format!("components/{}/e.wasm", "e".repeat(64)),
                sandbox: Sandbox::default(),
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
                    "component: {component}, sandbox: {{pool: 2, instances: 1}}"
                )),
                "routes[0]: sandbox: instances=1 is below pool=2".into(),
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
        ];
        for (yaml, error) in cases {
            let failed = read(&yaml);
            assert!(
                matches!(&failed, Err(e) if e.contains(&error)),
                "{yaml}: {failed:?}"
            );
        }
    }
}
