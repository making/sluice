//! The desired state: one organization and space, the shared domains, the
//! apps with their web process and environment variables, the routes,
//! packages, builds, droplets, deployments and jobs, the service instances
//! and their bindings. In memory, written by a rename to `state.json` in the
//! data directory on every change; the blobs next to it
//! (`packages/<guid>.zip`, `droplets/<sha256>.wasm`). Every file is its
//! owner's only (0600): the state holds the apps' environment and the
//! credentials of their images.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// The annotation of an app setting its routes' `budget-ms`.
pub const BUDGET_ANNOTATION: &str = "wicket.sluice.ik.am/budget-ms";
/// The annotation of an app setting its routes' `instances` cap.
pub const INSTANCES_ANNOTATION: &str = "wicket.sluice.ik.am/instances";

/// The stack every app is staged on.
pub const STACK: &str = "wasip3";

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct State {
    pub org: Named,
    pub space: Named,
    /// The first takes the default and random routes.
    pub domains: Vec<Named>,
    pub apps: Vec<App>,
    pub routes: Vec<Route>,
    pub packages: Vec<Package>,
    pub builds: Vec<Build>,
    pub droplets: Vec<Droplet>,
    pub deployments: Vec<Deployment>,
    pub jobs: Vec<Job>,
    #[serde(default)]
    pub instances: Vec<Instance>,
    #[serde(default)]
    pub bindings: Vec<Binding>,
    /// The keyvalue namespaces of deleted instances whose data the wicket
    /// is yet to delete.
    #[serde(default)]
    pub purges: Vec<Purge>,
    /// Where the watch of the flume resumes: past the last line it took
    /// (`flume`); None before one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flume: Option<FlumeCursor>,
    /// What the wicket answered of each app, by guid; not kept: a start
    /// lists the wicket.
    #[serde(skip)]
    pub wicket: BTreeMap<String, Outcome>,
}

/// A position in the flume's lines: its run (`epoch`) and the line's `seq`.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq)]
pub struct FlumeCursor {
    pub epoch: u64,
    pub seq: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct Named {
    pub guid: String,
    pub name: String,
    pub created_at: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct App {
    pub guid: String,
    pub name: String,
    pub started: bool,
    pub created_at: String,
    pub updated_at: String,
    /// What the app was last pushed or staged as: `docker` an image's.
    #[serde(default)]
    pub lifecycle: Lifecycle,
    pub buildpacks: Vec<String>,
    pub stack: String,
    pub labels: BTreeMap<String, String>,
    pub annotations: BTreeMap<String, String>,
    pub process: Process,
    pub current_droplet: Option<String>,
    /// The user's environment variables (`crate::env`); the guest gets them
    /// at its next start.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    /// What the wicket serves since the last start, restart or deployment:
    /// the snapshot taken then.
    pub running: Option<Running>,
}

/// An app's lifecycle, as CC names it: a droplet is a component either way.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, Default, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Lifecycle {
    /// A zip holding the component.
    #[default]
    Buildpack,
    /// An OCI image holding it.
    Docker,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Process {
    pub guid: String,
    pub instances: u64,
    pub memory_mb: u64,
    pub disk_mb: u64,
    pub log_rate_limit: i64,
    pub command: Option<String>,
    pub health_check: HealthCheck,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct HealthCheck {
    pub kind: String,
    pub timeout: Option<u64>,
    pub invocation_timeout: Option<u64>,
    pub endpoint: Option<String>,
}

impl Default for HealthCheck {
    fn default() -> Self {
        HealthCheck {
            kind: "port".into(),
            timeout: None,
            invocation_timeout: None,
            endpoint: None,
        }
    }
}

/// The droplet and the sandbox the wicket got at the last start.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Running {
    pub droplet: String,
    pub sha256: String,
    /// The component's file name in the wicket (the droplet's web process,
    /// less its directories).
    pub file: String,
    pub memory_mib: u64,
    pub budget_ms: Option<u64>,
    pub instances: Option<u64>,
    /// The capabilities the bindings granted.
    #[serde(default)]
    pub wasi: Vec<String>,
    /// The keyvalue map the bindings made: identifier -> `store/namespace`.
    #[serde(default)]
    pub keyvalue: BTreeMap<String, String>,
    /// The guest's environment (`crate::env::guest`); none in a snapshot
    /// taken before the cf-api gave one.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    /// Tells the wicket's app of this snapshot from another
    /// (`crate::env::tag`): the wicket lists no env back.
    #[serde(default)]
    pub tag: Option<String>,
    /// Seconds since the epoch.
    pub since: u64,
}

/// A service instance: a namespace of a keyvalue store (offering
/// `keyvalue`, the store its plan) or a capability (offering `wasi`).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Instance {
    pub guid: String,
    pub name: String,
    pub offering: String,
    pub plan: String,
    pub tags: Vec<String>,
    pub created_at: String,
    pub updated_at: String,
}

/// An app bound to a service instance; applied at the app's next start.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Binding {
    pub guid: String,
    /// `--binding-name`.
    pub name: Option<String>,
    pub app: String,
    pub instance: String,
    /// The keyvalue identifier the app opens the namespace by; empty for a
    /// capability.
    pub identifier: String,
    pub created_at: String,
    pub updated_at: String,
}

/// The data of a deleted keyvalue instance, deleted once no app maps it.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Purge {
    pub store: String,
    pub namespace: String,
    /// The instance's name, for the log.
    pub instance: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Route {
    pub guid: String,
    pub host: String,
    /// The domain's guid.
    pub domain: String,
    /// The app mapped, and the destination's guid.
    pub app: Option<String>,
    pub destination: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Package {
    pub guid: String,
    pub app: String,
    /// `AWAITING_UPLOAD`, `READY` or `FAILED`.
    pub state: String,
    pub sha256: Option<String>,
    /// A docker package's image; None: the bits uploaded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub docker: Option<DockerImage>,
    pub created_at: String,
    pub updated_at: String,
}

/// The image of a docker package and the credentials it is pulled with: the
/// password is for the pull only, shown nowhere.
#[derive(Serialize, Deserialize, Clone, PartialEq)]
pub struct DockerImage {
    pub image: String,
    pub username: Option<String>,
    pub password: Option<String>,
}

impl DockerImage {
    /// The username and the password, both given.
    pub fn credentials(&self) -> Option<(&str, &str)> {
        Some((self.username.as_deref()?, self.password.as_deref()?))
    }
}

impl std::fmt::Debug for DockerImage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DockerImage")
            .field("image", &self.image)
            .field("username", &self.username)
            .field("password", &self.password.as_ref().map(|_| "***"))
            .finish()
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Build {
    pub guid: String,
    pub app: String,
    pub package: String,
    /// `STAGING`, `STAGED` or `FAILED`.
    pub state: String,
    /// The package's: `docker` for an image.
    #[serde(default)]
    pub lifecycle: Lifecycle,
    pub error: Option<String>,
    pub droplet: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Droplet {
    pub guid: String,
    pub app: String,
    pub package: String,
    pub sha256: String,
    /// The component's path in the package; its file name for an image.
    pub file: String,
    pub bytes: u64,
    /// The image it was pulled from, pinned to its manifest's digest; None
    /// for a zip's.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Deployment {
    pub guid: String,
    pub app: String,
    pub droplet: String,
    pub previous_droplet: String,
    pub strategy: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Job {
    pub guid: String,
    pub operation: String,
    pub errors: Vec<JobError>,
    /// A link besides `self`: its name and path.
    pub resource: Option<(String, String)>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct JobError {
    pub code: u32,
    pub title: String,
    pub detail: String,
}

/// What the wicket last answered of an app.
#[derive(Clone, Debug, PartialEq)]
pub enum Outcome {
    Running,
    Refused(String),
    Unreachable,
}

impl State {
    /// A new state with the organization, space and domains named.
    pub fn new(org: &str, space: &str, domains: &[String]) -> State {
        let named = |name: &str| Named {
            guid: guid(),
            name: name.to_string(),
            created_at: now(),
        };
        State {
            org: named(org),
            space: named(space),
            domains: domains.iter().map(|domain| named(domain)).collect(),
            ..State::default()
        }
    }

    pub fn app(&self, guid: &str) -> Option<&App> {
        self.apps.iter().find(|app| app.guid == guid)
    }

    pub fn app_mut(&mut self, guid: &str) -> Option<&mut App> {
        self.apps.iter_mut().find(|app| app.guid == guid)
    }

    pub fn app_named(&self, name: &str) -> Option<&App> {
        self.apps.iter().find(|app| app.name == name)
    }

    pub fn route(&self, guid: &str) -> Option<&Route> {
        self.routes.iter().find(|route| route.guid == guid)
    }

    pub fn route_mut(&mut self, guid: &str) -> Option<&mut Route> {
        self.routes.iter_mut().find(|route| route.guid == guid)
    }

    pub fn route_at(&self, host: &str, domain: &str) -> Option<&Route> {
        self.routes
            .iter()
            .find(|route| route.host == host && route.domain == domain)
    }

    pub fn domain(&self, guid: &str) -> Option<&Named> {
        self.domains.iter().find(|domain| domain.guid == guid)
    }

    pub fn domain_named(&self, name: &str) -> Option<&Named> {
        self.domains.iter().find(|domain| domain.name == name)
    }

    /// Takes the flags' domains, in their order: the first takes the default and random
    /// routes. A domain gone from the flags stays behind them, so its routes keep its guid.
    pub fn follow_domains(&mut self, domains: &[String]) {
        for domain in domains {
            if self.domain_named(domain).is_none() {
                self.domains.push(Named {
                    guid: guid(),
                    name: domain.clone(),
                    created_at: now(),
                });
            }
        }
        // a gone domain has no position: last, not first as None would sort
        self.domains.sort_by_key(|domain| {
            domains
                .iter()
                .position(|name| *name == domain.name)
                .unwrap_or(usize::MAX)
        });
    }

    pub fn package(&self, guid: &str) -> Option<&Package> {
        self.packages.iter().find(|package| package.guid == guid)
    }

    pub fn build(&self, guid: &str) -> Option<&Build> {
        self.builds.iter().find(|build| build.guid == guid)
    }

    pub fn droplet(&self, guid: &str) -> Option<&Droplet> {
        self.droplets.iter().find(|droplet| droplet.guid == guid)
    }

    pub fn deployment(&self, guid: &str) -> Option<&Deployment> {
        self.deployments
            .iter()
            .find(|deployment| deployment.guid == guid)
    }

    pub fn job(&self, guid: &str) -> Option<&Job> {
        self.jobs.iter().find(|job| job.guid == guid)
    }

    pub fn instance(&self, guid: &str) -> Option<&Instance> {
        self.instances.iter().find(|instance| instance.guid == guid)
    }

    pub fn instance_named(&self, name: &str) -> Option<&Instance> {
        self.instances.iter().find(|instance| instance.name == name)
    }

    pub fn binding(&self, guid: &str) -> Option<&Binding> {
        self.bindings.iter().find(|binding| binding.guid == guid)
    }

    /// The bindings of the app `guid`, as they were made.
    pub fn bindings_of(&self, guid: &str) -> impl Iterator<Item = &Binding> {
        self.bindings
            .iter()
            .filter(move |binding| binding.app == guid)
    }

    /// The routes mapped to the app `guid`.
    pub fn routes_of(&self, guid: &str) -> impl Iterator<Item = &Route> {
        self.routes
            .iter()
            .filter(move |route| route.app.as_deref() == Some(guid))
    }

    /// `<host>.<domain>` of the route.
    pub fn url(&self, route: &Route) -> String {
        let domain = self
            .domain(&route.domain)
            .map_or("", |domain| domain.name.as_str());
        format!("{}.{domain}", route.host)
    }

    /// Adds the app `name`, stopped, with its web process.
    pub fn create_app(&mut self, name: &str) -> &mut App {
        let at = now();
        self.apps.push(App {
            guid: guid(),
            name: name.to_string(),
            started: false,
            created_at: at.clone(),
            updated_at: at.clone(),
            lifecycle: Lifecycle::Buildpack,
            buildpacks: Vec::new(),
            stack: STACK.into(),
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
            process: Process {
                guid: guid(),
                instances: 1,
                memory_mb: 256,
                disk_mb: 1024,
                log_rate_limit: -1,
                command: None,
                health_check: HealthCheck::default(),
                created_at: at.clone(),
                updated_at: at,
            },
            current_droplet: None,
            env: BTreeMap::new(),
            running: None,
        });
        self.apps.last_mut().expect("just pushed")
    }

    /// Adds the route `host` of the domain, unmapped.
    pub fn create_route(&mut self, host: &str, domain: &str) -> &mut Route {
        let at = now();
        self.routes.push(Route {
            guid: guid(),
            host: host.to_string(),
            domain: domain.to_string(),
            app: None,
            destination: None,
            created_at: at.clone(),
            updated_at: at,
        });
        self.routes.last_mut().expect("just pushed")
    }

    /// Maps the route to the app (a no-op when it is).
    pub fn map(&mut self, route: &str, app: &str) {
        if let Some(route) = self.route_mut(route)
            && route.app.as_deref() != Some(app)
        {
            route.app = Some(app.to_string());
            route.destination = Some(guid());
            route.updated_at = now();
        }
    }

    pub fn unmap(&mut self, route: &str) {
        if let Some(route) = self.route_mut(route)
            && route.app.is_some()
        {
            route.app = None;
            route.destination = None;
            route.updated_at = now();
        }
    }

    /// Records a job, complete, or failed with `errors`.
    pub fn add_job(
        &mut self,
        operation: &str,
        errors: Vec<JobError>,
        resource: Option<(String, String)>,
    ) -> String {
        let at = now();
        let job = Job {
            guid: guid(),
            operation: operation.to_string(),
            errors,
            resource,
            created_at: at.clone(),
            updated_at: at,
        };
        let guid = job.guid.clone();
        self.jobs.push(job);
        if self.jobs.len() > 1000 {
            self.jobs.remove(0);
        }
        guid
    }

    /// Removes the app, its routes' mapping, its bindings, packages, builds,
    /// droplets and deployments; the blobs no other app needs are listed to
    /// delete.
    pub fn remove_app(&mut self, guid: &str) -> Vec<Blob> {
        let mut blobs = Vec::new();
        self.bindings.retain(|binding| binding.app != guid);
        for route in self.routes.iter_mut() {
            if route.app.as_deref() == Some(guid) {
                route.app = None;
                route.destination = None;
            }
        }
        self.packages.retain(|package| {
            if package.app == guid {
                blobs.push(Blob::Package(package.guid.clone()));
            }
            package.app != guid
        });
        self.builds.retain(|build| build.app != guid);
        self.deployments.retain(|deployment| deployment.app != guid);
        let gone: Vec<Droplet> = self
            .droplets
            .iter()
            .filter(|droplet| droplet.app == guid)
            .cloned()
            .collect();
        self.droplets.retain(|droplet| droplet.app != guid);
        for droplet in gone {
            if !self
                .droplets
                .iter()
                .any(|kept| kept.sha256 == droplet.sha256)
            {
                blobs.push(Blob::Droplet(droplet.sha256));
            }
        }
        self.apps.retain(|app| app.guid != guid);
        self.wicket.remove(guid);
        blobs
    }

    /// Makes `droplet` the app's current one and keeps only it and the one
    /// before; the droplet blobs no droplet names any more are listed to
    /// delete.
    pub fn set_current_droplet(&mut self, app: &str, droplet: &str) -> Vec<Blob> {
        let previous = self
            .app(app)
            .and_then(|app| app.current_droplet.clone())
            .filter(|previous| previous != droplet);
        let keep = |guid: &str| guid == droplet || previous.as_deref() == Some(guid);
        let gone: Vec<Droplet> = self
            .droplets
            .iter()
            .filter(|kept| kept.app == app && !keep(&kept.guid))
            .cloned()
            .collect();
        self.droplets
            .retain(|kept| kept.app != app || keep(&kept.guid));
        if let Some(app) = self.app_mut(app) {
            app.current_droplet = Some(droplet.to_string());
            app.updated_at = now();
        }
        gone.into_iter()
            .filter(|droplet| {
                !self
                    .droplets
                    .iter()
                    .any(|kept| kept.sha256 == droplet.sha256)
            })
            .map(|droplet| Blob::Droplet(droplet.sha256))
            .collect()
    }

    /// Fails the builds a stop of the cf-api cut short: no staging outlives
    /// it.
    pub fn fail_unfinished_builds(&mut self) {
        for build in self
            .builds
            .iter_mut()
            .filter(|build| build.state == "STAGING")
        {
            build.state = "FAILED".into();
            build.error =
                Some("StagingError - cut short by a restart of the cf-api: stage again".into());
            build.updated_at = now();
        }
    }

    /// Keeps the app's newest package only; the others are listed to delete.
    pub fn keep_newest_package(&mut self, app: &str) -> Vec<Blob> {
        let newest = self
            .packages
            .iter()
            .filter(|package| package.app == app)
            .map(|package| package.guid.clone())
            .next_back();
        let mut blobs = Vec::new();
        self.packages.retain(|package| {
            let kept = package.app != app || Some(&package.guid) == newest.as_ref();
            if !kept {
                blobs.push(Blob::Package(package.guid.clone()));
            }
            kept
        });
        blobs
    }
}

/// A blob to delete from the data directory.
#[derive(Debug, PartialEq)]
pub enum Blob {
    Package(String),
    Droplet(String),
}

/// The state under a lock, written to the data directory on every change.
pub struct Store {
    dir: PathBuf,
    state: Mutex<State>,
    /// Signalled on every change written (`changed`).
    changes: tokio::sync::Notify,
}

impl Store {
    /// Opens the data directory: the state kept there, else a new one.
    pub fn open(dir: &Path, org: &str, space: &str, domains: &[String]) -> Result<Store, String> {
        for sub in ["packages", "droplets"] {
            std::fs::create_dir_all(dir.join(sub))
                .map_err(|e| format!("{}: {e}", dir.join(sub).display()))?;
        }
        let file = dir.join("state.json");
        let state = match std::fs::read(&file) {
            Ok(json) => {
                let mut state: State = serde_json::from_slice(&json)
                    .map_err(|e| format!("{}: {e}", file.display()))?;
                state.follow_domains(domains);
                state.fail_unfinished_builds();
                state
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => State::new(org, space, domains),
            Err(e) => return Err(format!("{}: {e}", file.display())),
        };
        let store = Store {
            dir: dir.to_path_buf(),
            state: Mutex::new(state),
            changes: tokio::sync::Notify::new(),
        };
        store.read(|state| store.save(state))?;
        Ok(store)
    }

    pub fn read<R>(&self, f: impl FnOnce(&State) -> R) -> R {
        f(&self.state.lock().expect("no panic under the lock"))
    }

    /// Changes the state, and writes it.
    pub fn update<R>(&self, f: impl FnOnce(&mut State) -> R) -> R {
        let mut state = self.state.lock().expect("no panic under the lock");
        let result = f(&mut state);
        if let Err(e) = self.save(&state) {
            eprintln!("cannot write the state: {e}");
        }
        drop(state);
        self.changes.notify_one();
        result
    }

    /// Resolves after an `update` since the last call resolved (at once
    /// when one was made meanwhile); for one waiter.
    pub async fn changed(&self) {
        self.changes.notified().await;
    }

    /// Changes what is not kept (the wicket's outcomes).
    pub fn note<R>(&self, f: impl FnOnce(&mut State) -> R) -> R {
        f(&mut self.state.lock().expect("no panic under the lock"))
    }

    fn save(&self, state: &State) -> Result<(), String> {
        let file = self.dir.join("state.json");
        let json = serde_json::to_vec_pretty(state).map_err(|e| e.to_string())?;
        write_by_rename(&file, &json)
    }

    pub fn package_path(&self, guid: &str) -> PathBuf {
        self.dir.join("packages").join(format!("{guid}.zip"))
    }

    pub fn droplet_path(&self, sha256: &str) -> PathBuf {
        self.dir.join("droplets").join(format!("{sha256}.wasm"))
    }

    /// Writes a blob by a rename.
    pub fn write_package(&self, guid: &str, bytes: &[u8]) -> Result<(), String> {
        write_by_rename(&self.package_path(guid), bytes)
    }

    pub fn write_droplet(&self, sha256: &str, bytes: &[u8]) -> Result<(), String> {
        let path = self.droplet_path(sha256);
        if path.exists() {
            return Ok(());
        }
        write_by_rename(&path, bytes)
    }

    pub fn delete(&self, blobs: &[Blob]) {
        for blob in blobs {
            let path = match blob {
                Blob::Package(guid) => self.package_path(guid),
                Blob::Droplet(sha256) => self.droplet_path(sha256),
            };
            if let Err(e) = std::fs::remove_file(&path)
                && e.kind() != std::io::ErrorKind::NotFound
            {
                eprintln!("cannot delete {}: {e}", path.display());
            }
        }
    }
}

/// Writes `bytes` to `path` by a rename, for its owner only (0600) from the
/// first byte on.
pub(crate) fn write_by_rename(path: &Path, bytes: &[u8]) -> Result<(), String> {
    use std::io::Write as _;
    let tmp = path.with_extension("tmp");
    // one a crash left keeps its mode: made anew
    let _ = std::fs::remove_file(&tmp);
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    options
        .open(&tmp)
        .and_then(|mut file| file.write_all(bytes))
        .and_then(|()| std::fs::rename(&tmp, path))
        .map_err(|e| format!("{}: {e}", path.display()))
}

pub fn guid() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// Seconds since the epoch.
pub fn epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_secs())
}

/// Nanoseconds since the epoch.
pub fn epoch_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_nanos())
}

/// The time now as `2026-10-30T01:17:47Z`.
pub fn now() -> String {
    rfc3339(epoch_secs())
}

/// `secs` since the epoch as `2026-10-30T01:17:47Z`.
pub fn rfc3339(secs: u64) -> String {
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
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_current_droplet_keeps_the_previous_the_packages_the_newest_and_no_build_outlives_a_restart()
     {
        let mut state = State::new("o", "s", &["localhost".into()]);
        let app = state.create_app("hello").guid.clone();
        let droplet = |guid: &str, sha256: &str| Droplet {
            guid: guid.into(),
            app: app.clone(),
            package: "p".into(),
            sha256: sha256.into(),
            file: "hello.wasm".into(),
            bytes: 1,
            image: None,
            created_at: now(),
            updated_at: now(),
        };
        // as pushes go: a droplet staged, then made current
        state.droplets.push(droplet("d1", "a"));
        assert_eq!(state.set_current_droplet(&app, "d1"), vec![]);
        state.droplets.push(droplet("d2", "b"));
        assert_eq!(state.set_current_droplet(&app, "d2"), vec![]);
        state.droplets.push(droplet("d3", "a"));
        assert_eq!(state.set_current_droplet(&app, "d3"), vec![]);
        // d1 (sha a) went, but d3 has sha a too; d2 (sha b) is the previous
        let kept: Vec<&str> = state.droplets.iter().map(|d| d.guid.as_str()).collect();
        assert_eq!(kept, vec!["d2", "d3"]);
        state.droplets.push(droplet("d4", "c"));
        assert_eq!(
            state.set_current_droplet(&app, "d4"),
            vec![Blob::Droplet("b".into())]
        );
        // a zip, then an image whose password no debug output shows
        for (guid, docker) in [
            ("p1", None),
            (
                "p2",
                Some(DockerImage {
                    image: "ghcr.io/me/hello:1".into(),
                    username: Some("me".into()),
                    password: Some("p4ss".into()),
                }),
            ),
        ] {
            state.packages.push(Package {
                guid: guid.into(),
                app: app.clone(),
                state: "READY".into(),
                sha256: None,
                docker,
                created_at: now(),
                updated_at: now(),
            });
        }
        assert_eq!(
            state.keep_newest_package(&app),
            vec![Blob::Package("p1".into())]
        );
        let shown = format!("{:?}", state.packages);
        assert!(
            shown.contains("\"***\"") && !shown.contains("p4ss"),
            "{shown}"
        );
        // a staging cut short fails, a staged build stays
        for (guid, staged) in [("b1", "STAGING"), ("b2", "STAGED")] {
            state.builds.push(Build {
                guid: guid.into(),
                app: app.clone(),
                package: "p2".into(),
                state: staged.into(),
                lifecycle: Lifecycle::Docker,
                error: None,
                droplet: None,
                created_at: now(),
                updated_at: now(),
            });
        }
        state.fail_unfinished_builds();
        let states: Vec<&str> = state.builds.iter().map(|b| b.state.as_str()).collect();
        assert_eq!(states, ["FAILED", "STAGED"]);
        // its bindings go with it, another app's stay
        for (guid, bound) in [("b1", app.as_str()), ("b2", "other")] {
            state.bindings.push(Binding {
                guid: guid.into(),
                name: None,
                app: bound.into(),
                instance: "i".into(),
                identifier: String::new(),
                created_at: now(),
                updated_at: now(),
            });
        }
        assert_eq!(
            state.remove_app(&app),
            vec![
                Blob::Package("p2".into()),
                Blob::Droplet("a".into()),
                Blob::Droplet("c".into())
            ]
        );
        assert!(state.apps.is_empty());
        let kept: Vec<&str> = state.bindings.iter().map(|b| b.guid.as_str()).collect();
        assert_eq!(kept, ["b2"]);
    }

    #[test]
    fn the_flags_first_domain_takes_the_default_routes_a_gone_one_stays_behind() {
        let mut state = State::new("o", "s", &["localhost".into()]);
        let localhost = state.domains[0].guid.clone();
        state.follow_domains(&["apps.example.com".into()]);
        let names: Vec<&str> = state.domains.iter().map(|d| d.name.as_str()).collect();
        assert_eq!(names, ["apps.example.com", "localhost"]);
        // back in the flags, with the guid its routes hold
        state.follow_domains(&["localhost".into(), "apps.example.com".into()]);
        assert_eq!(state.domains.len(), 2);
        assert_eq!(state.domains[0].guid, localhost);
    }

    #[test]
    fn rfc3339_formats_the_epoch_seconds() {
        assert_eq!(rfc3339(0), "1970-01-01T00:00:00Z");
        assert_eq!(rfc3339(1_793_323_067), "2026-10-30T01:17:47Z");
    }
}
