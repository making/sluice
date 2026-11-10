//! The wicket's app API (its console, `--console`): an app is hosts served
//! by one component in one sandbox, put and removed by its owner; its
//! component is uploaded by digest first.
//!
//! ```text
//! GET    /api/apps[?after=<revision>]          the apps, the capabilities and the keyvalue
//!                                              stores; held until they change with `after`
//! PUT    /api/components/<sha256>              keeps the body under its digest
//! PUT    /api/files/<sha256>                   a wasm wicket: unpacks the body, a tar of an
//!                                              app's files, under its digest
//! PUT    /api/images                           a Firecracker wicket: makes an ext4 image of
//!                                              an OCI image, kept under its digest (with
//!                                              `layers` and `launch`: of a droplet over it)
//! PUT    /api/layers/<sha256>                  a Firecracker wicket: keeps the body, a tar: a
//!                                              build's source, a droplet built elsewhere
//! GET    /api/layers/<sha256>?at=<n>           the kept tar from byte n on, a piece a request
//! POST   /api/builds                           a Firecracker wicket: builds the source the
//!                                              body names with a builder's buildpacks
//! GET    /api/builds/<id>?after=<n>            its output from line n on and, ended, its
//!                                              droplet or its error; held until there is more
//! DELETE /api/builds/<id>                      ends and forgets it
//! PUT    /api/apps/<name>                      serves the app of the body
//! DELETE /api/apps/<name>?owner=<owner>        removes the app
//! DELETE /api/keyvalue/<store>/<namespace>     deletes the data of a namespace no route maps
//! PUT    /api/evacuated/<owner>                a wicket leaving: the owner moved what of its
//!                                              apps it could, the rest staying until it drains
//! PUT    /api/confirmed/<owner>                the owner's apps the body names are as it wants
//!                                              them: those a start of the wicket holds served
//! ```

use std::collections::BTreeMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// A PUT may compile the component: generous.
const EDIT_TIMEOUT: Duration = Duration::from_secs(240);
/// A rootfs made of an image, or uploaded (4 GiB at most): minutes, the wicket's own bound of
/// a make a little within it.
const IMAGE_TIMEOUT: Duration = Duration::from_secs(660);
/// A listing with `after` is held 25s at most.
const LIST_TIMEOUT: Duration = Duration::from_secs(40);
/// A build's output is held 20s at most.
const FOLLOW_TIMEOUT: Duration = Duration::from_secs(40);
/// A piece of a layer (8 MiB at most).
const PIECE_TIMEOUT: Duration = Duration::from_secs(120);
/// One without is answered at once: `cf app` does not wait long to show the wicket away.
const READ_TIMEOUT: Duration = Duration::from_secs(5);

/// A wicket's app API, as one of the wickets configured.
pub struct Wicket {
    client: reqwest::Client,
    base: String,
    token: Option<String>,
    pub owner: String,
    /// Its name: the configuration's, the state's placement's and `cf app`'s `host`.
    pub name: String,
    /// What the log lines call it: `the wicket`, or of several `the wicket <name>`.
    pub label: String,
}

/// The body of an app's PUT, and an app as listed: `S` its sandbox, as a wicket of a runtime has
/// it ([`Sandbox`]), or as listed before the listing's runtime tells which.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct AppBody<S = Sandbox> {
    pub owner: String,
    pub hosts: Vec<String>,
    pub component: ComponentRef,
    /// A static site's files, read-only at `/` for its component.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub files: Option<FilesRef>,
    pub sandbox: S,
    /// Opaque to the wicket, listed as put: the snapshot's
    /// (`crate::env::tag`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tag: Option<String>,
    /// How long a start of the wicket holds the app at most, until its owner confirms it: put
    /// to a wicket listing its start alone (`crate::controller`).
    #[serde(default, rename = "held-ms", skip_serializing_if = "Option::is_none")]
    pub held_ms: Option<u64>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct ComponentRef {
    pub sha256: String,
    pub name: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct FilesRef {
    pub sha256: String,
}

/// The sandbox: the route options in JSON, named as the wicket's route options are, as the
/// wicket of each runtime runs apps. A key left out takes the wicket's default; a listing names
/// the keys as [`WasmSandbox`] and [`VmSandbox`] say, the env by the names only.
#[derive(Serialize, Clone, Debug, PartialEq)]
#[serde(untagged)]
pub enum Sandbox {
    Wasm(WasmSandbox),
    Vm(VmSandbox),
}

/// A wasm wicket's: the `--wasm` options, the keyvalue map and the env. Listed, every key is
/// named but `instances` and `keep-ms`, unset.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default)]
#[serde(rename_all = "kebab-case")]
pub struct WasmSandbox {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub wasi: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub budget_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory_mib: Option<u64>,
    /// The instances kept warm.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warm: Option<u64>,
    /// The instances alive at once at most.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instances: Option<u64>,
    /// How long an instance beyond `warm` serving nothing is kept.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub keep_ms: Option<u64>,
    /// The requests an instance (a store) takes at once.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub concurrency: Option<u64>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub keyvalue: BTreeMap<String, String>,
    /// Put, never read back: the app's tag tells what the wicket serves.
    #[serde(skip_deserializing, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
}

/// A Firecracker wicket's: the `--vm` options and the env. Listed, `mode` and `memory-mib` are
/// named, the rest away from their defaults.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default)]
#[serde(rename_all = "kebab-case")]
pub struct VmSandbox {
    /// How a connection gets its VM.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
    /// A VM's memory in MiB.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory_mib: Option<u64>,
    /// The VMs alive at once at most: a pool's size, the VMs booted.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instances: Option<u64>,
    /// The VMs kept ahead of the connections.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warm: Option<u64>,
    /// How long a VM serving nothing is kept.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub keep_ms: Option<u64>,
    /// The connections a VM serves at most.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uses: Option<u64>,
    /// How long a VM serves at most.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lifetime_ms: Option<u64>,
    /// The connections a pool's VM holds before the pool grows.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub concurrency: Option<u64>,
    /// How long a VM's process may take to listen.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boot_ms: Option<u64>,
    /// Its readiness check, `port` or `http`, and its endpoint, timeout and interval.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub readiness: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub readiness_endpoint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub readiness_timeout_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub readiness_interval_ms: Option<u64>,
    /// Its liveness check, `http` (none: `port`, a VM up once its port accepts), and its
    /// endpoint, timeout and interval.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub liveness: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub liveness_endpoint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub liveness_timeout_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub liveness_interval_ms: Option<u64>,
    /// The destinations its VMs may connect to.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub egress: Vec<String>,
    /// Put, never read back: the app's tag tells what the wicket serves.
    #[serde(skip_deserializing, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
}

impl AppBody<serde_json::Value> {
    /// The app as a wicket of `runtime` lists it.
    pub fn typed(self, runtime: Runtime) -> serde_json::Result<AppBody> {
        Ok(AppBody {
            owner: self.owner,
            hosts: self.hosts,
            component: self.component,
            files: self.files,
            sandbox: match runtime {
                Runtime::Wasm => Sandbox::Wasm(serde_json::from_value(self.sandbox)?),
                Runtime::Firecracker => Sandbox::Vm(serde_json::from_value(self.sandbox)?),
            },
            tag: self.tag,
            held_ms: self.held_ms,
        })
    }
}

#[cfg(test)]
impl Sandbox {
    pub fn wasm(&self) -> &WasmSandbox {
        match self {
            Sandbox::Wasm(sandbox) => sandbox,
            Sandbox::Vm(_) => panic!("a VM's sandbox"),
        }
    }

    pub fn wasm_mut(&mut self) -> &mut WasmSandbox {
        match self {
            Sandbox::Wasm(sandbox) => sandbox,
            Sandbox::Vm(_) => panic!("a VM's sandbox"),
        }
    }

    pub fn vm(&self) -> &VmSandbox {
        match self {
            Sandbox::Vm(sandbox) => sandbox,
            Sandbox::Wasm(_) => panic!("a component's sandbox"),
        }
    }

    pub fn vm_mut(&mut self) -> &mut VmSandbox {
        match self {
            Sandbox::Vm(sandbox) => sandbox,
            Sandbox::Wasm(_) => panic!("a component's sandbox"),
        }
    }
}

impl Sandbox {
    /// The VMs a wicket keeps up of a route as listed, whatever its requests: of a pool `warm`,
    /// its default `instances`, booted `instances`, 1 going unlisted. None of the other modes, a
    /// clone coming and going with its connection, nor of a component, whose units the wicket
    /// lists as it has them.
    pub fn kept(&self) -> u64 {
        let Sandbox::Vm(vm) = self else {
            return 0;
        };
        match vm.mode.as_deref() {
            Some("pool") => vm.warm.or(vm.instances).unwrap_or(0),
            Some("boot") => vm.instances.unwrap_or(1),
            _ => 0,
        }
    }
}

/// What a wicket runs apps as, and so the stack of a droplet: what is pushed decides where it
/// runs.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum Runtime {
    /// wasm components: a wicket whose listing names none is one.
    #[serde(rename = "wasip3")]
    Wasm,
    /// ext4 images in microVMs.
    #[serde(rename = "firecracker")]
    Firecracker,
}

impl Runtime {
    /// The stack of its apps.
    pub fn stack(self) -> &'static str {
        match self {
            Runtime::Wasm => crate::store::STACK,
            Runtime::Firecracker => crate::store::ROOTFS_STACK,
        }
    }

    /// The runtime of the stack `stack`, if it names one.
    pub fn named(stack: &str) -> Option<Runtime> {
        [Runtime::Wasm, Runtime::Firecracker]
            .into_iter()
            .find(|runtime| runtime.stack() == stack)
    }

    /// The runtime of a droplet whose file is `file`: a rootfs's name ends so.
    pub fn of_file(file: &str) -> Runtime {
        match file.ends_with(crate::staging::ROOTFS) {
            true => Runtime::Firecracker,
            false => Runtime::Wasm,
        }
    }
}

/// `stacks` as a sentence names them: `wasip3, firecracker`; none known, so.
pub fn stacks_named(stacks: &[Runtime]) -> String {
    match stacks {
        [] => "none known yet".into(),
        stacks => {
            let names: Vec<&str> = stacks.iter().map(|stack| stack.stack()).collect();
            names.join(", ")
        }
    }
}

/// An image the wicket made of an OCI image: what an app names.
#[derive(Deserialize, Debug)]
pub struct MadeImage {
    /// The reference, pinned to its manifest's digest.
    pub image: String,
    pub name: String,
    pub sha256: String,
    pub bytes: u64,
}

/// A build the wicket is asked for (`POST /api/builds`).
#[derive(Serialize, Debug)]
pub struct BuildRequest<'a> {
    /// The builder image.
    pub builder: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub password: Option<&'a str>,
    /// The run image in place of the builder's.
    #[serde(rename = "run-image", skip_serializing_if = "Option::is_none")]
    pub run_image: Option<&'a str>,
    /// The digest of the source, a layer kept.
    pub source: &'a str,
    /// The app's environment, as the buildpacks see it.
    pub env: &'a BTreeMap<String, String>,
    /// The destinations the build may connect to.
    pub egress: &'a [String],
    /// The build VM's size and time.
    #[serde(rename = "memory-mib")]
    pub memory_mib: u64,
    #[serde(rename = "disk-mib")]
    pub disk_mib: u64,
    pub vcpus: u32,
    #[serde(rename = "timeout-ms")]
    pub timeout_ms: u64,
    /// The builder's buildpacks detecting, `<id>[@<version>]`: none, the builder's order.
    #[serde(skip_serializing_if = "<[String]>::is_empty")]
    pub buildpacks: &'a [String],
    /// What the droplet runs, a shell's command line: none, the default process.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command: Option<&'a str>,
    /// The name of the cache the wicket keeps between the builds: the app's guid.
    pub cache: &'a str,
}

/// A build as the wicket tells it: its output from the line asked for on and, once every line
/// was read, how it ended.
#[derive(Deserialize, Debug)]
pub struct BuildView {
    /// `building`, `built` or `failed`.
    pub state: String,
    /// The next request's `after`.
    pub next: u64,
    #[serde(default)]
    pub lines: Vec<BuildLine>,
    #[serde(default)]
    pub error: Option<String>,
    /// The droplet, a layer the wicket keeps.
    #[serde(default)]
    pub droplet: Option<LayerRef>,
    /// The run image, pinned to its digest.
    #[serde(default, rename = "run-image")]
    pub run_image: Option<String>,
    /// What the droplet runs, as `PUT /api/images` takes it back.
    #[serde(default)]
    pub launch: Option<serde_json::Value>,
}

#[derive(Deserialize, Debug)]
pub struct BuildLine {
    /// `stdout` or `stderr`.
    pub stream: String,
    pub line: String,
}

/// A layer the wicket keeps.
#[derive(Deserialize, Debug)]
pub struct LayerRef {
    pub sha256: String,
    pub bytes: u64,
}

/// The wicket's apps: `S` their sandboxes, as [`AppBody`]'s.
#[derive(Deserialize, Clone, Debug)]
pub struct Listing<S = Sandbox> {
    pub revision: u64,
    pub generation: u64,
    /// Which wicket it is: none from a wicket predating it.
    #[serde(default)]
    pub wicket: Option<ListedWicket>,
    /// Its budget: none from a wicket predating it.
    #[serde(default)]
    pub budget: Option<ListedBudget>,
    /// Told to stop, it serves on until the apps it waits for are elsewhere: none while it is
    /// not, as from a wicket predating it.
    #[serde(default)]
    pub leaving: Option<ListedLeaving>,
    /// `firecracker`, or none from a wasm wicket.
    #[serde(default)]
    pub runtime: Option<String>,
    /// What a sandbox may hold: the capabilities, in the wicket's order.
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub keyvalue_stores: Vec<ListedStore>,
    pub apps: Vec<ListedApp<S>>,
}

impl<S> Listing<S> {
    pub fn runtime(&self) -> Runtime {
        match self.runtime.as_deref() {
            Some("firecracker") => Runtime::Firecracker,
            _ => Runtime::Wasm,
        }
    }
}

impl Listing<serde_json::Value> {
    /// The listing, its apps' sandboxes those of its runtime.
    fn typed(self) -> serde_json::Result<Listing> {
        let runtime = self.runtime();
        let apps = self
            .apps
            .into_iter()
            .map(|listed| listed.typed(runtime))
            .collect::<serde_json::Result<_>>()?;
        Ok(Listing {
            revision: self.revision,
            generation: self.generation,
            wicket: self.wicket,
            budget: self.budget,
            leaving: self.leaving,
            runtime: self.runtime,
            capabilities: self.capabilities,
            keyvalue_stores: self.keyvalue_stores,
            apps,
        })
    }
}

/// Which wicket answers.
#[derive(Deserialize, Clone, Debug, PartialEq)]
pub struct ListedWicket {
    /// Made at its first start, kept in its console's directory.
    pub id: String,
    /// Its node among the flume's (`wicket:<client-id>`): none without a flume.
    #[serde(default)]
    pub node: Option<String>,
    pub build: String,
    /// Its start, in seconds since the epoch: from then on it holds an app put with `held-ms`
    /// until its owner confirms it. None from a wicket predating it, which takes no `held-ms`.
    #[serde(default)]
    pub since: Option<u64>,
}

/// The instances a wicket keeps at once at most, and what its routes keep of them for good: a
/// Firecracker wicket's guest memory too.
#[derive(Deserialize, Clone, Debug, PartialEq)]
pub struct ListedBudget {
    pub max_instances: u64,
    #[serde(default)]
    pub max_memory_mib: Option<u64>,
    pub instances: u64,
    #[serde(default)]
    pub memory_mib: Option<u64>,
}

/// A wicket leaving: serving on until the apps of its app API are elsewhere, or its bound.
#[derive(Deserialize, Clone, Debug, PartialEq)]
pub struct ListedLeaving {
    /// When it drains at the latest, in seconds since the epoch.
    pub until: u64,
    /// The owners that said they moved what of their apps they could.
    #[serde(default)]
    pub evacuated: Vec<String>,
}

#[derive(Deserialize, Clone, Debug, PartialEq)]
pub struct ListedStore {
    pub name: String,
    /// `sqlite`, `redis` or `gemfire`.
    #[serde(rename = "type")]
    pub kind: String,
    pub default: bool,
    /// Which store it is when the wickets of other hosts reach it too (`redis://cache:6379/0`);
    /// none for one of the wicket's host, as from a wicket predating it.
    #[serde(default)]
    pub shared: Option<String>,
}

#[derive(Deserialize, Clone, Debug)]
pub struct ListedApp<S = Sandbox> {
    pub name: String,
    #[serde(flatten)]
    pub app: AppBody<S>,
    pub deployed: String,
    /// The instances alive, lowest index first.
    pub alive: Vec<ListedInstance>,
    /// The VMs it keeps that are down, each starting again.
    #[serde(default)]
    pub crashed: Vec<ListedCrashed>,
    /// Why it is short of VMs: one it keeps went down or does not start, or none is alive and
    /// their restores fail.
    #[serde(default)]
    pub down: Option<ListedDown>,
    /// Whether the server of every tunnel stream of the wicket took its routes as they are: it
    /// sends them connections. None from a wicket predating it.
    #[serde(default)]
    pub advertised: Option<bool>,
    /// Whether the wicket holds it since its start until its owner confirms it: it serves it
    /// not.
    #[serde(default)]
    pub held: bool,
}

impl ListedApp<serde_json::Value> {
    /// The app as a wicket of `runtime` lists it.
    pub fn typed(self, runtime: Runtime) -> serde_json::Result<ListedApp> {
        Ok(ListedApp {
            name: self.name,
            app: self.app.typed(runtime)?,
            deployed: self.deployed,
            alive: self.alive,
            crashed: self.crashed,
            down: self.down,
            advertised: self.advertised,
            held: self.held,
        })
    }
}

#[derive(Deserialize, Clone, Debug)]
pub struct ListedCrashed {
    /// The wicket's index, as the live access log names it.
    pub index: u64,
    pub why: String,
    /// Since when it goes on, in seconds since the epoch.
    pub since: u64,
}

#[derive(Deserialize, Clone, Debug)]
pub struct ListedDown {
    pub why: String,
    /// Since when it goes on, in seconds since the epoch.
    pub since: u64,
}

#[derive(Deserialize, Clone, Debug)]
pub struct ListedInstance {
    /// The wicket's index, as the live access log names it.
    pub index: u64,
    /// Its start, in seconds since the epoch.
    pub since: u64,
    /// Of a wicket checking its readiness: whether it takes requests. None: it does.
    #[serde(default)]
    pub ready: Option<bool>,
}

#[derive(Deserialize, Debug)]
pub struct Edited {
    pub generation: u64,
    pub message: String,
}

/// Why the wicket did not do it.
#[derive(Debug, Clone, PartialEq)]
pub enum Failure {
    /// The wicket answered no (4xx): its reason, the host of the route that
    /// did not load, the components it has no upload of, whether the app's VMs did not start,
    /// whether its routes would need more than its budget.
    Refused {
        status: u16,
        error: String,
        host: Option<String>,
        missing: Vec<String>,
        did_not_start: bool,
        no_room: bool,
    },
    /// No answer, or none to act on (5xx).
    Unreachable(String),
}

impl Failure {
    /// The wicket refused it with `error` alone.
    pub fn refused(status: u16, error: String) -> Failure {
        Failure::Refused {
            status,
            error,
            host: None,
            missing: Vec::new(),
            did_not_start: false,
            no_room: false,
        }
    }

    /// Whether the wicket refused it for room.
    pub fn no_room(&self) -> bool {
        matches!(self, Failure::Refused { no_room: true, .. })
    }

    /// Whether a removal the wicket answered so is to be tried again: anything but another
    /// owner's app, which stays that way.
    pub fn retried(&self) -> bool {
        !matches!(self, Failure::Refused { status: 409, .. })
    }
}

impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Failure::Refused { error, .. } => f.write_str(error),
            Failure::Unreachable(error) => write!(f, "the wicket is unreachable: {error}"),
        }
    }
}

#[derive(Deserialize)]
struct ErrorBody {
    error: String,
    host: Option<String>,
    #[serde(default)]
    missing: Vec<String>,
    #[serde(default)]
    did_not_start: bool,
    #[serde(default)]
    no_room: bool,
}

impl Wicket {
    /// The wicket `name` at `base`, one of `of` wickets.
    pub fn new(name: &str, of: usize, base: &str, token: Option<String>, owner: &str) -> Wicket {
        Wicket {
            client: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(5))
                // let go of before its console closes it: no request lands on a closing one
                .pool_idle_timeout(Duration::from_secs(30))
                .build()
                .expect("a plain client"),
            base: base.trim_end_matches('/').to_string(),
            token,
            owner: owner.to_string(),
            name: name.to_string(),
            label: match of {
                1 => "the wicket".to_string(),
                _ => format!("the wicket {name}"),
            },
        }
    }

    /// [`Wicket::label`] opening a sentence.
    pub fn sentence(&self) -> String {
        let mut label = self.label.clone();
        label.replace_range(..1, "T");
        label
    }

    /// The apps; with `after` the current revision, once they changed (25s
    /// at most).
    pub async fn list(&self, after: Option<u64>) -> Result<Listing, Failure> {
        let (url, timeout) = match after {
            Some(after) => (
                format!("{}/api/apps?after={after}", self.base),
                LIST_TIMEOUT,
            ),
            None => (format!("{}/api/apps", self.base), READ_TIMEOUT),
        };
        let request = self.client.get(url).timeout(timeout);
        let (status, body) = self.send(request).await?;
        if !status.is_success() {
            return Err(refused(status, &body));
        }
        serde_json::from_slice::<Listing<serde_json::Value>>(&body)
            .and_then(Listing::typed)
            .map_err(|e| Failure::Unreachable(format!("the listing: {e}")))
    }

    pub async fn put(&self, name: &str, app: &AppBody) -> Result<Edited, Failure> {
        let request = self
            .client
            .put(format!("{}/api/apps/{name}", self.base))
            .timeout(EDIT_TIMEOUT)
            .json(app);
        let (status, body) = self.send(request).await?;
        if !status.is_success() {
            return Err(refused(status, &body));
        }
        serde_json::from_slice(&body).map_err(|e| Failure::Unreachable(format!("the answer: {e}")))
    }

    /// Uploads the component in `file` as it is read: it may be larger than this process's
    /// memory.
    pub async fn upload(&self, sha256: &str, file: tokio::fs::File) -> Result<(), Failure> {
        self.upload_as("components", "application/wasm", sha256, file)
            .await
    }

    /// Uploads a static site's files, the tar in `file`, as [`Wicket::upload`] a component.
    pub async fn upload_files(&self, sha256: &str, file: tokio::fs::File) -> Result<(), Failure> {
        self.upload_as("files", "application/x-tar", sha256, file)
            .await
    }

    /// PUTs `file`, of `content_type`, to `/api/<kind>/<sha256>`.
    async fn upload_as(
        &self,
        kind: &str,
        content_type: &str,
        sha256: &str,
        file: tokio::fs::File,
    ) -> Result<(), Failure> {
        let bytes = file
            .metadata()
            .await
            .map_err(|e| Failure::Unreachable(format!("the upload of {kind}: {e}")))?
            .len();
        let request = self
            .client
            .put(format!("{}/api/{kind}/{sha256}", self.base))
            .timeout(IMAGE_TIMEOUT)
            .header("content-type", content_type)
            .header("content-length", bytes)
            .body(file);
        let (status, body) = self.send(request).await?;
        if !status.is_success() {
            return Err(refused(status, &body));
        }
        Ok(())
    }

    /// Uploads a layer, the tar in `file`, as [`Wicket::upload`] a component: a build's source,
    /// a droplet built on another wicket. Kept as long as an upload no app names.
    pub async fn upload_layer(&self, sha256: &str, file: tokio::fs::File) -> Result<(), Failure> {
        self.upload_as("layers", "application/x-tar", sha256, file)
            .await
    }

    /// A piece of the layer `sha256` the wicket keeps, from the byte `at` on, and the layer's
    /// size.
    pub async fn layer_piece(&self, sha256: &str, at: u64) -> Result<(u64, bytes::Bytes), Failure> {
        let request = self
            .client
            .get(format!("{}/api/layers/{sha256}?at={at}", self.base))
            .timeout(PIECE_TIMEOUT);
        let (status, headers, body) = self.answer(request).await?;
        if !status.is_success() {
            return Err(refused(status, &body));
        }
        let bytes = headers
            .get("sluice-layer-bytes")
            .and_then(|bytes| bytes.to_str().ok()?.parse::<u64>().ok())
            .ok_or_else(|| Failure::Unreachable("the layer: no sluice-layer-bytes".into()))?;
        Ok((bytes, body))
    }

    /// Starts the build `build`: its id.
    pub async fn start_build(&self, build: &BuildRequest<'_>) -> Result<String, Failure> {
        let request = self
            .client
            .post(format!("{}/api/builds", self.base))
            .timeout(EDIT_TIMEOUT)
            .json(build);
        let (status, body) = self.send(request).await?;
        if !status.is_success() {
            return Err(refused(status, &body));
        }
        #[derive(Deserialize)]
        struct Started {
            id: String,
        }
        serde_json::from_slice::<Started>(&body)
            .map(|started| started.id)
            .map_err(|e| Failure::Unreachable(format!("the answer: {e}")))
    }

    /// The build `id` from its line `after` on, once there is more of it (20s at most).
    pub async fn follow_build(&self, id: &str, after: u64) -> Result<BuildView, Failure> {
        let request = self
            .client
            .get(format!("{}/api/builds/{id}?after={after}", self.base))
            .timeout(FOLLOW_TIMEOUT);
        let (status, body) = self.send(request).await?;
        if !status.is_success() {
            return Err(refused(status, &body));
        }
        serde_json::from_slice(&body).map_err(|e| Failure::Unreachable(format!("the answer: {e}")))
    }

    /// Forgets the cache of the builds `name` (an app's guid), if the wicket keeps one.
    pub async fn forget_cache(&self, name: &str) -> Result<(), Failure> {
        let request = self
            .client
            .delete(format!("{}/api/caches/{name}", self.base))
            .timeout(READ_TIMEOUT);
        let (status, body) = self.send(request).await?;
        if !status.is_success() && status != reqwest::StatusCode::NOT_FOUND {
            return Err(refused(status, &body));
        }
        Ok(())
    }

    /// Ends and forgets the build `id`.
    pub async fn forget_build(&self, id: &str) -> Result<(), Failure> {
        let request = self
            .client
            .delete(format!("{}/api/builds/{id}", self.base))
            .timeout(READ_TIMEOUT);
        let (status, body) = self.send(request).await?;
        if !status.is_success() && status != reqwest::StatusCode::NOT_FOUND {
            return Err(refused(status, &body));
        }
        Ok(())
    }

    /// Has the wicket make an ext4 image of the OCI image `image`, with the
    /// `(username, password)` of a private one; with `over`, of a droplet over it: the layer
    /// of that digest, which the wicket keeps, and the launch its build answered.
    pub async fn make_image(
        &self,
        image: &str,
        credentials: Option<(&str, &str)>,
        over: Option<(&str, &serde_json::Value)>,
    ) -> Result<MadeImage, Failure> {
        let mut body = serde_json::json!({"image": image});
        if let Some((username, password)) = credentials {
            body["username"] = username.into();
            body["password"] = password.into();
        }
        if let Some((layer, launch)) = over {
            body["layers"] = serde_json::json!([layer]);
            body["launch"] = launch.clone();
        }
        let request = self
            .client
            .put(format!("{}/api/images", self.base))
            .timeout(IMAGE_TIMEOUT)
            .json(&body);
        let (status, body) = self.send(request).await?;
        if !status.is_success() {
            return Err(refused(status, &body));
        }
        serde_json::from_slice(&body).map_err(|e| Failure::Unreachable(format!("the answer: {e}")))
    }

    /// Tells the wicket, leaving, that the owner moved what of its apps it could: those left stay
    /// until it drains, no longer waited for.
    pub async fn evacuated(&self) -> Result<(), Failure> {
        let owner: String = form_urlencoded::byte_serialize(self.owner.as_bytes()).collect();
        let request = self
            .client
            .put(format!("{}/api/evacuated/{owner}", self.base))
            .timeout(READ_TIMEOUT);
        let (status, body) = self.send(request).await?;
        if !status.is_success() {
            return Err(refused(status, &body));
        }
        Ok(())
    }

    /// Tells the wicket the owner's apps `names` are as it wants them: those a start of it holds
    /// are served.
    pub async fn confirmed(&self, names: &[String]) -> Result<(), Failure> {
        let owner: String = form_urlencoded::byte_serialize(self.owner.as_bytes()).collect();
        let request = self
            .client
            .put(format!("{}/api/confirmed/{owner}", self.base))
            .timeout(READ_TIMEOUT)
            .json(&serde_json::json!({ "apps": names }));
        let (status, body) = self.send(request).await?;
        if !status.is_success() {
            return Err(refused(status, &body));
        }
        Ok(())
    }

    /// Removes the app; None when the wicket has none of the name.
    pub async fn delete(&self, name: &str) -> Result<Option<Edited>, Failure> {
        let owner: String = form_urlencoded::byte_serialize(self.owner.as_bytes()).collect();
        let request = self
            .client
            .delete(format!("{}/api/apps/{name}?owner={owner}", self.base))
            .timeout(EDIT_TIMEOUT);
        let (status, body) = self.send(request).await?;
        if status == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !status.is_success() {
            return Err(refused(status, &body));
        }
        serde_json::from_slice(&body)
            .map(Some)
            .map_err(|e| Failure::Unreachable(format!("the answer: {e}")))
    }

    /// The hosts whose routes map the keyvalue namespace, "a route being drained" for one no
    /// longer advertised.
    pub async fn mapping(&self, store: &str, namespace: &str) -> Result<Vec<String>, Failure> {
        let request = self
            .client
            .get(format!(
                "{}/api/keyvalue/{}",
                self.base,
                place(store, namespace)
            ))
            .timeout(EDIT_TIMEOUT);
        let (status, body) = self.send(request).await?;
        if !status.is_success() {
            return Err(refused(status, &body));
        }
        #[derive(Deserialize)]
        struct Mapping {
            mapped_by: Vec<String>,
        }
        serde_json::from_slice::<Mapping>(&body)
            .map(|mapping| mapping.mapped_by)
            .map_err(|e| Failure::Unreachable(format!("the answer: {e}")))
    }

    /// Deletes the data of the keyvalue namespace: how many keys it held.
    /// Refused (409) while a route maps it.
    pub async fn purge(&self, store: &str, namespace: &str) -> Result<u64, Failure> {
        let request = self
            .client
            .delete(format!(
                "{}/api/keyvalue/{}",
                self.base,
                place(store, namespace)
            ))
            .timeout(EDIT_TIMEOUT);
        let (status, body) = self.send(request).await?;
        if !status.is_success() {
            return Err(refused(status, &body));
        }
        #[derive(Deserialize)]
        struct Purged {
            keys: u64,
        }
        serde_json::from_slice::<Purged>(&body)
            .map(|purged| purged.keys)
            .map_err(|e| Failure::Unreachable(format!("the answer: {e}")))
    }

    async fn send(
        &self,
        request: reqwest::RequestBuilder,
    ) -> Result<(reqwest::StatusCode, bytes::Bytes), Failure> {
        let (status, _, body) = self.answer(request).await?;
        Ok((status, body))
    }

    /// The answer to `request`, sent with the token: its status, headers and body.
    async fn answer(
        &self,
        mut request: reqwest::RequestBuilder,
    ) -> Result<
        (
            reqwest::StatusCode,
            reqwest::header::HeaderMap,
            bytes::Bytes,
        ),
        Failure,
    > {
        if let Some(token) = &self.token {
            request = request.bearer_auth(token);
        }
        let response = request
            .send()
            .await
            .map_err(|e| Failure::Unreachable(e.to_string()))?;
        let status = response.status();
        let headers = response.headers().clone();
        let body = response
            .bytes()
            .await
            .map_err(|e| Failure::Unreachable(e.to_string()))?;
        if status.is_server_error() {
            return Err(Failure::Unreachable(format!(
                "{status}: {}",
                error_text(&body)
            )));
        }
        Ok((status, headers, body))
    }
}

/// The path of a keyvalue namespace: no space in a store's name nor in `cf/<guid>`, so form
/// encoding is path encoding here.
fn place(store: &str, namespace: &str) -> String {
    form_urlencoded::byte_serialize(format!("{store}/{namespace}").as_bytes()).collect()
}

fn refused(status: reqwest::StatusCode, body: &[u8]) -> Failure {
    match serde_json::from_slice::<ErrorBody>(body) {
        Ok(error) => Failure::Refused {
            status: status.as_u16(),
            error: error.error,
            host: error.host,
            missing: error.missing,
            did_not_start: error.did_not_start,
            no_room: error.no_room,
        },
        Err(_) => Failure::refused(
            status.as_u16(),
            format!("{status}: {}", String::from_utf8_lossy(body)),
        ),
    }
}

fn error_text(body: &[u8]) -> String {
    serde_json::from_slice::<ErrorBody>(body)
        .map(|error| error.error)
        .unwrap_or_else(|_| String::from_utf8_lossy(body).into_owned())
}
