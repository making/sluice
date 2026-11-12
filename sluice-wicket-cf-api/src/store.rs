//! The desired state: the organizations and their spaces, the roles users and clients hold in
//! them, the shared domains, the apps of the spaces with their web process (each index placed
//! on a wicket: `crate::placement`) and environment variables, the routes,
//! packages, builds, droplets, deployments and jobs, the service brokers
//! registered with their catalogs, the service instances and their bindings.
//! Kept in `state.json` in the data directory, written by a rename on every change, or with
//! `--state` in the store several instances share (`crate::postgres`); the blobs in the
//! directory
//! (`packages/<guid>.zip`, `droplets/<sha256>.wasm`, a static site's files
//! `droplets/<sha256>.tar`). Every file is its
//! owner's only (0600): the state holds the apps' environment and the
//! credentials of their images. A write is durable when it returns, for a kill of a
//! VM holding the directory on its disk.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

pub use crate::blobs::{Blob, Spool};
use crate::blobs::{BlobSettings, Blobs, Local, Reading};
use crate::instances::{Member, Topology};
use crate::lease::{Lease, Seat, Term, Timing};
use crate::postgres::{Postgres, PostgresSettings, Shared, Writer};
use crate::staging::Buildpack;
use crate::wicket::Runtime;

/// The annotation of an app setting its routes' `budget-ms`.
pub const BUDGET_ANNOTATION: &str = "wicket.sluice.ik.am/budget-ms";
/// The annotation that set its routes' `instances` cap, refused: `cf scale -i` and
/// [`CONCURRENCY_ANNOTATION`] replace it.
pub const INSTANCES_ANNOTATION: &str = "wicket.sluice.ik.am/instances";
/// The annotation of an app setting what an instance takes at once: a component's requests, a
/// pool VM's connections: its routes' `concurrency`.
pub const CONCURRENCY_ANNOTATION: &str = "wicket.sluice.ik.am/concurrency";
/// The annotation of an app setting how a connection gets a VM: its routes' `mode`.
pub const MODE_ANNOTATION: &str = "wicket.sluice.ik.am/mode";
/// The annotation of an app setting how many of its instances are kept warm: its routes' `warm`.
pub const WARM_ANNOTATION: &str = "wicket.sluice.ik.am/warm";
/// The annotation of an app setting how long a VM, or a component's instance beyond its `warm`,
/// serving nothing lives: its routes' `keep-ms`.
pub const KEEP_ANNOTATION: &str = "wicket.sluice.ik.am/keep-ms";
/// The annotation of an app setting how many connections a VM serves: its routes' `uses`.
pub const USES_ANNOTATION: &str = "wicket.sluice.ik.am/uses";
/// The annotation of an app setting how long a VM serves: its routes' `lifetime-ms`.
pub const LIFETIME_ANNOTATION: &str = "wicket.sluice.ik.am/lifetime-ms";

/// The operation of the job of an app's delete waiting for its unbinds.
pub const APP_DELETE: &str = "app.delete";

/// The stack of a wasm wicket's apps.
pub const STACK: &str = "wasip3";
/// The stack of a Firecracker wicket's apps.
pub const ROOTFS_STACK: &str = "firecracker";

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct State {
    #[serde(default)]
    pub orgs: Vec<Named>,
    #[serde(default)]
    pub spaces: Vec<Space>,
    /// The one organization and space of a state written before there were more; taken into
    /// `orgs` and `spaces` when it is read ([`State::migrate`]).
    #[serde(default, skip_serializing)]
    org: Option<Named>,
    #[serde(default, skip_serializing)]
    space: Option<Named>,
    /// The roles given (`crate::roles`); a group's of the provider are not kept.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub roles: Vec<Role>,
    /// The users and clients the roles name.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub users: Vec<User>,
    /// The first takes the default and random routes.
    pub domains: Vec<Named>,
    pub apps: Vec<App>,
    pub routes: Vec<Route>,
    pub packages: Vec<Package>,
    pub builds: Vec<Build>,
    pub droplets: Vec<Droplet>,
    pub deployments: Vec<Deployment>,
    pub jobs: Vec<Job>,
    /// The brokers registered (`crate::brokers`), their catalogs with them.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub brokers: Vec<Broker>,
    #[serde(default)]
    pub instances: Vec<Instance>,
    #[serde(default)]
    pub bindings: Vec<Binding>,
    /// The keyvalue namespaces of deleted instances whose data the wickets
    /// are yet to delete.
    #[serde(default)]
    pub purges: Vec<Purge>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub security_groups: Vec<SecurityGroup>,
    /// The apps deleted while a wicket did not take it, to remove from it when it does. All
    /// an app of the owner is ever removed for without an app here to say so: a state that
    /// is not the one that put an app (a lost data directory, another cf-api) knows nothing
    /// of it, and leaves it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub removals: Vec<Removal>,
    /// The id each wicket last listed, by name: another behind a name is another wicket
    /// (`crate::controller`).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub wicket_ids: BTreeMap<String, String>,
    /// The wicket holding the data of each keyvalue namespace of a store of each wicket's host,
    /// by `store/namespace`: where the apps mapping it run (`crate::placement`).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub homes: BTreeMap<String, String>,
    /// The sweeps of the blobs ([`Store::sweep`]).
    #[serde(default, skip_serializing_if = "Sweep::is_none")]
    pub sweep: Sweep,
    /// What each wicket answered of each app, by guid, then by wicket: kept in the store several
    /// instances share for every instance (`crate::records`), not in `state.json`: a start lists
    /// the wickets.
    #[serde(skip)]
    pub outcomes: BTreeMap<String, BTreeMap<String, Outcome>>,
    /// The wickets the apps are placed on (`crate::placement`): the configuration's. Kept of
    /// them, as `wicket_stacks`: what each last listed it runs, by name, so that a wicket down
    /// since the start is still one of its stack.
    #[serde(
        default,
        rename = "wicket_stacks",
        skip_serializing_if = "crate::placement::Wickets::none_listed",
        serialize_with = "crate::placement::Wickets::write_runtimes",
        deserialize_with = "crate::placement::Wickets::read_runtimes"
    )]
    pub wickets: crate::placement::Wickets,
}

/// What the sweeps of the blobs keep: a blob is deleted between a sweep's begin and its end
/// only, so who names a blob can tell that it was not deleted since it saw it.
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct Sweep {
    /// The sweeps begun.
    #[serde(default)]
    pub rounds: u64,
    /// The one under way.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub by: Option<Sweeping>,
}

/// A sweep under way: the instance deleting, since when (seconds since the epoch), and the turn
/// of the lease it sweeps in (`crate::lease`; none of the file backend's one process, or of a
/// build before the lease).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Sweeping {
    pub instance: String,
    pub since: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub epoch: Option<u64>,
}

impl Sweep {
    fn is_none(&self) -> bool {
        *self == Sweep::default()
    }

    /// Whether a sweep may be deleting at `now`, the lease of the epoch `leading` as last seen:
    /// one of a turn before it was over with it (its instance stops sweeping as its turn ends),
    /// one begun longer ago than [`SWEEP_STALE`] was cut short.
    fn under_way(&self, now: u64, leading: Option<u64>) -> bool {
        (self.by.as_ref()).is_some_and(|by| {
            let ended = by
                .epoch
                .zip(leading)
                .is_some_and(|(by, leading)| by < leading);
            !ended && by.since + SWEEP_STALE.as_secs() > now
        })
    }
}

/// An app to remove from a wicket that did not take its removal.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(from = "RemovalForm")]
pub struct Removal {
    pub app: String,
    /// The wicket's name: none in a state of one wicket, which the first configured takes
    /// (`State::follow_wickets`).
    pub wicket: String,
}

/// A removal as a state has it: a state of one wicket's the app's guid alone.
#[derive(Deserialize)]
#[serde(untagged)]
enum RemovalForm {
    Guid(String),
    Removal { app: String, wicket: String },
}

impl From<RemovalForm> for Removal {
    fn from(form: RemovalForm) -> Removal {
        match form {
            RemovalForm::Guid(app) => Removal {
                app,
                wicket: String::new(),
            },
            RemovalForm::Removal { app, wicket } => Removal { app, wicket },
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct Named {
    pub guid: String,
    pub name: String,
    pub created_at: String,
}

/// A space of an organization: its apps, routes and service instances.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Space {
    pub guid: String,
    pub name: String,
    /// The organization's guid.
    pub org: String,
    pub created_at: String,
}

/// A role of a user or a client in an organization or a space, as CC's `/v3/roles` has it.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Role {
    pub guid: String,
    pub kind: crate::roles::RoleType,
    /// The user's guid ([`User::guid`]).
    pub user: String,
    /// An organization role's organization, a space role's space.
    pub at: String,
    pub created_at: String,
}

/// Who a role names: a user, by the name it logs in with, or a client, by its id.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct User {
    /// A user's: the `user_id` of its tokens; a client's: its id.
    pub guid: String,
    /// None for a client.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<String>,
    pub created_at: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct App {
    pub guid: String,
    pub name: String,
    /// The space's guid.
    #[serde(default)]
    pub space: String,
    pub started: bool,
    pub created_at: String,
    pub updated_at: String,
    /// What the app was last pushed or staged as: `docker` an image's.
    #[serde(default)]
    pub lifecycle: Lifecycle,
    pub buildpacks: Vec<String>,
    /// The stack `cf push -s` or a manifest's `stack:` pins: staging fails a package of another.
    /// None: what is pushed decides (the `stack` of a state written before named none).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pin: Option<Runtime>,
    pub labels: BTreeMap<String, String>,
    pub annotations: BTreeMap<String, String>,
    pub process: Process,
    pub current_droplet: Option<String>,
    /// The user's environment variables (`crate::env`); the guest gets them
    /// at its next start.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    /// What the wickets serve since the last start, restart or deployment:
    /// the snapshot taken then.
    pub running: Option<Running>,
    /// A deployment of `running` under way: what the shares it has not reached serve
    /// (`crate::deployments`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rolling: Option<Rolling>,
    /// The wicket of each index of the web process, by name (`crate::placement`): where it
    /// runs while the app is started.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub placement: Vec<String>,
    /// The indices moving to another wicket, by index: each runs where it is placed and on that
    /// wicket until that one serves it, then is placed there (`crate::placement`).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub moving: BTreeMap<u64, String>,
}

/// A deployment of an app's `running` snapshot under way (`crate::deployments`): the wickets
/// whose share it has not reached serve the snapshot before.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Rolling {
    /// The deployment's guid.
    pub deployment: String,
    /// The app's snapshot before the deployment: what a cancel puts back.
    pub previous: Running,
    /// The wickets whose share the deployment has not reached, by name, each serving
    /// `previous` or the snapshot given: one a deployment it superseded had not reached either.
    pub pending: BTreeMap<String, Option<Running>>,
}

impl App {
    /// The snapshot the share of `wicket` serves: `running`, but on a wicket a deployment has not
    /// reached.
    pub fn running_on(&self, wicket: &str) -> Option<&Running> {
        let running = self.running.as_ref()?;
        let Some(rolling) = &self.rolling else {
            return Some(running);
        };
        Some(match rolling.pending.get(wicket) {
            None => running,
            Some(other) => other.as_ref().unwrap_or(&rolling.previous),
        })
    }

    /// Every snapshot of it a share serves, or a cancel puts back.
    pub fn snapshots(&self) -> impl Iterator<Item = &Running> {
        let rolling = self.rolling.iter();
        let before = rolling.flat_map(|rolling| {
            std::iter::once(&rolling.previous).chain(rolling.pending.values().flatten())
        });
        self.running.iter().chain(before)
    }

    /// [`App::snapshots`], to change.
    pub fn snapshots_mut(&mut self) -> impl Iterator<Item = &mut Running> {
        let rolling = self.rolling.iter_mut();
        let before = rolling.flat_map(|rolling| {
            std::iter::once(&mut rolling.previous).chain(rolling.pending.values_mut().flatten())
        });
        self.running.iter_mut().chain(before)
    }
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
    /// Its readiness check: what has the wicket take an instance's connections.
    #[serde(default)]
    pub readiness_health_check: ReadinessCheck,
    pub created_at: String,
    pub updated_at: String,
}

/// A process's (liveness) health check, as `cf push -u`, `cf set-health-check` and a manifest's
/// `health-check-*` keys set it: `port`, `process` or `http`, its seconds; `timeout` that of its
/// start.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct HealthCheck {
    pub kind: String,
    pub timeout: Option<u64>,
    pub invocation_timeout: Option<u64>,
    #[serde(default)]
    pub interval: Option<u64>,
    pub endpoint: Option<String>,
}

impl Default for HealthCheck {
    fn default() -> Self {
        HealthCheck {
            kind: "port".into(),
            timeout: None,
            invocation_timeout: None,
            interval: None,
            endpoint: None,
        }
    }
}

/// A process's readiness check, as a manifest's `readiness-health-check-*` keys set it: `process`,
/// `port` or `http`, its seconds.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct ReadinessCheck {
    pub kind: String,
    pub endpoint: Option<String>,
    pub invocation_timeout: Option<u64>,
    pub interval: Option<u64>,
}

impl Default for ReadinessCheck {
    fn default() -> Self {
        ReadinessCheck {
            kind: "process".into(),
            endpoint: None,
            invocation_timeout: None,
            interval: None,
        }
    }
}

/// How an app's instances come and go below `cf scale`'s number and what each takes at once, as
/// annotated (`crate::manifest::check_annotations`); none annotated, the number alone says.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default)]
pub struct Lifetime {
    pub mode: Option<String>,
    pub warm: Option<u64>,
    pub keep_ms: Option<u64>,
    pub uses: Option<u64>,
    pub lifetime_ms: Option<u64>,
    pub concurrency: Option<u64>,
}

/// The droplet and the sandbox the wicket got at the last start.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default)]
pub struct Running {
    pub droplet: String,
    pub sha256: String,
    /// The droplet's [`Droplet::made`] as started, kept as it is.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub made: BTreeMap<String, String>,
    /// The component's file name in the wicket (the droplet's web process,
    /// less its directories).
    pub file: String,
    /// The droplet's stack; none in a snapshot taken before it was kept ([`Running::stack`]).
    #[serde(default, rename = "stack", skip_serializing_if = "Option::is_none")]
    pub staged_as: Option<Runtime>,
    /// The digest of a static site's files (`Files`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub files: Option<String>,
    pub memory_mib: u64,
    pub budget_ms: Option<u64>,
    /// As annotated since: an edit of them in the mode of this start is taken at once
    /// (`crate::manifest::live_lifetime`).
    #[serde(default)]
    pub lifetime: Lifetime,
    /// The process's readiness check as it was: another one waits for the next start, as on CF.
    #[serde(default)]
    pub readiness: ReadinessCheck,
    /// The process's health check as it was, likewise.
    #[serde(default)]
    pub liveness: HealthCheck,
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
    /// The destinations the security groups let a VM connect to, as the wicket names them
    /// (`crate::security_groups::egress`): a wasm app's are not put.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub egress: Vec<String>,
    /// Tells the wicket's app of this snapshot from another
    /// (`crate::env::tag`): the wicket lists no env back.
    #[serde(default)]
    pub tag: Option<String>,
    /// Seconds since the epoch.
    pub since: u64,
}

/// A service instance: a namespace of a keyvalue store (offering
/// `keyvalue`, the store its plan), a capability (offering `wasi`), the
/// user's credentials (offering `user-provided`, no plan), or a broker's
/// (`broker`; `offering` and `plan` the guids of its offering and plan in the
/// broker's catalog).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Instance {
    pub guid: String,
    pub name: String,
    /// The space's guid.
    #[serde(default)]
    pub space: String,
    pub offering: String,
    pub plan: String,
    pub tags: Vec<String>,
    /// A user-provided instance's credentials, what its bindings give the
    /// apps: a secret, as an environment variable's value.
    #[serde(default, skip_serializing_if = "serde_json::Map::is_empty")]
    pub credentials: serde_json::Map<String, serde_json::Value>,
    /// The guid of the broker of a broker's instance.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub broker: Option<String>,
    /// What the broker answered of a broker's instance.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dashboard_url: Option<String>,
    /// The maintenance info of its plan a broker's instance was made or
    /// updated to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub maintenance_info: Option<serde_json::Value>,
    /// The last operation of a broker's instance; None: created at once.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation: Option<Operation>,
    pub created_at: String,
    pub updated_at: String,
}

impl Instance {
    pub fn user_provided(&self) -> bool {
        self.offering == crate::services::USER_PROVIDED
    }

    pub fn brokered(&self) -> bool {
        self.broker.is_some()
    }

    /// Whether an operation of the broker on it is under way.
    pub fn busy(&self) -> bool {
        self.operation.as_ref().is_some_and(Operation::under_way)
    }
}

/// An operation of a broker on an instance or a binding, as CC keeps the
/// last one: under way until the broker says how it went (`crate::operations`).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Operation {
    pub kind: OperationKind,
    pub state: OperationState,
    pub description: String,
    /// Whether the broker took the request: until then (a restart of the
    /// cf-api included), it is sent again.
    pub sent: bool,
    /// The parameters to send, dropped once the broker took them: they may
    /// hold secrets.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parameters: Option<serde_json::Value>,
    /// An update's plan (its guid) and maintenance info, taken when it
    /// succeeds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub maintenance_info: Option<serde_json::Value>,
    /// The broker's `operation`, polled with.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub broker_operation: Option<String>,
    /// The job it settles.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job: Option<String>,
    /// The user_id of who asked: the broker's originating identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    /// When the polling gives up, seconds since the epoch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline: Option<u64>,
    /// Whether what it asked of the broker is still to be undone there (orphan mitigation):
    /// kept as it fails, before the undo is sent, and until the broker answered it.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub undo: bool,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum OperationKind {
    Create,
    Update,
    Delete,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum OperationState {
    #[serde(rename = "in progress")]
    InProgress,
    #[serde(rename = "succeeded")]
    Succeeded,
    #[serde(rename = "failed")]
    Failed,
}

impl OperationKind {
    pub fn as_str(self) -> &'static str {
        match self {
            OperationKind::Create => "create",
            OperationKind::Update => "update",
            OperationKind::Delete => "delete",
        }
    }
}

impl OperationState {
    pub fn as_str(self) -> &'static str {
        match self {
            OperationState::InProgress => "in progress",
            OperationState::Succeeded => "succeeded",
            OperationState::Failed => "failed",
        }
    }
}

impl Operation {
    /// A new operation under way, not sent yet.
    pub fn new(kind: OperationKind, job: Option<String>, user: Option<String>) -> Operation {
        let at = now();
        Operation {
            kind,
            state: OperationState::InProgress,
            description: String::new(),
            sent: false,
            parameters: None,
            plan: None,
            maintenance_info: None,
            broker_operation: None,
            job,
            user,
            deadline: None,
            undo: false,
            created_at: at.clone(),
            updated_at: at,
        }
    }

    pub fn under_way(&self) -> bool {
        self.state == OperationState::InProgress
    }

    /// Whether there is work of it left (`crate::operations`): under way, or to be undone.
    pub fn owed(&self) -> bool {
        self.under_way() || self.undo
    }
}

/// A broker registered (`crate::brokers`): where it is, its credentials (a
/// secret, shown nowhere) and its catalog as last fetched.
#[derive(Serialize, Deserialize, Clone, PartialEq)]
pub struct Broker {
    pub guid: String,
    pub name: String,
    pub url: String,
    pub username: String,
    pub password: String,
    /// A space-scoped broker's space: its plans are that space's alone.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub space: Option<String>,
    pub offerings: Vec<Offering>,
    pub created_at: String,
    pub updated_at: String,
}

impl std::fmt::Debug for Broker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Broker")
            .field("guid", &self.guid)
            .field("name", &self.name)
            .field("url", &self.url)
            .field("username", &self.username)
            .field("password", &"***")
            .field("space", &self.space)
            .field("offerings", &self.offerings)
            .finish()
    }
}

/// A service offering of a broker's catalog: the catalog's object as the
/// broker gave it, checked (`crate::osb::catalog`), less its plans.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Offering {
    pub guid: String,
    pub catalog: serde_json::Map<String, serde_json::Value>,
    pub plans: Vec<BrokerPlan>,
    /// Gone from the catalog, kept for the instances of its plans.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub gone: bool,
    pub created_at: String,
    pub updated_at: String,
}

/// A plan of a broker's offering: the catalog's object, and who sees it.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct BrokerPlan {
    pub guid: String,
    pub catalog: serde_json::Map<String, serde_json::Value>,
    /// `admin` (none but who edits the brokers and the admins), `public`, `organization`
    /// (those of `orgs`) or `space` (a space-scoped broker's).
    pub visibility: String,
    /// The organizations an `organization` plan is visible in.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub orgs: Vec<String>,
    /// Gone from the catalog, kept for its instances.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub gone: bool,
    pub created_at: String,
    pub updated_at: String,
}

/// A field of a catalog object: checked when fetched, so its type holds.
fn text<'a>(catalog: &'a serde_json::Map<String, serde_json::Value>, key: &str) -> &'a str {
    catalog
        .get(key)
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
}

fn flag(catalog: &serde_json::Map<String, serde_json::Value>, key: &str) -> Option<bool> {
    catalog.get(key).and_then(serde_json::Value::as_bool)
}

impl Offering {
    /// The broker's id of it.
    pub fn id(&self) -> &str {
        text(&self.catalog, "id")
    }

    pub fn name(&self) -> &str {
        text(&self.catalog, "name")
    }

    /// A feature of it (`bindable`, `plan_updateable`, `instances_retrievable`,
    /// `bindings_retrievable`, `allow_context_updates`), false unless said.
    pub fn feature(&self, key: &str) -> bool {
        flag(&self.catalog, key).unwrap_or(false)
    }

    pub fn plan(&self, guid: &str) -> Option<&BrokerPlan> {
        self.plans.iter().find(|plan| plan.guid == guid)
    }
}

impl BrokerPlan {
    pub fn id(&self) -> &str {
        text(&self.catalog, "id")
    }

    pub fn name(&self) -> &str {
        text(&self.catalog, "name")
    }

    /// Its own `bindable` or `plan_updateable`, else its offering's.
    pub fn feature(&self, offering: &Offering, key: &str) -> bool {
        flag(&self.catalog, key).unwrap_or_else(|| offering.feature(key))
    }

    /// How long the broker may take an operation: its
    /// `maximum_polling_duration`, seconds.
    pub fn maximum_polling_duration(&self) -> Option<u64> {
        self.catalog
            .get("maximum_polling_duration")
            .and_then(serde_json::Value::as_u64)
    }

    pub fn maintenance_info(&self) -> Option<&serde_json::Value> {
        self.catalog.get("maintenance_info")
    }

    /// Whether the apps of `space`, of the organization `org`, may have instances of it: a
    /// space-scoped broker's plans are its space's alone.
    pub fn visible(&self, broker: &Broker, org: &str, space: &str) -> bool {
        !self.gone
            && match self.visibility.as_str() {
                "public" => true,
                "organization" => self.orgs.iter().any(|guid| guid == org),
                "space" => broker.space.as_deref() == Some(space),
                _ => false,
            }
    }
}

/// A plan of a broker found by its guid, with its offering and broker.
#[derive(Clone, Copy)]
pub struct Brokered<'a> {
    pub broker: &'a Broker,
    pub offering: &'a Offering,
    pub plan: &'a BrokerPlan,
}

/// A security group (`crate::security_groups`): its rules, as given, and where they apply.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct SecurityGroup {
    pub guid: String,
    pub name: String,
    pub rules: Vec<SecurityRule>,
    /// `globally_enabled`: every space's apps, running and staging.
    pub running: bool,
    pub staging: bool,
    /// The guids of the spaces it is bound to.
    pub running_spaces: Vec<String>,
    pub staging_spaces: Vec<String>,
    pub created_at: String,
    pub updated_at: String,
}

/// A rule of a security group, as CC has it.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct SecurityRule {
    pub protocol: String,
    pub destination: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ports: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log: Option<bool>,
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
    /// What the broker of a broker's instance gave the app: a secret.
    #[serde(default, skip_serializing_if = "serde_json::Map::is_empty")]
    pub credentials: serde_json::Map<String, serde_json::Value>,
    /// The last operation of a broker's binding; None: made at once.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation: Option<Operation>,
    pub created_at: String,
    pub updated_at: String,
}

impl Binding {
    /// Whether the app has it: made, not only asked of the broker.
    pub fn made(&self) -> bool {
        self.operation.as_ref().is_none_or(|operation| {
            operation.kind != OperationKind::Create || operation.state == OperationState::Succeeded
        })
    }

    pub fn busy(&self) -> bool {
        self.operation.as_ref().is_some_and(Operation::under_way)
    }
}

/// The data of a deleted keyvalue instance, deleted through every wicket once no app maps it.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Purge {
    pub store: String,
    pub namespace: String,
    /// The instance's name, for the log.
    pub instance: String,
    /// The wickets that deleted it, or have no such store.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub purged: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Route {
    pub guid: String,
    /// The space's guid.
    #[serde(default)]
    pub space: String,
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
    /// What its staging needs to go on in the next turn, a stop of the cf-api or its turn's end
    /// cutting it short (`crate::staging::run`): none once it ended, and in a build staged
    /// before it was kept.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub staging: Option<Staging>,
    pub created_at: String,
    pub updated_at: String,
}

/// What a staging under way is asked for, and how far its build of source is.
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct Staging {
    /// The API's URL, which a build of source is told (`VCAP_APPLICATION`).
    pub base: String,
    /// The web process command it stages for.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
    /// Its build of source on a wicket, once started.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub building: Option<Building>,
    /// Whether an instance began it: one beginning it after that takes it up.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub taken: bool,
}

/// A build of source a wicket runs: followed from the line `after` on.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Building {
    /// The wicket's name.
    pub wicket: String,
    /// The build's id there.
    pub id: String,
    /// The lines of its output taken: kept as they are taken, written with the next change
    /// (the stop's last at least).
    pub after: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Droplet {
    pub guid: String,
    pub app: String,
    pub package: String,
    pub sha256: String,
    /// The component's path in the package; its file name for an image.
    pub file: String,
    /// Its stack, as staging detected it; none in a droplet staged before it was kept
    /// ([`Droplet::stack`]).
    #[serde(default, rename = "stack", skip_serializing_if = "Option::is_none")]
    pub staged_as: Option<Runtime>,
    /// The buildpack that staged it; none of an image's, nor in a droplet staged before it was
    /// kept ([`Droplet::buildpack`]).
    #[serde(default, rename = "buildpack", skip_serializing_if = "Option::is_none")]
    pub staged_by: Option<Buildpack>,
    pub bytes: u64,
    /// The image it was pulled from, pinned to its manifest's digest; None
    /// for a zip's.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
    /// An image's rootfs, which each Firecracker wicket makes itself, of its own guest: the
    /// digest a wicket made where it is not `sha256` (the one staging made), by wicket name. A
    /// wicket not named is asked for `sha256`: a state written before names none, its
    /// `sha256` the one wicket's.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub made: BTreeMap<String, String>,
    /// A static site's files, the component serving them its `sha256`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub files: Option<Files>,
    /// What buildpacks built of source ([`Buildpack::Cnb`]): its `sha256` is the rootfs a
    /// wicket made of it, as an image's is, [`Droplet::made`] the other wickets'.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub built: Option<Built>,
    pub created_at: String,
    pub updated_at: String,
}

/// What a build made of source: a droplet as CF's, what goes over its stack's root file
/// system. Kept by the cf-api, so any Firecracker wicket makes the rootfs of it, none building
/// it again.
#[derive(Serialize, Deserialize, Clone, PartialEq)]
pub struct Built {
    /// The builder's run image, pinned to its manifest's digest.
    pub run_image: String,
    /// The droplet, a tar (`droplets/<sha256>.tar`).
    pub layer: Files,
    /// What it runs, as the build answered it: opaque, the wicket's to read.
    pub launch: serde_json::Value,
    /// The credentials the run image is pulled with, as at the build (the builder's, of its
    /// registry): the droplet is placed when the cf-api has others, or none. A droplet built
    /// before they were kept has none: the builder's now.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
}

impl Built {
    /// The username and the password, both kept.
    pub fn credentials(&self) -> Option<(&str, &str)> {
        Some((self.username.as_deref()?, self.password.as_deref()?))
    }
}

impl std::fmt::Debug for Built {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Built")
            .field("run_image", &self.run_image)
            .field("layer", &self.layer)
            .field("launch", &self.launch)
            .field("username", &self.username)
            .field("password", &self.password.as_ref().map(|_| "***"))
            .finish()
    }
}

impl Droplet {
    /// The digest of its tar: a site's files, what buildpacks built; one of them at most.
    fn tar(&self) -> Option<&str> {
        let built = self.built.as_ref().map(|built| &built.layer);
        (self.files.as_ref().or(built)).map(|files| files.sha256.as_str())
    }

    /// Its stack: where it runs. Of one staged before it was kept, what its file's name tells.
    pub fn stack(&self) -> Runtime {
        (self.staged_as).unwrap_or_else(|| Runtime::of_file(&self.file))
    }

    /// The buildpack that staged it, an image's none. Of one staged before it was kept, what
    /// it holds tells: a site by its files, else by its stack.
    pub fn buildpack(&self) -> Option<Buildpack> {
        if self.image.is_some() {
            return None;
        }
        Some(self.staged_by.unwrap_or(match self.stack() {
            Runtime::Firecracker => Buildpack::Rootfs,
            Runtime::Wasm if self.files.is_some() => Buildpack::Site,
            Runtime::Wasm => Buildpack::Wasm,
        }))
    }

    /// What the wicket `wicket` serves it as ([`Droplet::made`]).
    pub fn sha256_on(&self, wicket: &str) -> &str {
        made_on(&self.sha256, &self.made, wicket)
    }

    /// The wicket `wicket` made it as `sha256`.
    pub fn made_as(&mut self, wicket: &str, sha256: &str) {
        note_made(&self.sha256, &mut self.made, wicket, sha256);
    }
}

impl Running {
    /// Its droplet's stack ([`Droplet::stack`]).
    pub fn stack(&self) -> Runtime {
        (self.staged_as).unwrap_or_else(|| Runtime::of_file(&self.file))
    }

    /// What the wicket `wicket` serves it as ([`Droplet::made`]).
    pub fn sha256_on(&self, wicket: &str) -> &str {
        made_on(&self.sha256, &self.made, wicket)
    }

    /// The wicket `wicket` made it as `sha256`.
    pub fn made_as(&mut self, wicket: &str, sha256: &str) {
        note_made(&self.sha256, &mut self.made, wicket, sha256);
    }
}

/// The digest `wicket` serves a droplet of `sha256` as, `made` its own ([`Droplet::made`]).
fn made_on<'a>(sha256: &'a str, made: &'a BTreeMap<String, String>, wicket: &str) -> &'a str {
    made.get(wicket).map_or(sha256, String::as_str)
}

/// `wicket` made a droplet of `sha256` as `digest`: named in `made` when it is another.
fn note_made(sha256: &str, made: &mut BTreeMap<String, String>, wicket: &str, digest: &str) {
    match digest == sha256 {
        true => made.remove(wicket),
        false => made.insert(wicket.to_string(), digest.to_string()),
    };
}

/// A tar of a static site's files (`droplets/<sha256>.tar`), the wicket's
/// read-only at `/` for its component.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Files {
    pub sha256: String,
    /// The tar's.
    pub bytes: u64,
}

/// A deployment of an app's droplet (`crate::deployments`).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Deployment {
    pub guid: String,
    pub app: String,
    pub droplet: String,
    pub previous_droplet: String,
    /// `rolling` or `canary`.
    pub strategy: String,
    pub created_at: String,
    pub updated_at: String,
    /// Where it is; one written before there were more is deployed.
    #[serde(default)]
    pub state: DeploymentState,
    /// Finalized by the next deployment, or a start or stop of its app, before its end.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub superseded: bool,
    /// The instances a step puts at most, a wicket's share at least.
    #[serde(default = "one")]
    pub max_in_flight: u64,
    /// A canary's steps, each the percentage of the instances on the new droplet; none: one
    /// instance.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub canary_steps: Vec<u64>,
    /// A canary's step under way or paused after, from 1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub canary_step: Option<u64>,
    /// What it set of the web process, as asked.
    #[serde(default, skip_serializing_if = "Scaled::is_empty")]
    pub scaled: Scaled,
    /// Why it does not go on: a wicket refusing, or not answering, a share it reached.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// When every share it reached last served it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_healthy_at: Option<String>,
    /// When its state last changed; none in one written before: `updated_at`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status_changed_at: Option<String>,
}

fn one() -> u64 {
    1
}

/// Where a deployment is, as Cloud Controller names it.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Default)]
#[serde(rename_all = "UPPERCASE")]
pub enum DeploymentState {
    /// Taking its steps.
    Deploying,
    /// Taking a canary's step.
    Prepaused,
    /// A canary's, after a step: until it is continued.
    Paused,
    /// Its app going back to the droplet before.
    Canceling,
    #[default]
    Deployed,
    Canceled,
}

impl DeploymentState {
    /// Its name, Cloud Controller's (a deployment's deprecated `state`).
    pub fn as_str(self) -> &'static str {
        match self {
            DeploymentState::Deploying => "DEPLOYING",
            DeploymentState::Prepaused => "PREPAUSED",
            DeploymentState::Paused => "PAUSED",
            DeploymentState::Canceling => "CANCELING",
            DeploymentState::Deployed => "DEPLOYED",
            DeploymentState::Canceled => "CANCELED",
        }
    }
}

/// The web process's scale a deployment set (its `options`).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default)]
pub struct Scaled {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub web_instances: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_in_mb: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disk_in_mb: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log_rate_limit_in_bytes_per_second: Option<i64>,
}

impl Scaled {
    pub fn is_empty(&self) -> bool {
        *self == Scaled::default()
    }
}

impl Deployment {
    /// Cloud Controller's `status.value` and `status.reason`.
    pub fn status(&self) -> (&'static str, &'static str) {
        match (self.state, self.superseded) {
            (DeploymentState::Deploying | DeploymentState::Prepaused, _) => ("ACTIVE", "DEPLOYING"),
            (DeploymentState::Paused, _) => ("ACTIVE", "PAUSED"),
            (DeploymentState::Canceling, _) => ("ACTIVE", "CANCELING"),
            (_, true) => ("FINALIZED", "SUPERSEDED"),
            (DeploymentState::Deployed, false) => ("FINALIZED", "DEPLOYED"),
            (DeploymentState::Canceled, false) => ("FINALIZED", "CANCELED"),
        }
    }

    /// Whether it takes its steps: deploying, a canary's step included.
    pub fn stepping(&self) -> bool {
        matches!(
            self.state,
            DeploymentState::Deploying | DeploymentState::Prepaused
        )
    }

    /// Whether a cancel takes it: under way, paused or canceling.
    pub fn cancelable(&self) -> bool {
        self.stepping()
            || matches!(
                self.state,
                DeploymentState::Paused | DeploymentState::Canceling
            )
    }

    /// Whether `cf continue-deployment` takes it: paused.
    pub fn continuable(&self) -> bool {
        self.state == DeploymentState::Paused
    }

    /// Moves it to `state`, now.
    pub fn set(&mut self, state: DeploymentState) {
        let at = now();
        self.state = state;
        self.status_changed_at = Some(at.clone());
        self.updated_at = at;
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Job {
    pub guid: String,
    pub operation: String,
    pub errors: Vec<JobError>,
    /// A link besides `self`: its name and path.
    pub resource: Option<(String, String)>,
    /// What it waits for (the guids of the instances and bindings a broker
    /// operates on, an app to delete once they are unbound): PROCESSING
    /// until none is left.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub waits: Vec<String>,
    /// Whether the broker took its operation and is polled: POLLING.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub polling: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
    pub created_at: String,
    pub updated_at: String,
}

impl Job {
    /// `PROCESSING`, `POLLING`, `COMPLETE` or `FAILED`.
    pub fn state(&self) -> &'static str {
        match (self.waits.is_empty(), self.polling, self.errors.is_empty()) {
            (false, false, _) => "PROCESSING",
            (false, true, _) => "POLLING",
            (true, _, true) => "COMPLETE",
            (true, _, false) => "FAILED",
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct JobError {
    pub code: u32,
    pub title: String,
    pub detail: String,
}

/// What a wicket last answered of its share of an app.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Running,
    Refused(String),
    Unreachable,
}

impl State {
    /// What `wicket` last answered of its share of the app `guid`.
    pub fn outcome(&self, guid: &str, wicket: &str) -> Option<&Outcome> {
        self.outcomes.get(guid)?.get(wicket)
    }

    /// A new state with the organization, its space and the domains named.
    pub fn new(org: &str, space: &str, domains: &[String]) -> State {
        let named = |name: &str| Named {
            guid: guid(),
            name: name.to_string(),
            created_at: now(),
        };
        let mut state = State {
            domains: domains.iter().map(|domain| named(domain)).collect(),
            ..State::default()
        };
        let org = state.create_org(org).guid.clone();
        state.create_space(space, &org);
        state
    }

    /// Takes the one organization and space of a state written before there were more: what
    /// had no space is theirs, an `organization` plan the organization's.
    pub fn migrate(&mut self) {
        if let Some(org) = self.org.take() {
            if self.org(&org.guid).is_none() {
                self.orgs.push(org.clone());
            }
            for plan in self
                .brokers
                .iter_mut()
                .flat_map(|broker| &mut broker.offerings)
                .flat_map(|offering| &mut offering.plans)
                .filter(|plan| plan.visibility == "organization" && plan.orgs.is_empty())
            {
                plan.orgs.push(org.guid.clone());
            }
            if let Some(space) = self.space.take()
                && self.space(&space.guid).is_none()
            {
                self.spaces.push(Space {
                    guid: space.guid,
                    name: space.name,
                    org: org.guid,
                    created_at: space.created_at,
                });
            }
        }
        let Some(first) = self.spaces.first().map(|space| space.guid.clone()) else {
            return;
        };
        let unplaced = |space: &mut String| {
            if space.is_empty() {
                space.clone_from(&first);
            }
        };
        self.apps
            .iter_mut()
            .for_each(|app| unplaced(&mut app.space));
        self.routes
            .iter_mut()
            .for_each(|route| unplaced(&mut route.space));
        self.instances
            .iter_mut()
            .for_each(|instance| unplaced(&mut instance.space));
    }

    pub fn org(&self, guid: &str) -> Option<&Named> {
        self.orgs.iter().find(|org| org.guid == guid)
    }

    pub fn org_named(&self, name: &str) -> Option<&Named> {
        self.orgs.iter().find(|org| org.name == name)
    }

    pub fn space(&self, guid: &str) -> Option<&Space> {
        self.spaces.iter().find(|space| space.guid == guid)
    }

    pub fn space_named(&self, org: &str, name: &str) -> Option<&Space> {
        self.spaces
            .iter()
            .find(|space| space.org == org && space.name == name)
    }

    /// The organization of the space `guid`.
    pub fn org_of(&self, space: &str) -> Option<&Named> {
        self.space(space).and_then(|space| self.org(&space.org))
    }

    pub fn create_org(&mut self, name: &str) -> &Named {
        self.orgs.push(Named {
            guid: guid(),
            name: name.to_string(),
            created_at: now(),
        });
        self.orgs.last().expect("just pushed")
    }

    pub fn create_space(&mut self, name: &str, org: &str) -> &Space {
        self.spaces.push(Space {
            guid: guid(),
            name: name.to_string(),
            org: org.to_string(),
            created_at: now(),
        });
        self.spaces.last().expect("just pushed")
    }

    pub fn user(&self, guid: &str) -> Option<&User> {
        self.users.iter().find(|user| user.guid == guid)
    }

    pub fn role(&self, guid: &str) -> Option<&Role> {
        self.roles.iter().find(|role| role.guid == guid)
    }

    pub fn app(&self, guid: &str) -> Option<&App> {
        self.apps.iter().find(|app| app.guid == guid)
    }

    pub fn app_mut(&mut self, guid: &str) -> Option<&mut App> {
        self.apps.iter_mut().find(|app| app.guid == guid)
    }

    /// The app `name` of the space `space`.
    pub fn app_named(&self, space: &str, name: &str) -> Option<&App> {
        self.apps
            .iter()
            .find(|app| app.space == space && app.name == name)
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

    /// What the staging of the build `guid` under way keeps.
    pub fn staging_mut(&mut self, guid: &str) -> Option<&mut Staging> {
        let build = self.builds.iter_mut().find(|build| build.guid == guid)?;
        build.staging.as_mut()
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

    /// The service instance `name` of the space `space`.
    pub fn instance_named(&self, space: &str, name: &str) -> Option<&Instance> {
        self.instances
            .iter()
            .find(|instance| instance.space == space && instance.name == name)
    }

    pub fn binding(&self, guid: &str) -> Option<&Binding> {
        self.bindings.iter().find(|binding| binding.guid == guid)
    }

    pub fn instance_mut(&mut self, guid: &str) -> Option<&mut Instance> {
        self.instances
            .iter_mut()
            .find(|instance| instance.guid == guid)
    }

    pub fn binding_mut(&mut self, guid: &str) -> Option<&mut Binding> {
        self.bindings
            .iter_mut()
            .find(|binding| binding.guid == guid)
    }

    pub fn broker(&self, guid: &str) -> Option<&Broker> {
        self.brokers.iter().find(|broker| broker.guid == guid)
    }

    /// The broker's plan of the guid `plan`, with its offering and broker.
    pub fn brokered(&self, plan: &str) -> Option<Brokered<'_>> {
        self.brokers.iter().find_map(|broker| {
            broker.offerings.iter().find_map(|offering| {
                offering.plan(plan).map(|plan| Brokered {
                    broker,
                    offering,
                    plan,
                })
            })
        })
    }

    /// The plan of a broker's instance.
    pub fn brokered_of(&self, instance: &Instance) -> Option<Brokered<'_>> {
        instance
            .broker
            .as_ref()
            .and_then(|_| self.brokered(&instance.plan))
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

    /// The stack of the current droplet of `app`.
    fn staged_stack(&self, app: &App) -> Option<Runtime> {
        let droplet = self.droplet(app.current_droplet.as_deref()?)?;
        Some(droplet.stack())
    }

    /// The stack `app` shows: its pin, else its current droplet's. None before it is staged.
    pub fn shown_stack(&self, app: &App) -> Option<Runtime> {
        app.pin.or_else(|| self.staged_stack(app))
    }

    /// The stack an edit checks `app` against: its pin, else its current droplet's, else the
    /// one stack the wickets run. With several and neither, none: the start checks, knowing
    /// the droplet.
    pub fn stack_of(&self, app: &App) -> Option<Runtime> {
        self.shown_stack(app).or_else(|| match self.stacks()[..] {
            [only] => Some(only),
            _ => None,
        })
    }

    /// The stack named `name`, to pin an app to: one the wickets run, as CC takes a stack it
    /// has; while a wicket has yet to list what it runs, any there is.
    pub fn stack_named(&self, name: &str) -> Result<Runtime, String> {
        let stacks = self.stacks();
        match Runtime::named(name) {
            Some(stack) if stacks.contains(&stack) || !self.all_listed() => Ok(stack),
            _ => Err(format!(
                "Stack \"{name}\" must be an existing stack: the wickets run {}",
                crate::wicket::stacks_named(&stacks)
            )),
        }
    }

    /// Adds the app `name` to the space `space`, stopped, with its web process.
    pub fn create_app(&mut self, name: &str, space: &str) -> &mut App {
        let at = now();
        self.apps.push(App {
            guid: guid(),
            name: name.to_string(),
            space: space.to_string(),
            started: false,
            created_at: at.clone(),
            updated_at: at.clone(),
            lifecycle: Lifecycle::Buildpack,
            buildpacks: Vec::new(),
            pin: None,
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
            process: Process {
                guid: guid(),
                instances: 1,
                memory_mb: 1024, // Cloud Controller's default_app_memory
                disk_mb: 1024,
                log_rate_limit: -1,
                command: None,
                health_check: HealthCheck::default(),
                readiness_health_check: ReadinessCheck::default(),
                created_at: at.clone(),
                updated_at: at,
            },
            current_droplet: None,
            env: BTreeMap::new(),
            running: None,
            rolling: None,
            placement: Vec::new(),
            moving: BTreeMap::new(),
        });
        self.apps.last_mut().expect("just pushed")
    }

    /// Adds the route `host` of the domain to the space `space`, unmapped.
    pub fn create_route(&mut self, host: &str, domain: &str, space: &str) -> &mut Route {
        let at = now();
        self.routes.push(Route {
            guid: guid(),
            space: space.to_string(),
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
        let guid = self.add_waiting_job(operation, Vec::new(), resource);
        self.job_mut(&guid).expect("just added").errors = errors;
        guid
    }

    /// Records a job waiting for `waits` ([`Job::waits`]).
    pub fn add_waiting_job(
        &mut self,
        operation: &str,
        waits: Vec<String>,
        resource: Option<(String, String)>,
    ) -> String {
        let at = now();
        let job = Job {
            guid: guid(),
            operation: operation.to_string(),
            errors: Vec::new(),
            resource,
            waits,
            polling: false,
            warnings: Vec::new(),
            created_at: at.clone(),
            updated_at: at,
        };
        let guid = job.guid.clone();
        self.jobs.push(job);
        if self.jobs.len() > 1000 {
            // the oldest settled: one waiting is still polled by its CLI
            let settled = self.jobs.iter().position(|job| job.waits.is_empty());
            self.jobs.remove(settled.unwrap_or(0));
        }
        guid
    }

    pub fn job_mut(&mut self, guid: &str) -> Option<&mut Job> {
        self.jobs.iter_mut().find(|job| job.guid == guid)
    }

    /// Takes `what` off the waits of the job `guid`, failed with `error`
    /// if it did.
    pub fn settle(&mut self, guid: &str, what: &str, error: Option<JobError>) {
        if let Some(job) = self.job_mut(guid) {
            job.waits.retain(|waited| waited != what);
            job.errors.extend(error);
            if job.waits.is_empty() {
                job.polling = false;
            }
            job.updated_at = now();
        }
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
        blobs.extend(self.unnamed(gone));
        self.apps.retain(|app| app.guid != guid);
        self.outcomes.remove(guid);
        blobs
    }

    /// Makes `droplet` the app's current one and keeps only it, the one before and those its
    /// shares serve (a deployment under way's); the droplet blobs no droplet names any more are
    /// listed to delete.
    pub fn set_current_droplet(&mut self, app: &str, droplet: &str) -> Vec<Blob> {
        let previous = self
            .app(app)
            .and_then(|app| app.current_droplet.clone())
            .filter(|previous| previous != droplet);
        let serving: Vec<String> = (self.app(app).into_iter())
            .flat_map(App::snapshots)
            .map(|running| running.droplet.clone())
            .collect();
        let keep = |guid: &str| {
            guid == droplet
                || previous.as_deref() == Some(guid)
                || serving.iter().any(|s| s == guid)
        };
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
        self.unnamed(gone)
    }

    /// Whether a package or a droplet kept names the blob: one not named is swept.
    pub fn names(&self, blob: &Blob) -> bool {
        match blob {
            Blob::Package(guid) => self.package(guid).is_some(),
            Blob::Droplet(sha256) => self.droplets.iter().any(|kept| kept.sha256 == *sha256),
            Blob::Files(sha256) => (self.droplets.iter()).any(|kept| kept.tar() == Some(sha256)),
        }
    }

    /// The blobs of the droplets `gone` no droplet kept names: their components, their
    /// files.
    fn unnamed(&self, gone: Vec<Droplet>) -> Vec<Blob> {
        let mut blobs = Vec::new();
        for droplet in gone {
            let component = Blob::Droplet(droplet.sha256.clone());
            if !self.names(&component) && !blobs.contains(&component) {
                blobs.push(component);
            }
            let Some(files) = droplet.tar() else {
                continue;
            };
            let files = Blob::Files(files.to_string());
            if !self.names(&files) && !blobs.contains(&files) {
                blobs.push(files);
            }
        }
        blobs
    }

    /// Fails the builds a stop of the cf-api cut short that keep nothing to go on from: those
    /// of a state written before they kept it. The others are taken up (`crate::staging::run`).
    pub fn fail_unfinished_builds(&mut self) {
        for build in (self.builds.iter_mut())
            .filter(|build| build.state == "STAGING" && build.staging.is_none())
        {
            build.state = "FAILED".into();
            build.error =
                Some("StagingError - cut short by a restart of the cf-api: stage again".into());
            build.updated_at = now();
        }
    }

    /// Settles what the jobs wait for that no operation under way will, failing the job: a
    /// binding's operation a stop of the cf-api cut short before it was kept. An app's delete
    /// after its unbinds is taken up (`crate::controller::Controller::follow`): an app deleted
    /// already is waited for no more.
    pub fn settle_unfinished_waits(&mut self) {
        let under_way: Vec<String> = self
            .instances
            .iter()
            .filter(|instance| instance.busy())
            .map(|instance| instance.guid.clone())
            .chain(
                self.bindings
                    .iter()
                    .filter(|binding| binding.busy())
                    .map(|binding| binding.guid.clone()),
            )
            .collect();
        let apps: Vec<&str> = self.apps.iter().map(|app| app.guid.as_str()).collect();
        let bindings: Vec<&str> = (self.bindings.iter()).map(|b| b.guid.as_str()).collect();
        for job in &mut self.jobs {
            let deleting = job.operation == APP_DELETE;
            let mut cut = false;
            job.waits.retain(|wait| {
                if under_way.contains(wait) || deleting && apps.contains(&wait.as_str()) {
                    return true;
                }
                // of an app's delete, neither an app nor a binding: the app, deleted
                cut |= !deleting || bindings.contains(&wait.as_str());
                false
            });
            if cut && job.errors.is_empty() {
                job.errors.push(JobError {
                    code: 10001,
                    title: "UnknownError".into(),
                    detail: "cut short by a restart of the cf-api: try again".into(),
                });
            }
            if job.waits.is_empty() {
                job.polling = false;
            }
        }
    }

    /// Whether `package` is the newest of the app `app`: an older one is gone with a newer's
    /// upload.
    pub fn is_newest_package(&self, package: &str, app: &str) -> bool {
        self.packages
            .iter()
            .rfind(|package| package.app == app)
            .is_some_and(|newest| newest.guid == package)
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

/// An upload's file, by the name of its extension: what a stop cuts short goes at the next
/// start.
const SPOOLED: &str = ".upload";

/// How long a blob no package nor droplet names is kept in a bucket: longer than a read of it
/// begun while it was named takes (a put to a wicket, a package unpacked), on another instance
/// as it may be. The directory's reader holds its file, deleted or not.
pub const SWEEP_GRACE: Duration = Duration::from_secs(30 * 60);

/// How often the blobs are swept.
const SWEEP_EVERY: Duration = Duration::from_secs(5 * 60);

/// How long after its begin a sweep sends its last delete, and what a delete is given.
const SWEEP_WINDOW: Duration = Duration::from_secs(60);
const SWEEP_CALL: Duration = Duration::from_secs(30);

/// A sweep begun that long ago deletes no more ([`SWEEP_WINDOW`], [`SWEEP_CALL`], and the
/// instances' clocks apart): its instance was cut short, another takes its place.
const SWEEP_STALE: Duration = Duration::from_secs(10 * 60);

/// The blobs a sweeper saw unnamed, and since when.
#[derive(Default)]
pub struct Unnamed(HashMap<Blob, Instant>);

/// How long a request waits for the store's revision (`Store::sync`).
const SYNC_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// How long what was seen of the wickets waits to be written with what is seen after it: the
/// answers to a listing's puts written at once.
const OBSERVED_EVERY: Duration = Duration::from_millis(100);

/// The state: a copy the reads are answered from, changed one change at a time, each written
/// before it is seen. Kept in `state.json` of the data directory, written whole on every
/// change: one process's (`crate::check_data_dir`); or, with `--state`, in PostgreSQL
/// (`crate::postgres`): the store of several, the copy following what the others commit.
///
/// And the turn of this instance reconciling the wickets ([`Store::terms`]): the one process
/// of a data directory's from its start, of several the lease's holder (`crate::lease`). What
/// the instance in its turn alone changes is made in it only ([`Store::lead`]), and what the
/// wickets were seen doing is written for every instance ([`Store::observe`]).
///
/// And the blobs the state names (`crate::blobs`), in the directory or, with `--blobs`, in a
/// bucket. One the state names no more is swept: deleted between a sweep's begin and its end,
/// both changes of the state ([`Sweep`]), by one instance at a time. A droplet is named in
/// the state only where no sweep began since its blobs were seen ([`Store::unswept`]): a
/// digest made again while its blob is swept is made once more.
pub struct Store {
    dir: PathBuf,
    blobs: Blobs,
    /// This process, to the sweeps.
    instance: String,
    /// Held by the sweep of this process under way.
    sweeping: tokio::sync::Mutex<()>,
    shared: Arc<Shared>,
    /// Held by the one changing the copy.
    writer: Arc<tokio::sync::Mutex<Writer>>,
    postgres: Option<Arc<Postgres>>,
    /// Follows the others' commits.
    listener: Option<tokio::task::JoinHandle<()>>,
    /// Told what was seen of the wickets ([`Store::observe`]).
    noted: Arc<tokio::sync::Notify>,
    /// Something seen of the wickets may not be written yet.
    unwritten: std::sync::atomic::AtomicBool,
    /// Writes what was seen of the wickets, as it is seen.
    flusher: Option<tokio::task::JoinHandle<()>>,
    flags: Arc<Flags>,
}

impl Drop for Store {
    fn drop(&mut self) {
        for task in [&self.listener, &self.flusher].into_iter().flatten() {
            task.abort();
        }
    }
}

/// The topology as an instance's flags have it: the state's of the one process of a data
/// directory at its start, of several instances the store's once one of these flags takes the
/// lease (`crate::instances`).
pub struct Flags {
    /// `--wicket`'s names, in their order.
    pub wickets: Vec<String>,
    /// `--domain`'s.
    pub domains: Vec<String>,
    /// What the wickets answering at the start run, by name.
    runs: BTreeMap<String, Runtime>,
}

impl Flags {
    pub fn topology(&self) -> Topology {
        Topology {
            wickets: self.wickets.clone(),
            domains: self.domains.clone(),
        }
    }

    /// Makes them the topology of `state`: the domains followed, the apps placed on the wickets
    /// (`State::follow_wickets`, the lines saying so).
    pub fn follow(&self, state: &mut State) -> Vec<String> {
        state.follow_domains(&self.domains);
        state.wickets.runtimes.extend(self.runs.clone());
        state.follow_wickets(&self.wickets)
    }
}

/// The state `file` keeps, as a start takes it: None without the file.
fn kept(file: &Path, domains: &[String]) -> Result<Option<State>, String> {
    match std::fs::read(file) {
        Ok(json) => {
            let mut state: State =
                serde_json::from_slice(&json).map_err(|e| format!("{}: {e}", file.display()))?;
            state.migrate();
            state.follow_domains(domains);
            state.fail_unfinished_builds();
            state.settle_unfinished_waits();
            Ok(Some(state))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(format!("{}: {e}", file.display())),
    }
}

impl Store {
    /// Opens the data directory and the state: the one kept in the directory, or with
    /// `postgres` the store's (an empty store takes the directory's `state.json`, once); else
    /// a new one. The directory's apps are placed on the `wickets` configured
    /// (`State::follow_wickets`, logged), of which those answering now run `runs`, by name: an
    /// index is placed on a wicket of its app's stack. A store's are by the store's topology,
    /// these flags' only where it has none yet: made theirs in a turn of this instance
    /// (`crate::lease`). The blobs are the directory's, or with `bucket` that one's.
    #[allow(clippy::too_many_arguments)]
    pub async fn open(
        dir: &Path,
        org: &str,
        space: &str,
        domains: &[String],
        wickets: &[String],
        runs: &BTreeMap<String, Runtime>,
        postgres: Option<&PostgresSettings>,
        bucket: Option<&BlobSettings>,
    ) -> Result<Store, String> {
        for sub in ["packages", "droplets"] {
            std::fs::create_dir_all(dir.join(sub))
                .map_err(|e| format!("{}: {e}", dir.join(sub).display()))?;
        }
        // what a stop cut short: an upload, a file being written, a droplet being made
        for sub in [".", "packages", "droplets"] {
            for entry in std::fs::read_dir(dir.join(sub))
                .into_iter()
                .flatten()
                .flatten()
            {
                let path = entry.path();
                let name = path.to_string_lossy();
                if name.ends_with(SPOOLED) || name.ends_with(".tmp") || name.ends_with(".droplet") {
                    let _ = std::fs::remove_file(&path);
                }
            }
        }
        let file = dir.join("state.json");
        let mut store = Store {
            dir: dir.to_path_buf(),
            blobs: match bucket {
                Some(bucket) => Blobs::S3(crate::s3::S3::new(bucket)?),
                None => Blobs::Dir(dir.to_path_buf()),
            },
            instance: guid(),
            sweeping: tokio::sync::Mutex::default(),
            shared: Arc::default(),
            writer: Arc::default(),
            postgres: None,
            listener: None,
            noted: Arc::default(),
            unwritten: Default::default(),
            flusher: None,
            flags: Arc::new(Flags {
                wickets: wickets.to_vec(),
                domains: domains.to_vec(),
                runs: runs.clone(),
            }),
        };
        let flags = store.flags.clone();
        let Some(settings) = postgres else {
            let mut state = match kept(&file, domains)? {
                Some(state) => state,
                None => State::new(org, space, domains),
            };
            for line in flags.follow(&mut state) {
                println!("{line}");
            }
            // the one process's, cut short
            state.sweep.by = None;
            store.save(&state)?;
            *store.shared.state.write().expect("no panic under the lock") = state;
            // the one process of the directory reconciles the wickets from its start
            store.shared.term.send_replace(Some(Term { epoch: None }));
            return Ok(store);
        };
        let postgres = Arc::new(Postgres::new(settings, &store.instance)?);
        let mut taken = false;
        let first = || {
            Ok(match kept(&file, domains)? {
                Some(state) => {
                    taken = true;
                    state
                }
                None => State::new(org, space, domains),
            })
        };
        let mut writer = store.writer.lock().await;
        (postgres.load(&mut writer, &store.shared, first)).await?;
        drop(writer);
        if taken {
            println!("STATE: {} taken into the store", file.display());
        }
        store.postgres = Some(postgres.clone());
        // a store without a topology (a new one, one of a build before it was the store's)
        // takes the flags of this instance
        let first = |state: &mut State| match state.wickets.configured.is_empty() {
            true => flags.follow(state),
            false => Vec::new(),
        };
        for line in store.update(first).await {
            println!("{line}");
        }
        let (shared, writer) = (store.shared.clone(), store.writer.clone());
        let listening = postgres.clone();
        store.listener = Some(tokio::spawn(async move {
            listening.keep_listening(&shared, &writer).await;
        }));
        let (shared, writer, noted) = (
            store.shared.clone(),
            store.writer.clone(),
            store.noted.clone(),
        );
        store.flusher = Some(tokio::spawn(async move {
            loop {
                noted.notified().await;
                tokio::time::sleep(OBSERVED_EVERY).await;
                let mut writing = writer.lock().await;
                postgres.update(&mut writing, &shared, |_| (), false).await;
                drop(writing);
                shared.changed();
            }
        }));
        Ok(store)
    }

    pub fn read<R>(&self, f: impl FnOnce(&State) -> R) -> R {
        f(&self.shared.state.read().expect("no panic under the lock"))
    }

    /// The store several instances share (`--state`), for what they keep beside the state.
    pub fn shared(&self) -> Option<Arc<Postgres>> {
        self.postgres.clone()
    }

    /// Changes the state, its apps placed as they are by then (`State::place`), and writes it:
    /// seen by the reads once written. `f` does nothing but change the state it is given: with
    /// `--state` it runs again when the store did not answer, on the state as it is then.
    pub async fn update<R>(&self, f: impl Fn(&mut State) -> R) -> R {
        let made = self.change(f, false).await;
        made.expect("a change of any instance is made")
    }

    /// [`Store::update`] of what the instance reconciling the wickets alone changes (a
    /// deployment's step, a move, a purge done): made in its turn only, which another taking
    /// the lease ends, checked as it is made. None: it was not, the turn over.
    pub async fn lead<R>(&self, f: impl Fn(&mut State) -> R) -> Option<R> {
        self.change(f, true).await
    }

    async fn change<R>(&self, f: impl Fn(&mut State) -> R, leader: bool) -> Option<R> {
        let mut writer = self.writer.lock().await;
        let result = match &self.postgres {
            Some(postgres) => postgres.update(&mut writer, &self.shared, f, leader).await,
            None if leader && !self.leads() => None,
            None => {
                let mut state = self.shared.state.write().expect("no panic under the lock");
                let result = f(&mut state);
                state.place();
                if let Err(e) = self.save(&state) {
                    eprintln!("cannot write the state: {e}");
                }
                Some(result)
            }
        };
        drop(writer);
        self.shared.changed();
        result
    }

    /// The changes of the copy, this process's or another's, as a count: each waiter has its
    /// own, told of a change since it last looked.
    pub fn changes(&self) -> tokio::sync::watch::Receiver<u64> {
        self.shared.changes.subscribe()
    }

    /// This instance's turn reconciling the wickets as it begins and ends: None between.
    pub fn terms(&self) -> tokio::sync::watch::Receiver<Option<Term>> {
        self.shared.term.subscribe()
    }

    /// Whether this instance reconciles the wickets now.
    pub fn leads(&self) -> bool {
        self.shared.term.borrow().is_some()
    }

    /// The turn of the instance reconciling the wickets as this one last saw the lease: its
    /// epoch, and where the others reach it (`--instance-url`) while it is another. Told of a
    /// change by [`Store::leaders`].
    pub fn leader(&self) -> (Option<u64>, Option<String>) {
        let epoch = *self.shared.leading.borrow();
        (epoch, self.shared.leader.borrow().clone())
    }

    /// The lease as this instance sees it, as a turn's epoch: each waiter told when another
    /// turn began.
    pub fn leaders(&self) -> tokio::sync::watch::Receiver<Option<u64>> {
        self.shared.leading.subscribe()
    }

    /// Ends this instance's turn of `epoch` (a wicket refused it), if it is still the one.
    pub fn end_term(&self, epoch: Option<u64>, why: &str) {
        self.shared.end_term(epoch, why);
    }

    /// The lease of the turns of several instances on the store, as `timing` says, this one
    /// saying itself to the others with the `settings` they are to have alike
    /// (`crate::instances`); of the one process of a data directory, held from its start.
    pub fn lease(&self, timing: Timing, settings: BTreeMap<String, String>) -> Arc<Lease> {
        let seat = self.postgres.as_ref().map(|postgres| Seat {
            postgres: postgres.clone(),
            writer: self.writer.clone(),
            member: Member {
                build: crate::describe_build(),
                reads: crate::postgres::SCHEMA,
                address: postgres.address(),
                topology: self.flags.topology(),
                settings,
            },
            flags: self.flags.clone(),
        });
        Arc::new(Lease::new(seat, self.shared.clone(), timing))
    }

    /// Has the reads see what every instance committed by now: a request's first step, failing
    /// while the store does not answer. Nothing to do without `--state`.
    pub async fn sync(&self) -> Result<(), String> {
        let Some(postgres) = &self.postgres else {
            return Ok(());
        };
        let synced = async {
            let ours = self.shared.rev.load(Ordering::Acquire);
            if (postgres.rev().await).is_ok_and(|rev| rev <= ours) {
                return Ok(());
            }
            let mut writer = self.writer.lock().await;
            postgres.catch_up(&mut writer, &self.shared).await
        };
        match tokio::time::timeout(SYNC_TIMEOUT, synced).await {
            Ok(synced) => synced,
            Err(_) => Err("--state: no answer".to_string()),
        }
    }

    /// Changes what was seen of the wickets (their answers of each app's share, what they list
    /// of themselves), written with what is seen after it, a moment later, for every instance
    /// (`crate::records`); not in `state.json`. As [`Store::note`].
    pub async fn observe<R>(&self, f: impl FnOnce(&mut State) -> R) -> R {
        let result = self.note(f).await;
        if self.postgres.is_some() {
            self.unwritten.store(true, Ordering::Relaxed);
            self.noted.notify_one();
        }
        result
    }

    /// Writes what was seen of the wickets and is not written yet, now and not a moment later:
    /// a request is answered after it, so what its answer tells of them (a share a wicket
    /// refused) every instance reads from then on.
    pub async fn observed(&self) {
        let Some(postgres) = &self.postgres else {
            return;
        };
        if self.unwritten.swap(false, Ordering::Relaxed) {
            let mut writing = self.writer.lock().await;
            postgres
                .update(&mut writing, &self.shared, |_| (), false)
                .await;
            drop(writing);
            self.shared.changed();
        }
    }

    /// Forgets what was seen of the wickets and not written: this instance's turn reconciling
    /// them ended, the next's sees them anew.
    pub async fn forget_observed(&self) {
        if self.postgres.is_some() {
            let writer = self.writer.lock().await;
            let mut state = self.shared.state.write().expect("no panic under the lock");
            writer.book.forget_observed(&mut state);
        }
    }

    /// Changes of what is kept whole (`crate::records`: the keyvalue homes, the stacks the
    /// wickets list) what the next change may write, or what is not kept at all. Nothing of
    /// the state's lists, which only a change writes.
    pub async fn note<R>(&self, f: impl FnOnce(&mut State) -> R) -> R {
        let _writer = self.writer.lock().await;
        let mut state = self.shared.state.write().expect("no panic under the lock");
        #[cfg(debug_assertions)]
        let before = state.clone();
        let result = f(&mut state);
        #[cfg(debug_assertions)]
        assert!(
            crate::records::same_lists(&before, &state),
            "a note changed what only an update writes"
        );
        result
    }

    /// Changes what the staging of the build `guid` under way keeps, written with the next
    /// change (how far a build of source was followed: lines taken twice at most after a
    /// crash).
    pub async fn note_staging(&self, guid: &str, f: impl FnOnce(&mut Staging)) {
        let mut writer = self.writer.lock().await;
        let mut state = self.shared.state.write().expect("no panic under the lock");
        if let Some(staging) = state.staging_mut(guid) {
            f(staging);
            writer.book.defer("build", guid);
        }
    }

    fn save(&self, state: &State) -> Result<(), String> {
        let file = self.dir.join("state.json");
        let json = serde_json::to_vec_pretty(state).map_err(|e| e.to_string())?;
        write_by_rename(&file, &json)
    }

    /// A file of this instance's disk, for its owner only, gone with what is returned: an
    /// upload received, a blob made before it is kept ([`Store::keep`]).
    pub fn spool(&self) -> Result<(Spool, std::fs::File), String> {
        let path = self
            .dir
            .join("packages")
            .join(format!("{}{SPOOLED}", guid()));
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
        let file = options
            .open(&path)
            .map_err(|e| format!("{}: {e}", path.display()))?;
        Ok((Spool(path), file))
    }

    /// Keeps the file spooled, of the digest `sha256`, as `blob`: whole, or not at all.
    pub async fn keep(&self, blob: &Blob, spool: Spool, sha256: &str) -> Result<(), String> {
        self.blobs.put(blob, spool, sha256).await
    }

    /// Whether `blob` is kept.
    pub async fn has(&self, blob: &Blob) -> Result<bool, String> {
        self.blobs.has(blob).await
    }

    /// `blob` as a stream; None: it is not kept.
    pub async fn reading(&self, blob: &Blob) -> Result<Option<Reading>, String> {
        self.blobs.read(blob).await
    }

    /// `blob` in a file of this instance's disk; None: it is not kept.
    pub async fn local(&self, blob: &Blob) -> Result<Option<Local>, String> {
        self.blobs.local(blob, || self.spool()).await
    }

    /// The sweeps begun, once none is under way: the blobs seen kept after this are kept
    /// while the state says as many ([`Sweep::rounds`]), which a change naming them checks.
    pub async fn unswept(&self) -> u64 {
        loop {
            if let Err(e) = self.sync().await {
                eprintln!("BLOBS: a droplet waits for the store: {e}");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
            let now = epoch_secs();
            let sweep = self.read(|state| state.sweep.clone());
            if !sweep.under_way(now, *self.shared.leading.borrow()) {
                return sweep.rounds;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Deletes the blobs the state names no more, as a change of it listed them: at once in
    /// the directory, whose readers hold their files; a bucket's are left to the sweep
    /// ([`Store::sweep`]), another instance reading them as it may be.
    pub async fn delete(&self, blobs: &[Blob]) {
        if !self.blobs.shared() {
            self.sweep_of(blobs.to_vec()).await;
        }
    }

    /// Sweeps the blobs for good: [`SWEEP_GRACE`] after a blob was first seen named by
    /// nothing, it is deleted. A bucket's by the instance reconciling the wickets alone, in
    /// its turn; the directory's by its one process.
    pub async fn keep_swept(self: Arc<Self>) {
        let mut unnamed = Unnamed::default();
        loop {
            tokio::time::sleep(SWEEP_EVERY).await;
            if self.blobs.shared() && !self.leads() {
                unnamed = Unnamed::default();
                continue;
            }
            if let Err(e) = self.sweep(&mut unnamed, SWEEP_GRACE).await {
                eprintln!("BLOBS: not swept: {e}");
            }
        }
    }

    /// One sweep: of the blobs kept, those the state does not name are `unnamed` from now on,
    /// and deleted once they were for `grace`: what a change dropped, what was made and never
    /// named (a staging cut short).
    pub async fn sweep(&self, unnamed: &mut Unnamed, grace: Duration) -> Result<(), String> {
        let listed: HashSet<Blob> = self.blobs.list().await?.into_iter().collect();
        self.sync().await?;
        let now = Instant::now();
        self.read(|state| {
            (unnamed.0).retain(|blob, _| listed.contains(blob) && !state.names(blob));
            for blob in listed.into_iter().filter(|blob| !state.names(blob)) {
                unnamed.0.entry(blob).or_insert(now);
            }
        });
        let due =
            |(blob, since): (&Blob, &Instant)| (since.elapsed() >= grace).then(|| blob.clone());
        let due: Vec<Blob> = unnamed.0.iter().filter_map(due).collect();
        for blob in self.sweep_of(due).await {
            unnamed.0.remove(&blob);
        }
        Ok(())
    }

    /// Deletes those of `blobs` the state does not name, between a sweep's begin and its
    /// end; none while another instance's sweep is under way. A bucket's in the turn of this
    /// instance reconciling the wickets ([`Store::lead`]), as long as it lasts. Those deleted.
    async fn sweep_of(&self, blobs: Vec<Blob>) -> Vec<Blob> {
        let leading = || *self.shared.leading.borrow();
        // nothing to delete, no sweep cut short to end: no change of the state
        let cut_short =
            |sweep: &Sweep| sweep.by.is_some() && !sweep.under_way(epoch_secs(), leading());
        let idle =
            |state: &State| blobs.iter().all(|blob| state.names(blob)) && !cut_short(&state.sweep);
        if self.read(idle) {
            return Vec::new();
        }
        let _one = self.sweeping.lock().await;
        let began = Instant::now();
        let shared = self.blobs.shared();
        let term = *self.shared.term.borrow();
        let epoch = term.and_then(|term| term.epoch);
        let begin = |state: &mut State| {
            let now = epoch_secs();
            let ours = |by: &Sweeping| by.instance == self.instance;
            if state.sweep.under_way(now, leading()) && !state.sweep.by.as_ref().is_some_and(ours) {
                return Vec::new();
            }
            let unnamed: Vec<Blob> = (blobs.iter())
                .filter(|blob| !state.names(blob))
                .cloned()
                .collect();
            state.sweep.by = None;
            if !unnamed.is_empty() {
                state.sweep.rounds += 1;
                state.sweep.by = Some(Sweeping {
                    instance: self.instance.clone(),
                    since: now,
                    epoch,
                });
            }
            unnamed
        };
        let unnamed = match shared {
            true => self.lead(begin).await.unwrap_or_default(),
            false => self.update(begin).await,
        };
        if unnamed.is_empty() {
            return unnamed;
        }
        let mut deleted = Vec::new();
        for blob in unnamed {
            // the turn it began in over: the next instance's sweeps take its place
            let over = shared && *self.shared.term.borrow() != term;
            if began.elapsed() >= SWEEP_WINDOW || over {
                break;
            }
            let failed = match tokio::time::timeout(SWEEP_CALL, self.blobs.delete(&blob)).await {
                Ok(Ok(())) => {
                    deleted.push(blob);
                    continue;
                }
                Ok(Err(e)) => e,
                Err(_) => "no answer".to_string(),
            };
            eprintln!("BLOBS: cannot delete {}: {failed}", blob.key());
        }
        self.update(|state| {
            if (state.sweep.by.as_ref()).is_some_and(|by| by.instance == self.instance) {
                state.sweep.by = None;
            }
        })
        .await;
        deleted
    }
}

/// Writes `bytes` to `path`, as [`make_by_rename`] does.
pub(crate) fn write_by_rename(path: &Path, bytes: &[u8]) -> Result<(), String> {
    use std::io::Write as _;
    make_by_rename(path, |file| {
        file.write_all(bytes)
            .map_err(|e| format!("{}: {e}", path.display()))
    })
}

/// Makes `path` of what `write` writes, by a rename, for its owner only (0600) from the
/// first byte on, on the disk once it returns: in a VM, a kill loses what its guest's page
/// cache held, and an edit answered must be there. Failing, nothing is left.
pub(crate) fn make_by_rename<T>(
    path: &Path,
    write: impl FnOnce(&mut std::fs::File) -> Result<T, String>,
) -> Result<T, String> {
    // of its own: two uploads to one package may be under way
    let tmp = path.with_extension(format!("{}.tmp", guid()));
    let failed = |e: std::io::Error| format!("{}: {e}", path.display());
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    let mut file = options.open(&tmp).map_err(failed)?;
    let made = write(&mut file).and_then(|made| {
        file.sync_all()
            .and_then(|()| std::fs::rename(&tmp, path))
            .and_then(|()| match path.parent() {
                Some(dir) => std::fs::File::open(dir)?.sync_all(),
                None => Ok(()),
            })
            .map_err(failed)?;
        Ok(made)
    });
    if made.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    made
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
#[path = "../tests/support/postgres_server.rs"]
pub(crate) mod postgres_server;

#[cfg(test)]
#[path = "../tests/support/s3_server.rs"]
pub(crate) mod s3_server;

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// A data directory of its own, and the store opened on it: the state in the directory,
    /// or in the database `url`, its TLS verified against `ca`; the first `domains` flag takes
    /// the default routes.
    async fn try_open(
        dir: &Path,
        url: Option<&str>,
        ca: Option<Vec<u8>>,
        domains: &[&str],
    ) -> Result<Store, String> {
        try_open_on(dir, url, ca, domains, &["a", "b"], None).await
    }

    /// As `try_open`, its flags naming the `wickets`, the blobs in `bucket`.
    async fn try_open_on(
        dir: &Path,
        url: Option<&str>,
        ca: Option<Vec<u8>>,
        domains: &[&str],
        wickets: &[&str],
        bucket: Option<&BlobSettings>,
    ) -> Result<Store, String> {
        let domains: Vec<String> = domains.iter().map(|name| name.to_string()).collect();
        let wickets: Vec<String> = wickets.iter().map(|name| name.to_string()).collect();
        let settings = url.map(|url| PostgresSettings {
            url: url.to_string(),
            ca_pem: ca,
            instance_url: None,
        });
        let runs = BTreeMap::new();
        Store::open(
            dir,
            "o",
            "s",
            &domains,
            &wickets,
            &runs,
            settings.as_ref(),
            bucket,
        )
        .await
    }

    /// As `try_open`, verified against the test server's CA.
    async fn open(dir: &Path, url: Option<&str>, domains: &[&str]) -> Store {
        let opened = try_open(dir, url, postgres_server::ca(), domains).await;
        opened.unwrap()
    }

    /// The lease of the tests: a turn ends past a second of no renewal.
    pub(crate) const TIMING: Timing = Timing {
        lease: Duration::from_millis(1000),
        renew_every: Duration::from_millis(200),
        take_every: Duration::from_millis(100),
        edits_end: Duration::from_millis(200),
    };

    /// `store`'s lease kept, as `TIMING` says: the task keeping it.
    pub(crate) fn keep(store: &Store) -> (Arc<Lease>, tokio::task::JoinHandle<()>) {
        let lease = store.lease(TIMING, BTreeMap::new());
        let keeping = tokio::spawn(lease.clone().run());
        (lease, keeping)
    }

    /// Once `store`'s turn began.
    pub(crate) async fn led(store: &Store) {
        let mut terms = store.terms();
        let led = terms.wait_for(|term| term.is_some());
        (tokio::time::timeout(Duration::from_secs(10), led).await)
            .expect("a turn")
            .unwrap();
    }

    /// [`keep`], once its turn began.
    pub(crate) async fn leading(store: &Store) -> (Arc<Lease>, tokio::task::JoinHandle<()>) {
        let kept = keep(store);
        led(store).await;
        kept
    }

    /// The epoch of `store`'s turn.
    pub(crate) fn epoch(store: &Store) -> Option<u64> {
        store.terms().borrow().and_then(|term| term.epoch)
    }

    /// What a store keeps, as `state.json` has it.
    fn kept(store: &Store) -> serde_json::Value {
        store.read(|state| serde_json::to_value(state).unwrap())
    }

    fn names(store: &Store) -> Vec<String> {
        store.read(|state| state.apps.iter().map(|app| app.name.clone()).collect())
    }

    /// A blob is kept whole and read, as a stream and from a file, by any instance; swept
    /// once the state names it no more: in the directory at once, in a bucket (its TLS
    /// verified against a CA given, the keys under a prefix) after the grace, by the instance
    /// whose turn it is alone, a read under way getting it all either way; what was never named
    /// after the grace in both. A sweep is seen by who is to name a blob, and waits for another
    /// instance's under way until its turn ended or it is stale.
    #[tokio::test]
    async fn a_blob_is_kept_for_every_instance_and_swept_once_unnamed() {
        use http_body_util::BodyExt as _;
        let _ = rustls::crypto::ring::default_provider().install_default();
        for shared in [false, true] {
            let dir = std::env::temp_dir().join(format!("cf-api-blobs-{}", guid()));
            // the directory's one instance; the bucket's two, on one state
            let (first, second) = if shared {
                let (url, _) = postgres_server::database().await;
                let url = format!("{url}?sslmode=disable");
                let bucket = s3_server::bucket().await;
                let open = async |dir: PathBuf| {
                    let (domains, wickets) = (["localhost"], ["a", "b"]);
                    let opened =
                        try_open_on(&dir, Some(&url), None, &domains, &wickets, Some(&bucket));
                    Arc::new(opened.await.unwrap())
                };
                (open(dir.join("a")).await, open(dir.join("b")).await)
            } else {
                let store = Arc::new(open(&dir, None, &["localhost"]).await);
                (store.clone(), store)
            };
            // the second's turn: it alone sweeps the bucket
            let _lease = match shared {
                true => Some(leading(&second).await),
                false => None,
            };
            let keep = async |blob: &Blob, bytes: &[u8]| {
                use std::io::Write as _;
                let (spool, mut file) = first.spool().unwrap();
                file.write_all(bytes).unwrap();
                drop(file);
                let sha256 = crate::staging::sha256(bytes);
                first.keep(blob, spool, &sha256).await.unwrap();
            };
            let zip = b"a zip".to_vec();
            let component: Vec<u8> = (0..300_000u32).map(|at| (at % 251) as u8).collect();
            let package = Blob::Package(guid());
            let droplet = Blob::Droplet(crate::staging::sha256(&component));
            let orphan = Blob::Files(crate::staging::sha256(b"never named"));
            keep(&package, &zip).await;
            keep(&droplet, &component).await;
            keep(&orphan, b"never named").await;
            let named = |state: &mut State| {
                state.packages.push(Package {
                    guid: package.name().to_string(),
                    app: "a".into(),
                    state: "READY".into(),
                    sha256: None,
                    docker: None,
                    created_at: now(),
                    updated_at: now(),
                });
                state.droplets.push(Droplet {
                    guid: "d".into(),
                    app: "a".into(),
                    package: package.name().to_string(),
                    sha256: droplet.name().to_string(),
                    file: "hello.wasm".into(),
                    staged_as: None,
                    staged_by: None,
                    bytes: 1,
                    image: None,
                    made: BTreeMap::new(),
                    files: None,
                    built: None,
                    created_at: now(),
                    updated_at: now(),
                });
            };
            first.update(named).await;
            second.sync().await.unwrap();

            // uploaded through one instance, unpacked through another
            let local = second.local(&package).await.unwrap().expect("the package");
            assert_eq!(std::fs::read(local.path()).unwrap(), zip, "shared {shared}");
            let gone = Blob::Package(guid());
            assert!(second.local(&gone).await.unwrap().is_none());
            assert!(second.reading(&gone).await.unwrap().is_none());

            // dropped while it is read
            let rounds = second.unswept().await;
            let reading = second
                .reading(&droplet)
                .await
                .unwrap()
                .expect("the droplet");
            assert_eq!(reading.bytes, component.len() as u64);
            first.update(|state| state.droplets.clear()).await;
            first.delete(std::slice::from_ref(&droplet)).await;
            let mut unnamed = Unnamed::default();
            second.sweep(&mut unnamed, SWEEP_GRACE).await.unwrap();
            let read = reading.body.collect().await.unwrap().to_bytes();
            assert_eq!(read, component, "shared {shared}");
            // the directory's is gone at once, the bucket's and what was never named stay
            // for the grace, and the bucket's for an instance in no turn
            assert_eq!(second.has(&droplet).await.unwrap(), shared);
            assert!(second.has(&orphan).await.unwrap());
            if shared {
                let mut unnamed = Unnamed::default();
                first.sweep(&mut unnamed, Duration::ZERO).await.unwrap();
                assert!(first.has(&orphan).await.unwrap(), "not its turn");
            }
            second.sweep(&mut unnamed, Duration::ZERO).await.unwrap();
            for (blob, kept) in [(&package, true), (&droplet, false), (&orphan, false)] {
                assert_eq!(
                    second.has(blob).await.unwrap(),
                    kept,
                    "{blob:?}, shared {shared}"
                );
            }
            assert!(first.unswept().await > rounds, "a sweep is seen");
            assert!(unnamed.0.is_empty());

            // another instance's sweep under way: nothing deleted, nothing named
            let swept_by = async |since: u64, epoch: Option<u64>| {
                let by = Some(Sweeping {
                    instance: "another".into(),
                    since,
                    epoch,
                });
                first.update(|state| state.sweep.by = by.clone()).await;
                second.sync().await.unwrap();
            };
            swept_by(epoch_secs(), None).await;
            keep(&orphan, b"never named").await;
            second.sweep(&mut unnamed, Duration::ZERO).await.unwrap();
            assert!(second.has(&orphan).await.unwrap());
            let waited = tokio::time::timeout(Duration::from_millis(300), second.unswept());
            assert!(waited.await.is_err(), "a sweep under way is waited for");
            // cut short, stale or of a turn before the one leading: taken over
            let mut cut_short = vec![(epoch_secs() - SWEEP_STALE.as_secs(), None)];
            if let Some(epoch) = epoch(&second) {
                cut_short.push((epoch_secs(), Some(epoch - 1)));
            }
            for (since, of) in cut_short {
                swept_by(since, of).await;
                second.unswept().await;
                second.sweep(&mut unnamed, Duration::ZERO).await.unwrap();
                assert!(!second.has(&orphan).await.unwrap(), "{of:?}");
                keep(&orphan, b"never named").await;
            }
            second.sweep(&mut unnamed, Duration::ZERO).await.unwrap();
            first.sync().await.unwrap();
            assert_eq!(first.read(|state| state.sweep.by.clone()), None);
            let _ = std::fs::remove_dir_all(&dir);
        }
    }

    /// What a change wrote is read at the next start, the lists in their order, with what was
    /// noted aside from a change before it: in the directory, and in PostgreSQL, whose empty
    /// store takes the directory's state, once; over a connection without TLS. Other flags are
    /// the directory's topology at the start, a store's in a turn of the instance.
    #[tokio::test]
    async fn a_change_is_kept_for_the_next_start() {
        let (url, _) = postgres_server::database().await;
        let url = format!("{url}?sslmode=disable");
        for url in [None, Some(url.as_str())] {
            let dir = std::env::temp_dir().join(format!("cf-api-store-{}", guid()));
            // as the release before left it: the directory's state
            let store = open(&dir, None, &["localhost"]).await;
            let build = (store.update(|state| {
                let space = state.spaces[0].guid.clone();
                let app = state.create_app("first", &space).guid.clone();
                state.builds.push(Build {
                    guid: guid(),
                    app,
                    package: "p".into(),
                    state: "STAGING".into(),
                    lifecycle: Lifecycle::Buildpack,
                    error: None,
                    droplet: None,
                    staging: Some(Staging::default()),
                    created_at: now(),
                    updated_at: now(),
                });
                state.builds[0].guid.clone()
            }))
            .await;
            drop(store);
            let store = open(&dir, url, &["localhost"]).await;
            assert_eq!(names(&store), ["first"]);
            let second = (store.update(|state| {
                let space = state.spaces[0].guid.clone();
                state.create_app("second", &space).guid.clone()
            }))
            .await;
            // written with the next change
            let followed = |staging: &mut Staging| staging.command = Some("followed".into());
            store.note_staging(&build, followed).await;
            store
                .note(|state| state.homes = [("kv".to_string(), "a".to_string())].into())
                .await;
            let moved = store.update(|state| state.app_mut(&second).unwrap().started = true);
            moved.await;
            let was = kept(&store);
            drop(store);
            // the first flag's domain goes before the one there was
            let store = open(&dir, url, &["apps.example.com", "localhost"]).await;
            let _lease = match url {
                Some(_) => Some(leading(&store).await),
                None => None,
            };
            let domains: Vec<String> =
                store.read(|state| state.domains.iter().map(|d| d.name.clone()).collect());
            assert_eq!(domains, ["apps.example.com", "localhost"]);
            let mut is = kept(&store);
            assert_eq!(is["domains"][1], was["domains"][0]);
            is["domains"] = was["domains"].clone();
            assert_eq!(is, was);
            assert_eq!(names(&store), ["first", "second"]);
            assert_eq!(is["homes"]["kv"], "a");
            assert_eq!(is["builds"][0]["staging"]["command"], "followed");
            drop(store);
            let store = open(&dir, url, &["apps.example.com", "localhost"]).await;
            let mut again = kept(&store);
            again["domains"] = was["domains"].clone();
            assert_eq!(again, was);
        }
    }

    /// Two instances on one PostgreSQL: a change of one is seen by the other (told of it, and
    /// at a request's start at the latest), the changes of both are one after another, none
    /// lost, and a connection lost is opened again; over TLS, the server verified, where the
    /// test server takes it. One at a time leads, its own changes and what it saw of the
    /// wickets the other's: one paused past its lease has its turn taken, its changes refused
    /// and what it saw since forgotten; one letting go of it has it taken at once. What a row
    /// of a newer build holds is kept by a change of this one's; a store of the layout before
    /// is moved to this build's by the one leading, and one of a newer layout is not opened.
    /// An instance of other flags leaves the topology the store's, the apps where they are,
    /// until it is the one on the store: then the apps are placed by its flags.
    #[tokio::test(flavor = "multi_thread")]
    async fn two_instances_change_one_state() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let (plain, behind) = postgres_server::database().await;
        let dir = |of: &str| std::env::temp_dir().join(format!("cf-api-store-{of}-{}", guid()));
        let tls = postgres_server::ca().is_some();
        let url = match tls {
            true => format!("{plain}?sslmode=require"),
            false => plain.clone(),
        };
        if tls {
            // a server the CA does not verify is refused, and not gone on to without TLS
            // (`prefer`, a URL's default)
            for url in [&url, &plain] {
                let refused = try_open(&dir("refused"), Some(url), None, &["localhost"]).await;
                let error = refused.err().expect("refused");
                assert!(error.contains("certificate"), "{error}");
            }
        }
        let a = open(&dir("a"), Some(&url), &["localhost"]).await;
        let b = open(&dir("b"), Some(&url), &["localhost"]).await;
        assert_eq!(kept(&a), kept(&b));
        let app = |name: &'static str| {
            a.update(move |state| {
                let space = state.spaces[0].guid.clone();
                state.create_app(name, &space).guid.clone()
            })
        };
        // b is told of a's change
        let mut changes = b.changes();
        let x = app("x").await;
        let told = changes.changed();
        (tokio::time::timeout(std::time::Duration::from_secs(10), told).await)
            .unwrap()
            .unwrap();
        b.sync().await.unwrap();
        assert_eq!(names(&b), ["x"]);
        let y = app("y").await;
        // both edit one app, and each another, at once
        let scale = async |store: &Store, guid: &str, key: &str| {
            for _ in 0..20 {
                (store.update(|state| {
                    state.app_mut(&x).unwrap().process.instances += 1;
                    let app = state.app_mut(guid).unwrap();
                    *app.env.entry(key.to_string()).or_default() += "+";
                }))
                .await;
            }
        };
        tokio::join!(scale(&a, &x, "A"), scale(&b, &y, "B"));
        // every connection of both, the changing and the listening, is over TLS
        let over = "SELECT ssl FROM pg_stat_ssl JOIN pg_stat_activity USING (pid) \
                    WHERE datname = current_database() AND pid <> pg_backend_pid()";
        let over = behind.query(over, &[]).await.unwrap();
        let over: Vec<bool> = over.iter().map(|row| row.get(0)).collect();
        assert_eq!(over, [tls; 4]);
        a.sync().await.unwrap();
        b.sync().await.unwrap();
        assert_eq!(kept(&a), kept(&b));
        a.read(|state| {
            assert_eq!(state.app(&x).unwrap().process.instances, 41);
            assert_eq!(state.app(&x).unwrap().env["A"].len(), 20);
            assert_eq!(state.app(&y).unwrap().env["B"].len(), 20);
            // each index placed by whoever wrote the count
            assert_eq!(state.app(&x).unwrap().placement.len(), 41);
        });
        // their connections ended under them: a change waits for a new one
        let ended = "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
                     WHERE datname = current_database() AND pid <> pg_backend_pid()";
        behind.batch_execute(ended).await.unwrap();
        a.update(|state| state.remove_app(&x)).await;
        app("z").await;
        b.sync().await.unwrap();
        assert_eq!(names(&b), ["y", "z"]);
        assert_eq!(kept(&a), kept(&b));
        // the rows removed are swept: one behind them reads all again
        let swept = "DELETE FROM sluice_cf_records WHERE body IS NULL; \
                     UPDATE sluice_cf_meta SET oldest = rev + 1";
        behind.batch_execute(swept).await.unwrap();
        a.update(|state| state.remove_app(&y)).await;
        b.sync().await.unwrap();
        assert_eq!(names(&b), ["z"]);
        assert_eq!(kept(&a), kept(&b));
        // one starting later reads the same, and its change keeps what a newer build wrote of
        // the row, at any depth
        let later = "UPDATE sluice_cf_records SET body = jsonb_set(jsonb_set(body::jsonb, \
                     '{later}', '{\"of\": 1}'), '{process,later}', 'true')::text \
                     WHERE kind = 'app' AND body IS NOT NULL";
        assert_eq!(behind.execute(later, &[]).await.unwrap(), 1);
        let c = open(&dir("c"), Some(&url), &["localhost"]).await;
        assert_eq!(kept(&c), kept(&a));
        c.update(|state| {
            state.apps[0].started = true;
            state.apps[0].process.instances = 2;
        })
        .await;
        let body = "SELECT body FROM sluice_cf_records WHERE kind = 'app' AND body IS NOT NULL";
        let body: String = behind.query_one(body, &[]).await.unwrap().get(0);
        let body: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(body["later"]["of"], 1, "{body}");
        assert_eq!(body["process"]["later"], true, "{body}");
        assert_eq!(body["process"]["instances"], 2, "{body}");
        a.sync().await.unwrap();
        b.sync().await.unwrap();
        assert_eq!(kept(&c), kept(&a));

        // a leads: b makes none of the leader's changes, and sees what a saw of the wickets
        let (_, keeping_a) = leading(&a).await;
        let (lease_b, keeping_b) = keep(&b);
        tokio::time::sleep(TIMING.take_every * 3).await;
        assert!(!b.leads());
        let apps = |state: &mut State| state.apps.len();
        assert_eq!((a.lead(apps).await, b.lead(apps).await), (Some(1), None));
        let z = a.read(|state| state.apps[0].guid.clone());
        let seen = |outcome: Outcome| {
            let z = z.clone();
            move |state: &mut State| {
                let outcomes = state.outcomes.entry(z.clone()).or_default();
                outcomes.insert("w".into(), outcome);
            }
        };
        let outcome = |store: &Store| store.read(|state| state.outcome(&z, "w").cloned());
        a.observe(seen(Outcome::Running)).await;
        let started = Instant::now();
        while outcome(&b) != Some(Outcome::Running) {
            assert!(started.elapsed() < Duration::from_secs(10), "not seen by b");
            b.sync().await.unwrap();
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        // a paused past its lease, its turn on: b's turn, of a later epoch; a's change and what
        // it saw since are not written, its turn ends, what it saw forgotten
        keeping_a.abort();
        let paused = Instant::now();
        led(&b).await;
        println!("taken over {:?} after the last renewal", paused.elapsed());
        assert!(epoch(&b) > epoch(&a));
        a.observe(seen(Outcome::Unreachable)).await;
        assert_eq!(a.lead(apps).await, None);
        assert!(!a.leads());
        assert_eq!(outcome(&a), Some(Outcome::Running));
        b.sync().await.unwrap();
        assert_eq!(outcome(&b), Some(Outcome::Running));
        // b lets go of it: a's at once
        let (lease_a, _keeping_a) = keep(&a);
        keeping_b.abort();
        let was = epoch(&b);
        lease_b.release().await;
        let released = Instant::now();
        led(&a).await;
        assert!(
            released.elapsed() < TIMING.lease,
            "{:?}",
            released.elapsed()
        );
        assert!(epoch(&a) > was);

        // a store of the layout before, holding what no build reads any more: moved to this
        // build's by a, every instance on it reading that one
        let before = "UPDATE sluice_cf_meta SET layout = 1; \
                      INSERT INTO sluice_cf_records (kind, id, ord, rev, body) \
                      VALUES ('single', 'flume', 0, 1, '{}')";
        behind.batch_execute(before).await.unwrap();
        let layout = "SELECT layout, (SELECT count(*) FROM sluice_cf_records WHERE id = 'flume') \
                      FROM sluice_cf_meta";
        let moved = Instant::now();
        loop {
            let row = behind.query_one(layout, &[]).await.unwrap();
            if (row.get::<_, i32>(0), row.get::<_, i64>(1)) == (crate::postgres::SCHEMA, 0) {
                break;
            }
            assert!(moved.elapsed() < Duration::from_secs(10), "not moved");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        // an instance whose flags name other wickets, beside a: the store's topology and the
        // app's indices stay, and it takes no turn
        let topology = |store: &Store| {
            store.read(|state| {
                (
                    state.wickets.configured.clone(),
                    state.apps[0].placement.clone(),
                )
            })
        };
        let (wickets, placement) = topology(&a);
        let first = ["a", "b"].map(String::from).to_vec();
        assert_eq!((&wickets, &placement), (&first, &first));
        let (flags, ca) = (["b", "c"], postgres_server::ca());
        let d = try_open_on(&dir("d"), Some(&url), ca, &["localhost"], &flags, None)
            .await
            .unwrap();
        let (_, _keeping_d) = keep(&d);
        tokio::time::sleep(TIMING.take_every * 3).await;
        assert!(!d.leads());
        a.sync().await.unwrap();
        assert_eq!(topology(&a), (wickets.clone(), placement.clone()));
        assert_eq!(topology(&d), (wickets, placement));
        // a stopped, its flags are every instance's: its turn, the app placed by them
        lease_a.release().await;
        led(&d).await;
        a.sync().await.unwrap();
        for store in [&a, &d] {
            let (wickets, placement) = topology(store);
            assert_eq!(wickets, flags);
            assert_eq!(placement.len(), 2);
            assert!(
                placement.iter().all(|on| flags.contains(&on.as_str())),
                "{placement:?}"
            );
        }

        // a store of a newer build's layout is not opened
        let newer = "UPDATE sluice_cf_meta SET layout = layout + 1";
        behind.batch_execute(newer).await.unwrap();
        let refused = try_open(
            &dir("newer"),
            Some(&url),
            postgres_server::ca(),
            &["localhost"],
        )
        .await;
        let error = refused.err().expect("refused");
        assert!(error.contains("of a newer sluice-wicket-cf-api"), "{error}");
    }

    #[test]
    fn the_current_droplet_keeps_the_previous_the_packages_the_newest_and_a_restart_what_it_takes_up()
     {
        let mut state = State::new("o", "s", &["localhost".into()]);
        let space = state.spaces[0].guid.clone();
        let app = state.create_app("hello", &space).guid.clone();
        let droplet = |guid: &str, sha256: &str| Droplet {
            guid: guid.into(),
            app: app.clone(),
            package: "p".into(),
            sha256: sha256.into(),
            file: "hello.wasm".into(),
            staged_as: None,
            staged_by: None,
            bytes: 1,
            image: None,
            made: BTreeMap::new(),
            files: None,
            built: None,
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
        // a static site's files go with the last droplet naming them, its component stays
        let site = |guid: &str, files: &str| Droplet {
            files: Some(Files {
                sha256: files.into(),
                bytes: 1,
            }),
            built: None,
            ..droplet(guid, "s")
        };
        state.droplets.push(site("d5", "f"));
        assert_eq!(
            state.set_current_droplet(&app, "d5"),
            vec![Blob::Droplet("a".into())]
        );
        state.droplets.push(site("d6", "g"));
        assert_eq!(
            state.set_current_droplet(&app, "d6"),
            vec![Blob::Droplet("c".into())]
        );
        state.droplets.push(site("d7", "g"));
        assert_eq!(
            state.set_current_droplet(&app, "d7"),
            vec![Blob::Files("f".into())]
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
        // a staging cut short keeping nothing to go on from fails, one keeping it is taken up,
        // a staged build stays
        let kept = Some(Staging::default());
        for (guid, staged, staging) in [
            ("b1", "STAGING", None),
            ("b2", "STAGED", None),
            ("b3", "STAGING", kept),
        ] {
            state.builds.push(Build {
                guid: guid.into(),
                app: app.clone(),
                package: "p2".into(),
                state: staged.into(),
                lifecycle: Lifecycle::Docker,
                error: None,
                droplet: None,
                staging,
                created_at: now(),
                updated_at: now(),
            });
        }
        state.fail_unfinished_builds();
        let states: Vec<&str> = state.builds.iter().map(|b| b.state.as_str()).collect();
        assert_eq!(states, ["FAILED", "STAGED", "STAGING"]);
        // its bindings go with it, another app's stay
        for (guid, bound) in [("b1", app.as_str()), ("b2", "other")] {
            state.bindings.push(Binding {
                guid: guid.into(),
                name: None,
                app: bound.into(),
                instance: "i".into(),
                identifier: String::new(),
                credentials: Default::default(),
                operation: None,
                created_at: now(),
                updated_at: now(),
            });
        }
        // what jobs wait for at a start: an unbind under way and an app's delete after it
        // stay, an app deleted already is done, an operation no longer under way is cut
        let unbinding = Operation::new(OperationKind::Delete, None, None);
        state.bindings[0].operation = Some(unbinding);
        let waits = [
            (APP_DELETE, vec!["b1", app.as_str()]),
            (APP_DELETE, vec!["deleted"]),
            (APP_DELETE, vec!["b2", app.as_str()]),
            ("service_bindings.delete", vec!["b2"]),
        ];
        for (operation, waits) in waits {
            let waits = waits.into_iter().map(String::from).collect();
            state.add_waiting_job(operation, waits, None);
        }
        state.settle_unfinished_waits();
        let settled: Vec<(&str, Vec<&str>, usize)> = (state.jobs.iter())
            .map(|job| {
                let waits = job.waits.iter().map(String::as_str).collect();
                (job.state(), waits, job.errors.len())
            })
            .collect();
        assert_eq!(
            settled,
            [
                ("PROCESSING", vec!["b1", app.as_str()], 0),
                ("COMPLETE", vec![], 0),
                ("PROCESSING", vec![app.as_str()], 1),
                ("FAILED", vec![], 1),
            ]
        );
        assert_eq!(
            state.remove_app(&app),
            vec![
                Blob::Package("p2".into()),
                Blob::Droplet("s".into()),
                Blob::Files("g".into())
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

    /// A state of one organization and space, before there were more, takes them as its first;
    /// one of one wicket has its apps' indices and its removals on the first configured. An
    /// index on a wicket no longer configured is placed again, a removal from one dropped, each
    /// logged.
    #[test]
    fn a_state_of_one_space_and_one_wicket_migrates_to_their_first() {
        use serde_json::json;
        let mut written = State::new("o", "s", &["localhost".into()]);
        let space = written.spaces[0].clone();
        let org = written.orgs[0].clone();
        let hello = written.create_app("hello", &space.guid).guid.clone();
        written.app_mut(&hello).unwrap().process.instances = 2;
        let mut legacy = serde_json::to_value(&written).unwrap();
        legacy["org"] = json!(org);
        legacy["space"] =
            json!({"guid": space.guid, "name": space.name, "created_at": space.created_at});
        legacy["orgs"] = json!([]);
        legacy["spaces"] = json!([]);
        legacy["apps"][0]["space"] = json!("");
        legacy["removals"] = json!(["gone"]);
        // the stack an app had before what is pushed decided: the wicket's, or a CF manifest's,
        // no pin; a droplet's and a snapshot's are what their files tell
        legacy["apps"][0]["stack"] = json!("cflinuxfs4");
        let staged = Droplet {
            guid: "d".into(),
            app: hello.clone(),
            package: "p".into(),
            sha256: "ab".repeat(32),
            file: "app/r.ext4".into(),
            staged_as: Some(Runtime::Firecracker),
            staged_by: Some(Buildpack::Rootfs),
            bytes: 1,
            image: None,
            made: BTreeMap::new(),
            files: None,
            built: None,
            created_at: now(),
            updated_at: now(),
        };
        legacy["droplets"] = json!([staged]);
        let kept = legacy["droplets"][0].as_object_mut().unwrap();
        assert_eq!(kept.remove("buildpack"), Some(json!("rootfs")));
        kept.remove("stack");
        legacy["apps"][0]["current_droplet"] = json!("d");
        let mut state: State = serde_json::from_value(legacy).unwrap();
        state.migrate();
        assert_eq!(state.apps[0].pin, None);
        assert_eq!(state.droplets[0].staged_as, None);
        assert_eq!(state.droplets[0].stack(), Runtime::Firecracker);
        assert_eq!(state.droplets[0].buildpack(), Some(Buildpack::Rootfs));
        assert_eq!(
            state.shown_stack(&state.apps[0]),
            Some(Runtime::Firecracker)
        );
        state.apps[0].current_droplet = None;
        assert_eq!(state.orgs, [org]);
        assert_eq!(state.spaces, std::slice::from_ref(&space));
        assert_eq!(state.apps[0].space, space.guid);
        let names =
            |names: &[&str]| -> Vec<String> { names.iter().map(|n| n.to_string()).collect() };
        assert_eq!(
            state.follow_wickets(&names(&["a", "b"])),
            Vec::<String>::new()
        );
        assert_eq!(state.apps[0].placement, ["a", "a"]);
        let gone = |wicket: &str| Removal {
            app: "gone".into(),
            wicket: wicket.into(),
        };
        assert_eq!(state.removals, [gone("a")]);
        // written again, none of the one organization or space is kept apart; an index moving
        // kept
        state.apps[0].moving.insert(1, "b".into());
        // what each wicket listed it runs is kept: one down since the start is of its stack
        state.wickets.runtimes = [("b".to_string(), Runtime::Firecracker)].into();
        let again = serde_json::to_value(&state).unwrap();
        assert!(again.get("org").is_none() && again.get("space").is_none());
        assert_eq!(again["wicket_stacks"], json!({"b": "firecracker"}));
        let mut again: State = serde_json::from_value(again).unwrap();
        assert_eq!(again.stacks(), [], "b is not configured yet");
        assert_eq!(std::mem::take(&mut again.wickets.runtimes).len(), 1);
        assert_eq!(
            (&again.apps[0].placement, &again.removals),
            (&names(&["a", "a"]), &vec![gone("a")])
        );
        assert_eq!(again.apps[0].moving, [(1, "b".to_string())].into());
        assert_eq!(
            again.follow_wickets(&names(&["b", "c"])),
            [
                "[gone] CELL: not removed from the wicket a: no --wicket names it".to_string(),
                format!("[{hello}] CELL: index 0 placed on the wicket b: no --wicket names a"),
                format!("[{hello}] CELL: index 1 placed on the wicket c: no --wicket names a"),
            ]
        );
        assert_eq!(again.apps[0].placement, ["b", "c"]);
        assert!(again.removals.is_empty());
        // its count followed at every change
        again.app_mut(&hello).unwrap().process.instances = 3;
        again.place();
        assert_eq!(again.apps[0].placement, ["b", "c", "b"]);
    }
}
