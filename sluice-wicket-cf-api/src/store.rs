//! The desired state: the organizations and their spaces, the roles users and clients hold in
//! them, the shared domains, the apps of the spaces with their web process (each index placed
//! on a wicket: `crate::placement`) and environment variables, the routes,
//! packages, builds, droplets, deployments and jobs, the service brokers
//! registered with their catalogs, the service instances and their bindings.
//! In memory, written by a rename to `state.json` in the
//! data directory on every change; the blobs next to it
//! (`packages/<guid>.zip`, `droplets/<sha256>.wasm`, a static site's files
//! `droplets/<sha256>.tar`). Every file is its
//! owner's only (0600): the state holds the apps' environment and the
//! credentials of their images. A write is durable when it returns, for a kill of a
//! VM holding the directory on its disk.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

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
    /// Where the watch of the flume resumes: past the last line it took
    /// (`flume`); None before one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flume: Option<FlumeCursor>,
    /// The id each wicket last listed, by name: another behind a name is another wicket
    /// (`crate::controller`).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub wicket_ids: BTreeMap<String, String>,
    /// What each wicket answered of each app, by guid, then by wicket; not kept: a start
    /// lists the wickets.
    #[serde(skip)]
    pub outcomes: BTreeMap<String, BTreeMap<String, Outcome>>,
    /// The wickets the apps are placed on (`crate::placement`): the configuration's.
    #[serde(skip)]
    pub wickets: crate::placement::Wickets,
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
    pub stack: String,
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
    /// The wicket of each index of the web process, by name (`crate::placement`): where it
    /// runs while the app is started.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub placement: Vec<String>,
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
    /// The digest of a static site's files (`Files`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub files: Option<String>,
    pub memory_mib: u64,
    pub budget_ms: Option<u64>,
    /// As annotated since: an edit of them in the mode of this start is taken at once
    /// (`crate::manifest::live_lifetime`).
    #[serde(default)]
    pub lifetime: Lifetime,
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
            created_at: at.clone(),
            updated_at: at,
        }
    }

    pub fn under_way(&self) -> bool {
        self.state == OperationState::InProgress
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
    /// An image's rootfs, which each Firecracker wicket makes itself, of its own guest: the
    /// digest a wicket made where it is not `sha256` (the one staging made), by wicket name. A
    /// wicket not named is asked for `sha256`: a state written before names none, its
    /// `sha256` the one wicket's.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub made: BTreeMap<String, String>,
    /// A static site's files, the component serving them its `sha256`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub files: Option<Files>,
    pub created_at: String,
    pub updated_at: String,
}

impl Droplet {
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
#[derive(Clone, Debug, PartialEq)]
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

    /// Adds the app `name` to the space `space`, stopped, with its web process.
    pub fn create_app(&mut self, name: &str, stack: &str, space: &str) -> &mut App {
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
            stack: stack.into(),
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
                created_at: at.clone(),
                updated_at: at,
            },
            current_droplet: None,
            env: BTreeMap::new(),
            running: None,
            placement: Vec::new(),
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
        self.unnamed(gone)
    }

    /// The blobs of the droplets `gone` no droplet kept names: their components, their
    /// files.
    fn unnamed(&self, gone: Vec<Droplet>) -> Vec<Blob> {
        let mut blobs = Vec::new();
        for droplet in gone {
            let named = |kept: &Droplet| kept.sha256 == droplet.sha256;
            let component = Blob::Droplet(droplet.sha256.clone());
            if !self.droplets.iter().any(named) && !blobs.contains(&component) {
                blobs.push(component);
            }
            let Some(files) = droplet.files else {
                continue;
            };
            let files_named = |kept: &Droplet| {
                kept.files
                    .as_ref()
                    .is_some_and(|kept| kept.sha256 == files.sha256)
            };
            let named = self.droplets.iter().any(files_named);
            let files = Blob::Files(files.sha256);
            if !named && !blobs.contains(&files) {
                blobs.push(files);
            }
        }
        blobs
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

    /// Settles what the jobs wait for that no operation under way will: an app's delete
    /// after its unbinds, which a stop of the cf-api cut short, fails.
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
        for job in &mut self.jobs {
            let before = job.waits.len();
            job.waits.retain(|wait| under_way.contains(wait));
            if job.waits.len() < before && job.errors.is_empty() {
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

/// An upload received to a file, removed with this.
pub struct Spool(PathBuf);

impl Spool {
    pub fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for Spool {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// A blob to delete from the data directory.
#[derive(Debug, PartialEq)]
pub enum Blob {
    Package(String),
    Droplet(String),
    Files(String),
}

/// The state under a lock, written to the data directory on every change.
pub struct Store {
    dir: PathBuf,
    state: Mutex<State>,
    /// Signalled on every change written (`changed`).
    changes: tokio::sync::Notify,
}

impl Store {
    /// Opens the data directory: the state kept there, else a new one, its apps placed on the
    /// `wickets` configured (`State::follow_wickets`, logged).
    pub fn open(
        dir: &Path,
        org: &str,
        space: &str,
        domains: &[String],
        wickets: &[String],
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
        let mut state = match std::fs::read(&file) {
            Ok(json) => {
                let mut state: State = serde_json::from_slice(&json)
                    .map_err(|e| format!("{}: {e}", file.display()))?;
                state.migrate();
                state.follow_domains(domains);
                state.fail_unfinished_builds();
                state.settle_unfinished_waits();
                state
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => State::new(org, space, domains),
            Err(e) => return Err(format!("{}: {e}", file.display())),
        };
        for line in state.follow_wickets(wickets) {
            println!("{line}");
        }
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

    /// Changes the state, its apps placed as they are by then (`State::place`), and writes it.
    pub fn update<R>(&self, f: impl FnOnce(&mut State) -> R) -> R {
        let mut state = self.state.lock().expect("no panic under the lock");
        let result = f(&mut state);
        state.place();
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

    /// Changes what is not kept (the wickets' outcomes, those of another runtime).
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

    /// A static site's files, a tar.
    pub fn files_path(&self, sha256: &str) -> PathBuf {
        self.dir.join("droplets").join(format!("{sha256}.tar"))
    }

    /// Makes the package `guid` of what `write` writes, by a rename.
    pub fn make_package<T>(
        &self,
        guid: &str,
        write: impl FnOnce(&mut std::fs::File) -> Result<T, String>,
    ) -> Result<T, String> {
        make_by_rename(&self.package_path(guid), write)
    }

    pub fn write_droplet(&self, sha256: &str, bytes: &[u8]) -> Result<(), String> {
        let path = self.droplet_path(sha256);
        if path.exists() {
            return Ok(());
        }
        write_by_rename(&path, bytes)
    }

    /// Makes the droplet of what `write` writes, named by the digest it answers with what
    /// it made of it (a droplet of that digest kept stays as it is).
    pub fn make_droplet<T>(
        &self,
        write: impl FnOnce(&mut std::fs::File) -> Result<(T, String), String>,
    ) -> Result<T, String> {
        self.make_digested(|sha256| self.droplet_path(sha256), write)
    }

    /// Makes a static site's files of what `write` writes, as [`Store::make_droplet`] a
    /// droplet.
    pub fn make_files<T>(
        &self,
        write: impl FnOnce(&mut std::fs::File) -> Result<(T, String), String>,
    ) -> Result<T, String> {
        self.make_digested(|sha256| self.files_path(sha256), write)
    }

    /// Makes the blob at `path_of` the digest `write` answers with of what it wrote.
    fn make_digested<T>(
        &self,
        path_of: impl Fn(&str) -> PathBuf,
        write: impl FnOnce(&mut std::fs::File) -> Result<(T, String), String>,
    ) -> Result<T, String> {
        let partial = self
            .dir
            .join("droplets")
            .join(format!("{}.droplet", guid()));
        let (made, sha256) = make_by_rename(&partial, write)?;
        let path = path_of(&sha256);
        let failed = |e: std::io::Error| format!("{}: {e}", path.display());
        if path.exists() {
            std::fs::remove_file(&partial).map_err(failed)?;
        } else {
            std::fs::rename(&partial, &path)
                .and_then(|()| std::fs::File::open(self.dir.join("droplets"))?.sync_all())
                .map_err(failed)?;
        }
        Ok(made)
    }

    /// Where an upload is received, for its owner only, gone with what is returned.
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

    pub fn delete(&self, blobs: &[Blob]) {
        for blob in blobs {
            let path = match blob {
                Blob::Package(guid) => self.package_path(guid),
                Blob::Droplet(sha256) => self.droplet_path(sha256),
                Blob::Files(sha256) => self.files_path(sha256),
            };
            if let Err(e) = std::fs::remove_file(&path)
                && e.kind() != std::io::ErrorKind::NotFound
            {
                eprintln!("cannot delete {}: {e}", path.display());
            }
        }
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
mod tests {
    use super::*;

    #[test]
    fn the_current_droplet_keeps_the_previous_the_packages_the_newest_and_no_build_outlives_a_restart()
     {
        let mut state = State::new("o", "s", &["localhost".into()]);
        let space = state.spaces[0].guid.clone();
        let app = state.create_app("hello", STACK, &space).guid.clone();
        let droplet = |guid: &str, sha256: &str| Droplet {
            guid: guid.into(),
            app: app.clone(),
            package: "p".into(),
            sha256: sha256.into(),
            file: "hello.wasm".into(),
            bytes: 1,
            image: None,
            made: BTreeMap::new(),
            files: None,
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
                credentials: Default::default(),
                operation: None,
                created_at: now(),
                updated_at: now(),
            });
        }
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
        let hello = written.create_app("hello", STACK, &space.guid).guid.clone();
        written.app_mut(&hello).unwrap().process.instances = 2;
        let mut legacy = serde_json::to_value(&written).unwrap();
        legacy["org"] = json!(org);
        legacy["space"] =
            json!({"guid": space.guid, "name": space.name, "created_at": space.created_at});
        legacy["orgs"] = json!([]);
        legacy["spaces"] = json!([]);
        legacy["apps"][0]["space"] = json!("");
        legacy["removals"] = json!(["gone"]);
        let mut state: State = serde_json::from_value(legacy).unwrap();
        state.migrate();
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
        // written again, none of the one organization or space is kept apart
        let again = serde_json::to_value(&state).unwrap();
        assert!(again.get("org").is_none() && again.get("space").is_none());
        let mut again: State = serde_json::from_value(again).unwrap();
        assert_eq!(
            (&again.apps[0].placement, &again.removals),
            (&names(&["a", "a"]), &vec![gone("a")])
        );
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
