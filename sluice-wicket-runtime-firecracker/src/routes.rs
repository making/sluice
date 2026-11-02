//! The routes the tunnel advertises, and how a connection to one is served: a
//! lease on a guest stream ([`Route::open`]), relayed to the tunnel
//! connection in the vsock framing ([`wire::relay`]).
//!
//! The routes are reloaded without a restart ([`Reloader`]): on a change of
//! their files (the `--config` file, a rootfs, the kernel, the firecracker
//! binary) or on SIGHUP. A reload reads the routes again and starts the VMs
//! of every target that is new or whose spec or files changed -- a shared VM
//! booted, a cloned route's template booted and snapshotted again --
//! before it changes anything: one failure keeps the current table. A target
//! kept as it was keeps its VMs. A changed target serves its next connection
//! from the new VMs; a connection open stays on the VMs it got, which end
//! with the last of them. A removed target is served until every node stream
//! acknowledged its removal: the server may have routed a connection to it
//! before. A target kept serves under the hosts of the reload: its VMs' output
//! is named after them.
//!
//! A disk has one writer (`disk`), so a route's VM on a disk is the exception to the
//! order: once every other target started, the VM holding the disk is stopped, then
//! the new one started. A start failing then starts the old one again, on the disk
//! it left, and the reload fails as any other; failing as well, the route serves
//! nothing until a restart succeeds, which the failure says. Its connections fail
//! meanwhile, a boot long.
//!
//! A shared VM that goes down (`route::Down`) is replaced, in the order of a disk's: stopped
//! at once, on the room it held, and another one started from the same spec, after a wait
//! growing as they keep going down ([`Restart`]); a route stopped with none to follow (a start
//! that failed) too. Its start is not the reloads': one route failing holds no other's edit,
//! and no reload fails for it. A change of the target takes over from it.
//!
//! The console's edits ([`Edit`]) are reloads too, of the routes as they would
//! be with them: kept -- written to the console's directory -- only when they
//! started. An app's are one reload: every host of it or none. An edit's VMs start in a task
//! ([`Ahead`]), the edit made once they did, on the routes as they are by then: no other edit
//! waits for a boot, which a process never listening takes a `boot-ms` long. A later edit of the
//! same app or host takes over from it, its VMs ended. A start in the order of a disk's is
//! within its reload, as is a reload of the files or on SIGHUP: those hold the edits for it.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use sluice_wicket_console::http::Received;
use sluice_wicket_console::status::Status;
use sluice_wicket_tunnel::proto::Upstream;
use sluice_wicket_tunnel::routes::{Binding, Routes};
use sluice_wicket_tunnel::tunnel::TunnelIo;
use sluice_wicket_tunnel::workers::Workers;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;
use tokio_util::task::AbortOnDropHandle;

use crate::BoxError;
use crate::capacity::{Capacity, Full, Slot};
use crate::config::{Origin, RouteSource};
use crate::deployed::{self, AppSpec, Deployed, Options};
use crate::output::Sink;
use crate::route::{Down, Lease, Route, RouteSpec, Starting};
use crate::vm::Places;
use crate::wire;

/// The wait before a shared VM's next start, past the first: doubled up to [`RESTART_WAIT_MAX`].
const RESTART_WAIT: Duration = Duration::from_secs(1);

const RESTART_WAIT_MAX: Duration = Duration::from_secs(60);

/// A shared VM up that long going down is started again at once, as the first one was.
const RESTART_STABLE: Duration = Duration::from_secs(60);

/// The routes at one point, as every node stream advertises them.
pub struct Table {
    /// Counts the reloads that changed the routes; the startup table is 1.
    generation: u64,
    advertised: Vec<Upstream>,
    /// Target (the dial address) -> its route: the advertised targets, plus
    /// removed ones until every stream acknowledged their removal.
    routes: HashMap<String, Arc<Route>>,
    /// Host -> where its route comes from.
    origins: HashMap<String, Origin>,
    /// The wicket has a console, editing its routes.
    console: bool,
}

impl Table {
    /// The advertised routes in their order, each with where it comes from
    /// and its target's route.
    pub fn served(&self) -> impl Iterator<Item = (&Upstream, &Origin, &Arc<Route>)> {
        self.advertised.iter().filter_map(|up| {
            Some((
                up,
                self.origins.get(&up.host)?,
                self.routes.get(&up.target_url)?,
            ))
        })
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// The advertised hosts, for the log.
    pub fn advertised_hosts(&self) -> String {
        match &self.advertised[..] {
            [] => "none".to_string(),
            all => all
                .iter()
                .map(|up| up.host.as_str())
                .collect::<Vec<_>>()
                .join(", "),
        }
    }

    /// The route taking any host on `--listen`
    /// (`sluice_wicket_tunnel::routes::Table::sole`), if one does.
    pub fn sole(&self) -> Option<Arc<Route>> {
        let sole = sluice_wicket_tunnel::routes::Table::sole(self)?;
        self.routes.get(&sole.target_url).cloned()
    }

    /// The route of `host` (no port) on `--listen`
    /// (`sluice_wicket_tunnel::routes::Table::host_route`).
    pub fn by_host(&self, host: &str) -> Option<Arc<Route>> {
        let target = &sluice_wicket_tunnel::routes::Table::host_route(self, host)?.target_url;
        self.routes.get(target).cloned()
    }

    /// Stops the VMs holding a disk, each given its time: the wicket is ending, and what
    /// they wrote goes to their disks first.
    pub async fn stop_disks(&self) {
        let mut stopping = JoinSet::new();
        for route in self.routes.values().filter(|route| route.disk().is_some()) {
            let route = route.clone();
            stopping.spawn(async move { route.stop().await });
        }
        while stopping.join_next().await.is_some() {}
    }
}

impl sluice_wicket_tunnel::routes::Table for Table {
    type Binding = Connection;

    fn generation(&self) -> u64 {
        self.generation
    }

    fn advertised(&self) -> &[Upstream] {
        &self.advertised
    }

    /// A `--vm` route, on a wicket without a console: not a config file's, whose
    /// routes come and go as well.
    fn takes_any_host(&self, route: &Upstream) -> bool {
        !self.console && self.origins.get(&route.host) == Some(&Origin::Flag)
    }

    fn bind(routes: &Arc<Routes<Table>>, address: &str) -> Result<Connection, String> {
        routes
            .current()
            .routes
            .get(address)
            .map(|route| Connection {
                route: route.clone(),
            })
            .ok_or_else(|| format!("no firecracker route for '{address}'"))
    }
}

/// A tunnel connection to a route.
pub struct Connection {
    route: Arc<Route>,
}

impl Binding for Connection {
    /// Relays the connection to a guest stream of the route until both
    /// directions closed, their CLOSEs passed on. A guest stream that could
    /// not be had (the client's share of the route taken included), ended
    /// without its CLOSE, or relaying no byte for the route's `idle-ms`, fails
    /// the connection with an ERROR: the server drops its client's connection
    /// instead of ending it as if the answer were complete.
    async fn serve(self, mut io: TunnelIo) {
        let conn_id = io.conn_id();
        let ending = io.ending();
        let lease = self.route.open(io.client_address()).await;
        let Lease { stream, hold } = match lease {
            Ok(lease) => lease,
            Err(e) => {
                eprintln!("conn {conn_id} ({}): {e}", self.route.label());
                ending.fail(&format!("guest unreachable: {e}"));
                return;
            }
        };
        let failed = io.failed();
        let relayed = tokio::select! {
            relayed = wire::relay(&mut io, stream, self.route.spec.idle) => relayed,
            // the server's ERROR or the stream's end: nothing is sent any more
            () = failed => Ok(()),
        };
        let ended = hold.end(relayed.is_ok());
        match relayed {
            Ok(()) => ending.finish(),
            Err(e) => {
                eprintln!("conn {conn_id} ({}): {e}", self.route.label());
                self.route.suspect();
                ending.fail(&e.to_string());
            }
        }
        ended.await;
    }
}

/// A file's identity as a poll sees it; None for a missing file.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct Stamp {
    modified: Option<SystemTime>,
    len: u64,
    /// Changes when the file is replaced by a rename.
    inode: u64,
}

fn stamp(path: &Path) -> Option<Stamp> {
    let metadata = std::fs::metadata(path).ok()?;
    Some(Stamp {
        modified: metadata.modified().ok(),
        len: metadata.len(),
        inode: std::os::unix::fs::MetadataExt::ino(&metadata),
    })
}

type Stamps = HashMap<PathBuf, Option<Stamp>>;

/// The files a route's VMs are started from: a change starts new ones.
fn files(spec: &RouteSpec) -> Vec<&Path> {
    vec![&spec.vm.rootfs, &spec.vm.kernel, &spec.vm.firecracker]
}

fn stamps<'a>(paths: impl IntoIterator<Item = &'a Path>) -> Stamps {
    paths
        .into_iter()
        .map(|path| (path.to_path_buf(), stamp(path)))
        .collect()
}

/// A target's route, and the spec and the stamps of its files it was started
/// from.
struct Started {
    spec: RouteSpec,
    stamps: Stamps,
    route: Arc<Route>,
}

/// A target's shared VM going down, and being started again.
#[derive(Default)]
struct Restart {
    /// The VMs started so, since its spec was new.
    count: u64,
    /// The wait before its next start: none, then [`RESTART_WAIT`] doubled.
    wait: Duration,
    /// Since when its VM is up, once one was started so.
    up: Option<SystemTime>,
    /// The room of the VM that went down: none after a reload stopped it.
    held: Option<Arc<Slot>>,
    /// The stop of the VM that went down, which its next start waits for.
    stopping: Option<JoinHandle<()>>,
    /// Its start under way, ended with it: [`Restarted::attempt`] tells the one it is.
    starting: Option<AbortOnDropHandle<()>>,
    attempt: u64,
}

/// A start of a [`Restart`] ended.
struct Restarted {
    target: String,
    attempt: u64,
    started: Result<Started, BoxError>,
}

/// What a reload changed, by host.
#[derive(Default, Debug)]
pub struct Change {
    added: Vec<String>,
    removed: Vec<String>,
    /// Hosts whose VMs were started again: their spec or files changed.
    restarted: Vec<String>,
}

impl Change {
    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.removed.is_empty() && self.restarted.is_empty()
    }
}

impl std::fmt::Display for Change {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut parts = Vec::new();
        for (what, hosts) in [
            ("added", &self.added),
            ("removed", &self.removed),
            ("restarted", &self.restarted),
        ] {
            if !hosts.is_empty() {
                parts.push(format!("{what} {}", hosts.join(", ")));
            }
        }
        f.write_str(&parts.join("; "))
    }
}

/// A change of the console's routes, and where its outcome goes.
pub struct Request {
    pub edit: Edit,
    pub reply: oneshot::Sender<Result<Outcome, Refused>>,
}

/// A change of the console's routes (one host each).
pub enum Edit {
    /// Serves `host` by the uploaded image `received` (named `name`) with
    /// `options`.
    Deploy {
        host: String,
        name: String,
        received: Received,
        options: Options,
    },
    /// A deploy, its image kept: what can be made again on the routes as they are by then.
    Kept { route: deployed::Route },
    /// Gives the console's route of `host` other options.
    Options { host: String, options: Options },
    /// Moves the console's route of `host` to `to` as it is.
    Move { host: String, to: String },
    /// Removes the console's route of `host`.
    Remove { host: String },
    /// Keeps the image `received` under its digest `sha256` for an app to name.
    Upload { sha256: String, received: Received },
    /// Serves the app `name` as `app` has it.
    PutApp { name: String, app: AppSpec },
    /// Removes the app `name` of `owner`.
    DeleteApp { name: String, owner: String },
    /// Removes the disk of the app `name` of `owner` that was.
    DeleteDisk { name: String, owner: String },
}

impl Edit {
    /// How the log names it; not by the name uploaded, which may hold
    /// anything.
    fn describe(&self) -> String {
        match self {
            Edit::Deploy { host, .. } => format!("console: deploy to {host}"),
            Edit::Kept { route } => format!("console: deploy to {}", route.host),
            Edit::Options { host, .. } => format!("console: options of {host}"),
            Edit::Move { host, to } => format!("console: move {host} to {to}"),
            Edit::Remove { host } => format!("console: remove {host}"),
            Edit::Upload { sha256, .. } => format!("api: upload {sha256}"),
            Edit::PutApp { name, app } => format!("api: app {name} of {}", app.owner),
            Edit::DeleteApp { name, owner } => format!("api: remove app {name} of {owner}"),
            Edit::DeleteDisk { name, owner } => format!("api: remove disk {name} of {owner}"),
        }
    }

    /// The console's route it changes.
    fn host(&self) -> Option<&str> {
        match self {
            Edit::Deploy { host, .. }
            | Edit::Options { host, .. }
            | Edit::Move { host, .. }
            | Edit::Remove { host } => Some(host),
            Edit::Kept { route } => Some(&route.host),
            Edit::Upload { .. }
            | Edit::PutApp { .. }
            | Edit::DeleteApp { .. }
            | Edit::DeleteDisk { .. } => None,
        }
    }

    /// Whether it takes over from `waiting`, an edit waiting for its VMs: another of the same
    /// route, or of the same app by its owner.
    fn takes_over(&self, waiting: &Edit) -> bool {
        if self.host().is_some() {
            return self.host() == waiting.host();
        }
        let Edit::PutApp { name, app: put } = waiting else {
            return false;
        };
        match self {
            Edit::PutApp { name: of, app } => of == name && app.owner == put.owner,
            Edit::DeleteApp { name: of, owner } => of == name && *owner == put.owner,
            _ => false,
        }
    }
}

/// An edit the routes took.
#[derive(Debug)]
pub struct Outcome {
    /// The generation serving it.
    pub generation: u64,
    /// None when it changed nothing.
    pub change: Option<Change>,
}

/// Why an edit was refused; the routes stay as they were.
#[derive(Debug, PartialEq)]
pub enum Refused {
    /// The edit is malformed.
    Invalid(String),
    /// The console routes no such host.
    Unknown(String),
    /// Another source routes the host.
    Taken(String),
    /// An app names an image of a digest not kept: upload it first.
    Missing(String),
    /// The routes did not start with it: the reload's error.
    Rejected(String),
    /// Its VMs did not start, of what they are: another try, nothing else changed, fails alike.
    DidNotStart(String),
    /// The console's directory failed.
    Failed(String),
}

impl std::fmt::Display for Refused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Refused::Invalid(message)
            | Refused::Unknown(message)
            | Refused::Taken(message)
            | Refused::Rejected(message)
            | Refused::DidNotStart(message)
            | Refused::Failed(message) => f.write_str(message),
            Refused::Missing(sha256) => {
                write!(f, "no image of the digest {sha256} kept: upload it first")
            }
        }
    }
}

/// A route's VMs did not start, room for them aside ([`Full`]).
#[derive(Debug)]
struct DidNotStart(String);

impl std::fmt::Display for DidNotStart {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for DidNotStart {}

/// What a reload failing refuses an edit with.
fn refused(e: &BoxError) -> Refused {
    match e.is::<DidNotStart>() {
        true => Refused::DidNotStart(e.to_string()),
        false => Refused::Rejected(e.to_string()),
    }
}

/// A target to start: its spec, its hosts and the stamps of its files.
struct Start {
    target: String,
    spec: RouteSpec,
    hosts: Vec<String>,
    stamps: Stamps,
}

/// How a target gets the VMs its spec asks.
enum Way {
    /// It has them.
    Kept,
    Start,
    /// Started once the route holding its disk stopped.
    Swap(Arc<Route>),
}

/// An edit waiting for its VMs.
struct Ahead {
    edit: Edit,
    reason: String,
    reply: oneshot::Sender<Result<Outcome, Refused>>,
    /// The images they start from, relative to the console's directory: no collection takes them.
    images: Vec<String>,
    /// Their starts, ended with it.
    _starting: AbortOnDropHandle<()>,
}

/// The starts of an [`Ahead`] ended.
struct StartedAhead {
    id: u64,
    started: Result<HashMap<String, Started>, BoxError>,
}

/// Starts the routes and keeps them current: on a change of their files, on
/// request (SIGHUP), or on an edit of the console's. Reloads run one at a
/// time, an edit's VMs starting before its own ([`Ahead`]).
pub struct Reloader {
    routes: Arc<Routes<Table>>,
    /// What the console shows of the reloads.
    status: watch::Sender<Status>,
    source: RouteSource,
    capacity: Arc<Capacity>,
    /// Where the VMs' output goes.
    sink: Arc<Sink>,
    /// Where the egress's connections are relayed.
    workers: Arc<Workers>,
    /// Where the routes' VMs start, the `n`th route started its place.
    places: Arc<Places>,
    next_place: u64,
    /// Target -> its route as started; the advertised targets only.
    started: HashMap<String, Started>,
    /// Removed targets still in the table -> the generation that removed them.
    lingering: HashMap<String, u64>,
    /// The config file as the routes were last read from it.
    read: Stamps,
    /// Targets whose shared VM went down -> how that goes.
    restarts: HashMap<String, Restart>,
    attempts: u64,
    /// Edits waiting for their VMs, by a number of their own.
    ahead: HashMap<u64, Ahead>,
    next_ahead: u64,
    /// The VMs the edit being made started, by target: the reload's to take.
    ready: HashMap<String, Started>,
    started_ahead: mpsc::UnboundedReceiver<StartedAhead>,
    tell_started_ahead: mpsc::UnboundedSender<StartedAhead>,
    downs: mpsc::UnboundedReceiver<Down>,
    tell_down: mpsc::UnboundedSender<Down>,
    restarted: mpsc::UnboundedReceiver<Restarted>,
    tell_restarted: mpsc::UnboundedSender<Restarted>,
}

impl Reloader {
    /// Starts the VMs of every route of `source` within `capacity`, each target
    /// in a place of its own of `places`, their output to `sink`; the first
    /// failure ends them all.
    pub async fn start(
        source: RouteSource,
        capacity: Arc<Capacity>,
        places: Arc<Places>,
        sink: Arc<Sink>,
        workers: Arc<Workers>,
    ) -> Result<Reloader, BoxError> {
        let (tell_down, downs) = mpsc::unbounded_channel();
        let (tell_restarted, restarted) = mpsc::unbounded_channel();
        let (tell_started_ahead, started_ahead) = mpsc::unbounded_channel();
        let mut reloader = Reloader {
            routes: Routes::new(Table {
                generation: 0,
                advertised: Vec::new(),
                routes: HashMap::new(),
                origins: HashMap::new(),
                console: source.deployed().is_some(),
            }),
            status: watch::Sender::new(Status::default()),
            source,
            capacity,
            sink,
            workers,
            places,
            next_place: 0,
            started: HashMap::new(),
            lingering: HashMap::new(),
            read: HashMap::new(),
            restarts: HashMap::new(),
            attempts: 0,
            ahead: HashMap::new(),
            next_ahead: 0,
            ready: HashMap::new(),
            started_ahead,
            tell_started_ahead,
            downs,
            tell_down,
            restarted,
            tell_restarted,
        };
        // read before it: a change while it starts is a change for the next poll
        reloader.read = stamps(reloader.source.config());
        let specs = reloader.source.read()?;
        reloader.apply(specs).await?;
        Ok(reloader)
    }

    pub fn routes(&self) -> &Arc<Routes<Table>> {
        &self.routes
    }

    /// Follows what the console shows of the reloads.
    pub fn status(&self) -> watch::Receiver<Status> {
        self.status.subscribe()
    }

    /// Reloads on SIGHUP (`hangups`) and on a change of the routes' files,
    /// polled every `interval` (zero: never), applies each of the console's
    /// `requests`, drops a removed target once every stream acknowledged
    /// its removal and starts again a shared VM that went down; until `stop`.
    pub async fn run(
        mut self,
        interval: Duration,
        mut hangups: mpsc::Receiver<()>,
        mut requests: mpsc::Receiver<Request>,
        stop: CancellationToken,
    ) {
        let mut seen = self.watched();
        let mut requests_open = true;
        loop {
            let oldest = self.lingering.values().min().copied();
            let routes = self.routes.clone();
            let acknowledged = async move {
                match oldest {
                    Some(generation) => routes.acknowledged(generation).await,
                    None => std::future::pending().await,
                }
            };
            let poll = async {
                if interval.is_zero() {
                    std::future::pending::<()>().await;
                }
                tokio::time::sleep(interval).await;
            };
            let reason = tokio::select! {
                () = stop.cancelled() => return,
                () = acknowledged => {
                    self.expire(oldest.expect("waited on it"));
                    continue;
                }
                request = requests.recv(), if requests_open => {
                    match request {
                        Some(request) => {
                            self.take(request).await;
                            seen = self.watched();
                        }
                        None => requests_open = false,
                    }
                    continue;
                }
                Some(started) = self.started_ahead.recv() => {
                    self.ahead_ended(started).await;
                    seen = self.watched();
                    continue;
                }
                Some(down) = self.downs.recv() => {
                    self.went_down(down);
                    continue;
                }
                Some(restarted) = self.restarted.recv() => {
                    self.restart_ended(restarted);
                    continue;
                }
                Some(()) = hangups.recv() => "SIGHUP",
                () = poll => {
                    // a change is reloaded once a poll found it unchanged since the
                    // previous one: a file still being written is not read half way
                    let now = self.watched();
                    let stable = now == seen;
                    seen = now;
                    if !stable || seen == self.as_started() {
                        continue;
                    }
                    "file change"
                }
            };
            match self.reload().await {
                Ok(change) => {
                    self.logged(reason, &change);
                    Status::recovered(&self.status);
                }
                Err(e) => {
                    self.failed(reason, &e.to_string());
                    Status::failed(&self.status, reason, e.to_string());
                }
            }
            seen = self.watched();
        }
    }

    fn logged(&self, reason: &str, change: &Change) {
        match change.is_empty() {
            true => println!("routes reload ({reason}): no change"),
            false => println!(
                "routes reloaded ({reason}), generation {}: {change}",
                self.routes.current().generation
            ),
        }
    }

    fn failed(&self, reason: &str, error: &str) {
        eprintln!(
            "routes reload ({reason}) failed; keeping generation {}: {error}",
            self.routes.current().generation
        );
    }

    /// Takes an edit of the console's: its VMs to start, they start in a task and the edit is
    /// made when they did ([`Reloader::ahead_ended`]); else it is made here. It takes over from
    /// an edit of the same route or app waiting for its VMs, which is refused.
    async fn take(&mut self, request: Request) {
        let Request { edit, reply } = request;
        let over: Vec<u64> = self
            .ahead
            .iter()
            .filter(|(_, ahead)| edit.takes_over(&ahead.edit))
            .map(|(id, _)| *id)
            .collect();
        for id in over {
            let ahead = self.ahead.remove(&id).expect("just found");
            let why = format!("{} took over from it", edit.describe());
            self.failed(&ahead.reason, &why);
            let _ = ahead.reply.send(Err(Refused::Rejected(why)));
        }
        if let Edit::DeleteDisk { name, owner } = &edit
            && self.ahead.values().any(|ahead| {
                matches!(&ahead.edit, Edit::PutApp { name: of, app } if of == name && app.owner == *owner)
            })
        {
            let held = format!("the app '{name}' is starting on that disk");
            let _ = reply.send(Err(Refused::Taken(held)));
            return;
        }
        match self.prepared(edit) {
            Err(refused) => {
                let _ = reply.send(Err(refused));
            }
            Ok((edit, starts)) if starts.is_empty() => {
                let _ = reply.send(self.edit(edit).await);
            }
            Ok((edit, starts)) => self.start_ahead(edit, reply, starts),
        }
    }

    /// The edit as it goes on, a deploy's image kept, and the VMs to start ahead of it: none
    /// when it starts none, the routes would refuse it before they start any, or a start is
    /// to wait for another VM to stop.
    fn prepared(&mut self, edit: Edit) -> Result<(Edit, Vec<Start>), Refused> {
        taken(&edit, &self.routes.current().origins)?;
        let deployed = self
            .source
            .deployed()
            .expect("edits come from the console, which deploys");
        let now = SystemTime::now();
        let edit = match edit {
            Edit::Upload { .. } | Edit::DeleteDisk { .. } => return Ok((edit, Vec::new())),
            Edit::Deploy {
                host,
                name,
                received,
                options,
            } => Edit::Kept {
                route: deployed.deploy(&host, &name, received, options, now)?,
            },
            edit => edit,
        };
        let edited = edited(deployed, &edit, now)?;
        if &edited == deployed {
            return Ok((edit, Vec::new()));
        }
        let previous = self.source.deploy(edited).expect("the console's routes");
        let specs = self.source.read();
        self.source.deploy(previous);
        let Ok(specs) = specs else {
            return Ok((edit, Vec::new()));
        };
        let specs: Vec<RouteSpec> = specs.into_iter().map(|(spec, _)| spec).collect();
        if fits(&specs, &self.capacity).is_err() {
            return Ok((edit, Vec::new()));
        }
        let mut starts = Vec::new();
        for (target, spec, hosts) in targets(specs) {
            let stamps = stamps(files(&spec));
            match self.way(&target, &spec, &stamps) {
                Way::Kept => {}
                Way::Start => starts.push(Start {
                    target,
                    spec,
                    hosts,
                    stamps,
                }),
                Way::Swap(_) => return Ok((edit, Vec::new())),
            }
        }
        Ok((edit, starts))
    }

    /// Starts `starts` in a task: `edit` waits for them.
    fn start_ahead(
        &mut self,
        edit: Edit,
        reply: oneshot::Sender<Result<Outcome, Refused>>,
        starts: Vec<Start>,
    ) {
        let reason = edit.describe();
        let dir = self.source.deployed().expect("an edit").dir().to_path_buf();
        let images = starts
            .iter()
            .filter_map(|start| start.spec.vm.rootfs.strip_prefix(&dir).ok())
            .map(|image| image.to_string_lossy().into_owned())
            .collect();
        let starting: Vec<_> = starts
            .into_iter()
            .map(|start| {
                let Start {
                    target,
                    spec,
                    hosts,
                    stamps,
                } = start;
                (target, self.starter(spec, hosts, stamps, None))
            })
            .collect();
        self.next_ahead += 1;
        let id = self.next_ahead;
        let tell = self.tell_started_ahead.clone();
        let task = tokio::spawn(async move {
            let started = all_started(starting).await;
            let _ = tell.send(StartedAhead { id, started });
        });
        self.ahead.insert(
            id,
            Ahead {
                edit,
                reason,
                reply,
                images,
                _starting: AbortOnDropHandle::new(task),
            },
        );
    }

    /// The VMs of an edit started: it is made, of the routes as they are by now. Of one that
    /// was taken over: nothing, the VMs ending. Not started: it is refused.
    async fn ahead_ended(&mut self, started: StartedAhead) {
        let StartedAhead { id, started } = started;
        let Some(ahead) = self.ahead.remove(&id) else {
            return;
        };
        let outcome = match started {
            Ok(started) => {
                self.ready = started;
                let outcome = self.edit(ahead.edit).await;
                self.ready.clear();
                outcome
            }
            Err(e) => {
                self.failed(&ahead.reason, &e.to_string());
                self.collect_garbage();
                Err(refused(&e))
            }
        };
        let _ = ahead.reply.send(outcome);
    }

    /// Removes the images nothing names but the edits waiting for their VMs.
    fn collect_garbage(&self) {
        let starting = self.ahead.values().flat_map(|ahead| &ahead.images);
        self.source
            .deployed()
            .expect("edits come from the console, which deploys")
            .collect_garbage(false, starting);
    }

    /// Applies an edit of the console's routes or apps (a deploy's image kept: [`Edit::Kept`]):
    /// the routes as they would be are reloaded, and kept -- written to the
    /// console's directory -- only when they started; refused, the routes and
    /// the directory stay as they were. An upload keeps its image and changes
    /// no route.
    async fn edit(&mut self, edit: Edit) -> Result<Outcome, Refused> {
        let reason = edit.describe();
        let current = self.routes.current();
        taken(&edit, &current.origins)?;
        let deployed = self
            .source
            .deployed()
            .expect("edits come from the console, which deploys");
        let now = SystemTime::now();
        let edited = match edit {
            Edit::Upload { sha256, received } => {
                let uploaded = deployed.upload(&sha256, received);
                // the uploads kept long enough go
                self.collect_garbage();
                uploaded?;
                return Ok(Outcome {
                    generation: current.generation,
                    change: None,
                });
            }
            Edit::DeleteDisk { name, owner } => {
                deployed.remove_disk(&name, &owner)?;
                println!("{reason}: removed");
                return Ok(Outcome {
                    generation: current.generation,
                    change: None,
                });
            }
            edit => edited(deployed, &edit, now),
        }?;
        // the same again changes nothing: a VM down comes back by itself
        if &edited == deployed {
            return Ok(Outcome {
                generation: current.generation,
                change: None,
            });
        }
        let previous = self.source.deploy(edited).expect("the console's routes");
        match self.reload().await {
            Ok(change) => {
                self.logged(&reason, &change);
                Status::recovered(&self.status);
                let saved = self.source.deployed().expect("just deployed").save();
                self.collect_garbage();
                saved.map_err(|e| {
                    eprintln!("{reason}: {e}");
                    Refused::Failed(e)
                })?;
                Ok(Outcome {
                    generation: self.routes.current().generation,
                    change: (!change.is_empty()).then_some(change),
                })
            }
            Err(e) => {
                self.failed(&reason, &e.to_string());
                self.source.deploy(previous);
                self.collect_garbage();
                Err(refused(&e))
            }
        }
    }

    /// Reads the routes again and applies them.
    pub async fn reload(&mut self) -> Result<Change, BoxError> {
        // stamped before it is read: a change while it is read is one more
        self.read = stamps(self.source.config());
        let specs = self.source.read()?;
        let applied = self.apply(specs).await;
        self.tend_restarts();
        applied
    }

    /// The hosts advertised for `target`.
    fn hosts_of(&self, target: &str) -> Vec<String> {
        let current = self.routes.current();
        current
            .advertised
            .iter()
            .filter(|up| up.target_url == target)
            .map(|up| up.host.clone())
            .collect()
    }

    /// A shared VM serves no more: it is stopped, its room held, another one to start. Of a
    /// route replaced or removed since, or that a reload stopped: nothing.
    fn went_down(&mut self, down: Down) {
        let found = self
            .started
            .iter()
            .find(|(_, started)| started.route.id() == down.route)
            .filter(|(_, started)| !started.route.stopped());
        let Some((target, started)) = found else {
            return;
        };
        let (target, route) = (target.clone(), started.route.clone());
        eprintln!("route {}: {}; its VM stopped", route.label(), down.why);
        route.set_down(down.why.to_string());
        let restart = self.restarts.entry(target).or_default();
        let lived = restart
            .up
            .and_then(|up| up.elapsed().ok())
            .is_none_or(|up| up >= RESTART_STABLE);
        if lived {
            restart.wait = Duration::ZERO;
        }
        let memory = u64::from(route.spec.vm.mem_mib);
        restart.held = Some(Arc::new(self.capacity.hold(memory)));
        restart.stopping = Some(tokio::spawn(route.stopping()));
        self.status.send_modify(|status| status.revision += 1);
        self.tend_restarts();
    }

    /// Starts again, each in a task and after its wait, the targets stopped with no start under
    /// way; forgets the restarts of the targets gone.
    fn tend_restarts(&mut self) {
        let started = &self.started;
        self.restarts
            .retain(|target, _| started.contains_key(target));
        let restarts = &self.restarts;
        let due: Vec<(String, RouteSpec, Stamps)> = started
            .iter()
            .filter(|(target, started)| {
                started.route.stopped()
                    && restarts
                        .get(*target)
                        .is_none_or(|restart| restart.starting.is_none())
            })
            .map(|(target, started)| (target.clone(), started.spec.clone(), started.stamps.clone()))
            .collect();
        for (target, spec, stamps) in due {
            let hosts = self.hosts_of(&target);
            let restart = self.restarts.entry(target.clone()).or_default();
            let (wait, held, stopping) =
                (restart.wait, restart.held.clone(), restart.stopping.take());
            restart.wait = (wait * 2).clamp(RESTART_WAIT, RESTART_WAIT_MAX);
            self.attempts += 1;
            let attempt = self.attempts;
            restart.attempt = attempt;
            eprintln!(
                "route {}: its VM starts again in {}ms",
                hosts.join(", "),
                wait.as_millis()
            );
            let start = self.starter(spec, hosts, stamps, held);
            let tell = self.tell_restarted.clone();
            let starting = {
                let target = target.clone();
                tokio::spawn(async move {
                    if let Some(stopping) = stopping {
                        let _ = stopping.await;
                    }
                    tokio::time::sleep(wait).await;
                    let started = start.await;
                    let _ = tell.send(Restarted {
                        target,
                        attempt,
                        started,
                    });
                })
            };
            let restart = self.restarts.get_mut(&target).expect("just there");
            restart.starting = Some(AbortOnDropHandle::new(starting));
        }
    }

    /// A start of a shared VM ended: it serves the route, or the next one is due. Of one a change
    /// of the target took over from: nothing.
    fn restart_ended(&mut self, restarted: Restarted) {
        let Restarted {
            target,
            attempt,
            started,
        } = restarted;
        let hosts = self.hosts_of(&target);
        let restart = self
            .restarts
            .get_mut(&target)
            .filter(|restart| restart.attempt == attempt);
        let Some(restart) = restart else {
            return;
        };
        restart.starting = None;
        match started {
            Ok(started) => {
                restart.count += 1;
                restart.up = Some(SystemTime::now());
                restart.held = None;
                started.route.set_restarts(restart.count);
                started.route.rename(&hosts);
                self.serve(target, started);
            }
            Err(e) => {
                eprintln!("route {}: its VM's start failed: {e}", hosts.join(", "));
                if let Some(started) = self.started.get(&target) {
                    started.route.set_down(format!("its start failed: {e}"));
                }
            }
        }
        self.status.send_modify(|status| status.revision += 1);
        self.tend_restarts();
    }

    /// `started` serves `target` from now on, in the generation it is: nothing to advertise.
    fn serve(&mut self, target: String, started: Started) {
        let current = self.routes.current();
        let mut routes = current.routes.clone();
        routes.insert(target.clone(), started.route.clone());
        started.route.watch(self.tell_down.clone());
        self.started.insert(target, started);
        self.routes.publish(Table {
            generation: current.generation,
            advertised: current.advertised.clone(),
            routes,
            origins: current.origins.clone(),
            console: current.console,
        });
    }

    /// The files the routes come from, as they are now.
    fn watched(&self) -> Stamps {
        let mut paths: Vec<&Path> = self.source.config().into_iter().collect();
        for started in self.started.values() {
            paths.extend(files(&started.spec));
        }
        stamps(paths)
    }

    /// The files the routes come from, as the routes were started from them
    /// (the config file as it was last read).
    fn as_started(&self) -> Stamps {
        let mut as_started = self.read.clone();
        for started in self.started.values() {
            as_started.extend(started.stamps.clone());
        }
        as_started
    }

    /// How `target` gets the VMs `spec` asks, the files as `stamps` tell.
    fn way(&self, target: &str, spec: &RouteSpec, stamps: &Stamps) -> Way {
        // stopped too: it comes back by itself, no reload failing for it
        if let Some(started) = self.started.get(target)
            && started.spec.same_vms(spec)
            && started.stamps == *stamps
        {
            return Way::Kept;
        }
        let holder = spec.vm.disk.as_ref().and_then(|disk| {
            let current = self.routes.current();
            current
                .routes
                .values()
                .find(|route| route.disk() == Some(disk.file.as_path()))
                .cloned()
        });
        holder.map_or(Way::Start, Way::Swap)
    }

    /// Starts what `specs` need that is not running as they ask, nor started by the edit being
    /// made, and publishes them; nothing changes when a start fails, or when they do not fit
    /// the VMs' budget.
    async fn apply(&mut self, specs: Vec<(RouteSpec, Origin)>) -> Result<Change, BoxError> {
        let (specs, origins): (Vec<RouteSpec>, Vec<Origin>) = specs.into_iter().unzip();
        fits(&specs, &self.capacity)?;
        let origins: HashMap<String, Origin> = specs
            .iter()
            .map(|spec| spec.host.clone())
            .zip(origins)
            .collect();
        let advertised: Vec<Upstream> = specs
            .iter()
            .map(|spec| Upstream {
                host: spec.host.clone(),
                target_url: spec.target(),
                ..Default::default()
            })
            .collect();
        let current = self.routes.current();
        let mut change = Change::default();
        let mut starting = Vec::new();
        let mut kept = Vec::new();
        let mut ready = HashMap::new();
        // the targets a VM on their disk stands in the way of, and that VM
        let mut swaps = Vec::new();
        for (target, spec, hosts) in targets(specs) {
            let stamps = stamps(files(&spec));
            let way = self.way(&target, &spec, &stamps);
            if matches!(way, Way::Kept) {
                kept.push((target, hosts));
                continue;
            }
            if self.started.contains_key(&target) {
                change.restarted.extend(hosts.iter().cloned());
                // this start takes over: on no room held, and with no wait
                self.restarts.remove(&target);
            }
            if let Some(started) = self
                .ready
                .remove(&target)
                .filter(|started| started.spec.same_vms(&spec) && started.stamps == stamps)
            {
                started.route.rename(&hosts);
                ready.insert(target, started);
                continue;
            }
            match way {
                Way::Swap(holder) => swaps.push((holder, target, spec, hosts, stamps)),
                _ => starting.push((target, self.starter(spec, hosts, stamps, None))),
            }
        }
        let mut new = all_started(starting).await?;
        new.extend(ready);
        let mut stopped: Vec<Arc<Route>> = Vec::new();
        let mut swapped: Vec<String> = Vec::new();
        for (holder, target, spec, hosts, stamps) in swaps {
            holder.stop().await;
            stopped.push(holder);
            let start = self.starter(spec, hosts, stamps, None);
            match start.await {
                Ok(started) => {
                    new.insert(target.clone(), started);
                    swapped.push(target);
                }
                Err(e) => {
                    let back = self.back(new, &swapped, &stopped).await;
                    return Err(match back {
                        Ok(()) => e,
                        Err(again) => format!(
                            "{e}; starting the old VM again failed as well, the route serves \
                             nothing until a restart succeeds: {again}"
                        )
                        .into(),
                    });
                }
            }
        }

        for up in &advertised {
            match current.advertised.iter().find(|old| old.host == up.host) {
                None => change.added.push(up.host.clone()),
                // its image or options are another target's: VMs started anew all the same
                Some(old)
                    if old.target_url != up.target_url && !change.restarted.contains(&up.host) =>
                {
                    change.restarted.push(up.host.clone());
                }
                Some(_) => {}
            }
        }
        for up in &current.advertised {
            if !advertised.iter().any(|new| new.host == up.host) {
                change.removed.push(up.host.clone());
            }
        }
        if change.is_empty() && advertised == current.advertised {
            // where a route comes from changed alone (a console route's time): in the
            // generation it was, nothing to advertise
            if origins != current.origins {
                self.routes.publish(Table {
                    generation: current.generation,
                    advertised: current.advertised.clone(),
                    routes: current.routes.clone(),
                    origins,
                    console: current.console,
                });
                self.status.send_modify(|status| status.revision += 1);
            }
            return Ok(change);
        }
        let generation = current.generation + 1;
        let mut started: HashMap<String, Started> = kept
            .into_iter()
            .filter_map(|(target, hosts)| {
                let kept = self.started.remove_entry(&target)?;
                kept.1.route.rename(&hosts);
                Some(kept)
            })
            .collect();
        for new in new.values() {
            new.route.watch(self.tell_down.clone());
        }
        started.extend(new);
        let mut routes: HashMap<String, Arc<Route>> = started
            .iter()
            .map(|(target, started)| (target.clone(), started.route.clone()))
            .collect();
        // a removed target is served until every stream acknowledged it
        for (target, route) in &current.routes {
            if !routes.contains_key(target) {
                routes.insert(target.clone(), route.clone());
                self.lingering.entry(target.clone()).or_insert(generation);
            }
        }
        self.started = started;
        self.lingering
            .retain(|target, _| !self.started.contains_key(target));
        self.routes.publish(Table {
            generation,
            advertised,
            routes,
            origins,
            console: current.console,
        });
        self.status.send_modify(|status| status.revision += 1);
        Ok(change)
    }

    /// What starts the target of `spec`, the next place's, on the room `held` holds or else
    /// taking it.
    fn starter(
        &mut self,
        spec: RouteSpec,
        hosts: Vec<String>,
        stamps: Stamps,
        held: Option<Arc<Slot>>,
    ) -> impl Future<Output = Result<Started, BoxError>> + Send + 'static + use<> {
        let n = self.next_place;
        self.next_place += 1;
        let starting = Starting {
            places: self.places.clone(),
            n,
            capacity: self.capacity.clone(),
            held,
            sink: self.sink.clone(),
            workers: self.workers.clone(),
        };
        async move {
            let route = Route::start(spec.clone(), &hosts, starting)
                .await
                .map_err(|e| match e.is::<Full>() {
                    true => e,
                    false => DidNotStart(e.to_string()).into(),
                })?;
            Ok(Started {
                spec,
                stamps,
                route: Arc::new(route),
            })
        }
    }

    /// A reload failed after it stopped the VMs `stopped` for the targets `swapped` of the new
    /// ones started, `new`: they end, and the routes of `stopped` still advertised start
    /// again on their disks, the table following.
    async fn back(
        &mut self,
        mut new: HashMap<String, Started>,
        swapped: &[String],
        stopped: &[Arc<Route>],
    ) -> Result<(), BoxError> {
        for target in swapped {
            if let Some(started) = new.remove(target) {
                started.route.stop().await;
            }
        }
        drop(new);
        let mut failed = None;
        for holder in stopped {
            let Some((target, was)) = self
                .started
                .iter()
                .find(|(_, started)| Arc::ptr_eq(&started.route, holder))
            else {
                continue;
            };
            let target = target.clone();
            let (spec, stamps) = (was.spec.clone(), was.stamps.clone());
            let start = self.starter(spec, self.hosts_of(&target), stamps, None);
            match start.await {
                Ok(started) => self.serve(target, started),
                Err(e) => failed = Some(e),
            }
        }
        failed.map_or(Ok(()), Err)
    }

    /// Drops the removed targets every stream acknowledged by `generation`, the VM of one with
    /// a disk stopped: no connection of its is to outlive it, what it wrote on the disk.
    fn expire(&mut self, generation: u64) {
        let expired: Vec<String> = self
            .lingering
            .iter()
            .filter(|(_, removed)| **removed <= generation)
            .map(|(target, _)| target.clone())
            .collect();
        for target in &expired {
            self.lingering.remove(target);
        }
        let current = self.routes.current();
        for target in &expired {
            if let Some(route) = current.routes.get(target).cloned()
                && route.disk().is_some()
            {
                tokio::spawn(async move { route.stop().await });
            }
        }
        self.routes.publish(Table {
            generation: current.generation,
            advertised: current.advertised.clone(),
            routes: current
                .routes
                .iter()
                .filter(|(target, _)| !expired.contains(target))
                .map(|(target, route)| (target.clone(), route.clone()))
                .collect(),
            origins: current.origins.clone(),
            console: current.console,
        });
    }
}

/// What `edit` makes of `deployed` since `now`. An upload, the removal of a disk and a deploy
/// whose image is not kept yet are no change of it.
fn edited(deployed: &Deployed, edit: &Edit, now: SystemTime) -> Result<Deployed, Refused> {
    match edit {
        Edit::Kept { route } => Ok(deployed.with(route.clone())),
        Edit::Options { host, options } => deployed.with_options(host, options.clone(), now),
        Edit::Move { host, to } => deployed.moved(host, to),
        Edit::Remove { host } => deployed.without(host),
        Edit::PutApp { name, app } => deployed.with_app(name, app.clone(), now),
        Edit::DeleteApp { name, owner } => deployed.without_app(name, owner),
        Edit::Deploy { .. } | Edit::Upload { .. } | Edit::DeleteDisk { .. } => Ok(deployed.clone()),
    }
}

/// Target -> its spec and hosts, in the order of the specs.
fn targets(specs: Vec<RouteSpec>) -> Vec<(String, RouteSpec, Vec<String>)> {
    let mut targets: Vec<(String, RouteSpec, Vec<String>)> = Vec::new();
    for spec in specs {
        let target = spec.target();
        match targets.iter_mut().find(|(known, ..)| *known == target) {
            Some((_, _, hosts)) => hosts.push(spec.host),
            None => {
                let host = spec.host.clone();
                targets.push((target, spec, vec![host]));
            }
        }
    }
    targets
}

/// Runs the starts at once: the first failure ends the others, and with them their VMs and files.
async fn all_started<F>(starts: Vec<(String, F)>) -> Result<HashMap<String, Started>, BoxError>
where
    F: Future<Output = Result<Started, BoxError>> + Send + 'static,
{
    let mut starting = JoinSet::new();
    for (target, start) in starts {
        starting.spawn(async move { (target, start.await) });
    }
    let mut new = HashMap::new();
    while let Some(joined) = starting.join_next().await {
        match joined {
            Ok((target, Ok(started))) => {
                new.insert(target, started);
            }
            Ok((_, Err(e))) => {
                starting.shutdown().await;
                return Err(e);
            }
            Err(e) => {
                starting.shutdown().await;
                return Err(e.into());
            }
        }
    }
    Ok(new)
}

/// Refuses an edit of a host another source routes: the console edits its own
/// routes, and moves one only to a host no other source routes; an app takes
/// no host another source or app routes. The hosts are checked (lower case) by
/// the console before.
fn taken(edit: &Edit, origins: &HashMap<String, Origin>) -> Result<(), Refused> {
    let foreign = |host: &str| {
        origins
            .get(host)
            .filter(|origin| !matches!(origin, Origin::Console { .. }))
    };
    match edit {
        Edit::Deploy { .. }
        | Edit::Kept { .. }
        | Edit::Options { .. }
        | Edit::Move { .. }
        | Edit::Remove { .. } => {
            let host = edit.host().expect("a route's");
            if let Some(origin) = foreign(host) {
                return Err(Refused::Taken(format!(
                    "{host} is routed by {origin}, not by the console"
                )));
            }
            if let Edit::Move { to, .. } = edit
                && let Some(origin) = foreign(to)
            {
                return Err(Refused::Taken(format!("{to} is routed by {origin}")));
            }
        }
        Edit::PutApp { name, app } => {
            for host in &app.hosts {
                match origins.get(host) {
                    None => {}
                    Some(Origin::Api { name: own, .. }) if own == name => {}
                    Some(Origin::Console { .. }) => {
                        return Err(Refused::Taken(format!("{host} is a route of the console")));
                    }
                    Some(origin) => {
                        return Err(Refused::Taken(format!("{host} is routed by {origin}")));
                    }
                }
            }
        }
        Edit::Upload { .. } | Edit::DeleteApp { .. } | Edit::DeleteDisk { .. } => {}
    }
    Ok(())
}

/// Whether the routes can start within `capacity`: their shared VMs and their pools'
/// warm VMs for good, beside a template at a time (the largest).
fn fits(specs: &[RouteSpec], capacity: &Capacity) -> Result<(), BoxError> {
    let mut targets: Vec<(String, &RouteSpec)> = Vec::new();
    for spec in specs {
        let target = spec.target();
        if !targets.iter().any(|(known, _)| *known == target) {
            targets.push((target, spec));
        }
    }
    let template = targets
        .iter()
        .filter(|(_, spec)| spec.mode.cloned())
        .map(|(_, spec)| u64::from(spec.vm.mem_mib))
        .max();
    let vms = targets
        .iter()
        .map(|(_, spec)| spec.resident())
        .sum::<usize>()
        + usize::from(template.is_some());
    let memory: u64 = targets
        .iter()
        .map(|(_, spec)| spec.resident() as u64 * u64::from(spec.vm.mem_mib))
        .sum::<u64>()
        + template.unwrap_or(0);
    if vms > capacity.max_vms() || capacity.max_memory().is_some_and(|max| memory > max) {
        return Err(format!(
            "the routes need {vms} VM(s) and {memory} MiB of guest memory to start (their shared \
             VMs, their pools' warm VMs, and a template at a time): over --max-vms {} / --max-memory {}",
            capacity.max_vms(),
            capacity
                .max_memory()
                .map_or("-".to_string(), |mib| mib.to_string())
        )
        .into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A route takes any host alone, from a flag, on a wicket without a console.
    #[test]
    fn only_a_flag_route_alone_without_a_console_takes_any_host() {
        let console = || Origin::Console {
            deployed: String::new(),
        };
        for (origin, on_console, takes) in [
            (Origin::Flag, false, true),
            (Origin::Flag, true, false),
            (Origin::Config, false, false),
            (console(), true, false),
        ] {
            let table = Table {
                generation: 1,
                advertised: vec![Upstream {
                    host: "a.local".into(),
                    ..Default::default()
                }],
                routes: HashMap::new(),
                origins: HashMap::from([("a.local".to_string(), origin.clone())]),
                console: on_console,
            };
            let routed = sluice_wicket_tunnel::routes::Table::host_route(&table, "b.local");
            assert_eq!(routed.is_some(), takes, "{origin:?}, console {on_console}");
        }
    }
}
