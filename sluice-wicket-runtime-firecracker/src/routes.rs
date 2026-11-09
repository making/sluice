//! The routes the tunnel advertises, and how a connection to one is served: a
//! lease on a guest stream ([`Route::open`]), relayed to the tunnel
//! connection in the vsock framing ([`wire::relay`]).
//!
//! The routes are reloaded without a restart ([`Reloader`]): on a change of
//! their files (the `--config` file, a rootfs, the kernel, the firecracker
//! binary) or on SIGHUP. A reload reads the routes again and starts the VMs
//! of every target that is new or whose VMs or files changed -- its VMs
//! booted, a cloned route's template booted and snapshotted again --
//! before it changes anything: one failure keeps the current table. A target
//! kept as it was keeps its VMs. A changed target serves its next connection
//! from the new VMs; a connection open stays on the VMs it got, which end
//! with the last of them. A removed target is served until every node stream
//! acknowledged its removal: the server may have routed a connection to it
//! before. A target kept serves under the hosts of the reload: its VMs' output
//! is named after them.
//!
//! How many VMs and how long (`route::Bounds`) is no change of them: the route running takes it
//! as it is ([`Route::adjust`]). An app's target names none of it, so nothing is advertised; a
//! route's does, so its VMs go on under the target of its new numbers, the old one served by them
//! as a removed one is.
//!
//! A disk has one writer (`disk`), so a route's VM on a disk is the exception to the
//! order: once every other target started, the VM holding the disk is stopped, then
//! the new one started. A start failing then has the old route start one again, on the disk
//! it left, and the reload fails as any other; failing as well, the route serves
//! nothing until a restart succeeds, which the failure says. Its connections fail
//! meanwhile, a boot long.
//!
//! Every node stream advertises the routes whose next connection finds a VM ([`Route::ready`]):
//! one with none, every VM down or failing its readiness check, leaves the advertisement until it
//! has one, a route starting with the wicket included, and is still bound (the server may have
//! routed a connection to it before).
//!
//! A VM that goes down is the route's to replace (`route::VmPool`), its start not the reloads':
//! one route failing holds no other's edit, and no reload fails for it. A change of the target
//! takes over from it. A route stopped with none to follow goes on once nothing starts a VM on
//! its disk ([`Reloader::settled`]).
//!
//! The console's edits ([`Edit`]) are reloads too, of the routes as they would
//! be with them: kept -- written to the console's directory -- only when they
//! started. An app's are one reload: every host of it or none. An edit takes the config file as
//! the routes last took it, and the files of what it does not change as they were: a change of
//! those is a reload of its own, whose failure refuses no edit.
//!
//! A reload's VMs start in a task ([`Ahead`]), in the order of a disk's too, and it is made once
//! they did, on the routes as they are by then: nothing waits for a boot, which a process never
//! listening takes a `boot-ms` long ([`Reloader::run`] awaits none). No route stopped comes back on a disk
//! such a task starts one on. A later edit of the same app or host takes over from an edit, its
//! VMs ended (on a disk, stopped: `vm::Vm`'s drop; the next start waits for the disk); a later
//! change of the files from a reload of them; an edit, on a disk, from a reload, which is made
//! again after it.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use sluice_wicket_console::http::Received;
use sluice_wicket_console::status::Status;
use sluice_wicket_tunnel::listener::Replay;
use sluice_wicket_tunnel::proto::Upstream;
use sluice_wicket_tunnel::routes::{Binding, Routes};
use sluice_wicket_tunnel::tunnel::TunnelIo;
use sluice_wicket_tunnel::workers::Workers;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tokio_util::task::{AbortOnDropHandle, TaskTracker};

use crate::BoxError;
use crate::capacity::{Capacity, Full};
use crate::config::{File, Origin, RouteSource};
use crate::deployed::{self, AppSpec, Deployed, Options};
use crate::listener;
use crate::output::Sink;
use crate::route::{Lease, Route, RouteSpec, Starting};
use crate::vm::Places;
use crate::wire;

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
    /// The VMs the advertised routes keep for good (their booted VMs, their pools' warm), and
    /// their guest memory: what an edit's routes may not need more of than the budget.
    pub fn resident(&self) -> (usize, u64) {
        let targets: HashSet<&String> = self.advertised.iter().map(|up| &up.target_url).collect();
        let specs: Vec<RouteSpec> = targets
            .into_iter()
            .filter_map(|target| Some(self.routes.get(target)?.spec()))
            .collect();
        resident(specs.iter())
    }

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

    /// Stops the VMs holding a disk, each given its time, and waits for a stop under way: the
    /// wicket is ending, and what they wrote goes to their disks first.
    pub async fn stop_disks(&self) {
        let mut stopping = JoinSet::new();
        let disks = self
            .routes
            .values()
            .filter(|route| route.vm().disk.is_some());
        for route in disks {
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

    /// Whether its route finds a VM for its next connection ([`Route::ready`]).
    fn ready(&self, route: &Upstream) -> bool {
        let route = self.routes.get(&route.target_url);
        route.is_none_or(|route| route.ready())
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
    /// directions closed, their CLOSEs passed on. A request finding no room for
    /// its VM is answered so ([`listener::bad_gateway`]). A guest stream that
    /// could not be had otherwise (the client's share of the route taken
    /// included), ended without its CLOSE, or relaying no byte for the route's
    /// `idle-ms`, fails the connection with an ERROR: the server drops its
    /// client's connection instead of ending it as if the answer were complete.
    async fn serve(self, mut io: TunnelIo) {
        let conn_id = io.conn_id();
        let ending = io.ending();
        let lease = self.route.open(io.client_address()).await;
        let Lease { stream, hold } = match lease {
            Ok(lease) => lease,
            Err(e) => {
                eprintln!("conn {conn_id} ({}): {e}", self.route.label());
                if let Some(why) = self.route.no_room(&e)
                    && listener::bad_gateway(&mut Replay::new(io), &why).await
                {
                    return ending.finish();
                }
                ending.fail(&format!("guest unreachable: {e}"));
                return;
            }
        };
        let failed = io.failed();
        let relayed = tokio::select! {
            relayed = wire::relay(&mut io, stream, self.route.idle()) => relayed,
            // the server's ERROR or the stream's end: nothing is sent any more
            () = failed => Ok(()),
        };
        let ended = hold.end(relayed.is_ok());
        match relayed {
            Ok(()) => ending.finish(),
            Err(e) => {
                eprintln!("conn {conn_id} ({}): {e}", self.route.label());
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

/// A target's route, the spec it takes (its bounds changing as it runs) and the stamps of the
/// files it was started from.
struct Started {
    spec: RouteSpec,
    stamps: Stamps,
    route: Arc<Route>,
}

impl Started {
    /// Whether it is what `spec` and its files, as `stamps` tell, ask of its target.
    fn asked(&self, spec: &RouteSpec, stamps: &Stamps, files: Files) -> bool {
        self.spec.same_vms(spec) && (matches!(files, Files::Aside) || self.stamps == *stamps)
    }
}

/// VMs started for a reload to take, by target.
type Vms = HashMap<String, Started>;

/// What a reload makes of a change of the files a target's VMs started from.
#[derive(Clone, Copy)]
enum Files {
    /// Starts them anew.
    Followed,
    /// Leaves them, for a reload of the files: an edit.
    Aside,
}

/// What a reload changed, by host.
#[derive(Default, Debug)]
pub struct Change {
    added: Vec<String>,
    removed: Vec<String>,
    /// Hosts whose VMs were started again: what they are or their files changed.
    restarted: Vec<String>,
    /// Hosts whose VMs took how many and how long as they run.
    adjusted: Vec<String>,
}

impl Change {
    pub fn is_empty(&self) -> bool {
        self.added.is_empty()
            && self.removed.is_empty()
            && self.restarted.is_empty()
            && self.adjusted.is_empty()
    }
}

impl std::fmt::Display for Change {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut parts = Vec::new();
        for (what, hosts) in [
            ("added", &self.added),
            ("removed", &self.removed),
            ("restarted", &self.restarted),
            ("adjusted", &self.adjusted),
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
    pub reply: Reply,
}

/// Where an edit's outcome goes.
pub type Reply = oneshot::Sender<Result<Outcome, Refused>>;

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
    /// The routes need more than the VMs' budget (`--max-instances`, `--max-memory-mib`).
    NoRoom(String),
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
            | Refused::NoRoom(message)
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
    if e.is::<DidNotStart>() {
        Refused::DidNotStart(e.to_string())
    } else if e.is::<Full>() {
        Refused::NoRoom(e.to_string())
    } else {
        Refused::Rejected(e.to_string())
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
    /// They serve this other target, which no route asks for any more: its numbers changed.
    Moved(String),
    Start,
    /// Started once the route holding its disk stopped.
    Swap(Arc<Route>),
}

/// What the routes need started.
#[derive(Default)]
struct Plan {
    starts: Vec<Start>,
    /// Started after those, each once the route holding its disk stopped.
    swaps: Vec<(Arc<Route>, Start)>,
    /// Target -> the target whose VMs it takes ([`Way::Moved`]): none to start.
    moved: Moved,
}

/// Target -> the target whose VMs it takes.
type Moved = HashMap<String, String>;

impl Plan {
    fn is_empty(&self) -> bool {
        self.starts.is_empty() && self.swaps.is_empty()
    }

    fn specs(&self) -> impl Iterator<Item = &RouteSpec> {
        let swaps = self.swaps.iter().map(|(_, start)| &start.spec);
        self.starts.iter().map(|start| &start.spec).chain(swaps)
    }

    /// The disks its VMs start on.
    fn disks(&self) -> Vec<PathBuf> {
        self.specs()
            .filter_map(|spec| spec.vm.disk.as_ref())
            .map(|disk| disk.file.clone())
            .collect()
    }
}

/// A reload waiting for its VMs.
struct Ahead {
    awaited: Awaited,
    reason: String,
    /// The images they start from, relative to the console's directory: no collection takes them.
    images: Vec<String>,
    /// The disks they start on, or those started are on: no VM comes back on one meanwhile.
    disks: Vec<PathBuf>,
    /// Their starts, ended with it.
    _starting: AbortOnDropHandle<()>,
}

impl Ahead {
    /// Whether it starts a VM on one of `disks`.
    fn claims(&self, disks: &[PathBuf]) -> bool {
        self.disks.iter().any(|disk| disks.contains(disk))
    }
}

/// What a reload waiting for its VMs is for.
enum Awaited {
    Edit {
        edit: Edit,
        reply: Reply,
    },
    /// A change of the files or SIGHUP: the config file it read, the files it found.
    Files {
        file: Option<Box<File>>,
        watched: Stamps,
    },
}

/// A swap's parts as its task takes them, a start being `F`.
struct Swapping<F> {
    holder: Arc<Route>,
    target: String,
    start: F,
    /// The holder goes on, a swap failing: it is one the routes advertise.
    back: bool,
}

/// The starts of an [`Ahead`] ended.
struct StartedAhead {
    id: u64,
    started: Result<Vms, BoxError>,
}

/// What an edit of the routes comes to.
enum Routed {
    Made(Outcome),
    /// Once these VMs started, the ones started so far with them.
    Waits(Plan, Vms),
}

/// Starts the routes and keeps them current: on a change of their files, on
/// request (SIGHUP), or on an edit of the console's. Reloads are made one at a
/// time, their VMs starting before, in a task ([`Ahead`]).
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
    /// The stops no route waits for any more, which the wicket's end does.
    stops: TaskTracker,
    /// Target -> its route as started; the advertised targets only.
    started: HashMap<String, Started>,
    /// Removed targets still in the table -> the generation that removed them.
    lingering: HashMap<String, u64>,
    /// The config file as a reload last read it.
    read: Stamps,
    /// The config file as the routes took it, which an edit goes by.
    file: Option<Box<File>>,
    /// A reload of the files to make once no edit starts a VM on a disk it would: its reason.
    reload_due: Option<String>,
    /// Reloads waiting for their VMs, by a number of their own.
    ahead: HashMap<u64, Ahead>,
    next_ahead: u64,
    started_ahead: mpsc::UnboundedReceiver<StartedAhead>,
    tell_started_ahead: mpsc::UnboundedSender<StartedAhead>,
}

impl Reloader {
    /// Starts the VMs of every route of `source` within `capacity`, each target
    /// in a place of its own of `places`, their output to `sink`; the first
    /// failure ends them all. The stops of VMs on a disk that no route waits for go to `stops`.
    pub async fn start(
        source: RouteSource,
        capacity: Arc<Capacity>,
        places: Arc<Places>,
        sink: Arc<Sink>,
        workers: Arc<Workers>,
        stops: TaskTracker,
    ) -> Result<Reloader, BoxError> {
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
            stops,
            started: HashMap::new(),
            lingering: HashMap::new(),
            read: HashMap::new(),
            file: None,
            reload_due: None,
            ahead: HashMap::new(),
            next_ahead: 0,
            started_ahead,
            tell_started_ahead,
        };
        // read before it: a change while it starts is a change for the next poll
        reloader.read = stamps(reloader.source.config());
        reloader.file = reloader.source.file()?.map(Box::new);
        let specs = reloader.fitting(reloader.file.as_deref())?;
        let plan = reloader.plan(&specs, &mut HashMap::new(), Files::Followed);
        let starting: Vec<_> = plan
            .starts
            .into_iter()
            .map(|start| (start.target.clone(), reloader.starter(start)))
            .collect();
        let started = all_started(starting).await?;
        reloader.publish(specs, started, Moved::new());
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
    /// `requests` and drops a removed target once every stream acknowledged
    /// its removal; until `stop`.
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
                            self.take(request);
                            seen = self.watched();
                        }
                        None => requests_open = false,
                    }
                    continue;
                }
                Some(started) = self.started_ahead.recv() => {
                    self.ahead_ended(started);
                    seen = self.watched();
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
            self.reload(reason);
            self.settled();
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

    /// Takes an edit of the console's, which takes over from an edit of the same route or app
    /// waiting for its VMs.
    fn take(&mut self, request: Request) {
        let Request { edit, reply } = request;
        let over: Vec<u64> = self
            .ahead
            .iter()
            .filter(|(_, ahead)| {
                matches!(&ahead.awaited, Awaited::Edit { edit: waiting, .. } if edit.takes_over(waiting))
            })
            .map(|(id, _)| *id)
            .collect();
        let why = format!("{} took over from it", edit.describe());
        for id in over {
            self.taken_over(id, &why);
        }
        if let Edit::DeleteDisk { name, owner } = &edit
            && let file = self.deployed().disk_file(owner, name)
            && self
                .ahead
                .values()
                .any(|ahead| ahead.claims(std::slice::from_ref(&file)))
        {
            let held = format!("the app '{name}' is starting on that disk");
            let _ = reply.send(Err(Refused::Taken(held)));
            return;
        }
        match self.image_kept(edit) {
            Ok(edit) => self.edit(edit, reply, HashMap::new()),
            Err(refused) => {
                let _ = reply.send(Err(refused));
            }
        }
        self.settled();
    }

    /// The reload `id` is given up for what `why` tells, its VMs ending.
    fn taken_over(&mut self, id: u64, why: &str) {
        let ahead = self.ahead.remove(&id).expect("just found");
        self.failed(&ahead.reason, why);
        match ahead.awaited {
            Awaited::Edit { reply, .. } => {
                let _ = reply.send(Err(Refused::Rejected(why.to_string())));
            }
            Awaited::Files { .. } => self.reload_due = Some(ahead.reason),
        }
    }

    /// What is left to do once a reload was made or a task started: a reload that was to wait, a
    /// route stopped for a swap that did not come to serve its target, nothing starting a VM on
    /// its disk.
    fn settled(&mut self) {
        if let Some(reason) = self.reload_due.take() {
            self.reload(&reason);
        }
        for started in self.started.values() {
            let on_disk = started.spec.vm.disk.as_ref();
            if started.route.stopped()
                && on_disk.is_none_or(|disk| {
                    let file = std::slice::from_ref(&disk.file);
                    !self.ahead.values().any(|ahead| ahead.claims(file))
                })
            {
                eprintln!("route {}: its VM starts again", started.route.label());
                started.route.resume();
            }
        }
    }

    /// The edit as it goes on: a deploy's image kept.
    fn image_kept(&mut self, edit: Edit) -> Result<Edit, Refused> {
        taken(&edit, &self.routes.current().origins)?;
        let Edit::Deploy {
            host,
            name,
            received,
            options,
        } = edit
        else {
            return Ok(edit);
        };
        let deployed = self
            .source
            .deployed()
            .expect("edits come from the console, which deploys");
        let route = deployed.deploy(&host, &name, received, options, SystemTime::now())?;
        Ok(Edit::Kept { route })
    }

    /// Makes an edit of the console's routes or apps (a deploy's image kept: [`Edit::Kept`]), or
    /// starts the VMs it needs, `ready` those it has: it is made again when they did. An upload
    /// keeps its image and changes no route.
    fn edit(&mut self, edit: Edit, reply: Reply, ready: Vms) {
        let reason = edit.describe();
        let outcome = match edit {
            Edit::Upload { sha256, received } => self.upload(&sha256, received),
            Edit::DeleteDisk { name, owner } => self.delete_disk(&name, &owner, &reason),
            edit => match self.routed(&edit, ready) {
                Ok(Routed::Made(outcome)) => Ok(outcome),
                Ok(Routed::Waits(plan, ready)) => {
                    self.start_ahead(Awaited::Edit { edit, reply }, reason, plan, ready);
                    return;
                }
                Err(refused) => Err(refused),
            },
        };
        let _ = reply.send(outcome);
    }

    fn upload(&mut self, sha256: &str, received: Received) -> Result<Outcome, Refused> {
        let uploaded = self.deployed().upload(sha256, received);
        // the uploads kept long enough go
        self.collect_garbage();
        uploaded?;
        Ok(self.unchanged())
    }

    fn delete_disk(&mut self, name: &str, owner: &str, reason: &str) -> Result<Outcome, Refused> {
        self.deployed().remove_disk(name, owner)?;
        println!("{reason}: removed");
        Ok(self.unchanged())
    }

    fn deployed(&self) -> &Deployed {
        self.source
            .deployed()
            .expect("edits come from the console, which deploys")
    }

    /// An edit's outcome that changed no route.
    fn unchanged(&self) -> Outcome {
        Outcome {
            generation: self.routes.current().generation,
            change: None,
        }
    }

    /// Reloads the routes as they would be with `edit`, of the routes as they are by now, with
    /// the VMs started for it: kept -- written to the console's directory -- only when they
    /// started; refused, the routes and the directory stay as they were.
    fn routed(&mut self, edit: &Edit, mut ready: Vms) -> Result<Routed, Refused> {
        let reason = edit.describe();
        taken(edit, &self.routes.current().origins)?;
        let edited = edited(self.deployed(), edit, SystemTime::now())?;
        // the same again changes nothing: a VM down comes back by itself
        if &edited == self.deployed() {
            return Ok(Routed::Made(self.unchanged()));
        }
        let previous = self.source.deploy(edited).expect("the console's routes");
        let specs = match self.fitting(self.file.as_deref()) {
            Ok(specs) => specs,
            Err(e) => {
                self.failed(&reason, &e.to_string());
                self.source.deploy(previous);
                self.collect_garbage();
                return Err(refused(&e));
            }
        };
        let plan = self.plan(&specs, &mut ready, Files::Aside);
        if !plan.is_empty() {
            self.source.deploy(previous);
            return Ok(Routed::Waits(plan, ready));
        }
        let change = self.publish(specs, ready, plan.moved);
        self.logged(&reason, &change);
        let saved = self.deployed().save();
        self.collect_garbage();
        saved.map_err(|e| {
            eprintln!("{reason}: {e}");
            Refused::Failed(e)
        })?;
        Ok(Routed::Made(Outcome {
            generation: self.routes.current().generation,
            change: (!change.is_empty()).then_some(change),
        }))
    }

    /// A change of the files or SIGHUP: the files read again and reloaded, unless a reload of
    /// them as they are is under way. One of them as they were is given up.
    fn reload(&mut self, reason: &str) {
        let watched = self.watched();
        let mut over = Vec::new();
        for (id, ahead) in &self.ahead {
            if let Awaited::Files { watched: of, .. } = &ahead.awaited {
                // what an edit added or removed since is no change of the files
                let same = of
                    .iter()
                    .all(|(path, stamp)| watched.get(path).is_none_or(|now| now == stamp));
                if same {
                    return;
                }
                over.push(*id);
            }
        }
        let why = format!("{reason} took over from it");
        for id in over {
            self.taken_over(id, &why);
        }
        self.reload_due = None;
        // stamped before it is read: a change while it is read is one more
        self.read = stamps(self.source.config());
        match self.source.file() {
            Ok(file) => {
                let file = file.map(Box::new);
                self.reload_of(reason.to_string(), file, watched, HashMap::new());
            }
            Err(e) => {
                self.failed(reason, &e);
                Status::failed(&self.status, reason, e);
            }
        }
    }

    /// Reloads the routes of the config file `file`, the files as `watched` found them, with the
    /// VMs started for it: made, or the VMs it needs started, the rest of it when they did; or
    /// due again, an edit starting a VM on a disk it would.
    fn reload_of(
        &mut self,
        reason: String,
        file: Option<Box<File>>,
        watched: Stamps,
        mut ready: Vms,
    ) {
        let specs = match self.fitting(file.as_deref()) {
            Ok(specs) => specs,
            Err(e) => {
                self.failed(&reason, &e.to_string());
                Status::failed(&self.status, &reason, e.to_string());
                return;
            }
        };
        let plan = self.plan(&specs, &mut ready, Files::Followed);
        if plan.is_empty() {
            self.file = file;
            let change = self.publish(specs, ready, plan.moved);
            self.logged(&reason, &change);
            Status::recovered(&self.status);
            return;
        }
        let disks = plan.disks();
        if self.ahead.values().any(|ahead| ahead.claims(&disks)) {
            self.reload_due = Some(reason);
            return;
        }
        self.start_ahead(Awaited::Files { file, watched }, reason, plan, ready);
    }

    /// The routes of the config file `file` and the console's as they are, if they fit the VMs'
    /// budget.
    fn fitting(&self, file: Option<&File>) -> Result<Vec<(RouteSpec, Origin)>, BoxError> {
        let specs = self.source.specs(file)?;
        fits(&specs, &self.capacity)?;
        Ok(specs)
    }

    /// What `specs` need started that is not running as they ask, nor in `ready`; what they do
    /// not ask of `ready` goes.
    fn plan(&self, specs: &[(RouteSpec, Origin)], ready: &mut Vms, files: Files) -> Plan {
        let specs: Vec<RouteSpec> = specs.iter().map(|(spec, _)| spec.clone()).collect();
        let targets = targets(specs);
        // the VMs of the targets no route asks for any more, for one of other numbers to take
        let mut free: Vec<String> = self
            .started
            .keys()
            .filter(|running| !targets.iter().any(|(target, ..)| target == *running))
            .cloned()
            .collect();
        free.sort();
        let mut plan = Plan::default();
        let mut taken = HashMap::new();
        for (target, spec, hosts) in targets {
            let stamps = stamps(self::files(&spec));
            let way = self.way(&target, &spec, &hosts, &stamps, files, &free);
            if let Way::Moved(from) = &way {
                free.retain(|running| running != from);
                plan.moved.insert(target.clone(), from.clone());
            }
            if matches!(way, Way::Start | Way::Swap(_))
                && let Some((target, started)) = ready
                    .remove_entry(&target)
                    .filter(|(_, started)| started.asked(&spec, &stamps, files))
            {
                taken.insert(target, started);
                continue;
            }
            let start = Start {
                target,
                spec,
                hosts,
                stamps,
            };
            match way {
                Way::Kept | Way::Moved(_) => {}
                Way::Start => plan.starts.push(start),
                Way::Swap(holder) => plan.swaps.push((holder, start)),
            }
        }
        // the others end now: one may hold a disk a start here is to wait for
        *ready = taken;
        plan
    }

    /// Starts what `plan` needs in a task: `awaited` waits for it, `ready` going along. An edit
    /// takes over from a reload of the files starting a VM on its disks.
    fn start_ahead(&mut self, awaited: Awaited, reason: String, plan: Plan, ready: Vms) {
        let mut disks = plan.disks();
        let held = ready
            .values()
            .filter_map(|started| started.spec.vm.disk.as_ref());
        disks.extend(held.map(|disk| disk.file.clone()));
        if matches!(awaited, Awaited::Edit { .. }) {
            let over: Vec<u64> = self
                .ahead
                .iter()
                .filter(|(_, ahead)| ahead.claims(&disks))
                .map(|(id, _)| *id)
                .collect();
            let why = format!("{reason} took over from it");
            for id in over {
                self.taken_over(id, &why);
            }
        }
        let images = match self.source.deployed() {
            Some(deployed) => plan
                .specs()
                .chain(ready.values().map(|started| &started.spec))
                .filter_map(|spec| spec.vm.rootfs.strip_prefix(deployed.dir()).ok())
                .map(|image| image.to_string_lossy().into_owned())
                .collect(),
            None => Vec::new(),
        };
        let Plan { starts, swaps, .. } = plan;
        let starts: Vec<_> = starts
            .into_iter()
            .map(|start| (start.target.clone(), self.starter(start)))
            .collect();
        let swaps: Vec<_> = swaps
            .into_iter()
            .map(|(holder, start)| Swapping {
                back: self
                    .started
                    .values()
                    .any(|started| Arc::ptr_eq(&started.route, &holder)),
                holder,
                target: start.target.clone(),
                start: self.starter(start),
            })
            .collect();
        self.next_ahead += 1;
        let id = self.next_ahead;
        let tell = self.tell_started_ahead.clone();
        let task = tokio::spawn(async move {
            let started = started_ahead(starts, swaps, ready).await;
            let _ = tell.send(StartedAhead { id, started });
        });
        println!("routes reload ({reason}): its VMs starting");
        self.ahead.insert(
            id,
            Ahead {
                awaited,
                reason,
                images,
                disks,
                _starting: AbortOnDropHandle::new(task),
            },
        );
    }

    /// The VMs of a reload started: it is made, of the routes as they are by now, or waits for
    /// what they need by then. Of one that was taken over: nothing, the VMs ending. Not started:
    /// it fails, the routes stopped for it back as far as they could.
    fn ahead_ended(&mut self, started: StartedAhead) {
        let StartedAhead { id, started } = started;
        let Some(ahead) = self.ahead.remove(&id) else {
            return;
        };
        let Ahead {
            awaited, reason, ..
        } = ahead;
        match (awaited, started) {
            (Awaited::Edit { edit, reply }, Ok(started)) => self.edit(edit, reply, started),
            (Awaited::Edit { reply, .. }, Err(e)) => {
                self.failed(&reason, &e.to_string());
                self.collect_garbage();
                let _ = reply.send(Err(refused(&e)));
            }
            (Awaited::Files { file, watched }, Ok(started)) => {
                self.reload_of(reason, file, watched, started);
            }
            (Awaited::Files { .. }, Err(e)) => {
                self.failed(&reason, &e.to_string());
                Status::failed(&self.status, &reason, e.to_string());
            }
        }
        self.settled();
    }

    /// Removes the images nothing names but the reloads waiting for their VMs.
    fn collect_garbage(&self) {
        let starting = self.ahead.values().flat_map(|ahead| &ahead.images);
        self.deployed().collect_garbage(false, starting);
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

    /// How `target` of `hosts` gets the VMs `spec` asks, the files as `stamps` tell, the VMs of
    /// the targets `free` serving no route any more.
    fn way(
        &self,
        target: &str,
        spec: &RouteSpec,
        hosts: &[String],
        stamps: &Stamps,
        files: Files,
        free: &[String],
    ) -> Way {
        // stopped too: it goes on by itself, no reload failing for it
        if self
            .started
            .get(target)
            .is_some_and(|started| started.asked(spec, stamps, files))
        {
            return Way::Kept;
        }
        let from = free.iter().find(|from| {
            self.started[*from].asked(spec, stamps, files)
                && self.hosts_of(from).iter().any(|host| hosts.contains(host))
        });
        if let Some(from) = from {
            return Way::Moved(from.clone());
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

    /// Publishes `specs`, the VMs of `ready` serving their targets from now on, the other
    /// targets' as they are, `moved`'s under the target of their numbers, each taking its bounds.
    fn publish(&mut self, specs: Vec<(RouteSpec, Origin)>, mut ready: Vms, moved: Moved) -> Change {
        let (specs, origins): (Vec<RouteSpec>, Vec<Origin>) = specs.into_iter().unzip();
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
        let mut new = HashMap::new();
        let mut kept = Vec::new();
        for (target, spec, hosts) in targets(specs) {
            let Some(mut started) = ready.remove(&target) else {
                let of = moved.get(&target).unwrap_or(&target);
                if let Some(running) = self.started.get(of)
                    && running.route.adjust(spec.bounds.clone())
                {
                    change.adjusted.extend(hosts.iter().cloned());
                }
                kept.push((target, spec, hosts));
                continue;
            };
            if self.started.contains_key(&target) {
                change.restarted.extend(hosts.iter().cloned());
            }
            started.route.rename(&hosts);
            // they may have changed as it started
            started.route.adjust(spec.bounds.clone());
            started.spec = spec;
            new.insert(target, started);
        }

        for up in &advertised {
            match current.advertised.iter().find(|old| old.host == up.host) {
                None => change.added.push(up.host.clone()),
                // its image or options are another target's: VMs started anew all the same
                Some(old)
                    if old.target_url != up.target_url
                        && !change.restarted.contains(&up.host)
                        && !change.adjusted.contains(&up.host) =>
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
        let same_routes =
            change.added.is_empty() && change.removed.is_empty() && change.restarted.is_empty();
        if same_routes && advertised == current.advertised {
            for (target, spec, _) in kept {
                if let Some(started) = self.started.get_mut(&target) {
                    started.spec = spec;
                }
            }
            // how many and how long, or where a route comes from (a console route's time),
            // changed alone: in the generation it was, nothing to advertise
            let moved_on = origins != current.origins;
            if moved_on {
                self.routes.publish(Table {
                    generation: current.generation,
                    advertised: current.advertised.clone(),
                    routes: current.routes.clone(),
                    origins,
                    console: current.console,
                });
            }
            if moved_on || !change.adjusted.is_empty() {
                self.status.send_modify(|status| status.revision += 1);
            }
            return change;
        }
        let generation = current.generation + 1;
        let mut started: HashMap<String, Started> = kept
            .into_iter()
            .filter_map(|(target, spec, hosts)| {
                let of = moved.get(&target).unwrap_or(&target);
                let (_, mut kept) = self.started.remove_entry(of)?;
                kept.route.rename(&hosts);
                kept.spec = spec;
                Some((target, kept))
            })
            .collect();
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
        change
    }

    /// What starts `start`, the next place's.
    fn starter(
        &mut self,
        start: Start,
    ) -> impl Future<Output = Result<Started, BoxError>> + Send + 'static + use<> {
        let Start {
            spec,
            hosts,
            stamps,
            ..
        } = start;
        let n = self.next_place;
        self.next_place += 1;
        let routes = Arc::downgrade(&self.routes);
        let starting = Starting {
            places: self.places.clone(),
            n,
            capacity: self.capacity.clone(),
            sink: self.sink.clone(),
            workers: self.workers.clone(),
            stops: self.stops.clone(),
            status: self.status.clone(),
            ready_changed: Arc::new(move || {
                if let Some(routes) = routes.upgrade() {
                    routes.readiness_changed();
                }
            }),
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
                self.stops.spawn(async move { route.stop().await });
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

/// What an [`Ahead`] does: `starts`, then each of `swaps` in the order of a disk's: the route
/// holding it stopped, its VM started; `ready` going along. A swap failing ends the VMs swapped
/// in, and has the routes it stopped go on, on their disks: back, their first VM up, or the failure
/// says so.
async fn started_ahead<F>(
    starts: Vec<(String, F)>,
    swaps: Vec<Swapping<F>>,
    ready: Vms,
) -> Result<Vms, BoxError>
where
    F: Future<Output = Result<Started, BoxError>> + Send + 'static,
{
    let mut new = all_started(starts).await?;
    new.extend(ready);
    let mut swapped = Vec::new();
    let mut stopped = Vec::new();
    for swap in swaps {
        // a task of its own: a VM is given its time, whatever becomes of this one
        let _ = tokio::spawn(swap.holder.stopping()).await;
        stopped.push((swap.holder, swap.back));
        match swap.start.await {
            Ok(started) => {
                swapped.push(swap.target.clone());
                new.insert(swap.target, started);
            }
            Err(e) => {
                for target in &swapped {
                    if let Some(started) = new.remove(target) {
                        started.route.stop().await;
                    }
                }
                drop(new);
                let mut failed = None;
                for (was, back) in stopped {
                    if !back {
                        continue;
                    }
                    was.resume();
                    if let Err(again) = was.resumed().await {
                        failed = Some(again);
                    }
                }
                return Err(match failed {
                    None => e,
                    Some(again) => {
                        let message = format!(
                            "{e}; starting the old VM again failed as well, the route serves \
                             nothing until a restart succeeds: {again}"
                        );
                        match e.is::<DidNotStart>() {
                            true => DidNotStart(message).into(),
                            false => message.into(),
                        }
                    }
                });
            }
        }
    }
    Ok(new)
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

/// Whether the routes can start within `capacity`: their booted VMs and their pools' warm VMs for
/// good, and a template at a time beside the other routes' (its own come as it is gone).
fn fits(specs: &[(RouteSpec, Origin)], capacity: &Capacity) -> Result<(), BoxError> {
    let mut targets: Vec<(String, &RouteSpec)> = Vec::new();
    for (spec, _) in specs {
        let target = spec.target();
        if !targets.iter().any(|(known, _)| *known == target) {
            targets.push((target, spec));
        }
    }
    let kept = |spec: &RouteSpec| spec.resident() as u64 * u64::from(spec.vm.memory_mib);
    let (resident, in_memory) = resident(targets.iter().map(|(_, spec)| *spec));
    let cloned = || targets.iter().filter(|(_, spec)| spec.mode.cloned());
    let vms = cloned()
        .map(|(_, spec)| resident - spec.resident() + 1)
        .fold(resident, usize::max);
    let memory = cloned()
        .map(|(_, spec)| in_memory - kept(spec) + u64::from(spec.vm.memory_mib))
        .fold(in_memory, u64::max);
    if vms > capacity.max_instances() || capacity.max_memory_mib().is_some_and(|max| memory > max) {
        return Err(Full::over(format!(
            "the routes need {vms} VM(s) and {memory} MiB of guest memory to start (their booted \
             VMs, their pools' warm VMs, and a template at a time beside the other routes'): over \
             --max-instances {} / --max-memory-mib {}",
            capacity.max_instances(),
            capacity
                .max_memory_mib()
                .map_or("-".to_string(), |mib| mib.to_string())
        ))
        .into());
    }
    Ok(())
}

/// The VMs the routes of `specs`, one of each target, keep for good, and their guest memory.
fn resident<'a>(specs: impl Iterator<Item = &'a RouteSpec>) -> (usize, u64) {
    specs.fold((0, 0), |(vms, memory), spec| {
        let resident = spec.resident();
        let mib = resident as u64 * u64::from(spec.vm.memory_mib);
        (vms + resident, memory + mib)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The routes' VMs fit for good, and a template beside the other routes': a pool's own come
    /// as it is gone, so a pool of one needs its one VM, what serves no connection none.
    #[test]
    fn the_routes_fit_with_a_template_beside_the_others() {
        let specs = |routes: &[(&str, &[(&str, &str)])]| {
            routes
                .iter()
                .map(|(host, options)| {
                    let spec = RouteSpec::configure(
                        host,
                        host,
                        PathBuf::from(format!("/{host}.ext4")),
                        options.iter().copied(),
                        Path::new("firecracker"),
                        Path::new("vmlinux"),
                    );
                    (spec.unwrap(), Origin::Flag)
                })
                .collect::<Vec<_>>()
        };
        let pool: &[(&str, &str)] = &[("mode", "pool"), ("instances", "1"), ("memory-mib", "64")];
        let large: &[(&str, &str)] = &[
            ("mode", "pool"),
            ("instances", "1"),
            ("warm", "0"),
            ("memory-mib", "256"),
        ];
        let booted: &[(&str, &str)] = &[("memory-mib", "64")];
        let scaled: &[(&str, &str)] = &[("memory-mib", "64"), ("instances", "3")];
        for (routes, max_instances, max_memory_mib, fit) in [
            (&[("a", pool), ("b", pool)][..], 2, 128, true),
            (&[("a", pool), ("b", pool)], 1, 128, false),
            (&[("a", pool), ("b", pool)], 2, 127, false),
            (&[("a", booted), ("b", pool)], 2, 128, true),
            // N x memory-mib, all of them for good
            (&[("a", scaled), ("b", pool)], 4, 256, true),
            (&[("a", scaled), ("b", pool)], 3, 256, false),
            (&[("a", scaled), ("b", pool)], 4, 255, false),
            // no VM of its own to take a template's place
            (&[("a", pool), ("b", large)], 1, 320, false),
            (&[("a", pool), ("b", large)], 2, 319, false),
            (&[("a", pool), ("b", large)], 2, 320, true),
        ] {
            let capacity = Capacity::new(max_instances, Some(max_memory_mib));
            let hosts: Vec<_> = routes.iter().map(|(host, _)| host).collect();
            assert_eq!(
                fits(&specs(routes), &capacity).is_ok(),
                fit,
                "{hosts:?} {max_instances} {max_memory_mib}"
            );
        }
    }

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
