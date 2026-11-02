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

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use sluice_wicket_tunnel::proto::Upstream;
use sluice_wicket_tunnel::routes::{Binding, Routes};
use sluice_wicket_tunnel::tunnel::TunnelIo;
use sluice_wicket_tunnel::workers::Workers;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::BoxError;
use crate::capacity::Capacity;
use crate::config::RouteSource;
use crate::output::Sink;
use crate::route::{Lease, Route, RouteSpec};
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
}

impl Table {
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

    /// The route when one alone is advertised (`--listen`: whatever the host).
    pub fn sole(&self) -> Option<Arc<Route>> {
        match &self.advertised[..] {
            [sole] => self.routes.get(&sole.target_url).cloned(),
            _ => None,
        }
    }

    /// The route of `host` (no port) on `--listen`
    /// (`sluice_wicket_tunnel::routes::Table::host_route`).
    pub fn by_host(&self, host: &str) -> Option<Arc<Route>> {
        let target = &sluice_wicket_tunnel::routes::Table::host_route(self, host)?.target_url;
        self.routes.get(target).cloned()
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

/// What a reload changed, by host.
#[derive(Default, Debug)]
pub struct Change {
    added: Vec<String>,
    removed: Vec<String>,
    /// Hosts whose VMs were started again: their spec or files changed.
    restarted: Vec<String>,
}

impl Change {
    fn is_empty(&self) -> bool {
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

/// Starts the routes and keeps them current: on a change of their files, or
/// on request (SIGHUP). Reloads run one at a time.
pub struct Reloader {
    routes: Arc<Routes<Table>>,
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
}

impl Reloader {
    /// Starts the VMs of every route of `specs` (read from `source`) within
    /// `capacity`, each target in a place of its own of `places`, their output to
    /// `sink`; the first failure ends them all.
    pub async fn start(
        source: RouteSource,
        specs: Vec<RouteSpec>,
        capacity: Arc<Capacity>,
        places: Arc<Places>,
        sink: Arc<Sink>,
        workers: Arc<Workers>,
    ) -> Result<Reloader, BoxError> {
        fits(&specs, &capacity)?;
        let mut reloader = Reloader {
            routes: Routes::new(Table {
                generation: 0,
                advertised: Vec::new(),
                routes: HashMap::new(),
            }),
            source,
            capacity,
            sink,
            workers,
            places,
            next_place: 0,
            started: HashMap::new(),
            lingering: HashMap::new(),
            read: HashMap::new(),
        };
        // read before it: a change while it starts is a change for the next poll
        reloader.read = stamps(reloader.source.config());
        reloader.apply(specs).await?;
        Ok(reloader)
    }

    pub fn routes(&self) -> &Arc<Routes<Table>> {
        &self.routes
    }

    /// Reloads on SIGHUP (`hangups`) and on a change of the routes' files,
    /// polled every `interval` (zero: never), and drops a removed target once
    /// every stream acknowledged its removal; until `stop`.
    pub async fn run(
        mut self,
        interval: Duration,
        mut hangups: mpsc::Receiver<()>,
        stop: CancellationToken,
    ) {
        let mut seen = self.watched();
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
                Ok(change) if change.is_empty() => {
                    println!("routes reload ({reason}): no change");
                }
                Ok(change) => println!(
                    "routes reloaded ({reason}), generation {}: {change}",
                    self.routes.current().generation
                ),
                Err(e) => eprintln!(
                    "routes reload ({reason}) failed; keeping generation {}: {e}",
                    self.routes.current().generation
                ),
            }
            seen = self.watched();
        }
    }

    /// Reads the routes again and applies them.
    pub async fn reload(&mut self) -> Result<Change, BoxError> {
        // stamped before it is read: a change while it is read is one more
        self.read = stamps(self.source.config());
        let specs = self.source.read()?;
        self.apply(specs).await
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

    /// Starts what `specs` need that is not running as they ask, and
    /// publishes them; nothing changes when a start fails.
    async fn apply(&mut self, specs: Vec<RouteSpec>) -> Result<Change, BoxError> {
        // target -> its spec and hosts, in the order of the specs
        let mut targets: Vec<(String, RouteSpec, Vec<String>)> = Vec::new();
        let mut advertised = Vec::new();
        for spec in specs {
            let target = spec.target();
            advertised.push(Upstream {
                host: spec.host.clone(),
                target_url: target.clone(),
                ..Default::default()
            });
            match targets.iter_mut().find(|(known, ..)| *known == target) {
                Some((_, _, hosts)) => hosts.push(spec.host),
                None => {
                    let host = spec.host.clone();
                    targets.push((target, spec, vec![host]));
                }
            }
        }
        let mut change = Change::default();
        let mut starting = JoinSet::new();
        let mut kept = Vec::new();
        for (target, spec, hosts) in targets {
            let stamps = stamps(files(&spec));
            match self.started.get(&target) {
                Some(started) if started.spec.same_vms(&spec) && started.stamps == stamps => {
                    kept.push((target, hosts));
                    continue;
                }
                Some(_) => change.restarted.extend(hosts.iter().cloned()),
                None => {}
            }
            let n = self.next_place;
            self.next_place += 1;
            let (places, capacity, sink, workers) = (
                self.places.clone(),
                self.capacity.clone(),
                self.sink.clone(),
                self.workers.clone(),
            );
            starting.spawn(async move {
                let route =
                    Route::start(spec.clone(), &hosts, &places, n, capacity, sink, workers).await;
                let started = route.map(|route| Started {
                    spec,
                    stamps,
                    route: Arc::new(route),
                });
                (target, started)
            });
        }
        let mut new = HashMap::new();
        while let Some(joined) = starting.join_next().await {
            let (target, started) = match joined {
                Ok((target, Ok(started))) => (target, started),
                failed => {
                    // the starts under way end, and with them their VMs and files
                    starting.shutdown().await;
                    drop(new);
                    return Err(match failed {
                        Ok((_, Err(e))) => e,
                        Err(e) => e.into(),
                        Ok((_, Ok(_))) => unreachable!(),
                    });
                }
            };
            new.insert(target, started);
        }

        let current = self.routes.current();
        for up in &advertised {
            if !current.advertised.iter().any(|old| old.host == up.host) {
                change.added.push(up.host.clone());
            }
        }
        for up in &current.advertised {
            if !advertised.iter().any(|new| new.host == up.host) {
                change.removed.push(up.host.clone());
            }
        }
        if change.is_empty() && advertised == current.advertised {
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
        });
        Ok(change)
    }

    /// Drops the removed targets every stream acknowledged by `generation`.
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
        self.routes.publish(Table {
            generation: current.generation,
            advertised: current.advertised.clone(),
            routes: current
                .routes
                .iter()
                .filter(|(target, _)| !expired.contains(target))
                .map(|(target, route)| (target.clone(), route.clone()))
                .collect(),
        });
    }
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
