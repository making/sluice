//! The route table -- the handler serving each advertised target -- and its
//! reload without a restart.
//!
//! A reload ([`Reloader::reload`]) reads the routes again and loads every
//! component of them before it touches anything: one failure (a bad config,
//! a component that does not compile, a truncated file, a failed fetch, pools
//! past the instance limit) rejects the whole reload and the current table
//! stays. Otherwise a new [`Table`] is published: a target whose component
//! did not change keeps its handler (and its warm pool), a changed component
//! gets a new handler on the same target, and the replaced handlers are
//! retired. Every node stream re-advertises a changed route set
//! (`NodeTask::stream`); a removed target stays resolvable until every stream
//! acknowledged its removal, since the server may have routed a connection to
//! it before, and its connections then shut down gracefully.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use crate::config::RouteSource;
use crate::keyvalue;
use crate::proto::Upstream;
use crate::wasm_host::{self, Binding, Handler, Instances, Loaded, Locator, Sandbox};

/// One route as configured: a host served by a component in a sandbox.
#[derive(Clone, PartialEq, Debug)]
pub struct Spec {
    pub host: String,
    pub component: Locator,
    pub sandbox: Sandbox,
}

/// The advertised target of a route: the component locator plus its
/// canonical sandbox query (omitted when all defaults).
pub fn route_target(locator: &str, sandbox: &Sandbox) -> String {
    match sandbox.query() {
        query if query.is_empty() => locator.to_string(),
        query => format!("{locator}?{query}"),
    }
}

/// A target's handler, and the token its connections watch.
#[derive(Clone)]
struct Entry {
    handler: Arc<Handler>,
    /// Cancelled once the target left the table: its connections shut down
    /// gracefully. Kept across the handlers a reload puts on the target.
    gone: CancellationToken,
}

/// The routes at one point: what is advertised and what serves it.
pub struct Table {
    /// Counts the reloads that changed the routes; the startup table is 1. A
    /// stream acknowledges the generation whose routes the server took.
    pub generation: u64,
    /// The upstreams every stream advertises.
    pub advertised: Vec<Upstream>,
    /// Target (`wasm:<locator>[?<sandbox>]`, the dial address) -> its entry:
    /// the advertised targets, plus removed ones until every stream
    /// acknowledged their removal.
    entries: HashMap<String, Entry>,
}

/// The current [`Table`], shared by every node stream and connection, and the
/// acknowledgements of the streams.
pub struct Routes {
    table: watch::Sender<Arc<Table>>,
    /// Live stream -> the newest generation the server acknowledged on it (0
    /// before its first acknowledgement).
    acks: watch::Sender<HashMap<u64, u64>>,
    next_stream: AtomicU64,
}

impl Routes {
    fn new(table: Table) -> Arc<Routes> {
        Arc::new(Routes {
            table: watch::Sender::new(Arc::new(table)),
            acks: watch::Sender::new(HashMap::new()),
            next_stream: AtomicU64::new(0),
        })
    }

    /// Routes serving `handlers` by target for good, advertising nothing.
    #[cfg(test)]
    pub fn fixed(handlers: HashMap<String, Arc<Handler>>) -> Arc<Routes> {
        Routes::new(Table {
            generation: 1,
            advertised: Vec::new(),
            entries: handlers
                .into_iter()
                .map(|(target, handler)| {
                    let gone = CancellationToken::new();
                    (target, Entry { handler, gone })
                })
                .collect(),
        })
    }

    pub fn current(&self) -> Arc<Table> {
        self.table.borrow().clone()
    }

    /// Follows the table: a stream re-advertises on a change.
    pub fn subscribe(&self) -> watch::Receiver<Arc<Table>> {
        self.table.subscribe()
    }

    /// What a connection to `address` serves; None for an unknown target.
    pub fn bind(self: &Arc<Self>, address: &str) -> Option<Binding> {
        let entry = self.table.borrow().entries.get(address)?.clone();
        let (routes, target) = (self.clone(), address.to_string());
        Some(Binding {
            opened: entry.handler,
            current: Box::new(move || {
                let table = routes.table.borrow();
                table
                    .entries
                    .get(&target)
                    .map(|entry| entry.handler.clone())
            }),
            gone: entry.gone,
        })
    }

    /// The handler serving a request for `host` (no port) on the listener:
    /// the route of that host, or the sole route whatever the host; None for
    /// an unknown host.
    pub fn by_host(&self, host: &str) -> Option<Arc<Handler>> {
        let table = self.table.borrow();
        let target = match table.advertised.as_slice() {
            [sole] => &sole.target_url,
            all => {
                &all.iter()
                    .find(|up| up.host.eq_ignore_ascii_case(host))?
                    .target_url
            }
        };
        table.entries.get(target).map(|entry| entry.handler.clone())
    }

    /// Registers a live stream, unregistered when the guard drops.
    pub fn stream(self: &Arc<Self>) -> StreamAcks {
        let id = self.next_stream.fetch_add(1, Ordering::Relaxed);
        self.acks.send_modify(|acks| {
            acks.insert(id, 0);
        });
        StreamAcks {
            routes: self.clone(),
            id,
        }
    }

    /// Resolves once every live stream acknowledged `generation` or a later
    /// one; at once without a live stream.
    pub(crate) async fn acknowledged(&self, generation: u64) {
        let mut acks = self.acks.subscribe();
        let _ = acks
            .wait_for(|acks| acks.values().all(|acked| *acked >= generation))
            .await;
    }

    fn publish(&self, table: Table) {
        self.table.send_replace(Arc::new(table));
    }
}

/// A live stream's acknowledgements; dropping it unregisters the stream.
pub struct StreamAcks {
    routes: Arc<Routes>,
    id: u64,
}

impl StreamAcks {
    /// The server took the routes of `generation` on this stream.
    pub fn acked(&self, generation: u64) {
        self.routes.acks.send_modify(|acks| {
            if let Some(acked) = acks.get_mut(&self.id) {
                *acked = (*acked).max(generation);
            }
        });
    }
}

impl Drop for StreamAcks {
    fn drop(&mut self) {
        self.routes.acks.send_modify(|acks| {
            acks.remove(&self.id);
        });
    }
}

/// A file's identity as a poll sees it; None for a missing file.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct Stamp {
    modified: Option<std::time::SystemTime>,
    len: u64,
    /// Changes when the file is replaced by a rename.
    #[cfg(unix)]
    inode: u64,
}

async fn stamp(path: &Path) -> Option<Stamp> {
    let metadata = tokio::fs::metadata(path).await.ok()?;
    Some(Stamp {
        modified: metadata.modified().ok(),
        len: metadata.len(),
        #[cfg(unix)]
        inode: std::os::unix::fs::MetadataExt::ino(&metadata),
    })
}

type Stamps = HashMap<PathBuf, Option<Stamp>>;

/// How a planned target gets its handler.
enum Planned {
    /// The current entry: the component did not change.
    Keep(Entry),
    /// A new handler on this component, with its keyvalue buckets.
    New(Arc<Loaded>, Option<Arc<keyvalue::Buckets>>),
}

/// A reload's outcome, by host.
#[derive(Default, Debug, PartialEq)]
pub struct Change {
    pub added: Vec<String>,
    pub removed: Vec<String>,
    /// Served by another target (another component or sandbox).
    pub retargeted: Vec<String>,
    /// Same target, new content.
    pub updated: Vec<String>,
}

impl std::fmt::Display for Change {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let parts = [
            ("added", &self.added),
            ("removed", &self.removed),
            ("retargeted", &self.retargeted),
            ("updated", &self.updated),
        ];
        let described: Vec<_> = parts
            .iter()
            .filter(|(_, hosts)| !hosts.is_empty())
            .map(|(what, hosts)| format!("{what} {}", hosts.join(", ")))
            .collect();
        f.write_str(&described.join("; "))
    }
}

/// Reloads the routes, on a change of the files they come from (polled) or
/// on request (SIGHUP), and retires the targets removed once the streams
/// acknowledged it. Reloads run one at a time, on this task.
pub struct Reloader {
    routes: Arc<Routes>,
    instances: Arc<Instances>,
    /// The keyvalue stores the routes' maps name.
    stores: Arc<keyvalue::Stores>,
    source: RouteSource,
    /// The files the table was read from (config, components), stamped
    /// before each was read.
    watched: Stamps,
    /// A change the last poll saw: reloaded once the next poll sees the same,
    /// so a file still being written is not read half way.
    pending: Option<Stamps>,
    /// Removed targets still in the table -> the generation that removed them.
    lingering: HashMap<String, u64>,
}

impl Reloader {
    /// Reads the routes and builds the startup table; an error is fatal.
    pub async fn start(source: RouteSource, instances: Arc<Instances>) -> Result<Reloader, String> {
        let mut watched = Stamps::new();
        let specs = read(&source, &mut watched).await?;
        let mut reloader = Reloader {
            routes: Routes::new(Table {
                generation: 0,
                advertised: Vec::new(),
                entries: HashMap::new(),
            }),
            instances,
            stores: source.stores().clone(),
            source,
            watched: Stamps::new(),
            pending: None,
            lingering: HashMap::new(),
        };
        let planned = reloader.plan(&specs, true, &mut watched).await?;
        reloader.commit(planned);
        reloader.watched = watched;
        Ok(reloader)
    }

    pub fn routes(&self) -> &Arc<Routes> {
        &self.routes
    }

    /// Reloads until `stop`: polls the watched files every `interval` (zero:
    /// never) and reloads on each `hangups` message, re-fetching remote
    /// components too.
    pub async fn run(
        mut self,
        interval: Duration,
        mut hangups: mpsc::Receiver<()>,
        stop: CancellationToken,
    ) {
        let mut ticks = (!interval.is_zero()).then(|| {
            let start = tokio::time::Instant::now() + interval;
            let mut ticks = tokio::time::interval_at(start, interval);
            ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            ticks
        });
        let mut hangups_open = true;
        loop {
            let oldest = self.lingering.values().min().copied();
            let routes = self.routes.clone();
            let acknowledged = async move {
                match oldest {
                    Some(generation) => routes.acknowledged(generation).await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                () = stop.cancelled() => return,
                hangup = hangups.recv(), if hangups_open => match hangup {
                    Some(()) => self.reload_logged("SIGHUP", true).await,
                    None => hangups_open = false,
                },
                _ = async { ticks.as_mut().expect("polling").tick().await }, if ticks.is_some() => {
                    if self.poll().await {
                        self.reload_logged("file change", false).await;
                    }
                }
                () = acknowledged => self.expire(oldest.expect("waited on it")),
            }
        }
    }

    async fn reload_logged(&mut self, reason: &str, refetch: bool) {
        match self.reload(refetch).await {
            Ok(Some(change)) => println!(
                "routes reloaded ({reason}), generation {}: {change}",
                self.routes.current().generation
            ),
            Ok(None) => println!("routes reload ({reason}): no change"),
            Err(e) => eprintln!(
                "routes reload ({reason}) failed; keeping generation {}: {e}",
                self.routes.current().generation
            ),
        }
    }

    /// Reads the routes again and publishes them, unless anything fails:
    /// then the current table stays. `refetch` fetches remote components
    /// again; else only a new remote target is fetched. None when nothing
    /// changed.
    pub async fn reload(&mut self, refetch: bool) -> Result<Option<Change>, String> {
        let mut watched = Stamps::new();
        let outcome = async {
            let specs = read(&self.source, &mut watched).await?;
            self.plan(&specs, refetch, &mut watched).await
        }
        .await;
        self.pending = None;
        match outcome {
            Ok(planned) => {
                self.watched = watched;
                Ok(self.commit(planned))
            }
            Err(e) => {
                // not retried until a file changes again
                for path in self.watched.keys() {
                    if !watched.contains_key(path) {
                        watched.insert(path.clone(), stamp(path).await);
                    }
                }
                self.watched = watched;
                Err(e)
            }
        }
    }

    /// Whether the watched files changed and stayed so since the last poll.
    async fn poll(&mut self) -> bool {
        let mut now = Stamps::new();
        for path in self.watched.keys() {
            now.insert(path.clone(), stamp(path).await);
        }
        if now == self.watched {
            self.pending = None;
            return false;
        }
        if self.pending.as_ref() == Some(&now) {
            return true;
        }
        self.pending = Some(now);
        false
    }

    /// Resolves every route's target and component; touches nothing.
    async fn plan(
        &self,
        specs: &[Spec],
        refetch: bool,
        watched: &mut Stamps,
    ) -> Result<Plan, String> {
        let current = self.routes.current();
        let mut plan = Plan {
            advertised: Vec::with_capacity(specs.len()),
            targets: Vec::new(),
        };
        let mut seen = HashSet::new();
        for spec in specs {
            let failed = |e: String| format!("wasm route '{}': {e}", spec.host);
            spec.sandbox.check().map_err(failed)?;
            if let Some(path) = spec.component.path() {
                // before the read: a write after it shows as a change
                watched.insert(path.to_path_buf(), stamp(path).await);
            }
            let locator = spec.component.target().await.map_err(failed)?;
            let target = route_target(&locator, &spec.sandbox);
            plan.advertised.push(Upstream {
                host: spec.host.clone(),
                target_url: target.clone(),
                rewrite_host: false,
                ..Default::default()
            });
            // routes on one target share its handler: one pool, one cap
            if !seen.insert(target.clone()) {
                continue;
            }
            // a lingering handler was retired: its pool is closed
            let existing = current
                .entries
                .get(&target)
                .filter(|_| !self.lingering.contains_key(&target));
            let planned = match (existing, &spec.component) {
                (Some(entry), Locator::Http(_)) if !refetch => Planned::Keep(entry.clone()),
                _ => {
                    let fetched = wasm_host::fetch(&spec.component).await.map_err(failed)?;
                    match existing {
                        Some(entry) if entry.handler.loaded().digest() == fetched.digest() => {
                            Planned::Keep(entry.clone())
                        }
                        _ => Planned::New(
                            wasm_host::compile(fetched, spec.sandbox.caps)
                                .await
                                .map_err(failed)?,
                            self.stores
                                .buckets(&spec.sandbox.keyvalue)
                                .await
                                .map_err(failed)?,
                        ),
                    }
                }
            };
            plan.targets.push((target, spec.sandbox.clone(), planned));
        }
        // every pooled unit holds a slot for good
        let pooled: usize = plan
            .targets
            .iter()
            .map(|(_, sandbox, _)| sandbox.pool)
            .sum();
        if pooled > self.instances.max() {
            return Err(format!(
                "the routes' pools need {pooled} instance slots, more than --max-instances {}",
                self.instances.max()
            ));
        }
        Ok(plan)
    }

    /// Publishes `plan` and retires the handlers it replaced; None when it
    /// changes nothing.
    fn commit(&mut self, plan: Plan) -> Option<Change> {
        let current = self.routes.current();
        let unchanged = plan.advertised == current.advertised
            && plan
                .targets
                .iter()
                .all(|(_, _, planned)| matches!(planned, Planned::Keep(_)));
        if unchanged && current.generation > 0 {
            return None;
        }
        let generation = current.generation + 1;
        let mut entries = HashMap::new();
        let mut replaced = Vec::new();
        let mut updated = HashSet::new();
        for (target, sandbox, planned) in plan.targets {
            self.lingering.remove(&target);
            let entry = match planned {
                Planned::Keep(entry) => entry,
                Planned::New(loaded, buckets) => {
                    println!(
                        "loaded {} (sha256 {}) for {target}",
                        loaded.label(),
                        loaded.digest()
                    );
                    let gone = match current.entries.get(&target) {
                        Some(old) => {
                            replaced.push(old.handler.clone());
                            updated.insert(target.clone());
                            old.gone.clone()
                        }
                        None => CancellationToken::new(),
                    };
                    let handler = Handler::new(loaded, sandbox, buckets, &self.instances)
                        .expect("the sandbox was checked");
                    Entry { handler, gone }
                }
            };
            entries.insert(target, entry);
        }
        for (target, entry) in &current.entries {
            if entries.contains_key(target) {
                continue;
            }
            // resolvable until every stream acknowledged the removal
            if !self.lingering.contains_key(target) {
                self.lingering.insert(target.clone(), generation);
                replaced.push(entry.handler.clone());
            }
            entries.insert(target.clone(), entry.clone());
        }
        let change = change(&current.advertised, &plan.advertised, &updated);
        self.routes.publish(Table {
            generation,
            advertised: plan.advertised,
            entries,
        });
        for handler in replaced {
            handler.retire();
        }
        Some(change)
    }

    /// Drops the removed targets every stream acknowledged by `generation`;
    /// their connections shut down gracefully.
    fn expire(&mut self, generation: u64) {
        let gone: Vec<String> = self
            .lingering
            .iter()
            .filter(|(_, removed)| **removed <= generation)
            .map(|(target, _)| target.clone())
            .collect();
        let current = self.routes.current();
        let mut entries = current.entries.clone();
        let mut tokens = Vec::new();
        for target in &gone {
            self.lingering.remove(target);
            if let Some(entry) = entries.remove(target) {
                tokens.push(entry.gone);
            }
        }
        self.routes.publish(Table {
            generation: current.generation,
            advertised: current.advertised.clone(),
            entries,
        });
        for token in tokens {
            token.cancel();
        }
        println!(
            "route target(s) acknowledged gone by every stream: {}",
            gone.join(", ")
        );
    }
}

/// Reads the routes of `source`, stamping its config file before the read.
async fn read(source: &RouteSource, watched: &mut Stamps) -> Result<Vec<Spec>, String> {
    if let Some(path) = source.config() {
        watched.insert(path.to_path_buf(), stamp(path).await);
    }
    let (specs, warnings) = source.read()?;
    for warning in warnings {
        eprintln!("{warning}");
    }
    Ok(specs)
}

/// The routes resolved by a reload: what to advertise, and each distinct
/// target in order with its sandbox and handler.
struct Plan {
    advertised: Vec<Upstream>,
    targets: Vec<(String, Sandbox, Planned)>,
}

/// The hosts `old` -> `new` changed; `updated` holds the targets whose
/// component content changed.
fn change(old: &[Upstream], new: &[Upstream], updated: &HashSet<String>) -> Change {
    let before: HashMap<_, _> = old.iter().map(|up| (&up.host, &up.target_url)).collect();
    let after: HashMap<_, _> = new.iter().map(|up| (&up.host, &up.target_url)).collect();
    let mut change = Change::default();
    for up in new {
        match before.get(&up.host) {
            None => change.added.push(up.host.clone()),
            Some(target) if **target != up.target_url => change.retargeted.push(up.host.clone()),
            Some(_) if updated.contains(&up.target_url) => change.updated.push(up.host.clone()),
            Some(_) => {}
        }
    }
    for up in old {
        if !after.contains_key(&up.host) {
            change.removed.push(up.host.clone());
        }
    }
    change
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::wasm_host::tests::guest_wasm;

    /// A directory of its own under the system temp dir, removed on drop.
    pub(crate) struct Scratch(pub(crate) PathBuf);

    impl Scratch {
        pub(crate) fn new(name: &str) -> Scratch {
            let dir = std::env::temp_dir().join(format!(
                "sluice-wicket-{name}-{}-{}",
                std::process::id(),
                uuid::Uuid::new_v4()
            ));
            std::fs::create_dir_all(&dir).unwrap();
            Scratch(dir)
        }

        /// Writes `contents` to `name` (by a rename: no reader sees it half
        /// written) and returns its path.
        pub(crate) fn write(&self, name: &str, contents: impl AsRef<[u8]>) -> PathBuf {
            let path = self.0.join(name);
            let partial = self.0.join(format!(".{name}.partial"));
            std::fs::write(&partial, contents).unwrap();
            std::fs::rename(&partial, &path).unwrap();
            path
        }

        /// Copies the example guest `guest` to `name`.
        pub(crate) fn guest(&self, name: &str, guest: &str) -> PathBuf {
            self.write(name, std::fs::read(guest_wasm(guest)).unwrap())
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    impl Table {
        /// Targets in the table, the lingering ones included.
        pub(crate) fn targets(&self) -> usize {
            self.entries.len()
        }

        pub(crate) fn handler(&self, target: &str) -> Option<&Arc<Handler>> {
            self.entries.get(target).map(|entry| &entry.handler)
        }

        /// The advertised target of `host`.
        pub(crate) fn target(&self, host: &str) -> String {
            self.advertised
                .iter()
                .find(|up| up.host == host)
                .unwrap_or_else(|| panic!("{host} advertised"))
                .target_url
                .clone()
        }

        /// A table advertising `advertised`, serving nothing.
        pub(crate) fn advertising(generation: u64, advertised: Vec<Upstream>) -> Table {
            Table {
                generation,
                advertised,
                entries: HashMap::new(),
            }
        }
    }

    /// Serves `body` to every GET on an ephemeral port; the url of
    /// `/<name>` there and the count of requests served.
    async fn http_component(name: &str, body: Vec<u8>) -> (String, Arc<AtomicU64>) {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/{name}", listener.local_addr().unwrap());
        let served = Arc::new(AtomicU64::new(0));
        let count = served.clone();
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let mut head = vec![0u8; 4096];
                let _ = socket.read(&mut head).await;
                count.fetch_add(1, Ordering::SeqCst);
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                    body.len()
                );
                let _ = socket.write_all(response.as_bytes()).await;
                let _ = socket.write_all(&body).await;
            }
        });
        (url, served)
    }

    /// Polls `condition` every 5ms until it holds; panics past 10s.
    async fn eventually(what: &str, condition: impl Fn() -> bool) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while !condition() {
            assert!(tokio::time::Instant::now() < deadline, "{what}");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// Routes naming one component with the same options share a dial
    /// address, so they share one handler: its pool starts once (no unit
    /// orphaned on a slot), and its cap bounds the routes together.
    #[tokio::test]
    async fn routes_sharing_a_target_share_one_handler() {
        let path = guest_wasm("hello");
        let route = |host: &str| Spec {
            host: host.into(),
            component: Locator::parse(path.to_str().unwrap()).unwrap(),
            sandbox: Sandbox {
                pool: 1,
                instances: Some(1),
                ..Sandbox::default()
            },
        };
        let instances = Instances::new(4);
        let source = RouteSource::flags(vec![route("a.local"), route("b.local")]);
        let reloader = Reloader::start(source, instances.clone()).await.unwrap();
        let table = reloader.routes().current();
        let hosts: Vec<_> = table.advertised.iter().map(|up| up.host.as_str()).collect();
        assert_eq!(hosts, ["a.local", "b.local"], "both hosts advertised");
        assert_eq!(
            table.advertised[0].target_url,
            table.advertised[1].target_url
        );
        assert_eq!(table.targets(), 1);
        assert_eq!(instances.held(), 1, "one pool started");
    }

    fn config(routes: &[(&str, &str, &str)]) -> String {
        let mut yaml = String::from("routes:\n");
        for (host, component, sandbox) in routes {
            yaml.push_str(&format!(
                "  - host: {host}\n    component: {component}\n    sandbox: {{{sandbox}}}\n"
            ));
        }
        yaml
    }

    /// A reload publishes the routes read again: an added and a removed host,
    /// a host moved to another sandbox (another target) and a component
    /// changed on disk (its target keeps, with a new handler); an unchanged
    /// route keeps its handler. The replaced and removed handlers' pools are
    /// retired, and the removed targets stay resolvable until acknowledged.
    /// Read again unchanged, nothing is published.
    #[tokio::test]
    async fn a_reload_publishes_the_changed_routes_and_keeps_the_unchanged_handlers() {
        let dir = Scratch::new("reload");
        for name in ["a", "b", "c", "d", "e"] {
            dir.guest(&format!("{name}.wasm"), "hello");
        }
        let hello = std::fs::read(guest_wasm("hello")).unwrap();
        let (remote, fetches) = http_component("f.wasm", hello).await;
        let path = dir.write(
            "wicket.yaml",
            config(&[
                ("a.local", "a.wasm", "pool: 1"),
                ("b.local", "b.wasm", "pool: 1"),
                ("c.local", "c.wasm", ""),
                ("d.local", "d.wasm", ""),
                ("f.local", &remote, ""),
            ]),
        );
        let instances = Instances::new(4);
        let mut reloader = Reloader::start(RouteSource::file(&path).unwrap(), instances.clone())
            .await
            .unwrap();
        let before = reloader.routes().current();
        assert_eq!(before.generation, 1);
        assert_eq!(instances.held(), 2, "precondition: a's and b's pools");

        dir.guest("a.wasm", "fetch");
        dir.write(
            "wicket.yaml",
            config(&[
                ("a.local", "a.wasm", "pool: 1"),
                ("c.local", "c.wasm", "budget: 5s"),
                ("d.local", "d.wasm", ""),
                ("e.local", "e.wasm", ""),
                ("f.local", &remote, ""),
            ]),
        );
        let change = reloader.reload(false).await.unwrap();
        assert_eq!(
            change,
            Some(Change {
                added: vec!["e.local".into()],
                removed: vec!["b.local".into()],
                retargeted: vec!["c.local".into()],
                updated: vec!["a.local".into()],
            })
        );
        let after = reloader.routes().current();
        assert_eq!(after.generation, 2);
        let a = before.target("a.local");
        assert_eq!(after.target("a.local"), a, "a keeps its target");
        assert!(!Arc::ptr_eq(
            before.handler(&a).unwrap(),
            after.handler(&a).unwrap()
        ));
        assert_ne!(
            before.handler(&a).unwrap().loaded().digest(),
            after.handler(&a).unwrap().loaded().digest()
        );
        let d = before.target("d.local");
        assert!(Arc::ptr_eq(
            before.handler(&d).unwrap(),
            after.handler(&d).unwrap()
        ));
        assert_eq!(after.advertised.len(), 5);
        assert_eq!(
            after.targets(),
            7,
            "b's and c's old targets linger: no stream acknowledged yet"
        );
        eventually("the replaced pools free their slots", || {
            instances.held() == 1
        })
        .await;

        assert_eq!(reloader.reload(false).await, Ok(None));
        assert_eq!(reloader.routes().current().generation, 2);

        // back before its removal was acknowledged: a new handler, since the
        // lingering one was retired
        dir.write(
            "wicket.yaml",
            config(&[
                ("a.local", "a.wasm", "pool: 1"),
                ("b.local", "b.wasm", "pool: 1"),
                ("c.local", "c.wasm", "budget: 5s"),
                ("d.local", "d.wasm", ""),
                ("e.local", "e.wasm", ""),
                ("f.local", &remote, ""),
            ]),
        );
        let change = reloader.reload(false).await.unwrap().unwrap();
        assert_eq!(change.added, ["b.local"]);
        let b = before.target("b.local");
        assert!(!Arc::ptr_eq(
            before.handler(&b).unwrap(),
            reloader.routes().current().handler(&b).unwrap()
        ));
        eventually("b's new pool takes its slot", || instances.held() == 2).await;

        // a remote component is fetched when its route is new, or on request
        assert_eq!(fetches.load(Ordering::SeqCst), 1);
        assert_eq!(reloader.reload(true).await, Ok(None));
        assert_eq!(fetches.load(Ordering::SeqCst), 2);
    }

    /// Any failure rejects the whole reload: the current table stays.
    #[tokio::test]
    async fn a_failed_reload_keeps_the_current_table() {
        let dir = Scratch::new("failed");
        dir.guest("a.wasm", "hello");
        let good = config(&[("a.local", "a.wasm", "")]);
        let path = dir.write("wicket.yaml", &good);
        let mut reloader = Reloader::start(RouteSource::file(&path).unwrap(), Instances::new(4))
            .await
            .unwrap();
        let table = reloader.routes().current();
        let hello = std::fs::read(guest_wasm("hello")).unwrap();
        let silent = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let hung = format!("http://{}/a.wasm", silent.local_addr().unwrap());
        let cases: [(&str, String, &[u8], &str); 8] = [
            ("bad yaml", "routes: [".into(), &hello, "wicket.yaml"),
            (
                "unknown key",
                config(&[("a.local", "a.wasm", "fuel: 1")]),
                &hello,
                "unknown field",
            ),
            (
                "truncated component",
                good.clone(),
                &hello[..100],
                "component",
            ),
            (
                "missing component",
                config(&[("a.local", "missing.wasm", "")]),
                &hello,
                "missing.wasm",
            ),
            (
                "failed fetch",
                config(&[("a.local", "http://127.0.0.1:1/a.wasm", "")]),
                &hello,
                "GET http://127.0.0.1:1/a.wasm",
            ),
            (
                "a fetch never answered",
                config(&[("a.local", &hung, "")]),
                &hello,
                "no answer within 1s",
            ),
            (
                "pools past the limit",
                config(&[("a.local", "a.wasm", "pool: 5")]),
                &hello,
                "need 5 instance slots, more than --max-instances 4",
            ),
            (
                "a host twice",
                config(&[("a.local", "a.wasm", ""), ("a.local", "a.wasm", "pool: 1")]),
                &hello,
                "route host 'a.local' is configured twice",
            ),
        ];
        for (what, yaml, component, error) in cases {
            dir.write("wicket.yaml", &yaml);
            dir.write("a.wasm", component);
            let failed = reloader.reload(true).await;
            assert!(
                matches!(&failed, Err(e) if e.contains(error)),
                "{what}: {failed:?}"
            );
            assert!(
                Arc::ptr_eq(&table, &reloader.routes().current()),
                "{what}: the table stays"
            );
        }
        dir.write("wicket.yaml", &good);
        dir.write("a.wasm", &hello);
        assert_eq!(reloader.reload(true).await, Ok(None), "back as it was");
    }

    /// A change of a watched file is reloaded once a poll saw it unchanged
    /// since the previous one, not while it keeps changing; the reloader's
    /// task does so on its interval. The routes may start empty.
    #[tokio::test]
    async fn a_watched_change_is_reloaded_once_it_held_for_a_poll() {
        let dir = Scratch::new("watch");
        dir.guest("a.wasm", "hello");
        dir.guest("b.wasm", "hello");
        let path = dir.write("wicket.yaml", "routes: []\n");
        let mut reloader = Reloader::start(RouteSource::file(&path).unwrap(), Instances::new(4))
            .await
            .unwrap();
        assert_eq!(reloader.routes().current().advertised, []);
        assert!(!reloader.poll().await, "nothing changed");
        dir.write(
            "wicket.yaml",
            config(&[("a.local", "a.wasm", ""), ("b.local", "b.wasm", "")]),
        );
        assert!(!reloader.poll().await, "changed: wait a poll");
        dir.write(
            "wicket.yaml",
            config(&[("a.local", "a.wasm", ""), ("c.local", "b.wasm", "")]),
        );
        assert!(!reloader.poll().await, "changed again: wait another");
        assert!(reloader.poll().await, "held for a poll");
        reloader.reload(false).await.unwrap();
        assert!(!reloader.poll().await, "read: nothing pending");

        // a component the routes read is watched too
        dir.guest("b.wasm", "fetch");
        let routes = reloader.routes().clone();
        let mut table = routes.subscribe();
        let stop = CancellationToken::new();
        let (_hangups, hangups_received) = mpsc::channel(1);
        let task =
            tokio::spawn(reloader.run(Duration::from_millis(20), hangups_received, stop.clone()));
        tokio::time::timeout(
            Duration::from_secs(10),
            table.wait_for(|table| table.generation == 3),
        )
        .await
        .expect("reloaded on its interval")
        .unwrap();
        stop.cancel();
        task.await.unwrap();
    }
}
