//! The wickets serve what the state wants. Each index of an app's web process is placed on a
//! wicket (`crate::placement`), and an app started with a droplet and a route is, on each wicket
//! holding some of its indices, an app of that wicket named by its guid, its share: its routes'
//! hosts, the snapshot's component and sandbox (a deployment's under way once it reached the
//! wicket, else the one before: `crate::deployments`) -- its env, which a wicket lists by the
//! names only, told by the snapshot's tag -- and its indices' count as the route's `instances`,
//! those of them below the annotated `warm` as its `warm` (the warm ones are the lowest, as a wasm
//! wicket's units are `0..warm`), each taking `concurrency` requests at once, or a droplet's rootfs
//! as a Firecracker wicket's VMs: [`vm_sandbox`]; stopped, scaled to 0, or without a route, it is
//! none.
//! The desired state is the cf-api's: at startup and whenever a wicket's revision moves, its apps
//! of the owner are listed and the difference put / deleted, each wicket watched apart. The
//! wickets serve on while the cf-api is down.
//!
//! A put may take an app's health check timeout, so nothing but that app's share waits for it:
//! one edit of an app at a time ([`Controller::hold`]), one put of a share on a wicket
//! ([`Controller::hold_share`]); a start puts its shares at once, a listing's differences each in
//! a task of its own. The state's changes are atomic, an edit tried against the wickets making its
//! change on the state as it is by then. A share whose VMs did not start is not tried again until
//! what is asked of it changes, its wicket came back, or the cf-api started.
//!
//! An app of the owner the state does not know is left where it is, and logged: a state that
//! never knew an app did not put it (a lost data directory, another cf-api on the wicket), and its
//! absence says nothing of what is wanted. One the state knows but places none of on a wicket is
//! removed from it. What the state deletes it removes itself, at once or, a wicket not taking it,
//! from `State::removals`.
//!
//! Each wicket runs apps one way, the stack it lists, kept with the state: an app's share is on
//! the wickets of its droplet's stack (`crate::placement`), wasm and Firecracker wickets side by
//! side. The services they offer are what every wasm wicket does.
//!
//! A wicket lists which it is: its id, made at its first start and kept in its console's
//! directory, its flume node and its build. Two names of one id are refused at the start
//! ([`distinct`]); one coming to another's id later is logged and placed on no more. An id
//! changed behind a name, kept with the state, is logged and its apps put again, as on a lost
//! directory. A wicket listing no id (one predating it) is taken as it
//! is: logged once, its lines' instances told as of one wicket's. Its budget leaves the apps placed
//! on it what the routes that are not theirs do not keep (`crate::placement`): those beyond what
//! its apps of the owner list. A share it refuses for room has an index passed to a wicket with
//! room, the wicket taking none until its revision moves.
//!
//! The data of a deleted keyvalue instance is deleted once no app maps its namespace, a wicket's
//! draining routes included: of a store of each wicket's host through every wicket, each telling
//! its own routes, of a store the wickets share through one, once every wicket says none of its
//! routes maps it. Tried at the deletion and again after each listing. Which stores they share
//! is what they list (`crate::services::Offer::stores`); an app mapping a namespace of a store of
//! each one's host runs on the wicket holding its data (`crate::placement`).
//!
//! A wicket listing itself leaving (told to stop, it serves on until the apps it waits for are
//! elsewhere) has the indices of the started apps on it moved to the wickets with room, every
//! app's at once ([`Controller::evacuate`]): each started on its new wicket, then, once that one
//! serves it, out of the leaving one's share ([`Controller::relocate`]), which removes the app
//! from it with its last index; then the wicket is told the owner moved what it could, the
//! indices with nowhere to go staying until it stops. A wicket back, or new, takes the indices
//! that spread an app again the same way round, one at a time ([`Controller::respread`]). A
//! restart of the cf-api takes up the moves under way ([`Controller::resume_moves`]).
//!
//! A wicket that has not answered for `--wicket-away-grace` is gone, as Diego takes a cell whose
//! presence expired: placed on no more, the indices of the started apps on it move to the
//! wickets with room as off a wicket leaving ([`Away::Gone`]), removing nothing from it, which
//! answers nothing, as long as it does not answer. Back, its listing removes what moved, and it
//! is placed on again once it has answered for as long, taking the indices that spread an app
//! again: a wicket going and coming moves no app back and forth.
//!
//! A share is put with `held-ms` ([`HELD`]) to a wicket listing its start: started again, the
//! wicket holds it, serving it not, until the cf-api confirms it as wanted
//! ([`Controller::confirm`]) or puts it again, and removes what it holds that is not wanted: back
//! from a crash, a wicket serves no app deleted, stopped, moved or changed meanwhile. It is
//! listed again [`MAX_PAUSE`] at most after it answers again; the cf-api down, the wicket
//! serves what it holds once `held-ms` passed.
//!
//! Of several instances, one at a time reconciles the wickets, in its turn of the lease
//! (`crate::lease`, [`Controller::lead`]): the watches, the confirmations, the evacuations and
//! moves, the purges, the deployments' steps are its turn's, ended with it, the next taking them
//! up from the state as a start does. Its writes carry the turn's epoch, a wicket refusing one
//! below the highest it saw. What it saw of the wickets (their answers, budgets, offers, ...) is
//! the state's for every instance (`Store::observe`), each placing by it; the others list no
//! wicket but for a request (the process stats).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::future::join_all;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::blobs::Reading;
use crate::lease::Term;
use crate::logs::Logs;
use crate::placement::{Budget, Evacuated, Move};
use crate::services::Offer;
use crate::staging::Cnb;
use crate::stop::Stop;
use crate::store::{
    App, Blob, Built, HealthCheck, Lifetime, Outcome, Purge, ReadinessCheck, Removal, State, Store,
};
use crate::wicket::{
    AppBody, ComponentRef, Failure, FilesRef, ListedApp, Listing, MadeImage, Runtime, Sandbox,
    VmSandbox, WasmSandbox, Wicket,
};

/// A lock of each key: what is done one at a time.
type Locks<K> = Mutex<HashMap<K, Arc<tokio::sync::Mutex<()>>>>;

/// Between two looks at whether a wicket serves an index moved to it: its servers' acknowledgement
/// comes within a round trip.
const LOOK: Duration = Duration::from_millis(100);

/// How long a start of a wicket holds a share at most, until the cf-api confirms it (`held-ms`):
/// the cf-api lists a wicket back within [`MAX_PAUSE`].
pub const HELD: Duration = Duration::from_secs(15);

/// The first pause between two listings of a wicket that does not answer, and the longest: one
/// back is listed that soon, what it holds confirmed.
const MIN_PAUSE: Duration = Duration::from_secs(1);
pub const MAX_PAUSE: Duration = Duration::from_secs(2);

/// Why the indices on a wicket move off it.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Away {
    /// It lists itself leaving: told to stop, it serves on until its apps are elsewhere.
    Leaving,
    /// It has not answered for `--wicket-away-grace`: what is on it is removed once it answers.
    Gone,
}

/// A wicket's evacuation, as the cf-api goes through it while the wicket lists itself leaving or
/// is gone.
struct Evacuation {
    away: Away,
    /// Its task runs ([`Controller::evacuate`]).
    running: bool,
    /// The apps of which an index stays on it, for room or its data: said once, tried again.
    stayed: BTreeSet<String>,
    /// The apps whose moves were refused or not served: not tried again.
    undone: BTreeSet<String>,
    /// It was told the owner moved what it could.
    told: bool,
}

impl Evacuation {
    fn new(away: Away) -> Evacuation {
        Evacuation {
            away,
            running: false,
            stayed: BTreeSet::new(),
            undone: BTreeSet::new(),
            told: false,
        }
    }
}

/// What became of an app's moves ([`Controller::relocate`]).
#[derive(Debug, PartialEq)]
enum Relocated {
    /// Each index serves on its new wicket.
    All,
    /// Some stay where they were, for room alone.
    NoRoom,
    /// Some stay where they were, refused or not served.
    Undone,
}

/// What a wicket listed of itself that the leader alone keeps (the rest is the state's:
/// [`Controller::note`]).
struct Noted {
    /// None from a wicket predating it.
    id: Option<String>,
    revision: u64,
}

/// The work of a turn reconciling the wickets: cancelled at its end, which waits for it.
struct Work {
    cancel: CancellationToken,
    tasks: TaskTracker,
}

impl Work {
    /// Runs `task` until the turn's end.
    fn spawn(&self, task: impl Future<Output = ()> + Send + 'static) {
        let cancel = self.cancel.clone();
        self.tasks.spawn(async move {
            cancel.run_until_cancelled(task).await;
        });
    }
}

pub struct Controller {
    pub store: Arc<Store>,
    /// The wickets, as configured.
    pub wickets: Vec<Arc<Wicket>>,
    pub logs: Arc<Logs>,
    /// What is edited one at a time, by guid ([`Controller::hold`]).
    held: Locks<String>,
    /// What is put one at a time: an app's share on a wicket, by guid and wicket name
    /// ([`Controller::hold_share`]).
    shares: Locks<(String, String)>,
    /// The shares whose VMs did not start, as they were put, by wicket name and guid.
    did_not_start: Mutex<BTreeMap<(String, String), AppBody>>,
    /// What each wicket last listed of itself, by name.
    listed: Mutex<BTreeMap<String, Noted>>,
    /// The apps of the owner the state does not know, by wicket name and app name, as logged:
    /// once each.
    strangers: Mutex<BTreeSet<(String, String)>>,
    /// Held deleting the data of a store the wickets share: through one wicket at a time.
    purging: tokio::sync::Mutex<()>,
    /// The wickets leaving, by name, while they list themselves so.
    evacuations: Mutex<BTreeMap<String, Evacuation>>,
    /// The wickets whose apps' spread the indices moving to them restore, by name, while they do
    /// ([`Controller::respread`]).
    respreading: Mutex<BTreeSet<String>>,
    /// How long a wicket may not answer before it is gone: 0, never.
    away_grace: Duration,
    /// The builder staging source; None: no source is staged.
    pub cnb: Option<Cnb>,
    /// The wickets whose last listing failed, by name.
    down: Mutex<BTreeSet<String>>,
    /// The wickets whose watch's first listing came back, answered or not, by name.
    listed_once: Mutex<BTreeSet<String>>,
    /// The cf-api's stop: what it waits for of the puts, and whether one may begin.
    pub stop: Stop,
    /// The apps whose indices a wicket's stack, newly listed, placed again: their shares are
    /// to apply ([`Controller::apply_placed`]).
    placed_again: Mutex<BTreeSet<String>>,
    /// The work of this instance's turn reconciling the wickets, while it has one.
    work: Mutex<Option<Work>>,
    /// The deployments whose steps this turn takes, by guid.
    driving: Mutex<BTreeSet<String>>,
    /// The purges this turn tried as they came, by store and namespace: tried again after the
    /// listings.
    purges_tried: Mutex<BTreeSet<(String, String)>>,
}

impl Controller {
    /// The controller of `wickets`, of which one not answering for `away_grace` is gone (0:
    /// never), source staged by `cnb`.
    pub fn new(
        store: Arc<Store>,
        wickets: Vec<Arc<Wicket>>,
        logs: Arc<Logs>,
        away_grace: Duration,
        cnb: Option<Cnb>,
    ) -> Controller {
        Controller {
            store,
            wickets,
            logs,
            away_grace,
            cnb,
            down: Mutex::default(),
            listed_once: Mutex::default(),
            stop: Stop::default(),
            held: Mutex::default(),
            shares: Mutex::default(),
            did_not_start: Mutex::default(),
            listed: Mutex::default(),
            strangers: Mutex::default(),
            purging: tokio::sync::Mutex::default(),
            evacuations: Mutex::default(),
            respreading: Mutex::default(),
            placed_again: Mutex::default(),
            work: Mutex::default(),
            driving: Mutex::default(),
            purges_tried: Mutex::default(),
        }
    }

    /// Reconciles the wickets in each turn of this instance (`Store::terms`), from its begin as
    /// a start does (the deployments and moves under way taken up, every wicket watched) to its
    /// end, which stops its work, its writes carrying the turn's epoch meanwhile. What it saw of
    /// the wickets and did not write is forgotten at the end.
    pub async fn lead(self: Arc<Self>) {
        let mut terms = self.store.terms();
        loop {
            let term = *terms.borrow_and_update();
            if let Some(term) = term {
                let cancel = self.begin(term);
                // the turn ends with this too: a stop ends the work under way
                let _ending = cancel.clone().drop_guard();
                loop {
                    if terms.changed().await.is_err() {
                        return;
                    }
                    if *terms.borrow() != Some(term) {
                        break;
                    }
                }
                self.end().await;
            } else if terms.changed().await.is_err() {
                return;
            }
        }
    }

    /// Begins the turn `term`: its work under way. Its token, the work's end.
    fn begin(self: &Arc<Self>, term: Term) -> CancellationToken {
        for wicket in &self.wickets {
            wicket.set_epoch(term.epoch);
        }
        let cancel = CancellationToken::new();
        *self.work.lock().expect("no panic under the lock") = Some(Work {
            cancel: cancel.clone(),
            tasks: TaskTracker::new(),
        });
        // a turn begins as a start: what was a start's to know is learned anew
        (self.listed_once.lock().expect("no panic under the lock")).clear();
        self.resume();
        self.spawn(self.clone().watch());
        self.spawn(self.clone().follow());
        cancel
    }

    /// Ends the turn: its work stopped and waited for, what it saw of the wickets and did not
    /// write forgotten, its writes carrying no epoch from then on.
    async fn end(&self) {
        let work = (self.work.lock().expect("no panic under the lock")).take();
        if let Some(work) = work {
            work.cancel.cancel();
            work.tasks.close();
            work.tasks.wait().await;
        }
        for wicket in &self.wickets {
            wicket.set_epoch(None);
        }
        self.store.forget_observed().await;
        for held in [&self.respreading, &self.placed_again, &self.driving] {
            held.lock().expect("no panic under the lock").clear();
        }
        (self.evacuations.lock().expect("no panic under the lock")).clear();
        (self.did_not_start.lock().expect("no panic under the lock")).clear();
        (self.purges_tried.lock().expect("no panic under the lock")).clear();
        (self.listed.lock().expect("no panic under the lock")).clear();
        (self.down.lock().expect("no panic under the lock")).clear();
    }

    /// Runs `task` as work of this instance's turn, if it has one: it ends with it.
    pub(crate) fn spawn(&self, task: impl Future<Output = ()> + Send + 'static) {
        let work = self.work.lock().expect("no panic under the lock");
        if let Some(work) = work.as_ref() {
            work.spawn(task);
        }
    }

    /// Whether this instance reconciles the wickets now: in a turn whose work runs.
    pub fn leads(&self) -> bool {
        (self.work.lock().expect("no panic under the lock")).is_some()
    }

    /// Takes up, while this instance's turn lasts, the deployments under way it does not
    /// ([`Controller::take_up`]) and tries the purges it has not as they come: the requests of
    /// any instance make them.
    async fn follow(self: Arc<Self>) {
        let mut changes = self.store.changes();
        loop {
            let (stepping, purges) = self.store.read(|state| {
                let deployments = state.deployments.iter();
                let stepping: Vec<(String, String)> = (deployments.filter(|kept| kept.stepping()))
                    .map(|kept| (kept.app.clone(), kept.guid.clone()))
                    .collect();
                let purges: BTreeSet<(String, String)> = (state.purges.iter())
                    .map(|purge| (purge.store.clone(), purge.namespace.clone()))
                    .collect();
                (stepping, purges)
            });
            for (app, deployment) in stepping {
                self.take_up(&app, &deployment);
            }
            let new = {
                let mut tried = self.purges_tried.lock().expect("no panic under the lock");
                let new = purges.iter().any(|purge| !tried.contains(purge));
                *tried = purges;
                new
            };
            if new {
                let controller = self.clone();
                self.spawn(async move { controller.purge_pending().await });
            }
            if changes.changed().await.is_err() {
                return;
            }
        }
    }

    /// Takes the steps of the deployment `deployment` of the app `guid` in this instance's
    /// turn, unless it does already: none but the turn's (`crate::deployments`).
    pub fn take_up(self: &Arc<Self>, guid: &str, deployment: &str) {
        let work = self.work.lock().expect("no panic under the lock");
        let Some(work) = work.as_ref() else {
            return;
        };
        let mut driving = self.driving.lock().expect("no panic under the lock");
        if !driving.insert(deployment.to_string()) {
            return;
        }
        let (controller, guid, deployment) =
            (self.clone(), guid.to_string(), deployment.to_string());
        work.spawn(async move {
            controller.clone().deploy(guid, deployment.clone()).await;
            (controller.driving.lock().expect("no panic under the lock")).remove(&deployment);
        });
    }

    /// The owner the apps are put under.
    pub fn owner(&self) -> &str {
        self.wickets
            .first()
            .map_or(crate::OWNER, |wicket| wicket.owner.as_str())
    }

    /// Waits for the edit under way of the app or route `guid`, if any: its trial against the
    /// wickets and its commit are not interleaved with another's. A route is held before its
    /// apps, and an edit's apps at once ([`Controller::hold_all`]).
    pub async fn hold(&self, guid: &str) -> Held {
        lock_of(&self.held, guid.to_string()).lock_owned().await
    }

    /// [`Controller::hold`] of the apps `guids`, in an order every edit takes them in.
    pub async fn hold_all(&self, guids: &[String]) -> Vec<Held> {
        let mut sorted: Vec<&String> = guids.iter().collect();
        sorted.sort();
        sorted.dedup();
        let mut held = Vec::with_capacity(sorted.len());
        for guid in sorted {
            held.push(self.hold(guid).await);
        }
        held
    }

    /// Waits for the put under way of the app `guid`'s share on `wicket`, if any: a put, and an
    /// edit's from its trial to its commit, is not interleaved with another of that share. Taken
    /// with the app held, if at all ([`Controller::hold`]); a listing's difference does not
    /// wait for it.
    pub async fn hold_share(&self, guid: &str, wicket: &str) -> Held {
        let share = (guid.to_string(), wicket.to_string());
        lock_of(&self.shares, share).lock_owned().await
    }

    /// Deletes the app `guid` (`name`): gone from every wicket first; one not taking that
    /// removes it when it does. Each share is held until the state no longer has the app: a
    /// listing of a wicket that answered first does not put it back meanwhile. The cache of
    /// its builds of source is forgotten where a wicket keeps one.
    pub async fn remove(&self, guid: &str, name: &str) {
        let _app = self.hold(guid).await;
        let built = self.store.read(|state| {
            (state.droplets.iter()).any(|droplet| droplet.app == guid && droplet.built.is_some())
        });
        if built {
            join_all(self.makers().into_iter().map(|wicket| async move {
                if let Err(e) = wicket.forget_cache(guid).await {
                    println!(
                        "[{guid}] API: {} kept the cache of its builds: {e}",
                        wicket.label
                    );
                }
            }))
            .await;
        }
        let removed = join_all(self.wickets.iter().map(|wicket| async move {
            let share = self.hold_share(guid, &wicket.name).await;
            let removed = self.apply(wicket, guid, None).await;
            (wicket.name.clone(), removed, share)
        }))
        .await;
        let blobs = self
            .store
            .update(|state| {
                for (wicket, removed, _) in &removed {
                    if removed.as_ref().is_err_and(Failure::retried) {
                        state.removals.push(Removal {
                            app: guid.to_string(),
                            wicket: wicket.clone(),
                        });
                    }
                }
                state.remove_app(guid)
            })
            .await;
        self.store.delete(&blobs).await;
        self.logs.forget(guid);
        self.forget(guid);
        println!("[{guid}] API: deleted app {name}");
    }

    /// `guid`, deleted, is held no more.
    pub fn forget(&self, guid: &str) {
        let mut held = self.held.lock().expect("no panic under the lock");
        held.remove(guid);
        let mut shares = self.shares.lock().expect("no panic under the lock");
        shares.retain(|(app, _), _| app != guid);
    }

    /// The shares of `app` as `state` wants it, by wicket name: on each wicket holding some of its
    /// indices the app of them, of the snapshot it serves (a deployment's under way, or the one
    /// before where it has not reached: [`App::running_on`]). None when the app is stopped,
    /// scaled to 0, was never started with a droplet, or has no route.
    pub fn desired(state: &State, app: &App, owner: &str) -> BTreeMap<String, AppBody> {
        if app.running.is_none() || !app.started || app.process.instances == 0 {
            return BTreeMap::new();
        }
        let mut hosts: Vec<String> = state
            .routes_of(&app.guid)
            .map(|route| state.url(route))
            .collect();
        if hosts.is_empty() {
            return BTreeMap::new();
        }
        hosts.sort();
        let timeout = app
            .process
            .health_check
            .timeout
            .unwrap_or(DEFAULT_HEALTH_CHECK_TIMEOUT);
        let share = |wicket: &str, indices: &[u64]| {
            let running = app.running_on(wicket)?;
            let rootfs = running.stack() == Runtime::Firecracker;
            // the lowest indices are the warm ones: of a component all unless annotated, of a VM
            // those its wicket keeps unless annotated
            let warm = match rootfs {
                true => running.lifetime.warm,
                false => Some(running.lifetime.warm.unwrap_or(app.process.instances)),
            };
            let instances = indices.len() as u64;
            let warm =
                warm.map(|warm| indices.iter().filter(|index| **index < warm).count() as u64);
            let sandbox = match rootfs {
                true => Sandbox::Vm(vm_sandbox(
                    running.memory_mib,
                    instances,
                    timeout,
                    &running.env,
                    &running.egress,
                    &Lifetime {
                        warm,
                        ..running.lifetime.clone()
                    },
                    &running.readiness,
                    &running.liveness,
                )),
                false => Sandbox::Wasm(WasmSandbox {
                    wasi: running.wasi.clone(),
                    budget_ms: running.budget_ms,
                    memory_mib: Some(running.memory_mib),
                    warm,
                    instances: Some(instances),
                    keep_ms: running.lifetime.keep_ms,
                    concurrency: Some(running.lifetime.concurrency.unwrap_or(DEFAULT_CONCURRENCY)),
                    keyvalue: running.keyvalue.clone(),
                    env: running.env.clone(),
                }),
            };
            Some(AppBody {
                owner: owner.to_string(),
                hosts: hosts.clone(),
                component: ComponentRef {
                    sha256: running.sha256_on(wicket).to_string(),
                    name: running.file.clone(),
                },
                files: running.files.clone().map(|sha256| FilesRef { sha256 }),
                sandbox,
                tag: running.tag.clone(),
                // as the wicket takes it (`Controller::apply`)
                held_ms: None,
            })
        };
        let indices = state.indices(app).into_iter();
        indices
            .filter_map(|(wicket, indices)| {
                let share = share(&wicket, &indices)?;
                Some((wicket, share))
            })
            .collect()
    }

    /// The share of the app `guid` on the wicket `wicket` in `state`.
    pub fn desired_on(&self, state: &State, guid: &str, wicket: &str) -> Option<AppBody> {
        let app = state.app(guid)?;
        Controller::desired(state, app, self.owner()).remove(wicket)
    }

    /// What every wasm wicket offers, as each last listed it; none listed yet, the first
    /// answering now.
    pub async fn offer(&self) -> Result<Offer, Failure> {
        if let Some(offered) = self.offered() {
            return Ok(offered);
        }
        let listing = self.list_any().await?;
        Ok(self.offered().unwrap_or_else(|| match listing.runtime() {
            Runtime::Wasm => Offer::new(&listing.capabilities, &listing.keyvalue_stores),
            Runtime::Firecracker => Offer::default(),
        }))
    }

    /// The stacks the wickets run, the first configured's first ([`State::stacks`]); none known
    /// yet, that of the first answering now. None: no wicket ever answered.
    pub async fn stacks(&self) -> Vec<Runtime> {
        let known = self.store.read(State::stacks);
        if !known.is_empty() {
            return known;
        }
        let listing = self.list_any().await;
        let known = self.store.read(State::stacks);
        match (known.is_empty(), listing) {
            (true, Ok(listing)) => vec![listing.runtime()],
            _ => known,
        }
    }

    /// Lists the wickets in turn until one answers: what it lists, noted in this instance's
    /// turn reconciling them.
    async fn list_any(&self) -> Result<Listing, Failure> {
        let mut failed = None;
        for wicket in &self.wickets {
            match wicket.list(None).await {
                Ok(listing) => {
                    if self.leads() {
                        self.note(wicket, &listing).await;
                    }
                    return Ok(listing);
                }
                Err(e) => failed = Some(e),
            }
        }
        Err(failed.unwrap_or_else(|| Failure::Unreachable("no wicket configured".into())))
    }

    /// The wicket configured before `wicket` that listed the id it lists: it is a second name.
    /// One not listed in this turn yet by the id it listed last (`State::wicket_ids`).
    fn first_of(&self, wicket: &Wicket) -> Option<(&Wicket, String)> {
        let kept = self.store.read(|state| state.wicket_ids.clone());
        let listed = self.listed.lock().expect("no panic under the lock");
        let id = |wicket: &Wicket| match listed.get(&wicket.name) {
            Some(noted) => noted.id.clone(),
            None => kept.get(&wicket.name).cloned(),
        };
        let its = id(wicket)?;
        let before = self
            .wickets
            .iter()
            .take_while(|other| other.name != wicket.name);
        let first = before
            .into_iter()
            .find(|other| id(other).as_ref() == Some(&its))?;
        Some((first, its))
    }

    /// What every wasm wicket listed offers: the apps of the others take none of it.
    fn offered(&self) -> Option<Offer> {
        self.with_offers(Offer::common)
    }

    /// What `f` makes of the offers of the wasm wickets listed, as the state keeps them.
    fn with_offers<R>(&self, f: impl FnOnce(&[&Offer]) -> R) -> R {
        self.store.read(|state| {
            let wasm = |wicket: &&Arc<Wicket>| {
                state.wickets.runtimes.get(&wicket.name) == Some(&Runtime::Wasm)
            };
            let offers: Vec<&Offer> = (self.wickets.iter().filter(wasm))
                .filter_map(|wicket| state.wickets.offers.get(&wicket.name))
                .collect();
            f(&offers)
        })
    }

    /// Whether `wicket` lists the keyvalue store `store`: None before it listed anything.
    fn lists_store(&self, wicket: &Wicket, store: &str) -> Option<bool> {
        let offer = |state: &State| Some(state.wickets.offers.get(&wicket.name)?.has_store(store));
        self.store.read(offer)
    }

    /// Keeps what `wicket` listed it runs: the apps are placed by it. Another stack than the
    /// name had, or its first, places the apps' indices again, logged; their shares are to
    /// apply ([`Controller::apply_placed`]).
    async fn note_stack(&self, wicket: &Wicket, runs: Runtime) {
        let name = &wicket.name;
        let was = self
            .store
            .read(|state| state.wickets.runtimes.get(name).copied());
        if was == Some(runs) {
            return;
        }
        let placements = |state: &State| -> Vec<(String, Vec<String>)> {
            let apps = state.apps.iter();
            apps.map(|app| (app.guid.clone(), app.placement.clone()))
                .collect()
        };
        let before = self
            .store
            .lead(|state| {
                let before = placements(state);
                state.wickets.runtimes.insert(name.clone(), runs);
                before
            })
            .await;
        let Some(before) = before else {
            return;
        };
        let after = self.store.read(placements);
        let placed: Vec<String> = (after.into_iter())
            .filter(|placement| !before.contains(placement))
            .map(|(guid, _)| guid)
            .collect();
        match was {
            Some(was) => println!(
                "CELL: {} runs the stack {} (it ran {}): the indices on it are placed again",
                wicket.label,
                runs.stack(),
                was.stack()
            ),
            None => println!("CELL: {} runs the stack {}", wicket.label, runs.stack()),
        }
        (self.placed_again.lock().expect("no panic under the lock")).extend(placed);
    }

    /// Applies the shares of the apps whose indices a wicket's stack placed again
    /// ([`Controller::note_stack`]), each in a task.
    fn apply_placed(self: &Arc<Self>) {
        let placed =
            std::mem::take(&mut *self.placed_again.lock().expect("no panic under the lock"));
        for guid in placed {
            let controller = self.clone();
            self.spawn(async move { controller.sync(&guid).await });
        }
    }

    /// Notes what `wicket` listed: what it offers and runs apps as, which wicket it is, its
    /// budget. One placed on no more (a second name of one) or again is logged. Whether it is
    /// another wicket than the name had: its apps are put again.
    async fn note(&self, wicket: &Wicket, listing: &Listing) -> bool {
        self.note_stack(wicket, listing.runtime()).await;
        let id = listing.wicket.as_ref().map(|listed| listed.id.clone());
        let noted = Noted {
            id: id.clone(),
            revision: listing.revision,
        };
        let before = (self.listed.lock().expect("no panic under the lock"))
            .insert(wicket.name.clone(), noted);
        let offer = Offer::new(&listing.capabilities, &listing.keyvalue_stores);
        let holds = (listing.wicket.as_ref()).is_some_and(|listed| listed.since.is_some());
        let name = &wicket.name;
        self.store
            .observe(|state| {
                state.wickets.offers.insert(name.clone(), offer);
                match holds {
                    true => state.wickets.holds.insert(name.clone()),
                    false => state.wickets.holds.remove(name),
                };
            })
            .await;
        // which stores they share: the apps are placed by it
        let stores = self.with_offers(Offer::stores);
        let changed = self
            .store
            .observe(|state| std::mem::replace(&mut state.wickets.stores, stores.clone()) != stores)
            .await;
        if changed {
            self.store.update(|_| ()).await;
        }
        if id.is_none() && before.is_none_or(|before| before.id.is_some()) {
            println!(
                "CELL: {} lists no id nor budget (a wicket predating them): placed on with room \
                 enough, a second name of it not told apart, its lines' instances as one wicket's",
                wicket.label
            );
        }
        let barred: BTreeMap<&String, String> = (self.wickets.iter())
            .filter_map(|wicket| Some((&wicket.name, self.second_name(wicket)?)))
            .collect();
        let odd: BTreeSet<String> = barred.keys().map(|name| name.to_string()).collect();
        let was = self
            .store
            .observe(|state| std::mem::replace(&mut state.wickets.odd, odd.clone()))
            .await;
        for wicket in &self.wickets {
            match (was.contains(&wicket.name), barred.get(&wicket.name)) {
                (false, Some(why)) => println!("CELL: {} is put on no more: {why}", wicket.label),
                (true, None) => {
                    println!("CELL: {} is placed on again, as the others", wicket.label)
                }
                _ => {}
            }
        }
        let budget = listing.budget.as_ref().map(|budget| {
            // what its routes keep beyond its apps of the owner's is not theirs
            let ours = (listing.apps.iter()).filter(|app| app.app.owner == wicket.owner);
            let (instances, memory) = ours
                .map(|app| match &app.app.sandbox {
                    Sandbox::Wasm(sandbox) => (sandbox.instances.unwrap_or(0), 0),
                    Sandbox::Vm(sandbox) => {
                        let booted = sandbox.mode.as_deref() == Some("boot");
                        let instances = sandbox.instances.unwrap_or(u64::from(booted));
                        (instances, instances * sandbox.memory_mib.unwrap_or(0))
                    }
                })
                .fold((0, 0), |(i, m), (instances, mib)| (i + instances, m + mib));
            let others = budget.instances.saturating_sub(instances);
            let others_memory = budget.memory_mib.unwrap_or(0).saturating_sub(memory);
            Budget {
                instances: budget.max_instances.saturating_sub(others),
                memory_mib: (budget.max_memory_mib).map(|max| max.saturating_sub(others_memory)),
            }
        });
        let node = listing
            .wicket
            .as_ref()
            .and_then(|listed| listed.node.clone());
        self.store
            .observe(|state| {
                let wickets = &mut state.wickets;
                match budget {
                    Some(budget) => wickets.budgets.insert(wicket.name.clone(), budget),
                    None => wickets.budgets.remove(&wicket.name),
                };
                wickets.nodes.retain(|_, of| *of != wicket.name);
                if let Some(node) = node {
                    wickets.nodes.insert(node, wicket.name.clone());
                }
                // its revision moved since it refused a share for room: it may have some
                let full = wickets.full.get(&wicket.name);
                if full.is_some_and(|revision| *revision != listing.revision) {
                    wickets.full.remove(&wicket.name);
                }
                match &listing.leaving {
                    Some(leaving) => wickets.leaving.insert(wicket.name.clone(), leaving.until),
                    None => wickets.leaving.remove(&wicket.name),
                };
            })
            .await;
        let Some(id) = id else {
            return false;
        };
        let was = self
            .store
            .read(|state| state.wicket_ids.get(&wicket.name).cloned());
        if was.as_ref() == Some(&id) {
            return false;
        }
        let kept = (self.store)
            .lead(|state| state.wicket_ids.insert(wicket.name.clone(), id.clone()))
            .await;
        let (Some(_), Some(was)) = (kept, was) else {
            return false;
        };
        println!(
            "CELL: {} is another wicket (id {id}, was {was}): its apps are put again",
            wicket.label
        );
        (self.did_not_start.lock().expect("no panic under the lock"))
            .retain(|(at, _), _| *at != wicket.name);
        true
    }

    /// Why nothing is put on `wicket`: it is a second name of a wicket, as it listed it.
    fn second_name(&self, wicket: &Wicket) -> Option<String> {
        let (first, id) = self.first_of(wicket)?;
        Some(format!(
            "{} lists the id {id} of {}: a second name of one wicket",
            wicket.label, first.label
        ))
    }

    /// Why nothing is put on `wicket`: it is a second name of a wicket, as the state says.
    fn odd(&self, wicket: &Wicket) -> Option<String> {
        let odd = self
            .store
            .read(|state| state.wickets.odd.contains(&wicket.name));
        odd.then(|| {
            let generic = || format!("{} is a second name of one wicket", wicket.label);
            self.second_name(wicket).unwrap_or_else(generic)
        })
    }

    /// `e`, a wicket's answer to a write, ends this instance's turn reconciling them when the
    /// wicket refused its epoch: another instance took the lease.
    fn fenced(&self, wicket: &Wicket, e: &Failure) {
        if let Failure::Fenced { epoch, error } = e {
            let why = format!("{} refused its epoch: {error}", wicket.label);
            self.store.end_term(*epoch, &why);
        }
    }

    /// Deletes through every wicket at once the data of the deleted instances no started app
    /// maps any more ([`Controller::purge_on`]): in this instance's turn reconciling them, else
    /// by the instance whose turn it is, told by the change of the state.
    pub async fn purge_pending(&self) {
        if !self.leads() {
            return;
        }
        join_all(self.wickets.iter().map(|wicket| self.purge_on(wicket))).await;
    }

    /// Deletes through `wicket` the data of the deleted instances no started app's snapshot maps
    /// any more (a deployment's under way, or the one before), as far as it lets it: one a route
    /// of it still maps (409) waits, as does an unreachable wicket; one it cannot delete (a store
    /// gone: 404) is dropped there, and one of a store it does not list (a Firecracker wicket
    /// lists none) is none of its. Of a store of each wicket's host, gone through every wicket, a
    /// deletion is done; of a store they share, gone through this one once no wicket maps it
    /// ([`Controller::unmapped`]).
    async fn purge_on(&self, wicket: &Wicket) {
        let due: Vec<Purge> = self.store.read(|state| {
            let mapped = |place: &str| {
                state.apps.iter().any(|app| {
                    app.started
                        && app
                            .snapshots()
                            .any(|running| running.keyvalue.values().any(|kept| *kept == place))
                })
            };
            (state.purges.iter())
                .filter(|purge| !purge.purged.contains(&wicket.name))
                .filter(|purge| !mapped(&format!("{}/{}", purge.store, purge.namespace)))
                .cloned()
                .collect()
        });
        for purge in due {
            let same =
                |kept: &Purge| kept.store == purge.store && kept.namespace == purge.namespace;
            // through this wicket: gone, or none of its; of a store they share, deleted once.
            // Whether the turn goes on
            let done = async |all: bool| {
                self.store
                    .lead(|state| {
                        let configured = &state.wickets.configured;
                        if let Some(kept) = state.purges.iter_mut().find(|kept| same(kept)) {
                            match all {
                                true => kept.purged.clone_from(configured),
                                false => kept.purged.push(wicket.name.clone()),
                            }
                        }
                        let homes = &mut state.homes;
                        state.purges.retain(|kept| {
                            let done = configured.iter().all(|name| kept.purged.contains(name));
                            if done {
                                homes.remove(&format!("{}/{}", kept.store, kept.namespace));
                            }
                            !done
                        });
                    })
                    .await
                    .is_some()
            };
            if self.lists_store(wicket, &purge.store) == Some(false) {
                if !done(false).await {
                    return;
                }
                continue;
            }
            let shared = self
                .store
                .read(|state| state.wickets.stores.get(&purge.store) == Some(&true));
            // one wicket at a time, and once
            let _one = match shared {
                true => Some(self.purging.lock().await),
                false => None,
            };
            if shared
                && (!self.store.read(|state| state.purges.iter().any(same))
                    || !self.unmapped(&purge).await)
            {
                continue;
            }
            let named = format!(
                "the data of service instance {} on {} ({}/{})",
                purge.instance, wicket.label, purge.store, purge.namespace
            );
            let _put = self.stop.put().await;
            let deleted = match wicket.purge(&purge.store, &purge.namespace).await {
                Ok(keys) => {
                    println!("SERVICES: deleted {named}: {keys} key(s)");
                    true
                }
                Err(Failure::Refused {
                    status: status @ (400 | 404),
                    error,
                    ..
                }) => {
                    println!(
                        "SERVICES: dropped {named}: {} refused it ({status}): {error}",
                        wicket.label
                    );
                    false
                }
                Err(e) => {
                    self.fenced(wicket, &e);
                    continue;
                }
            };
            if !done(shared && deleted).await {
                return;
            }
        }
    }

    /// Whether no wicket's route maps the namespace of `purge`, one being drained included: every
    /// wicket listing the store answers so, or has no such store.
    async fn unmapped(&self, purge: &Purge) -> bool {
        let wickets = (self.wickets.iter())
            .filter(|wicket| self.lists_store(wicket, &purge.store) != Some(false));
        let mappings =
            join_all(wickets.map(|wicket| wicket.mapping(&purge.store, &purge.namespace))).await;
        mappings.into_iter().all(|mapping| match mapping {
            Ok(hosts) => hosts.is_empty(),
            Err(e) => matches!(e, Failure::Refused { status: 404, .. }),
        })
    }

    /// Removes from `wicket` the apps deleted while it did not take it, as far as it answers.
    async fn remove_pending(&self, wicket: &Wicket) {
        let pending: Vec<String> = self.store.read(|state| {
            let removals = state.removals.iter();
            removals
                .filter(|removal| removal.wicket == wicket.name)
                .map(|removal| removal.app.clone())
                .collect()
        });
        for guid in pending {
            let label = &wicket.label;
            let _put = self.stop.put().await;
            match wicket.delete(&guid).await {
                Ok(_) => println!("[{guid}] CELL: removed from {label}: the app was deleted"),
                Err(e) if e.retried() => {
                    self.fenced(wicket, &e);
                    println!("[{guid}] CELL: not removed from {label} yet: {e}");
                    continue;
                }
                Err(e) => println!("[{guid}] CELL: not removed from {label}: {e}"),
            }
            let done = self
                .store
                .lead(|state| {
                    let gone = |kept: &Removal| kept.app == guid && kept.wicket == wicket.name;
                    state.removals.retain(|kept| !gone(kept))
                })
                .await;
            if done.is_none() {
                return;
            }
        }
    }

    /// Puts the app `guid`'s share `desired` on `wicket` (or removes the app from it) and records
    /// the outcome: what the process stats and the log show. The share is the caller's to hold
    /// ([`Controller::hold_share`]). During a stop, only a request's begins ([`Stop::put`]).
    pub async fn apply(
        &self,
        wicket: &Wicket,
        guid: &str,
        desired: Option<&AppBody>,
    ) -> Result<(), Failure> {
        let _put = self.stop.put().await;
        let share = (wicket.name.clone(), guid.to_string());
        let Some(app) = desired else {
            (self.did_not_start.lock().expect("no panic under the lock")).remove(&share);
            return match wicket.delete(guid).await {
                Ok(edited) => {
                    let was = self
                        .store
                        .observe(|state| {
                            let outcomes = state.outcomes.get_mut(guid)?;
                            let was = outcomes.remove(&wicket.name);
                            if outcomes.is_empty() {
                                state.outcomes.remove(guid);
                            }
                            was
                        })
                        .await;
                    if let Some(edited) = edited.filter(|_| was.is_some()) {
                        self.logs.add(
                            guid,
                            "CELL",
                            format!("Removed from {}: {}", wicket.label, edited.message),
                        );
                    }
                    Ok(())
                }
                Err(e) => {
                    self.fenced(wicket, &e);
                    let line = format!("Not removed from {}: {e}", wicket.label);
                    self.logs.add(guid, "CELL", line);
                    Err(e)
                }
            };
        };
        if let Some(why) = self.odd(wicket) {
            let line = format!("{} took none of the app: {why}", wicket.sentence());
            self.record(wicket, guid, Outcome::Refused(why.clone()), &line)
                .await;
            return Err(Failure::refused(409, why));
        }
        let mut app = app.clone();
        app.held_ms = self.held_ms(wicket);
        let mut put = wicket.put(guid, &app).await;
        if let Err(Failure::Refused { missing, .. }) = &put
            && !missing.is_empty()
        {
            // the droplet is not uploaded (or no more): upload it and put again
            for sha256 in missing.clone() {
                if let Err(e) = self.provide(wicket, guid, &sha256, &mut app).await {
                    put = Err(e);
                    break;
                }
            }
            if put.as_ref().is_err_and(
                |e| matches!(e, Failure::Refused { missing, .. } if !missing.is_empty()),
            ) {
                put = wicket.put(guid, &app).await;
            }
        }
        if let Err(e @ Failure::Fenced { .. }) = &put {
            // not the wicket's word on the share: this instance's turn is over
            self.fenced(wicket, e);
            return put.map(|_| ());
        }
        let app = &app;
        {
            let mut crashed = self.did_not_start.lock().expect("no panic under the lock");
            match &put {
                Err(Failure::Refused {
                    did_not_start: true,
                    ..
                }) => crashed.insert(share, app.clone()),
                _ => crashed.remove(&share),
            };
        }
        let (outcome, line) = match &put {
            Ok(edited) => (
                Outcome::Running,
                format!(
                    "Deployed {} (sha256 {}) to {} as {}: {}",
                    app.component.name,
                    &app.component.sha256[..12],
                    wicket.label,
                    app.hosts.join(", "),
                    edited.message
                ),
            ),
            Err(Failure::Refused { error, .. }) => (
                Outcome::Refused(error.clone()),
                format!("{} refused the app: {error}", wicket.sentence()),
            ),
            // a write fenced was answered above
            Err(Failure::Unreachable(e) | Failure::Fenced { error: e, .. }) => (
                Outcome::Unreachable,
                format!(
                    "{} is unreachable, the app is put when it is back: {e}",
                    wicket.sentence()
                ),
            ),
        };
        self.record(wicket, guid, outcome, &line).await;
        if put.as_ref().is_err_and(Failure::no_room) {
            // no room there until its revision moves
            let revision = (self.listed.lock().expect("no panic under the lock"))
                .get(&wicket.name)
                .map_or(0, |noted| noted.revision);
            self.store
                .observe(|state| state.wickets.full.insert(wicket.name.clone(), revision))
                .await;
        }
        put.map(|_| ())
    }

    /// Passes an index of the app `guid` from `wicket`, which refused its share for room, to a
    /// wicket with room, logged: whether one had.
    pub async fn pass(&self, guid: &str, wicket: &str) -> bool {
        let Some(line) = self.store.update(|state| state.pass(guid, wicket)).await else {
            return false;
        };
        self.logs.add(guid, "CELL", line);
        true
    }

    /// Has `wicket` hold the droplet `sha256` of `app` (the app `guid`'s share): uploaded when the
    /// cf-api has its bytes, else made again of the image it was pinned from, or of what
    /// buildpacks built ([`Controller::make_built`]) (a wicket's own rootfs), logged. A wicket of another guest makes another digest: the droplet and the
    /// running apps are that one's on that wicket from here on ([`crate::store::Droplet::made`]),
    /// on the others as they were; a rebuild of the same image and guest the same.
    async fn provide(
        &self,
        wicket: &Wicket,
        guid: &str,
        sha256: &str,
        app: &mut AppBody,
    ) -> Result<(), Failure> {
        if app
            .files
            .as_ref()
            .is_some_and(|files| files.sha256 == sha256)
        {
            return match self.reading(&Blob::Files(sha256.to_string())).await? {
                Some(files) => wicket.upload_files(sha256, files).await,
                None => Err(gone(sha256)),
            };
        }
        if let Some(component) = self.reading(&Blob::Droplet(sha256.to_string())).await? {
            return wicket.upload(sha256, component).await;
        }
        let droplet = self.store.read(|state| {
            // the newest of that digest: an older one's package, and its credentials, may
            // be gone, the same image pushed again
            let droplet = state
                .droplets
                .iter()
                .rev()
                .find(|kept| kept.sha256_on(&wicket.name) == sha256)?;
            if let Some(built) = &droplet.built {
                return Some(Made::Built(built.clone()));
            }
            let image = droplet.image.clone()?;
            let credentials = state
                .packages
                .iter()
                .find(|package| package.guid == droplet.package)
                .and_then(|package| package.docker.as_ref())
                .and_then(|docker| {
                    let (username, password) = docker.credentials()?;
                    Some((username.to_string(), password.to_string()))
                });
            Some(Made::Image(image, credentials))
        });
        let (made, of) = match droplet {
            None => return Err(gone(sha256)),
            Some(Made::Image(image, credentials)) => {
                let credentials = credentials
                    .as_ref()
                    .map(|(username, password)| (username.as_str(), password.as_str()));
                (wicket.make_image(&image, credentials, None).await?, image)
            }
            Some(Made::Built(built)) => {
                let of = format!(
                    "the droplet {} on {}",
                    &built.layer.sha256[..12],
                    built.run_image
                );
                (self.make_built(wicket, &built).await?, of)
            }
        };
        let line = format!(
            "{} made the rootfs of {of} (sha256 {})",
            wicket.sentence(),
            made.sha256.get(..12).unwrap_or(&made.sha256)
        );
        self.logs.add(guid, "CELL", line);
        if made.sha256 != sha256 {
            let on = wicket.name.as_str();
            self.store
                .update(|state| {
                    for droplet in &mut state.droplets {
                        if droplet.sha256_on(on) == sha256 {
                            droplet.made_as(on, &made.sha256);
                        }
                    }
                    let running = state.apps.iter_mut().flat_map(App::snapshots_mut);
                    for running in running.filter(|running| running.sha256_on(on) == sha256) {
                        running.made_as(on, &made.sha256);
                    }
                })
                .await;
            app.component.sha256 = made.sha256;
        }
        Ok(())
    }

    /// Has a wicket make an ext4 image of the OCI image `image`, with the `(username, password)`
    /// of a private one: the first answering of the Firecracker wickets. The others make it at
    /// their first put of an app of it.
    pub async fn make_image(
        &self,
        image: &str,
        credentials: Option<(&str, &str)>,
    ) -> Result<MadeImage, Failure> {
        let mut failed = None;
        for wicket in self.makers() {
            match wicket.make_image(image, credentials, None).await {
                Err(e @ Failure::Unreachable(_)) => failed = Some(e),
                made => return made,
            }
        }
        Err(failed.unwrap_or_else(no_maker))
    }

    /// The Firecracker wickets a rootfs is made on, a build run on: those as they are meant.
    pub fn makers(&self) -> Vec<&Arc<Wicket>> {
        let makers: Vec<String> = self.store.read(|state| {
            let runtimes = state.wickets.runtimes.iter();
            runtimes
                .filter(|(_, runs)| **runs == Runtime::Firecracker)
                .map(|(name, _)| name.clone())
                .collect()
        });
        (self.wickets.iter())
            .filter(|wicket| makers.contains(&wicket.name) && self.odd(wicket).is_none())
            .collect()
    }

    /// Has `wicket` make the rootfs of what buildpacks built, `built`: the run image with the
    /// droplet over it, uploaded first where the wicket does not keep it (it built it a
    /// while ago, or another did). So a droplet is built once, and placed on any wicket. The
    /// run image is pulled with the credentials kept with the droplet, else the builder's.
    pub async fn make_built(&self, wicket: &Wicket, built: &Built) -> Result<MadeImage, Failure> {
        let credentials = built
            .credentials()
            .or_else(|| (self.cnb.as_ref()).and_then(|cnb| cnb.credentials_of(&built.run_image)));
        let layer = built.layer.sha256.as_str();
        let make =
            || wicket.make_image(&built.run_image, credentials, Some((layer, &built.launch)));
        match make().await {
            Err(Failure::Refused { missing, .. }) if !missing.is_empty() => {
                match self.reading(&Blob::Files(layer.to_string())).await? {
                    Some(tar) => wicket.upload_layer(layer, tar).await?,
                    None => return Err(gone(layer)),
                }
                make().await
            }
            made => made,
        }
    }

    /// The blob as it is read; None: it is not kept. Blobs that do not answer are a wicket
    /// that does not: tried again.
    async fn reading(&self, blob: &Blob) -> Result<Option<Reading>, Failure> {
        (self.store.reading(blob).await).map_err(Failure::Unreachable)
    }

    /// Records the outcome of `wicket`'s share, logging it when it changed.
    async fn record(&self, wicket: &Wicket, guid: &str, outcome: Outcome, line: &str) {
        let changed = self
            .store
            .observe(|state| {
                if state.app(guid).is_none() {
                    return false;
                }
                let outcomes = state.outcomes.entry(guid.to_string()).or_default();
                outcomes.insert(wicket.name.clone(), outcome.clone()) != Some(outcome)
            })
            .await;
        if changed {
            self.logs.add(guid, "CELL", line);
        }
    }

    /// The share of `guid` on `wicket` now, applied: the share held by the caller.
    pub async fn sync_held(&self, wicket: &Wicket, guid: &str) -> Result<(), Failure> {
        let desired = self
            .store
            .read(|state| self.desired_on(state, guid, &wicket.name));
        self.apply(wicket, guid, desired.as_ref()).await
    }

    /// The shares of `guid` now, applied on every wicket at once: none on a wicket removes the
    /// app from it. A share refused for room has an index passed to a wicket with room, and the
    /// shares applied again.
    pub async fn sync(&self, guid: &str) {
        self.sync_on(guid, None).await;
    }

    /// [`Controller::sync`] on the wickets `only` names (all: None): a share refused for room
    /// passes an index, and every share is applied again.
    pub async fn sync_on(&self, guid: &str, only: Option<&[String]>) {
        let mut only = only;
        // each pass fills a wicket: as many as there are
        for _ in 0..=self.wickets.len() {
            let wickets = (self.wickets.iter())
                .filter(|wicket| only.is_none_or(|only| only.contains(&wicket.name)));
            let full = join_all(wickets.map(|wicket| async move {
                let _share = self.hold_share(guid, &wicket.name).await;
                let synced = self.sync_held(wicket, guid).await;
                synced.is_err_and(|e| e.no_room()).then_some(&wicket.name)
            }))
            .await;
            let mut passed = false;
            for wicket in full.into_iter().flatten() {
                if self.pass(guid, wicket).await {
                    passed = true;
                    break;
                }
            }
            if !passed {
                return;
            }
            only = None;
        }
    }

    /// Applies the shares of `guid` that are not as `before` had them, at once
    /// ([`Controller::sync_on`]).
    pub async fn sync_changed(&self, guid: &str, before: &BTreeMap<String, AppBody>) {
        let owner = self.owner();
        let now = self.store.read(|state| {
            let app = state.app(guid);
            app.map(|app| Controller::desired(state, app, owner))
                .unwrap_or_default()
        });
        let changed: Vec<String> = (self.wickets.iter())
            .map(|wicket| wicket.name.clone())
            .filter(|name| now.get(name) != before.get(name))
            .collect();
        if !changed.is_empty() {
            self.sync_on(guid, Some(&changed)).await;
        }
    }

    /// Puts / deletes, each in a task, the difference between the apps `wicket` listed and the
    /// shares the state places on it, and logs the apps of the owner the state does not know. A
    /// share under a put or an edit is theirs, one whose VMs did not start as it is wanted is not
    /// tried again. The apps of the owner it holds that are as wanted, or the state does not
    /// know (left as they are), are to be confirmed to it ([`Controller::confirm`]): their names.
    async fn fix(self: &Arc<Self>, wicket: &Arc<Wicket>, listed: &[ListedApp]) -> Vec<String> {
        let owner = &wicket.owner;
        let held_ms = self.held_ms(wicket);
        let wanted: Vec<(String, Option<AppBody>)> = self.store.read(|state| {
            let apps = state.apps.iter();
            apps.map(|app| {
                let mut shares = Controller::desired(state, app, owner);
                let share = shares.remove(&wicket.name);
                // as it is put there
                let share = share.map(|share| AppBody { held_ms, ..share });
                (app.guid.clone(), share)
            })
            .collect()
        });
        let mut confirmed = Vec::new();
        for (guid, desired) in &wanted {
            let listed = listed
                .iter()
                .find(|app| app.name == *guid && app.app.owner == *owner);
            match (desired, listed) {
                (Some(desired), Some(listed)) if same(&listed.app, desired) => {
                    let line = format!("Found in {} as wanted", wicket.label);
                    self.record(wicket, guid, Outcome::Running, &line).await;
                    if listed.held {
                        confirmed.push(guid.clone());
                    }
                }
                (None, None) => {}
                _ if self.refused_as(wicket, guid, desired.as_ref()) => {}
                _ => {
                    let share = (guid.clone(), wicket.name.clone());
                    let Ok(held) = lock_of(&self.shares, share).try_lock_owned() else {
                        continue;
                    };
                    let (controller, wicket, guid) = (self.clone(), wicket.clone(), guid.clone());
                    self.spawn(async move {
                        let synced = {
                            let _held = held;
                            controller.sync_held(&wicket, &guid).await
                        };
                        if synced.is_err_and(|e| e.no_room())
                            && controller.pass(&guid, &wicket.name).await
                        {
                            controller.sync(&guid).await;
                        }
                    });
                }
            }
        }
        let removing: Vec<String> = self.store.read(|state| {
            let removals = state.removals.iter();
            removals
                .filter(|removal| removal.wicket == wicket.name)
                .map(|removal| removal.app.clone())
                .collect()
        });
        let strangers: BTreeSet<&str> = listed
            .iter()
            .filter(|app| app.app.owner == *owner)
            .map(|app| app.name.as_str())
            .filter(|name| !wanted.iter().any(|(guid, _)| guid == name))
            .filter(|name| !removing.iter().any(|guid| guid == name))
            .collect();
        // left as they are: served as they were
        let held = |name: &str| listed.iter().any(|app| app.name == name && app.held);
        let left = strangers.iter().filter(|name| held(name));
        confirmed.extend(left.map(|name| name.to_string()));
        let mut logged = self.strangers.lock().expect("no panic under the lock");
        logged.retain(|(at, name)| *at != wicket.name || strangers.contains(name.as_str()));
        for name in strangers {
            if logged.insert((wicket.name.clone(), name.to_string())) {
                println!(
                    "[{name}] CELL: left in {label}: this state did not put it (a lost \
                     --data-dir, another cf-api?); DELETE /api/apps/{name}?owner={owner} of \
                     {label} removes it",
                    label = wicket.label
                );
            }
        }
        confirmed
    }

    /// The `held-ms` of a share put to `wicket`: [`HELD`] of one listing its start, none of one
    /// predating it, which takes none.
    fn held_ms(&self, wicket: &Wicket) -> Option<u64> {
        let holds = self
            .store
            .read(|state| state.wickets.holds.contains(&wicket.name));
        holds.then_some(HELD.as_millis() as u64)
    }

    /// Tells `wicket` its apps of the owner `names` are as wanted: a start of it held them, it
    /// serves them now. Whether to tell it again: it did not answer.
    async fn confirm(&self, wicket: &Wicket, names: &[String]) -> bool {
        if names.is_empty() {
            return false;
        }
        match wicket.confirmed(names).await {
            Ok(()) => {
                println!(
                    "CELL: {} serves the {} app(s) it held since its start: as wanted",
                    wicket.sentence(),
                    names.len()
                );
                false
            }
            Err(e) => {
                self.fenced(wicket, &e);
                println!(
                    "CELL: {} not told its apps are as wanted: {e}",
                    wicket.label
                );
                matches!(e, Failure::Unreachable(_))
            }
        }
    }

    /// Whether `wicket`'s refusal of the share `desired` holds: its VMs did not start.
    fn refused_as(&self, wicket: &Wicket, guid: &str, desired: Option<&AppBody>) -> bool {
        let crashed = self.did_not_start.lock().expect("no panic under the lock");
        let share = (wicket.name.clone(), guid.to_string());
        crashed.get(&share).is_some_and(|put| Some(put) == desired)
    }

    /// Watches every wicket, each apart ([`Controller::watch_one`]).
    async fn watch(self: Arc<Self>) {
        let wickets = self.wickets.iter();
        join_all(wickets.map(|wicket| self.clone().watch_one(wicket.clone()))).await;
    }

    /// Lists `wicket`'s apps and fixes the difference at startup, whenever its revision moves and
    /// when it answers again, confirming what it holds as wanted, then tries its pending removals
    /// and purges (after a hold too: a drained route's end moves no revision); while it is
    /// unreachable, every share placed on it is `STARTING`, and it is tried again with a growing
    /// pause, [`MAX_PAUSE`] at most. Listing itself leaving, the apps' indices on it move
    /// elsewhere ([`Controller::start_evacuation`]), as they do once it has not answered for
    /// `--wicket-away-grace`: gone, it is placed on again once it has answered for as long.
    /// Serving at startup, or again after it left, was unreachable or gone, it takes those that
    /// spread an app again ([`Controller::start_respread`]).
    async fn watch_one(self: Arc<Self>, wicket: Arc<Wicket>) {
        let mut after: Option<u64> = None;
        let mut pause = MIN_PAUSE;
        // not seen serving since the start, or since it left or was unreachable
        let mut away = true;
        // since when it has not answered
        let mut unanswered: Option<Instant> = None;
        // gone and answering again: when it is placed on again
        let mut steady: Option<Instant> = None;
        loop {
            let listed = match steady {
                Some(at) => tokio::select! {
                    listed = wicket.list(after) => Some(listed),
                    () = tokio::time::sleep_until(at) => None,
                },
                None => Some(wicket.list(after).await),
            };
            let Some(listed) = listed else {
                steady = None;
                self.placed_on_again(&wicket).await;
                if std::mem::take(&mut away) {
                    self.start_respread(&wicket);
                }
                continue;
            };
            let listing = match listed {
                Ok(listing) => listing,
                Err(e) => {
                    self.unreachable(&wicket, &e).await;
                    self.came_back(&wicket);
                    away = true;
                    steady = None;
                    let since = *unanswered.get_or_insert_with(Instant::now);
                    if self.gone(&wicket, since).await {
                        self.start_evacuation(&wicket, Away::Gone, false);
                    }
                    // back, it is looked at whatever its revision: a wicket restarted
                    // may have come to the one it had
                    after = None;
                    (self.did_not_start.lock().expect("no panic under the lock"))
                        .retain(|(at, _), _| *at != wicket.name);
                    tokio::time::sleep(pause).await;
                    pause = (pause * 2).min(MAX_PAUSE);
                    continue;
                }
            };
            self.answered(&wicket);
            unanswered = None;
            pause = MIN_PAUSE;
            if steady.is_none() && self.is_gone(&wicket.name) {
                steady = Some(Instant::now() + self.away_grace);
                println!(
                    "CELL: {} answers again: placed on again once it has answered for {}s",
                    wicket.sentence(),
                    self.away_grace.as_secs()
                );
            }
            let another = self.note(&wicket, &listing);
            self.came_back(&wicket);
            self.apply_placed();
            let moved = another.await || after != Some(listing.revision);
            after = Some(listing.revision);
            let pending = self.store.read(|state| {
                (state.purges.iter()).any(|purge| !purge.purged.contains(&wicket.name))
                    || (state.removals.iter()).any(|removal| removal.wicket == wicket.name)
            });
            if !moved && !pending {
                continue;
            }
            if moved {
                let held = self.fix(&wicket, &listing.apps).await;
                if self.confirm(&wicket, &held).await {
                    // listed again at once, what it holds confirmed then
                    after = None;
                }
                match &listing.leaving {
                    Some(leaving) => {
                        away = true;
                        let told = leaving.evacuated.contains(&wicket.owner);
                        self.start_evacuation(&wicket, Away::Leaving, told);
                    }
                    None => {
                        let mut evacuations =
                            self.evacuations.lock().expect("no panic under the lock");
                        if (evacuations.get(&wicket.name))
                            .is_some_and(|evacuation| evacuation.away == Away::Leaving)
                        {
                            evacuations.remove(&wicket.name);
                        }
                        drop(evacuations);
                        if steady.is_none() && std::mem::take(&mut away) {
                            self.start_respread(&wicket);
                        }
                    }
                }
            }
            self.remove_pending(&wicket).await;
            self.purge_on(&wicket).await;
        }
    }

    /// Moves the apps' indices off `wicket`, leaving or gone ([`Controller::evacuate`]), unless
    /// that runs; `told`: it lists the owner as having moved what it could (before a restart of
    /// the cf-api).
    fn start_evacuation(self: &Arc<Self>, wicket: &Arc<Wicket>, away: Away, told: bool) {
        {
            let mut evacuations = self.evacuations.lock().expect("no panic under the lock");
            let fresh = (evacuations.get(&wicket.name))
                .is_none_or(|evacuation| evacuation.away != away && !evacuation.running);
            if fresh {
                if away == Away::Leaving {
                    println!(
                        "CELL: {} is leaving: the apps' indices on it move to the other wickets",
                        wicket.label
                    );
                }
                evacuations.insert(wicket.name.clone(), Evacuation::new(away));
            }
            let evacuation = evacuations.get_mut(&wicket.name).expect("just kept");
            // the other's runs: tried again at the next listing
            if evacuation.away != away {
                return;
            }
            evacuation.told |= told;
            if std::mem::replace(&mut evacuation.running, true) {
                return;
            }
        }
        let (controller, wicket) = (self.clone(), wicket.clone());
        self.spawn(async move {
            controller.evacuate(&wicket, away).await;
            let mut evacuations = controller
                .evacuations
                .lock()
                .expect("no panic under the lock");
            if let Some(evacuation) = evacuations.get_mut(&wicket.name) {
                evacuation.running = false;
            }
        });
    }

    /// What `f` makes of the evacuation of the wicket `name`, if it is leaving or gone.
    fn evacuation<R>(&self, name: &str, f: impl FnOnce(&mut Evacuation) -> R) -> Option<R> {
        let mut evacuations = self.evacuations.lock().expect("no panic under the lock");
        evacuations.get_mut(name).map(f)
    }

    /// Moves the indices of the started apps off `wicket`, leaving or gone, to the wickets with
    /// room ([`State::evacuate`]), every app's at once ([`Controller::relocate`]), as long as
    /// some are left to try (gone, as long as it does not answer): an index with nowhere to go
    /// stays until the wicket stops (is back), tried again, one not taken elsewhere for another
    /// reason than room for good. Then, leaving, once no index moves off it any more (or it
    /// drains), tells it the owner moved what it could.
    async fn evacuate(&self, wicket: &Wicket, away: Away) {
        let name = &wicket.name;
        let (why, until) = match away {
            Away::Leaving => (format!("{} is leaving", wicket.label), "until it stops"),
            Away::Gone => (
                format!(
                    "{} has not answered for {}s",
                    wicket.label,
                    self.away_grace.as_secs()
                ),
                "until it is back",
            ),
        };
        let on = || away == Away::Leaving || self.is_down(name);
        // a refusal for room has the wicket hold none until its revision moves: each pass
        // tries the indices on another, as many as there are
        for _ in 0..=self.wickets.len() {
            if !on() {
                return;
            }
            let undone = self.evacuation(name, |evacuation| evacuation.undone.clone());
            let guids: Vec<String> = self.store.read(|state| {
                (state.apps.iter())
                    .filter(|app| {
                        undone
                            .as_ref()
                            .is_none_or(|undone| !undone.contains(&app.guid))
                    })
                    .filter(|app| state.placement(app).contains(name))
                    .map(|app| app.guid.clone())
                    .collect()
            });
            let (why, on) = (&why, &on);
            // each app apart: one under a long edit holds no other's move
            let moved = join_all(guids.into_iter().map(|guid| async move {
                let moves = {
                    let _app = self.hold(&guid).await;
                    if !on() {
                        return false;
                    }
                    let mut evacuated = self.store.read(|state| state.evacuation(&guid, name));
                    if !evacuated.moves.is_empty() {
                        // kept: a restart, or the next turn, takes them up
                        evacuated = (self.store)
                            .lead(|state| state.evacuate(&guid, name))
                            .await
                            .unwrap_or_default();
                    }
                    let Evacuated {
                        moves,
                        stay,
                        why: nowhere,
                    } = evacuated;
                    for (index, to) in &moves {
                        let line = format!("Index {index} moves to the wicket {to}: {why}");
                        self.logs.add(&guid, "CELL", line);
                    }
                    let first = !stay.is_empty()
                        && self
                            .evacuation(name, |evacuation| evacuation.stayed.insert(guid.clone()))
                            == Some(true);
                    if first {
                        let stay = stays(&stay);
                        let line = format!("{stay} on {} {until}: {nowhere}", wicket.label);
                        self.logs.add(&guid, "CELL", line);
                    }
                    moves
                };
                if moves.is_empty() {
                    return false;
                }
                if self.relocate(&guid, moves).await == Relocated::Undone {
                    self.evacuation(name, |evacuation| evacuation.undone.insert(guid));
                }
                true
            }))
            .await;
            if !moved.contains(&true) {
                break;
            }
        }
        if away == Away::Gone {
            return;
        }
        // the moves off it others started (a wicket back taking indices, the cf-api's restart)
        let until = self
            .store
            .read(|state| state.wickets.leaving.get(name).copied());
        while let Some(until) = until {
            let moving = self.store.read(|state| {
                state.apps.iter().any(|app| {
                    let placement = state.placement(app);
                    let off = |index: &&u64| placement.get(**index as usize) == Some(name);
                    app.moving.keys().any(|index| off(&index))
                })
            });
            if !moving || crate::store::epoch_secs() >= until {
                break;
            }
            tokio::time::sleep(LOOK).await;
        }
        if self.evacuation(name, |evacuation| {
            std::mem::replace(&mut evacuation.told, true)
        }) == Some(false)
        {
            match wicket.evacuated().await {
                Ok(()) => println!(
                    "CELL: {} told the apps' indices left on it stay until it stops",
                    wicket.label
                ),
                Err(e) => {
                    self.fenced(wicket, &e);
                    println!(
                        "CELL: {} not told the apps' indices left on it stay: {e}",
                        wicket.label
                    )
                }
            }
        }
    }

    /// Moves to `wicket`, back or new, the indices that spread an app again
    /// ([`Controller::respread`]), unless that runs.
    fn start_respread(self: &Arc<Self>, wicket: &Arc<Wicket>) {
        let mut respreading = self.respreading.lock().expect("no panic under the lock");
        if !respreading.insert(wicket.name.clone()) {
            return;
        }
        let (controller, wicket) = (self.clone(), wicket.clone());
        self.spawn(async move {
            controller.respread(&wicket).await;
            let mut respreading = controller
                .respreading
                .lock()
                .expect("no panic under the lock");
            respreading.remove(&wicket.name);
        });
    }

    /// Moves to `wicket`, back or new, the indices that spread an app again, one at a time
    /// ([`State::respread`], [`Controller::relocate`]): an app one of whose moves stays where it
    /// was is not tried again.
    async fn respread(&self, wicket: &Wicket) {
        let mut skipped = BTreeSet::new();
        loop {
            let found = self
                .store
                .lead(|state| state.respread(&wicket.name, &skipped))
                .await;
            let Some(Some((guid, (index, to)))) = found else {
                return;
            };
            let line = format!(
                "Index {index} moves to {}: the app's instances spread over the wickets again",
                wicket.label
            );
            self.logs.add(&guid, "CELL", line);
            if self.relocate(&guid, vec![(index, to)]).await != Relocated::All {
                skipped.insert(guid);
            }
        }
    }

    /// Takes up the moves a stop of the cf-api left under way ([`Controller::relocate`]).
    pub fn resume_moves(self: &Arc<Self>) {
        let moving: Vec<(String, Vec<Move>)> = self.store.read(|state| {
            (state.apps.iter())
                .filter(|app| !app.moving.is_empty())
                .map(|app| {
                    let moves = app.moving.iter();
                    let moves = moves.map(|(index, to)| (*index, to.clone())).collect();
                    (app.guid.clone(), moves)
                })
                .collect()
        });
        for (guid, moves) in moving {
            let controller = self.clone();
            self.spawn(async move {
                controller.relocate(&guid, moves).await;
            });
        }
    }

    /// Moves the indices `moves` of the app `guid`, each recorded moving to its wicket
    /// ([`App::moving`]): the shares they grow put, the app held as an edit holds it, then each
    /// index placed on its wicket once that one serves it ([`Controller::serves`]), the shares
    /// it leaves applied. A move its wicket refuses, or does not serve within the app's health
    /// check timeout, is undone: the index stays where it was. Logged.
    async fn relocate(&self, guid: &str, moves: Vec<Move>) -> Relocated {
        let mut undone: Vec<(Move, String)> = Vec::new();
        // of them those refused for room
        let mut for_room = 0;
        let mut moves = moves;
        {
            let _app = self.hold(guid).await;
            let targets: BTreeSet<String> = moves.iter().map(|(_, to)| to.clone()).collect();
            let put = join_all(targets.into_iter().map(|to| async move {
                let put = self.put_share(guid, &to).await;
                (to, put)
            }))
            .await;
            for (to, put) in put {
                let Err(e) = put else {
                    continue;
                };
                let why = match e.no_room() {
                    true => format!("the wicket {to} has no room for it"),
                    false => format!("the wicket {to} refused it: {e}"),
                };
                let (refused, kept): (Vec<Move>, Vec<Move>) =
                    moves.into_iter().partition(|(_, at)| *at == to);
                moves = kept;
                if e.no_room() {
                    for_room += refused.len();
                }
                undone.extend(refused.into_iter().map(|refused| (refused, why.clone())));
            }
        }
        let timeout = self.store.read(|state| {
            let app = state.app(guid);
            let timeout = app.and_then(|app| app.process.health_check.timeout);
            let timeout = timeout.unwrap_or(DEFAULT_HEALTH_CHECK_TIMEOUT);
            timeout.clamp(1, MAXIMUM_HEALTH_CHECK_TIMEOUT)
        });
        let deadline = Instant::now() + Duration::from_secs(timeout);
        let served = join_all(moves.into_iter().map(|(index, to)| async move {
            let served = self.serves(guid, index, &to, deadline).await;
            ((index, to), served)
        }))
        .await;
        let _app = self.hold(guid).await;
        let mut changed = BTreeSet::new();
        // every move still asked for placed
        let mut placed = true;
        for ((index, to), served) in served {
            match served {
                Ok(()) => {
                    let from = self.store.lead(|state| state.moved(guid, index, &to)).await;
                    let Some(from) = from else {
                        // the turn over: the next takes the move up
                        return Relocated::Undone;
                    };
                    if let Some(from) = from {
                        let line = format!(
                            "Index {index} runs on the wicket {to}: out of the wicket {from}'s share"
                        );
                        self.logs.add(guid, "CELL", line);
                        changed.insert(from);
                    }
                }
                Err(None) => placed = false,
                Err(Some(why)) => undone.push(((index, to), why)),
            }
        }
        for ((index, to), why) in &undone {
            let at = self
                .store
                .lead(|state| state.unmoved(guid, *index, to))
                .await;
            let Some(at) = at else {
                return Relocated::Undone;
            };
            if let Some(at) = at {
                let line = format!("Index {index} stays on the wicket {at}: {why}");
                self.logs.add(guid, "CELL", line);
                changed.insert(to.clone());
            }
        }
        // one not answering (gone) has its shares fixed once it does: its listing
        changed.retain(|name| !self.is_down(name));
        if !changed.is_empty() {
            let changed: Vec<String> = changed.into_iter().collect();
            self.sync_on(guid, Some(&changed)).await;
        }
        match (placed, undone.len()) {
            (true, 0) => Relocated::All,
            (true, undone) if undone == for_room => Relocated::NoRoom,
            _ => Relocated::Undone,
        }
    }

    /// The share of the app `guid` on the wicket `name` now, put, the share held.
    async fn put_share(&self, guid: &str, name: &str) -> Result<(), Failure> {
        let Some(wicket) = self.wickets.iter().find(|wicket| wicket.name == name) else {
            return Err(Failure::refused(409, format!("no --wicket names {name}")));
        };
        let _share = self.hold_share(guid, name).await;
        self.sync_held(wicket, guid).await
    }

    /// Whether the wicket `to` serves the index `index` of the app `guid`, moving there, by
    /// `deadline`: it took its share and, with a readiness check, the VMs the share keeps are
    /// ready, and the server of each of its tunnel streams took the app's routes
    /// ([`Controller::advertised`]): the wicket it leaves stops advertising them once every
    /// server sends them here. Err with why not once it refused it or the deadline passed, None
    /// once the index moves there no more.
    async fn serves(
        &self,
        guid: &str,
        index: u64,
        to: &str,
        deadline: Instant,
    ) -> Result<(), Option<String>> {
        let Some(wicket) = self.wickets.iter().find(|wicket| wicket.name == to) else {
            return Err(None);
        };
        loop {
            let (moving, share, outcome) = self.store.read(|state| {
                let app = state.app(guid);
                let moving =
                    app.is_some_and(|app| app.moving.get(&index).is_some_and(|at| at == to));
                let share = self.desired_on(state, guid, to);
                (moving, share, state.outcome(guid, to).cloned())
            });
            if !moving {
                return Err(None);
            }
            match (outcome, share) {
                (Some(Outcome::Running), Some(share)) => {
                    if self.unready(wicket, guid, &share).await.is_none()
                        && self.advertised(wicket, guid, &share).await
                    {
                        return Ok(());
                    }
                }
                (Some(Outcome::Refused(error)), _) => {
                    return Err(Some(format!("{} refused it: {error}", wicket.label)));
                }
                _ => {}
            }
            if Instant::now() >= deadline {
                return Err(Some(format!(
                    "{} did not serve it within its health check's timeout",
                    wicket.label
                )));
            }
            tokio::time::sleep(LOOK).await;
        }
    }

    /// Whether `wicket` lists the app `guid` of `share` with its routes taken by the server of
    /// each of its tunnel streams: so in local mode, and from a wicket predating it.
    async fn advertised(&self, wicket: &Wicket, guid: &str, share: &AppBody) -> bool {
        let Ok(listing) = wicket.list(None).await else {
            return false;
        };
        let ours = |listed: &&ListedApp| {
            listed.name == guid && listed.app.owner == share.owner && listed.app.tag == share.tag
        };
        let listed = listing.apps.iter().find(ours);
        listed.is_some_and(|listed| listed.advertised != Some(false))
    }

    /// Every share placed on `wicket` is `STARTING` until it answers ([`Controller::answered`]).
    async fn unreachable(&self, wicket: &Wicket, e: &Failure) {
        (self.down.lock().expect("no panic under the lock")).insert(wicket.name.clone());
        let owner = self.owner();
        let guids: Vec<String> = self.store.read(|state| {
            let apps = state.apps.iter();
            apps.filter(|app| Controller::desired(state, app, owner).contains_key(&wicket.name))
                .map(|app| app.guid.clone())
                .collect()
        });
        let line = format!(
            "{} is unreachable, the app is put when it is back: {e}",
            wicket.sentence()
        );
        for guid in guids {
            self.record(wicket, &guid, Outcome::Unreachable, &line)
                .await;
        }
    }

    /// A listing of `wicket` came back, answered (and noted) or not.
    fn came_back(&self, wicket: &Wicket) {
        let mut listed = self.listed_once.lock().expect("no panic under the lock");
        if !listed.contains(&wicket.name) {
            listed.insert(wicket.name.clone());
        }
    }

    /// Whether the API is ready: the state read before, reconciling the wickets, every wicket
    /// listed once (or found not answering) in this turn; not from the stop on. Err with why
    /// not. An edit is tried against the wickets by the instance taking it: of several, only
    /// the one reconciling them is ready, so a balancer checking it sends it every request.
    pub fn ready(&self) -> Result<(), String> {
        if self.stop.stopping() {
            return Err("the cf-api stops".into());
        }
        if !self.store.leads() {
            return Err("another instance reconciles the wickets".into());
        }
        let listed = self.listed_once.lock().expect("no panic under the lock");
        let unlisted: Vec<&str> = (self.wickets.iter())
            .filter(|wicket| !listed.contains(&wicket.name))
            .map(|wicket| wicket.label.as_str())
            .collect();
        match unlisted.is_empty() {
            true => Ok(()),
            false => Err(format!("not listed yet: {}", unlisted.join(", "))),
        }
    }

    /// `wicket` answers again: an evacuation of it, gone, ends, what stayed tried again the next
    /// time it is.
    fn answered(&self, wicket: &Wicket) {
        (self.down.lock().expect("no panic under the lock")).remove(&wicket.name);
        self.evacuation(&wicket.name, |evacuation| {
            if evacuation.away == Away::Gone {
                evacuation.stayed.clear();
                evacuation.undone.clear();
            }
        });
    }

    /// Whether the last listing of the wicket `name` failed.
    fn is_down(&self, name: &str) -> bool {
        (self.down.lock().expect("no panic under the lock")).contains(name)
    }

    /// Whether the wicket `name` is gone: placed on no more.
    fn is_gone(&self, name: &str) -> bool {
        self.store.read(|state| state.wickets.gone.contains(name))
    }

    /// Whether `wicket`, not answering since `since`, is gone: `--wicket-away-grace` passed, or
    /// it was gone in the turn before, and another wicket is configured. Placed on no more from
    /// then on, logged once.
    async fn gone(&self, wicket: &Wicket, since: Instant) -> bool {
        if self.away_grace.is_zero() || self.wickets.len() < 2 {
            return false;
        }
        if since.elapsed() < self.away_grace && !self.is_gone(&wicket.name) {
            return false;
        }
        let gone = self
            .store
            .observe(|state| state.wickets.gone.insert(wicket.name.clone()))
            .await;
        if gone {
            println!(
                "CELL: {} has not answered for {}s: placed on no more, the apps' indices on it \
                 move to the other wickets",
                wicket.sentence(),
                self.away_grace.as_secs()
            );
        }
        true
    }

    /// `wicket`, gone, has answered for `--wicket-away-grace`: placed on again.
    async fn placed_on_again(&self, wicket: &Wicket) {
        let gone = self
            .store
            .observe(|state| state.wickets.gone.remove(&wicket.name))
            .await;
        if gone {
            println!(
                "CELL: {} has answered for {}s: placed on again",
                wicket.sentence(),
                self.away_grace.as_secs()
            );
        }
    }
}

/// `indices` staying, as a line names them: `Index 0 stays`, `Indices 0 and 2 stay`.
fn stays(indices: &[u64]) -> String {
    let named: Vec<String> = indices.iter().map(u64::to_string).collect();
    match &named[..] {
        [one] => format!("Index {one} stays"),
        [first @ .., last] => format!("Indices {} and {last} stay", first.join(", ")),
        [] => String::new(),
    }
}

/// Refuses two names of one wicket among `wickets`: those answering now, by the id they list.
pub async fn distinct(wickets: &[Arc<Wicket>]) -> Result<(), String> {
    let listed = join_all(wickets.iter().map(|wicket| wicket.list(None))).await;
    let mut named: BTreeMap<String, &Wicket> = BTreeMap::new();
    for (wicket, listing) in wickets.iter().zip(listed) {
        let Some(id) = listing.ok().and_then(|listing| Some(listing.wicket?.id)) else {
            continue;
        };
        if let Some(first) = named.get(&id) {
            return Err(format!(
                "--wicket {} and --wicket {} name one wicket (id {id}): name each once",
                first.name, wicket.name
            ));
        }
        named.insert(id, wicket);
    }
    Ok(())
}

/// What each of `wickets` answering now lists it runs, by name.
pub async fn stacks_of(wickets: &[Arc<Wicket>]) -> BTreeMap<String, Runtime> {
    let listed = join_all(wickets.iter().map(|wicket| wicket.list(None))).await;
    let listed = wickets.iter().zip(listed);
    listed
        .filter_map(|(wicket, listing)| Some((wicket.name.clone(), listing.ok()?.runtime())))
        .collect()
}

/// What a wicket makes a droplet's rootfs of ([`Controller::provide`]).
enum Made {
    /// An image, with its package's credentials.
    Image(String, Option<(String, String)>),
    Built(Built),
}

/// No wicket makes a rootfs, runs a build.
pub fn no_maker() -> Failure {
    Failure::Unreachable(format!(
        "no wicket of the stack {} configured",
        Runtime::Firecracker.stack()
    ))
}

/// The lock of `key` among `locks`, made if missing.
fn lock_of<K: Eq + std::hash::Hash>(locks: &Locks<K>, key: K) -> Arc<tokio::sync::Mutex<()>> {
    let mut locks = locks.lock().expect("no panic under the lock");
    locks.entry(key).or_default().clone()
}

/// What [`Controller::hold`] gives: held until dropped.
pub type Held = tokio::sync::OwnedMutexGuard<()>;

/// Cloud Controller's `default_health_check_timeout`: the seconds an app's process has to
/// listen.
const DEFAULT_HEALTH_CHECK_TIMEOUT: u64 = 60;

/// Its `maximum_health_check_timeout`.
const MAXIMUM_HEALTH_CHECK_TIMEOUT: u64 = 180;

/// What a Firecracker wicket does not list of a route's `boot-ms`: its default.
const WICKET_DEFAULT_BOOT_MS: u64 = 10_000;

/// What a Firecracker wicket does not list of a route's `keep-ms`: its default.
const WICKET_DEFAULT_KEEP_MS: u64 = 60_000;

/// What a Firecracker wicket does not list of a route's readiness and liveness checks: their
/// defaults.
const WICKET_DEFAULT_PROBE_ENDPOINT: &str = "/";
const WICKET_DEFAULT_PROBE_TIMEOUT_MS: u64 = 1_000;
const WICKET_DEFAULT_PROBE_INTERVAL_MS: u64 = 30_000;

/// What an instance takes at once, unannotated: a component's requests, a pool VM's connections.
/// Cloud Run's 80 is where it adds an instance; at `-i 1` this is where a request is turned
/// away, so it is what a CF app's one instance takes, as many as a JVM's request threads.
const DEFAULT_CONCURRENCY: u64 = 200;

/// What a Firecracker wicket does not list of a pool route's `concurrency`: its default.
const WICKET_DEFAULT_CONCURRENCY: u64 = 16;

/// A rootfs's sandbox on a Firecracker wicket: `instances` VMs at most (from 1: 0 is no app),
/// living as `lifetime` says. None annotated, a pool of that size cloned from a snapshot, at any
/// size: a scale adds to the VMs running, and takes from them, in a restore's time (`boot`, annotated,
/// a boot's). What is not annotated is the wicket's default, but a pool's `concurrency`
/// ([`DEFAULT_CONCURRENCY`]). Its process has `timeout` seconds to
/// listen, and connects to the destinations of `egress`. A VM takes connections once its
/// `readiness` check passes, `port` or `http` (`process`: once up, what the wicket does unasked),
/// and is up once its `liveness` check passes, down once it fails, `http` (`port` and `process`:
/// once its port accepts, down once it refuses, what the wicket does unasked).
#[allow(clippy::too_many_arguments)]
fn vm_sandbox(
    memory_mib: u64,
    instances: u64,
    timeout: u64,
    env: &BTreeMap<String, String>,
    egress: &[String],
    lifetime: &Lifetime,
    readiness: &ReadinessCheck,
    liveness: &HealthCheck,
) -> VmSandbox {
    let mode = lifetime.mode.as_deref().unwrap_or("pool");
    let checked = readiness.kind != "process";
    let millis = |seconds: Option<u64>| seconds.filter(|_| checked).map(|seconds| seconds * 1000);
    let probed = liveness.kind == "http";
    let probing = |seconds: Option<u64>| seconds.filter(|_| probed).map(|seconds| seconds * 1000);
    VmSandbox {
        mode: Some(mode.into()),
        memory_mib: Some(memory_mib),
        instances: Some(instances),
        warm: lifetime.warm,
        keep_ms: lifetime.keep_ms,
        uses: lifetime.uses,
        lifetime_ms: lifetime.lifetime_ms,
        // what only a pool takes
        concurrency: match mode {
            "pool" => Some(lifetime.concurrency.unwrap_or(DEFAULT_CONCURRENCY)),
            _ => lifetime.concurrency,
        },
        boot_ms: Some(timeout.clamp(1, MAXIMUM_HEALTH_CHECK_TIMEOUT) * 1000),
        readiness: checked.then(|| readiness.kind.clone()),
        readiness_endpoint: (readiness.kind == "http")
            .then(|| readiness.endpoint.clone())
            .flatten(),
        readiness_timeout_ms: millis(readiness.invocation_timeout),
        readiness_interval_ms: millis(readiness.interval),
        liveness: probed.then(|| liveness.kind.clone()),
        liveness_endpoint: liveness.endpoint.clone().filter(|_| probed),
        liveness_timeout_ms: probing(liveness.invocation_timeout),
        liveness_interval_ms: probing(liveness.interval),
        egress: egress.to_vec(),
        env: env.clone(),
    }
}

/// The droplet `sha256` is neither kept nor to be made again.
fn gone(sha256: &str) -> Failure {
    Failure::refused(409, format!("the droplet {sha256} is gone"))
}

/// Whether the wicket serves `desired` as listed: the capabilities and the egress in any order;
/// the env, which the wicket lists by its names, by the tag; the sandbox as its wicket lists it
/// ([`same_wasm`], [`same_vm`]).
fn same(listed: &AppBody, desired: &AppBody) -> bool {
    sorted(&listed.hosts) == desired.hosts
        && listed.component == desired.component
        && listed.files == desired.files
        && listed.tag == desired.tag
        && listed.held_ms == desired.held_ms
        && match (&listed.sandbox, &desired.sandbox) {
            (Sandbox::Wasm(listed), Sandbox::Wasm(desired)) => same_wasm(listed, desired),
            (Sandbox::Vm(listed), Sandbox::Vm(desired)) => same_vm(listed, desired),
            _ => false,
        }
}

/// A wasm wicket's sandbox: a `budget-ms` left to the wicket's default matches whatever it
/// chose.
fn same_wasm(listed: &WasmSandbox, desired: &WasmSandbox) -> bool {
    sorted(&listed.wasi) == sorted(&desired.wasi)
        && listed.warm == desired.warm
        && listed.instances == desired.instances
        && listed.keep_ms == desired.keep_ms
        && listed.concurrency == desired.concurrency
        && listed.memory_mib == desired.memory_mib
        && desired
            .budget_ms
            .is_none_or(|budget| listed.budget_ms == Some(budget))
        && listed.keyvalue == desired.keyvalue
}

/// A Firecracker wicket's sandbox: the options it lists only away from their defaults as those
/// defaults.
fn same_vm(listed: &VmSandbox, desired: &VmSandbox) -> bool {
    let warm = |sandbox: &VmSandbox| match (sandbox.warm, sandbox.mode.as_deref()) {
        (Some(warm), _) => warm,
        (None, Some("pool")) => sandbox.instances.unwrap_or(0),
        (None, _) => 0,
    };
    // booted, 1 goes unlisted
    let instances = |sandbox: &VmSandbox| match sandbox.mode.as_deref() {
        Some("boot") => Some(sandbox.instances.unwrap_or(1)),
        _ => sandbox.instances,
    };
    let keep_ms = |sandbox: &VmSandbox| sandbox.keep_ms.unwrap_or(WICKET_DEFAULT_KEEP_MS);
    let concurrency =
        |sandbox: &VmSandbox| sandbox.concurrency.unwrap_or(WICKET_DEFAULT_CONCURRENCY);
    // a check, its endpoint, timeout and interval as the wicket takes them: `check` the default
    let probe = |check: &Option<String>,
                 endpoint: &Option<String>,
                 timeout: Option<u64>,
                 interval: Option<u64>,
                 default: &str| {
        (
            check.clone().unwrap_or_else(|| default.into()),
            endpoint
                .clone()
                .unwrap_or_else(|| WICKET_DEFAULT_PROBE_ENDPOINT.into()),
            timeout.unwrap_or(WICKET_DEFAULT_PROBE_TIMEOUT_MS),
            interval.unwrap_or(WICKET_DEFAULT_PROBE_INTERVAL_MS),
        )
    };
    let readiness = |sandbox: &VmSandbox| {
        let (check, endpoint) = (&sandbox.readiness, &sandbox.readiness_endpoint);
        let (timeout, interval) = (sandbox.readiness_timeout_ms, sandbox.readiness_interval_ms);
        probe(check, endpoint, timeout, interval, "process")
    };
    let liveness = |sandbox: &VmSandbox| {
        let (check, endpoint) = (&sandbox.liveness, &sandbox.liveness_endpoint);
        let (timeout, interval) = (sandbox.liveness_timeout_ms, sandbox.liveness_interval_ms);
        probe(check, endpoint, timeout, interval, "port")
    };
    sorted(&listed.egress) == sorted(&desired.egress)
        && listed.mode == desired.mode
        && listed.memory_mib == desired.memory_mib
        && instances(listed) == instances(desired)
        && warm(listed) == warm(desired)
        && keep_ms(listed) == keep_ms(desired)
        && concurrency(listed) == concurrency(desired)
        && listed.uses == desired.uses
        && listed.lifetime_ms == desired.lifetime_ms
        && readiness(listed) == readiness(desired)
        && liveness(listed) == liveness(desired)
        && desired
            .boot_ms
            .is_none_or(|boot_ms| listed.boot_ms.unwrap_or(WICKET_DEFAULT_BOOT_MS) == boot_ms)
}

fn sorted(list: &[String]) -> Vec<String> {
    let mut list = list.to_vec();
    list.sort();
    list
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::Running;

    /// The share of `app` on the one wicket `w`, of the stack the app is started as.
    fn on_w(state: &State, app: &App) -> Option<AppBody> {
        let mut state = state.clone();
        let stack = app.running.as_ref().map(Running::stack);
        state.wickets.runtimes = stack
            .map(|stack| ("w".to_string(), stack))
            .into_iter()
            .collect();
        let mut shares = Controller::desired(&state, app, "cf");
        let share = shares.remove("w");
        assert!(shares.is_empty(), "on another wicket: {shares:?}");
        share
    }

    #[test]
    fn an_app_is_desired_started_with_a_droplet_and_a_route() {
        let mut state = State::new("o", "s", &["localhost".into()]);
        state.follow_wickets(&["w".into()]);
        let space = state.spaces[0].guid.clone();
        let guid = state.create_app("hello", &space).guid.clone();
        let domain = state.domains[0].guid.clone();
        let app = state.app(&guid).unwrap().clone();
        assert_eq!(on_w(&state, &app), None);
        let running = Running {
            droplet: "d".into(),
            sha256: "ab".repeat(32),
            made: BTreeMap::new(),
            file: "hello.wasm".into(),
            staged_as: None,
            files: Some("cd".repeat(32)),
            memory_mib: 64,
            budget_ms: Some(5000),
            lifetime: Lifetime::default(),
            readiness: ReadinessCheck::default(),
            liveness: HealthCheck::default(),
            wasi: vec!["keyvalue".into(), "cli".into()],
            keyvalue: [(String::new(), "local/cf/i".to_string())].into(),
            env: [("GREETING".to_string(), "hello".to_string())].into(),
            egress: vec!["db.internal:5432".into(), "10.0.0.0/24:443".into()],
            tag: Some("t1".into()),
            since: 0,
        };
        state.app_mut(&guid).unwrap().running = Some(running);
        state.app_mut(&guid).unwrap().started = true;
        state.app_mut(&guid).unwrap().process.instances = 2;
        let app = state.app(&guid).unwrap().clone();
        assert_eq!(on_w(&state, &app), None, "no route");
        let route = state.create_route("hello", &domain, &space).guid.clone();
        state.map(&route, &guid);
        let desired = on_w(&state, state.app(&guid).unwrap()).unwrap();
        assert_eq!(desired.hosts, ["hello.localhost"]);
        assert_eq!(
            desired.files,
            Some(FilesRef {
                sha256: "cd".repeat(32)
            })
        );
        let wasm = desired.sandbox.wasm();
        assert_eq!((wasm.warm, wasm.memory_mib), (Some(2), Some(64)));
        assert_eq!(wasm.keyvalue[""], "local/cf/i");
        // the env put, listed by its names: told by the tag
        let mut body = serde_json::to_value(&desired).unwrap();
        assert_eq!(
            (&body["sandbox"]["env"]["GREETING"], &body["tag"]),
            (&"hello".into(), &"t1".into())
        );
        body["sandbox"]["env"] = serde_json::json!(["GREETING"]);
        let listed = |body| {
            let body: AppBody<serde_json::Value> = serde_json::from_value(body).unwrap();
            body.typed(Runtime::Wasm).unwrap()
        };
        let mut listed = listed(body);
        assert!(same(&listed, &desired));
        listed.tag = Some("t0".into());
        assert!(!same(&listed, &desired), "another snapshot");
        listed.tag = desired.tag.clone();
        listed.files = None;
        assert!(!same(&listed, &desired), "other files");
        listed.files = desired.files.clone();
        listed.sandbox.wasm_mut().budget_ms = Some(5000);
        // the wicket lists the capabilities in its order
        listed.sandbox.wasm_mut().wasi = vec!["cli".into(), "keyvalue".into()];
        assert!(same(&listed, &desired));
        listed.sandbox.wasm_mut().warm = Some(3);
        assert!(!same(&listed, &desired));
        let mut unset = desired.clone();
        unset.sandbox.wasm_mut().budget_ms = None;
        listed.sandbox.wasm_mut().warm = Some(2);
        listed.sandbox.wasm_mut().budget_ms = Some(10_000);
        assert!(same(&listed, &unset));
        listed.sandbox.wasm_mut().keep_ms = Some(500);
        assert!(!same(&listed, &unset), "another keep-ms");
        listed.sandbox.wasm_mut().keep_ms = None;
        listed.sandbox.wasm_mut().concurrency = Some(1);
        assert!(!same(&listed, &unset), "another concurrency");
        listed.sandbox.wasm_mut().concurrency = unset.sandbox.wasm().concurrency;
        // the same options of a VM are not a component's
        let vm = Sandbox::Vm(VmSandbox::default());
        assert!(!same(
            &AppBody {
                sandbox: vm,
                ..listed.clone()
            },
            &unset
        ));
        // a count below what the start keeps warm, as a manifest leaves it: the wicket's kept
        let running = state.app_mut(&guid).unwrap().running.as_mut().unwrap();
        (running.file, running.lifetime.mode, running.lifetime.warm) =
            ("hello.ext4".into(), Some("pool".into()), Some(3));
        let floored = on_w(&state, state.app(&guid).unwrap()).unwrap();
        let vm = floored.sandbox.vm();
        assert_eq!((vm.instances, vm.warm), (Some(3), Some(3)));
        let running = state.app_mut(&guid).unwrap().running.as_mut().unwrap();
        (running.file, running.lifetime.mode) = ("hello.wasm".into(), None);
        // `cf scale` a component's limit too: the instances kept unless annotated, each taking
        // `concurrency` at once (200 unless annotated), past what the wicket runs or not, to
        // what is placed; as a VM's, no lower than what the start keeps warm, which the wicket
        // refuses. `keep-ms` as annotated
        for (instances, warm, concurrency, keep_ms, kept, cap) in [
            (1, None, None, None, 1, 1),
            (2, Some(1), None, Some(500), 1, 2),
            (2, None, Some(1), None, 2, 2),
            (2, Some(0), None, Some(60_000), 0, 2),
            (2, Some(1), Some(3), None, 1, 2),
            (1, Some(3), Some(1), None, 3, 3),
            (
                u64::MAX,
                Some(0),
                None,
                None,
                0,
                crate::placement::MAX_INSTANCES,
            ),
        ] {
            let app = state.app_mut(&guid).unwrap();
            app.process.instances = instances;
            let running = app.running.as_mut().unwrap();
            (
                running.lifetime.warm,
                running.lifetime.concurrency,
                running.lifetime.keep_ms,
            ) = (warm, concurrency, keep_ms);
            let limited = on_w(&state, state.app(&guid).unwrap()).unwrap();
            let limited = limited.sandbox.wasm();
            assert_eq!(
                (
                    limited.warm,
                    limited.instances,
                    limited.concurrency,
                    limited.keep_ms
                ),
                (
                    Some(kept),
                    Some(cap),
                    Some(concurrency.unwrap_or(200)),
                    keep_ms
                ),
                "{instances} {warm:?} {concurrency:?} {keep_ms:?}"
            );
        }
        let running = state.app_mut(&guid).unwrap().running.as_mut().unwrap();
        (
            running.lifetime.warm,
            running.lifetime.concurrency,
            running.lifetime.keep_ms,
        ) = (None, None, None);
        // 0 is as stopped, on any wicket
        state.app_mut(&guid).unwrap().process.instances = 0;
        assert_eq!(on_w(&state, state.app(&guid).unwrap()), None);
        state.app_mut(&guid).unwrap().process.instances = 2;

        // a rootfs is a Firecracker wicket's VMs, its env the same, the wasm options none
        let running = state.app_mut(&guid).unwrap().running.as_mut().unwrap();
        running.file = "hello.ext4".into();
        let desired = on_w(&state, state.app(&guid).unwrap()).unwrap();
        let body = serde_json::to_value(&desired).unwrap();
        assert_eq!(
            body["sandbox"],
            serde_json::json!({"mode": "pool", "memory-mib": 64, "instances": 2, "concurrency": 200,
                "boot-ms": 60_000, "egress": ["db.internal:5432", "10.0.0.0/24:443"], "env": {"GREETING": "hello"}})
        );
        // as the wicket lists it: its vCPUs too, the env by its names
        let mut listed = body;
        listed["sandbox"]["vcpus"] = 1.into();
        listed["sandbox"]["env"] = serde_json::json!(["GREETING"]);
        // the egress in the wicket's order
        listed["sandbox"]["egress"] = serde_json::json!(["10.0.0.0/24:443", "db.internal:5432"]);
        let listed: AppBody<serde_json::Value> = serde_json::from_value(listed).unwrap();
        let mut listed = listed.typed(Runtime::Firecracker).unwrap();
        assert!(same(&listed, &desired));
        listed.sandbox.vm_mut().egress.pop();
        assert!(!same(&listed, &desired), "another group's rules");
        listed.sandbox.vm_mut().egress = desired.sandbox.vm().egress.clone();
        listed.sandbox.vm_mut().instances = Some(3);
        assert!(!same(&listed, &desired));
        listed.sandbox.vm_mut().instances = Some(2);
        listed.sandbox.vm_mut().boot_ms = Some(10_000);
        assert!(!same(&listed, &desired));
        // the wicket's default goes unlisted
        let mut asked = desired.clone();
        asked.sandbox.vm_mut().boot_ms = Some(10_000);
        listed.sandbox.vm_mut().boot_ms = None;
        assert!(same(&listed, &asked));
        // the health check's timeout, within Cloud Controller's maximum
        for (timeout, boot_ms) in [(None, 60_000), (Some(5), 5_000), (Some(9_999), 180_000)] {
            let app = state.app_mut(&guid).unwrap();
            app.process.health_check.timeout = timeout;
            let desired = on_w(&state, state.app(&guid).unwrap()).unwrap();
            assert_eq!(desired.sandbox.vm().boot_ms, Some(boot_ms));
        }
        // `cf scale` the cap, the annotations the lifetime, each option only what it names
        let lifetime = |mode: Option<&str>, warm, keep_ms, uses, lifetime_ms| Lifetime {
            mode: mode.map(String::from),
            warm,
            keep_ms,
            uses,
            lifetime_ms,
            concurrency: None,
        };
        for (instances, annotated, mode, cap, warm) in [
            (1, Lifetime::default(), "pool", Some(1), None),
            (3, Lifetime::default(), "pool", Some(3), None),
            (
                1,
                lifetime(Some("boot"), None, None, None, None),
                "boot",
                Some(1),
                None,
            ),
            (
                3,
                lifetime(Some("boot"), None, None, None, None),
                "boot",
                Some(3),
                None,
            ),
            (
                1,
                lifetime(Some("per-connection"), None, None, None, None),
                "per-connection",
                Some(1),
                None,
            ),
            (
                3,
                lifetime(Some("per-connection"), Some(1), None, None, None),
                "per-connection",
                Some(3),
                Some(1),
            ),
            (
                3,
                lifetime(Some("reuse"), None, Some(500), Some(9), Some(800)),
                "reuse",
                Some(3),
                None,
            ),
            (
                1,
                lifetime(Some("pool"), None, None, None, None),
                "pool",
                Some(1),
                None,
            ),
            (
                3,
                lifetime(Some("pool"), None, Some(500), None, None),
                "pool",
                Some(3),
                None,
            ),
            (
                3,
                lifetime(Some("pool"), Some(1), Some(500), None, None),
                "pool",
                Some(3),
                Some(1),
            ),
            (
                3,
                Lifetime {
                    concurrency: Some(4),
                    ..lifetime(Some("pool"), None, None, None, None)
                },
                "pool",
                Some(3),
                None,
            ),
        ] {
            let sandbox = vm_sandbox(
                64,
                instances,
                60,
                &BTreeMap::new(),
                &[],
                &annotated,
                &ReadinessCheck::default(),
                &HealthCheck::default(),
            );
            assert_eq!(
                (sandbox.mode.as_deref(), sandbox.instances, sandbox.warm),
                (Some(mode), cap, warm)
            );
            assert_eq!(
                (
                    sandbox.keep_ms,
                    sandbox.uses,
                    sandbox.lifetime_ms,
                    sandbox.concurrency
                ),
                (
                    annotated.keep_ms,
                    annotated.uses,
                    annotated.lifetime_ms,
                    // a pool's 200 unless annotated, the others' none
                    (mode == "pool").then_some(annotated.concurrency.unwrap_or(200))
                )
            );
        }
        // the options the wicket lists only away from their defaults
        let annotated = |warm, keep_ms| Lifetime {
            mode: Some("pool".into()),
            warm,
            keep_ms,
            ..Lifetime::default()
        };
        let body = |sandbox: VmSandbox| AppBody {
            sandbox: Sandbox::Vm(sandbox),
            ..desired.clone()
        };
        let asked = vm_sandbox(
            64,
            2,
            60,
            &BTreeMap::new(),
            &[],
            &Lifetime {
                concurrency: Some(16),
                ..annotated(Some(2), Some(60_000))
            },
            &ReadinessCheck::default(),
            &HealthCheck::default(),
        );
        let mut told = asked.clone();
        (told.warm, told.keep_ms, told.concurrency) = (None, None, None);
        assert!(same(&body(told.clone()), &body(asked.clone())));
        told.warm = Some(0);
        assert!(!same(&body(told.clone()), &body(asked.clone())));
        told.warm = None;
        told.keep_ms = Some(500);
        assert!(!same(&body(told.clone()), &body(asked.clone())));
        told.keep_ms = None;
        told.concurrency = Some(3);
        assert!(!same(&body(told.clone()), &body(asked.clone())));
        // a readiness check and a liveness check, listed away from their defaults
        told.concurrency = None;
        let lifetime = Lifetime {
            concurrency: Some(16),
            ..annotated(Some(2), Some(60_000))
        };
        let checks = |readiness: &ReadinessCheck, liveness: &HealthCheck| {
            vm_sandbox(
                64,
                2,
                60,
                &BTreeMap::new(),
                &[],
                &lifetime,
                readiness,
                liveness,
            )
        };
        let checked = |kind: &str, endpoint: Option<&str>, interval| {
            let readiness = ReadinessCheck {
                kind: kind.into(),
                endpoint: endpoint.map(String::from),
                invocation_timeout: None,
                interval,
            };
            checks(&readiness, &HealthCheck::default())
        };
        let alive = |kind: &str, endpoint: Option<&str>, interval| {
            let liveness = HealthCheck {
                kind: kind.into(),
                endpoint: endpoint.map(String::from),
                invocation_timeout: Some(2),
                interval,
                timeout: Some(90),
            };
            checks(&ReadinessCheck::default(), &liveness)
        };
        let live = alive("http", Some("/live"), Some(5));
        assert_eq!(
            (
                live.liveness.as_deref(),
                live.liveness_endpoint.as_deref(),
                live.liveness_timeout_ms,
                live.liveness_interval_ms
            ),
            (Some("http"), Some("/live"), Some(2_000), Some(5_000))
        );
        for kind in ["port", "process"] {
            assert_eq!(
                alive(kind, None, Some(5)),
                asked,
                "{kind}: the wicket's own"
            );
        }
        let mut listed = told.clone();
        listed.liveness = Some("http".into());
        (listed.liveness_timeout_ms, listed.liveness_interval_ms) = (Some(2_000), Some(5_000));
        assert!(same(
            &body(listed.clone()),
            &body(alive("http", None, Some(5)))
        ));
        assert!(!same(&body(listed), &body(live)));
        let http = checked("http", Some("/"), Some(5));
        assert_eq!(
            (
                http.readiness.as_deref(),
                http.readiness_endpoint.as_deref(),
                http.readiness_interval_ms
            ),
            (Some("http"), Some("/"), Some(5_000))
        );
        assert_eq!(
            checked("process", Some("/ready"), Some(5)),
            asked,
            "nothing to put"
        );
        (told.readiness, told.readiness_interval_ms) = (Some("http".into()), Some(5_000));
        assert!(
            same(&body(told.clone()), &body(http.clone())),
            "its endpoint the default"
        );
        assert!(!same(
            &body(told.clone()),
            &body(checked("port", None, Some(5)))
        ));
        assert!(!same(&body(told), &body(checked("http", Some("/"), None))));
        // booted, one VM unlisted, more listed
        let booted = |instances| {
            let lifetime = Lifetime {
                mode: Some("boot".into()),
                ..Lifetime::default()
            };
            let readiness = ReadinessCheck::default();
            vm_sandbox(
                64,
                instances,
                60,
                &BTreeMap::new(),
                &[],
                &lifetime,
                &readiness,
                &HealthCheck::default(),
            )
        };
        let mut told = booted(1);
        told.instances = None;
        assert!(same(&body(told.clone()), &body(booted(1))));
        assert!(!same(&body(told), &body(booted(2))));
        assert!(same(&body(booted(2)), &body(booted(2))));
        // a removal the wicket did not take is tried again, but for another owner's app
        let refused = |status| Failure::refused(status, String::new());
        assert!(refused(422).retried());
        assert!(Failure::Unreachable(String::new()).retried());
        assert!(!refused(409).retried());

        // over two wickets a share each, of the indices placed there (0 and 2, 1): their count as
        // `instances`, those below the annotated `warm` (the lowest) as `warm`; a component's all
        // of them unannotated, a VM's as its wicket keeps them
        state.follow_wickets(&["w".into(), "v".into()]);
        let app = state.app_mut(&guid).unwrap();
        app.process.instances = 3;
        app.placement = vec!["w".into(), "v".into(), "w".into()];
        for (file, warm, on_w, on_v) in [
            ("hello.wasm", None, (2, Some(2)), (1, Some(1))),
            ("hello.wasm", Some(1), (2, Some(1)), (1, Some(0))),
            ("hello.ext4", None, (2, None), (1, None)),
            ("hello.ext4", Some(2), (2, Some(1)), (1, Some(1))),
        ] {
            let running = state.app_mut(&guid).unwrap().running.as_mut().unwrap();
            (running.file, running.lifetime.warm) = (file.into(), warm);
            let shares = Controller::desired(&state, state.app(&guid).unwrap(), "cf");
            let counts = |wicket: &str| match &shares[wicket].sandbox {
                Sandbox::Wasm(sandbox) => (sandbox.instances.unwrap(), sandbox.warm),
                Sandbox::Vm(sandbox) => (sandbox.instances.unwrap(), sandbox.warm),
            };
            assert_eq!((counts("w"), counts("v")), (on_w, on_v), "{file} {warm:?}");
        }
        // an image's rootfs v made of its own guest: v's share names v's digest, w's the one
        // staged; v listing it is as wanted, the staged one not
        let running = state.app_mut(&guid).unwrap().running.as_mut().unwrap();
        let (staged, own) = (running.sha256.clone(), "ef".repeat(32));
        running.made_as("v", &own);
        let shares = Controller::desired(&state, state.app(&guid).unwrap(), "cf");
        assert_eq!(
            (&shares["w"].component.sha256, &shares["v"].component.sha256),
            (&staged, &own)
        );
        let mut listed = shares["v"].clone();
        assert!(same(&listed, &shares["v"]));
        listed.component.sha256.clone_from(&staged);
        assert!(!same(&listed, &shares["v"]), "the digest w made");
        // v making the staged one again: named no more
        let running = state.app_mut(&guid).unwrap().running.as_mut().unwrap();
        running.made_as("v", &staged);
        assert!(running.made.is_empty());
        // scaled down, the highest go, v's with them; not below what the start keeps warm
        let app = state.app_mut(&guid).unwrap();
        app.process.instances = 1;
        let shares = Controller::desired(&state, state.app(&guid).unwrap(), "cf");
        assert_eq!(shares.keys().collect::<Vec<_>>(), ["v", "w"], "warm 2");
        let running = state.app_mut(&guid).unwrap().running.as_mut().unwrap();
        running.lifetime.warm = None;
        let shares = Controller::desired(&state, state.app(&guid).unwrap(), "cf");
        assert_eq!(shares.keys().collect::<Vec<_>>(), ["w"], "none on v");

        state.app_mut(&guid).unwrap().started = false;
        assert!(Controller::desired(&state, state.app(&guid).unwrap(), "cf").is_empty());
    }

    /// A wicket's app API as the controller uses it: a listing held until its revision moves,
    /// answered a little later, `PUT` and `DELETE` of an app, the latter answered after
    /// `deleting`, each fenced by its owner's epoch as a wicket fences it; a PUT of the tag
    /// `refusing` refused, an app of `unadvertised` listed so.
    struct FakeWicket {
        apps: Mutex<BTreeMap<String, serde_json::Value>>,
        revision: tokio::sync::watch::Sender<u64>,
        /// The revision of the listing held last: the watch listed it.
        held: tokio::sync::watch::Sender<Option<u64>>,
        deleting: Duration,
        fence: sluice_wicket_console::fence::Fence,
        /// Each write's epoch, and whether the fence admitted it, in the order they came.
        writes: Mutex<Vec<(Option<u64>, bool)>>,
        refusing: Mutex<Option<String>>,
        unadvertised: Mutex<BTreeSet<String>>,
    }

    impl FakeWicket {
        /// Serves it on a port of its own: its URL.
        async fn start(deleting: Duration) -> (Arc<FakeWicket>, String) {
            let dir = std::env::temp_dir().join(format!("cf-api-fake-{}", crate::store::guid()));
            let fake = Arc::new(FakeWicket {
                apps: Mutex::default(),
                revision: tokio::sync::watch::Sender::new(0),
                held: tokio::sync::watch::Sender::new(None),
                deleting,
                fence: sluice_wicket_console::fence::Fence::open(&dir).unwrap(),
                writes: Mutex::default(),
                refusing: Mutex::default(),
                unadvertised: Mutex::default(),
            });
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let serving = fake.clone();
            tokio::spawn(sluice_wicket_server::accept::run(
                listener,
                16,
                "fake",
                move |stream, _, slot| {
                    let fake = serving.clone();
                    tokio::spawn(sluice_wicket_server::http::serve(stream, slot, move |r| {
                        fake.clone().answer(r)
                    }));
                },
            ));
            (fake, url)
        }

        fn names(&self) -> Vec<String> {
            let apps = self.apps.lock().unwrap();
            apps.keys().cloned().collect()
        }

        /// The tag of the app `name` it serves.
        fn tag(&self, name: &str) -> Option<String> {
            let apps = self.apps.lock().unwrap();
            Some(apps.get(name)?["tag"].as_str()?.to_string())
        }

        /// Whether no write was admitted after one of a higher epoch.
        fn fenced_none(&self) -> bool {
            let writes = self.writes.lock().unwrap();
            let admitted = writes.iter().filter(|(_, admitted)| *admitted);
            let mut highest = None;
            admitted.into_iter().all(|(epoch, _)| match epoch {
                Some(epoch) if highest.is_some_and(|highest| *epoch < highest) => false,
                Some(epoch) => {
                    highest = highest.max(Some(*epoch));
                    true
                }
                None => true,
            })
        }

        /// The epochs of the writes its fence refused.
        fn refused(&self) -> Vec<Option<u64>> {
            let writes = self.writes.lock().unwrap();
            let refused = writes.iter().filter(|(_, admitted)| !admitted);
            refused.map(|(epoch, _)| *epoch).collect()
        }

        fn moved(&self) -> u64 {
            self.revision.send_modify(|revision| *revision += 1);
            *self.revision.borrow()
        }

        async fn answer(
            self: Arc<Self>,
            request: hyper::Request<sluice_wicket_server::http::Body>,
        ) -> hyper::Response<http_body_util::Full<bytes::Bytes>> {
            use http_body_util::BodyExt as _;
            let (head, body) = request.into_parts();
            let name = head.uri.path().strip_prefix("/api/apps/");
            let epoch = (head.headers.get(sluice_wicket_console::fence::HEADER))
                .and_then(|epoch| epoch.to_str().ok()?.parse::<u64>().ok());
            let fenced = async |owner: &str| {
                let admitted = self.fence.admit(Some(owner), &head).await;
                (self.writes.lock().unwrap()).push((epoch, admitted.is_ok()));
                admitted
                    .err()
                    .map(sluice_wicket_console::http::Response::from)
            };
            let answer = match (head.method.clone(), name.map(str::to_string)) {
                (http::Method::GET, None) => {
                    let after = head
                        .uri
                        .query()
                        .and_then(|query| query.strip_prefix("after="));
                    if let Some(after) = after.map(|after| after.parse::<u64>().unwrap()) {
                        self.held.send_replace(Some(after));
                        let mut revision = self.revision.subscribe();
                        revision
                            .wait_for(|revision| *revision != after)
                            .await
                            .unwrap();
                        // after the answer to the edit that moved it
                        tokio::time::sleep(LOOK).await;
                    }
                    let unadvertised = self.unadvertised.lock().unwrap().clone();
                    let apps: Vec<serde_json::Value> = (self.apps.lock().unwrap().iter())
                        .map(|(name, app)| {
                            let mut app = app.clone();
                            app["name"] = name.as_str().into();
                            app["deployed"] = "".into();
                            app["alive"] = serde_json::json!([]);
                            app["advertised"] = (!unadvertised.contains(name)).into();
                            app
                        })
                        .collect();
                    let revision = *self.revision.borrow();
                    serde_json::json!({"revision": revision, "generation": revision, "apps": apps})
                }
                (http::Method::PUT, Some(name)) => {
                    let body = body.collect().await.unwrap().to_bytes();
                    let app: serde_json::Value = serde_json::from_slice(&body).unwrap();
                    if let Some(refused) = fenced(app["owner"].as_str().unwrap()).await {
                        return refused;
                    }
                    let refusing = self.refusing.lock().unwrap().clone();
                    if refusing.is_some() && app["tag"].as_str() == refusing.as_deref() {
                        let refused = sluice_wicket_console::http::error(
                            http::StatusCode::UNPROCESSABLE_ENTITY,
                            "the tag is refused",
                        );
                        return refused;
                    }
                    self.apps.lock().unwrap().insert(name, app);
                    serde_json::json!({"generation": self.moved(), "message": "put"})
                }
                (http::Method::DELETE, Some(name)) => {
                    let query = head.uri.query().unwrap_or_default();
                    if let Some(refused) = fenced(query.trim_start_matches("owner=")).await {
                        return refused;
                    }
                    tokio::time::sleep(self.deleting).await;
                    if self.apps.lock().unwrap().remove(&name).is_none() {
                        let mut gone = hyper::Response::new(http_body_util::Full::default());
                        *gone.status_mut() = http::StatusCode::NOT_FOUND;
                        return gone;
                    }
                    serde_json::json!({"generation": self.moved(), "message": "removed"})
                }
                (method, name) => panic!("{method} {name:?}"),
            };
            hyper::Response::new(answer.to_string().into())
        }
    }

    /// An app removed from two wickets, the second answering late: the first, whose watch lists
    /// it gone meanwhile, does not have it put back, the state wanting it still. Its share is
    /// held until the state no longer has the app.
    #[tokio::test]
    async fn a_removal_is_not_undone_by_a_listing() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let dir = std::env::temp_dir().join(format!("cf-api-removal-{}", crate::store::guid()));
        let wickets = ["a".to_string(), "b".to_string()];
        let runs = wickets.iter().cloned().map(|name| (name, Runtime::Wasm));
        let localhost = ["localhost".into()];
        let runs = runs.collect();
        let store = Store::open(&dir, "o", "s", &localhost, &wickets, &runs, None, None);
        let store = store.await.unwrap();
        let guid = store
            .update(|state| {
                let space = state.spaces[0].guid.clone();
                let domain = state.domains[0].guid.clone();
                let app = state.create_app("hello", &space);
                app.process.instances = 2;
                app.started = true;
                app.running = Some(Running {
                    sha256: "ab".repeat(32),
                    file: "hello.wasm".into(),
                    memory_mib: 64,
                    ..Running::default()
                });
                let guid = app.guid.clone();
                let route = state.create_route("hello", &domain, &space).guid.clone();
                state.map(&route, &guid);
                guid
            })
            .await;
        let shares =
            store.read(|state| Controller::desired(state, state.app(&guid).unwrap(), crate::OWNER));
        assert_eq!(shares.keys().collect::<Vec<_>>(), ["a", "b"]);
        let (a, url_a) = FakeWicket::start(Duration::ZERO).await;
        let (b, url_b) = FakeWicket::start(Duration::from_millis(500)).await;
        let controller = Arc::new(Controller::new(
            Arc::new(store),
            vec![
                Arc::new(Wicket::new("a", 2, &url_a, None, crate::OWNER)),
                Arc::new(Wicket::new("b", 2, &url_b, None, crate::OWNER)),
            ],
            Arc::new(Logs::default()),
            Duration::ZERO,
            None,
        ));
        tokio::spawn(controller.clone().lead());
        let within = Duration::from_secs(10);
        let put = async {
            while a.names().is_empty() || b.names().is_empty() {
                tokio::time::sleep(LOOK).await;
            }
        };
        tokio::time::timeout(within, put).await.unwrap();

        controller.remove(&guid, "hello").await;
        let revision = *a.revision.borrow();
        let mut held = a.held.subscribe();
        let listed = held.wait_for(|held| *held == Some(revision));
        tokio::time::timeout(within, listed).await.unwrap().unwrap();
        assert_eq!((a.names(), b.names()), (vec![], vec![]));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Until `done` holds, 20s at most.
    async fn until(what: &str, done: impl Fn() -> bool) {
        let started = Instant::now();
        while !done() {
            assert!(started.elapsed() < Duration::from_secs(20), "{what}");
            tokio::time::sleep(LOOK).await;
        }
    }

    /// Two instances on one store, each a controller of the wickets: the one whose turn it is
    /// puts, the other nothing, seeing what the first saw of them. The first stopped, or only
    /// paused past its lease, the other takes the deployment and the move it had under way to
    /// their end; a wicket admits no write of the first's epoch after one of the other's, and
    /// refuses the first's late put. The first, back, leads no more.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_next_instance_takes_up_what_the_one_leading_left() {
        use crate::deployments::Asked;
        use crate::store::Scaled;
        use crate::store::tests::{epoch, keep, leading};
        let _ = rustls::crypto::ring::default_provider().install_default();
        for paused in [false, true] {
            let (url, _) = crate::store::postgres_server::database().await;
            let settings = crate::postgres::PostgresSettings {
                url: format!("{url}?sslmode=disable"),
                ca_pem: None,
            };
            let names = ["a".to_string(), "b".to_string(), "c".to_string()];
            let runs = names
                .iter()
                .map(|name| (name.clone(), Runtime::Wasm))
                .collect();
            let localhost = ["localhost".to_string()];
            let open = async |of: &str| {
                let dir =
                    std::env::temp_dir().join(format!("cf-api-{of}-{}", crate::store::guid()));
                let store = Store::open(
                    &dir,
                    "o",
                    "s",
                    &localhost,
                    &names,
                    &runs,
                    Some(&settings),
                    None,
                );
                Arc::new(store.await.unwrap())
            };
            let (first, second) = (open("first").await, open("second").await);
            // an app of two instances deploying d2 over d1, a wicket at a time, and an app's
            // index moving from b to c
            let snapshot = |tag: &str| Running {
                droplet: tag.into(),
                sha256: "ab".repeat(32),
                file: "app.wasm".into(),
                memory_mib: 64,
                tag: Some(tag.into()),
                ..Running::default()
            };
            let (rolling, moving, deployment) = first
                .update(|state| {
                    let space = state.spaces[0].guid.clone();
                    let domain = state.domains[0].guid.clone();
                    let mut started = |name: &str, placement: &[&str], tag: &str| {
                        let app = state.create_app(name, &space);
                        app.process.instances = placement.len() as u64;
                        app.started = true;
                        app.running = Some(snapshot(tag));
                        app.placement = placement.iter().map(|on| on.to_string()).collect();
                        let guid = app.guid.clone();
                        let route = state.create_route(name, &domain, &space).guid.clone();
                        state.map(&route, &guid);
                        guid
                    };
                    let rolling = started("rolling", &["a", "b"], "d1");
                    let moving = started("moving", &["b"], "m1");
                    state.app_mut(&moving).unwrap().moving.insert(0, "c".into());
                    let serving = state.serving(state.app(&rolling).unwrap());
                    let asked = Asked {
                        strategy: "rolling".into(),
                        droplet: None,
                        max_in_flight: 1,
                        canary_steps: Vec::new(),
                        scaled: Scaled::default(),
                    };
                    let deployed =
                        state.deploy(&rolling, snapshot("d2"), serving, "d1".into(), &asked);
                    (rolling, moving, deployed.0.guid)
                })
                .await;
            let mut fakes = BTreeMap::new();
            for name in &names {
                fakes.insert(name.clone(), FakeWicket::start(Duration::ZERO).await);
            }
            // b refuses d2 and c's servers take moving's routes not: both under way
            *fakes["b"].0.refusing.lock().unwrap() = Some("d2".into());
            fakes["c"]
                .0
                .unadvertised
                .lock()
                .unwrap()
                .insert(moving.clone());
            let controller = |store: &Arc<Store>| {
                let wickets = (names.iter())
                    .map(|name| Arc::new(Wicket::new(name, 3, &fakes[name].1, None, crate::OWNER)))
                    .collect();
                let logs = Arc::new(Logs::default());
                Arc::new(Controller::new(
                    store.clone(),
                    wickets,
                    logs,
                    Duration::ZERO,
                    None,
                ))
            };
            let (one, other) = (controller(&first), controller(&second));
            let (_, keeping_first) = leading(&first).await;
            let leading_first = tokio::spawn(one.clone().lead());
            let (_, keeping_second) = keep(&second);
            let _leading_second = tokio::spawn(other.clone().lead());
            let tag = |wicket: &str, guid: &str| fakes[wicket].0.tag(guid);
            until("d2 on a, moving on c", || {
                tag("a", &rolling).as_deref() == Some("d2") && tag("c", &moving).is_some()
            })
            .await;
            until("b refused d2", || {
                let error =
                    first.read(|state| state.deployment(&deployment).unwrap().error.clone());
                error.is_some_and(|error| error.contains("the tag is refused"))
            })
            .await;
            // the other put nothing, and sees what the first saw of the wickets
            let first_epoch = epoch(&first);
            assert!(!other.leads());
            for (fake, _) in fakes.values() {
                let writes = fake.writes.lock().unwrap();
                assert!(
                    writes.iter().all(|(epoch, _)| *epoch == first_epoch),
                    "{writes:?}"
                );
            }
            second.sync().await.unwrap();
            let outcome = second.read(|state| state.outcome(&rolling, "a").cloned());
            assert_eq!(outcome, Some(Outcome::Running));

            // the first stopped (killed), or its renewals paused, its turn on
            keeping_first.abort();
            if !paused {
                leading_first.abort();
            }
            let mut terms = second.terms();
            let led = terms.wait_for(|term| term.is_some());
            tokio::time::timeout(Duration::from_secs(10), led)
                .await
                .unwrap()
                .unwrap();
            let second_epoch = epoch(&second);
            assert!(second_epoch > first_epoch);
            *fakes["b"].0.refusing.lock().unwrap() = None;
            fakes["c"].0.unadvertised.lock().unwrap().clear();
            until("the deployment finished, the index moved", || {
                second.read(|state| {
                    let deployed = state.deployment(&deployment).unwrap().state;
                    let placed = &state.app(&moving).unwrap().placement;
                    deployed == crate::store::DeploymentState::Deployed && *placed == ["c"]
                }) && tag("b", &rolling).as_deref() == Some("d2")
                    && tag("b", &moving).is_none()
            })
            .await;
            for (fake, _) in fakes.values() {
                assert!(fake.fenced_none(), "{:?}", fake.writes.lock().unwrap());
            }
            // the first's late put refused: its turn's epoch, below the other's
            let late = Wicket::new("b", 3, &fakes["b"].1, None, crate::OWNER);
            late.set_epoch(first_epoch);
            let share = second
                .read(|state| other.desired_on(state, &rolling, "b"))
                .unwrap();
            let refused = late.put(&rolling, &share).await;
            assert!(
                matches!(refused, Err(Failure::Fenced { .. })),
                "{refused:?}"
            );
            assert!(fakes["b"].0.refused().contains(&first_epoch));
            // back, the first's turn is over: what it does put, it puts as an edit does
            if paused {
                let (_, _keeping) = keep(&first);
                let mut terms = first.terms();
                let ended = terms.wait_for(|term| term.is_none());
                tokio::time::timeout(Duration::from_secs(10), ended)
                    .await
                    .unwrap()
                    .unwrap();
                until("its work ended", || !one.leads()).await;
            }
            assert_eq!(epoch(&second), second_epoch, "the other's turn goes on");
            keeping_second.abort();
        }
    }
}
