//! The wickets serve what the state wants. Each index of an app's web process is placed on a
//! wicket (`crate::placement`), and an app started with a droplet and a route is, on each wicket
//! holding some of its indices, an app of that wicket named by its guid, its share: its routes'
//! hosts, the snapshot's component and sandbox -- its env, which a wicket lists by the names only,
//! told by the snapshot's tag -- and its indices' count as the route's `instances`, those of them
//! below the annotated `warm` as its `warm` (the warm ones are the lowest, as a wasm wicket's units
//! are `0..warm`), each taking `concurrency` requests at once, or a droplet's rootfs as a
//! Firecracker wicket's VMs: [`vm_sandbox`]; stopped, scaled to 0, or without a route, it is none.
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
//! The wickets run apps one way, the first listed's: one listing another is logged, and none
//! placed or put on it. What they offer is what every one of them does.
//!
//! The data of a deleted keyvalue instance is deleted through every wicket once no app maps its
//! namespace, a wicket's draining routes included: tried at the deletion and again after each
//! listing.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::future::join_all;

use crate::logs::Logs;
use crate::services::Offer;
use crate::store::{App, Lifetime, Outcome, Purge, Removal, State, Store};
use crate::wicket::{
    AppBody, ComponentRef, Failure, FilesRef, ListedApp, Listing, MadeImage, Runtime, Sandbox,
    VmSandbox, WasmSandbox, Wicket,
};

/// A lock of each key: what is done one at a time.
type Locks<K> = Mutex<HashMap<K, Arc<tokio::sync::Mutex<()>>>>;

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
    /// What each wicket offers and runs apps as, as it last listed it, by name.
    listed: Mutex<BTreeMap<String, (Offer, Runtime)>>,
    /// The apps of the owner the state does not know, by wicket name and app name, as logged:
    /// once each.
    strangers: Mutex<BTreeSet<(String, String)>>,
}

impl Controller {
    pub fn new(store: Arc<Store>, wickets: Vec<Arc<Wicket>>, logs: Arc<Logs>) -> Controller {
        Controller {
            store,
            wickets,
            logs,
            held: Mutex::default(),
            shares: Mutex::default(),
            did_not_start: Mutex::default(),
            listed: Mutex::default(),
            strangers: Mutex::default(),
        }
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
    /// removes it when it does.
    pub async fn remove(&self, guid: &str, name: &str) {
        let _app = self.hold(guid).await;
        let removed = join_all(self.wickets.iter().map(|wicket| async move {
            let _share = self.hold_share(guid, &wicket.name).await;
            (wicket.name.clone(), self.apply(wicket, guid, None).await)
        }))
        .await;
        let blobs = self.store.update(|state| {
            for (wicket, removed) in removed {
                if removed.is_err_and(|e| e.retried()) {
                    state.removals.push(Removal {
                        app: guid.to_string(),
                        wicket,
                    });
                }
            }
            state.remove_app(guid)
        });
        self.store.delete(&blobs);
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
    /// indices the app of them. None when the app is stopped, scaled to 0, was never started
    /// with a droplet, or has no route.
    pub fn desired(state: &State, app: &App, owner: &str) -> BTreeMap<String, AppBody> {
        let Some(running) = app.running.as_ref().filter(|_| app.started) else {
            return BTreeMap::new();
        };
        if app.process.instances == 0 {
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
        let rootfs = running.file.ends_with(crate::staging::ROOTFS);
        // the lowest indices are the warm ones: of a component all unless annotated, of a VM
        // those its wicket keeps unless annotated
        let warm = match rootfs {
            true => running.lifetime.warm,
            false => Some(running.lifetime.warm.unwrap_or(app.process.instances)),
        };
        let timeout = app
            .process
            .health_check
            .timeout
            .unwrap_or(DEFAULT_HEALTH_CHECK_TIMEOUT);
        let share = |indices: &[u64]| {
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
            AppBody {
                owner: owner.to_string(),
                hosts: hosts.clone(),
                component: ComponentRef {
                    sha256: running.sha256.clone(),
                    name: running.file.clone(),
                },
                files: running.files.clone().map(|sha256| FilesRef { sha256 }),
                sandbox,
                tag: running.tag.clone(),
            }
        };
        let indices = state.indices(app).into_iter();
        indices
            .map(|(wicket, indices)| (wicket, share(&indices)))
            .collect()
    }

    /// The share of the app `guid` on the wicket `wicket` in `state`.
    pub fn desired_on(&self, state: &State, guid: &str, wicket: &str) -> Option<AppBody> {
        let app = state.app(guid)?;
        Controller::desired(state, app, self.owner()).remove(wicket)
    }

    /// What every wicket offers, as each last listed it; none listed yet, the first answering now.
    pub async fn offer(&self) -> Result<Offer, Failure> {
        if self.runtimes().0.is_none() {
            self.list_any().await?;
        }
        Ok(self.offered().unwrap_or_default())
    }

    /// What the wickets run apps as: the first listed's, in the order configured; none listed
    /// yet, the first answering now.
    pub async fn runtime(&self) -> Result<Runtime, Failure> {
        if let (Some(runtime), _) = self.runtimes() {
            return Ok(runtime);
        }
        self.list_any().await?;
        let noted = self.runtimes().0;
        noted.ok_or_else(|| Failure::Unreachable("no wicket listed".into()))
    }

    /// Lists the wickets in turn until one answers, noting what it lists.
    async fn list_any(&self) -> Result<(), Failure> {
        let mut failed = None;
        for wicket in &self.wickets {
            match wicket.list(None).await {
                Ok(listing) => {
                    self.note(wicket, &listing);
                    return Ok(());
                }
                Err(e) => failed = Some(e),
            }
        }
        Err(failed.unwrap_or_else(|| Failure::Unreachable("no wicket configured".into())))
    }

    /// The wickets' runtime, the first listed's in the order configured, and the wickets listing
    /// another, by name.
    fn runtimes(&self) -> (Option<Runtime>, BTreeSet<String>) {
        let listed = self.listed.lock().expect("no panic under the lock");
        let noted = self.wickets.iter().filter_map(|wicket| {
            let (_, runtime) = listed.get(&wicket.name)?;
            Some((&wicket.name, *runtime))
        });
        let runtime = noted.clone().next().map(|(_, runtime)| runtime);
        let odd = noted.filter(|(_, its)| Some(*its) != runtime);
        (runtime, odd.map(|(name, _)| name.clone()).collect())
    }

    /// What every wicket listed of the wickets' runtime offers.
    fn offered(&self) -> Option<Offer> {
        let (runtime, _) = self.runtimes();
        let listed = self.listed.lock().expect("no panic under the lock");
        let offers: Vec<&Offer> = (self.wickets.iter())
            .filter_map(|wicket| listed.get(&wicket.name))
            .filter(|(_, its)| Some(*its) == runtime)
            .map(|(offer, _)| offer)
            .collect();
        Offer::common(&offers)
    }

    /// Notes what `wicket` listed: what it offers and runs apps as. A wicket of another runtime
    /// than the wickets' is logged, and none placed or put on it.
    fn note(&self, wicket: &Wicket, listing: &Listing) {
        let offer = Offer::new(&listing.capabilities, &listing.keyvalue_stores);
        let noted = (offer, listing.runtime());
        (self.listed.lock().expect("no panic under the lock")).insert(wicket.name.clone(), noted);
        let (runtime, odd) = self.runtimes();
        let was = self
            .store
            .note(|state| std::mem::replace(&mut state.wickets.odd, odd.clone()));
        for wicket in &self.wickets {
            match (was.contains(&wicket.name), odd.contains(&wicket.name)) {
                (false, true) => println!(
                    "CELL: {} is put on no more: {}",
                    wicket.label,
                    self.odd(wicket).unwrap_or_default()
                ),
                (true, false) => println!(
                    "CELL: {} runs the stack {} again, as the wickets: placed on as the others",
                    wicket.label,
                    runtime.map_or("", Runtime::stack)
                ),
                _ => {}
            }
        }
    }

    /// Why nothing is put on `wicket`: it runs apps another way than the wickets.
    fn odd(&self, wicket: &Wicket) -> Option<String> {
        let listed = self.listed.lock().expect("no panic under the lock");
        let runtime = |wicket: &Wicket| listed.get(&wicket.name).map(|(_, runtime)| *runtime);
        let its = runtime(wicket)?;
        let first = self.wickets.iter().find(|first| runtime(first).is_some())?;
        let theirs = runtime(first)?;
        (its != theirs).then(|| {
            format!(
                "{} runs the stack {}, {} {}: --wicket names wickets of one runtime",
                wicket.label,
                its.stack(),
                first.label,
                theirs.stack()
            )
        })
    }

    /// Deletes through every wicket at once the data of the deleted instances no started app
    /// maps any more ([`Controller::purge_on`]).
    pub async fn purge_pending(&self) {
        join_all(self.wickets.iter().map(|wicket| self.purge_on(wicket))).await;
    }

    /// Deletes through `wicket` the data of the deleted instances no started app's snapshot maps
    /// any more, as far as it lets it: one a route of it still maps (409) waits, as does an
    /// unreachable wicket; one it cannot delete (a store gone: 404) is dropped there. Gone
    /// through every wicket, a deletion is done.
    async fn purge_on(&self, wicket: &Wicket) {
        let due: Vec<Purge> = self.store.read(|state| {
            let mapped = |place: &str| {
                state.apps.iter().any(|app| {
                    app.started
                        && app.running.as_ref().is_some_and(|running| {
                            running.keyvalue.values().any(|kept| *kept == place)
                        })
                })
            };
            (state.purges.iter())
                .filter(|purge| !purge.purged.contains(&wicket.name))
                .filter(|purge| !mapped(&format!("{}/{}", purge.store, purge.namespace)))
                .cloned()
                .collect()
        });
        for purge in due {
            let named = format!(
                "the data of service instance {} on {} ({}/{})",
                purge.instance, wicket.label, purge.store, purge.namespace
            );
            match wicket.purge(&purge.store, &purge.namespace).await {
                Ok(keys) => println!("SERVICES: deleted {named}: {keys} key(s)"),
                Err(Failure::Refused {
                    status: status @ (400 | 404),
                    error,
                    ..
                }) => println!(
                    "SERVICES: dropped {named}: {} refused it ({status}): {error}",
                    wicket.label
                ),
                Err(_) => continue,
            }
            self.store.update(|state| {
                let same = |kept: &&mut Purge| {
                    kept.store == purge.store && kept.namespace == purge.namespace
                };
                if let Some(kept) = state.purges.iter_mut().find(same) {
                    kept.purged.push(wicket.name.clone());
                }
                let configured = &state.wickets.configured;
                state
                    .purges
                    .retain(|kept| !configured.iter().all(|name| kept.purged.contains(name)));
            });
        }
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
            match wicket.delete(&guid).await {
                Ok(_) => println!("[{guid}] CELL: removed from {label}: the app was deleted"),
                Err(e) if e.retried() => {
                    println!("[{guid}] CELL: not removed from {label} yet: {e}");
                    continue;
                }
                Err(e) => println!("[{guid}] CELL: not removed from {label}: {e}"),
            }
            self.store.update(|state| {
                let gone = |kept: &Removal| kept.app == guid && kept.wicket == wicket.name;
                state.removals.retain(|kept| !gone(kept))
            });
        }
    }

    /// Puts the app `guid`'s share `desired` on `wicket` (or removes the app from it) and records
    /// the outcome: what the process stats and the log show. The share is the caller's to hold
    /// ([`Controller::hold_share`]).
    pub async fn apply(
        &self,
        wicket: &Wicket,
        guid: &str,
        desired: Option<&AppBody>,
    ) -> Result<(), Failure> {
        let share = (wicket.name.clone(), guid.to_string());
        let Some(app) = desired else {
            (self.did_not_start.lock().expect("no panic under the lock")).remove(&share);
            return match wicket.delete(guid).await {
                Ok(edited) => {
                    let was = self.store.note(|state| {
                        let outcomes = state.outcomes.get_mut(guid)?;
                        let was = outcomes.remove(&wicket.name);
                        if outcomes.is_empty() {
                            state.outcomes.remove(guid);
                        }
                        was
                    });
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
                    let line = format!("Not removed from {}: {e}", wicket.label);
                    self.logs.add(guid, "CELL", line);
                    Err(e)
                }
            };
        };
        if let Some(why) = self.odd(wicket) {
            let line = format!("{} took none of the app: {why}", wicket.sentence());
            self.record(wicket, guid, Outcome::Refused(why.clone()), &line);
            return Err(Failure::Refused {
                status: 409,
                error: why,
                host: None,
                missing: Vec::new(),
                did_not_start: false,
            });
        }
        let mut app = app.clone();
        let mut put = wicket.put(guid, &app).await;
        if let Err(Failure::Refused { missing, .. }) = &put
            && !missing.is_empty()
        {
            // the droplet is not uploaded (or no more): upload it and put again
            for sha256 in missing.clone() {
                if let Err(e) = self.provide(wicket, &sha256, &mut app).await {
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
            Err(Failure::Unreachable(e)) => (
                Outcome::Unreachable,
                format!(
                    "{} is unreachable, the app is put when it is back: {e}",
                    wicket.sentence()
                ),
            ),
        };
        self.record(wicket, guid, outcome, &line);
        put.map(|_| ())
    }

    /// Has `wicket` hold the droplet `sha256` of `app`: uploaded when the cf-api has its bytes,
    /// else made again of the image it was pinned from (a wicket's own rootfs). A wicket of
    /// another guest makes another digest: the droplet and the running app are that one's from
    /// here on, a rebuild of the same image and guest the same.
    async fn provide(
        &self,
        wicket: &Wicket,
        sha256: &str,
        app: &mut AppBody,
    ) -> Result<(), Failure> {
        if app
            .files
            .as_ref()
            .is_some_and(|files| files.sha256 == sha256)
        {
            let path = self.store.files_path(sha256);
            return match tokio::fs::File::open(&path).await {
                Ok(files) => wicket.upload_files(sha256, files).await,
                Err(e) => Err(gone(sha256, &path, e)),
            };
        }
        let path = self.store.droplet_path(sha256);
        match tokio::fs::File::open(&path).await {
            Ok(component) => return wicket.upload(sha256, component).await,
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
                return Err(gone(sha256, &path, e));
            }
            Err(_) => {}
        }
        let image = self.store.read(|state| {
            // the newest of that digest: an older one's package, and its credentials, may
            // be gone, the same image pushed again
            let droplet = state
                .droplets
                .iter()
                .rev()
                .find(|kept| kept.sha256 == sha256)?;
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
            Some((image, credentials))
        });
        let Some((image, credentials)) = image else {
            let e = std::io::Error::from(std::io::ErrorKind::NotFound);
            return Err(gone(sha256, &path, e));
        };
        let credentials = credentials
            .as_ref()
            .map(|(username, password)| (username.as_str(), password.as_str()));
        let made = wicket.make_image(&image, credentials).await?;
        if made.sha256 != sha256 {
            self.store.update(|state| {
                for droplet in &mut state.droplets {
                    if droplet.sha256 == sha256 {
                        droplet.sha256 = made.sha256.clone();
                        droplet.bytes = made.bytes;
                    }
                }
                for kept in &mut state.apps {
                    if let Some(running) = &mut kept.running
                        && running.sha256 == sha256
                    {
                        running.sha256 = made.sha256.clone();
                    }
                }
            });
            app.component.sha256 = made.sha256;
        }
        Ok(())
    }

    /// Has a wicket make an ext4 image of the OCI image `image`, with the `(username, password)`
    /// of a private one: the first answering of the wickets' runtime. The others make it at their
    /// first put of an app of it.
    pub async fn make_image(
        &self,
        image: &str,
        credentials: Option<(&str, &str)>,
    ) -> Result<MadeImage, Failure> {
        let mut failed = None;
        for wicket in self
            .wickets
            .iter()
            .filter(|wicket| self.odd(wicket).is_none())
        {
            match wicket.make_image(image, credentials).await {
                Err(e @ Failure::Unreachable(_)) => failed = Some(e),
                made => return made,
            }
        }
        Err(failed.unwrap_or_else(|| Failure::Unreachable("no wicket configured".into())))
    }

    /// Records the outcome of `wicket`'s share, logging it when it changed.
    fn record(&self, wicket: &Wicket, guid: &str, outcome: Outcome, line: &str) {
        let changed = self.store.note(|state| {
            if state.app(guid).is_none() {
                return false;
            }
            let outcomes = state.outcomes.entry(guid.to_string()).or_default();
            outcomes.insert(wicket.name.clone(), outcome.clone()) != Some(outcome)
        });
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
    /// app from it.
    pub async fn sync(&self, guid: &str) {
        join_all(self.wickets.iter().map(|wicket| async move {
            let _share = self.hold_share(guid, &wicket.name).await;
            let _ = self.sync_held(wicket, guid).await;
        }))
        .await;
    }

    /// Puts / deletes, each in a task, the difference between the apps `wicket` listed and the
    /// shares the state places on it, and logs the apps of the owner the state does not know. A
    /// share under a put or an edit is theirs, one whose VMs did not start as it is wanted is not
    /// tried again.
    fn fix(self: &Arc<Self>, wicket: &Arc<Wicket>, listed: &[ListedApp]) {
        let owner = &wicket.owner;
        let wanted: Vec<(String, Option<AppBody>)> = self.store.read(|state| {
            let apps = state.apps.iter();
            apps.map(|app| {
                let mut shares = Controller::desired(state, app, owner);
                (app.guid.clone(), shares.remove(&wicket.name))
            })
            .collect()
        });
        for (guid, desired) in &wanted {
            let listed = listed
                .iter()
                .find(|app| app.name == *guid && app.app.owner == *owner);
            match (desired, listed) {
                (Some(desired), Some(listed)) if same(&listed.app, desired) => {
                    let line = format!("Found in {} as wanted", wicket.label);
                    self.record(wicket, guid, Outcome::Running, &line);
                }
                (None, None) => {}
                _ if self.refused_as(wicket, guid, desired.as_ref()) => {}
                _ => {
                    let share = (guid.clone(), wicket.name.clone());
                    let Ok(held) = lock_of(&self.shares, share).try_lock_owned() else {
                        continue;
                    };
                    let (controller, wicket, guid) = (self.clone(), wicket.clone(), guid.clone());
                    tokio::spawn(async move {
                        let _held = held;
                        let _ = controller.sync_held(&wicket, &guid).await;
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
    }

    /// Whether `wicket`'s refusal of the share `desired` holds: its VMs did not start.
    fn refused_as(&self, wicket: &Wicket, guid: &str, desired: Option<&AppBody>) -> bool {
        let crashed = self.did_not_start.lock().expect("no panic under the lock");
        let share = (wicket.name.clone(), guid.to_string());
        crashed.get(&share).is_some_and(|put| Some(put) == desired)
    }

    /// Watches every wicket, each apart ([`Controller::watch_one`]).
    pub async fn watch(self: Arc<Self>) {
        let wickets = self.wickets.iter();
        join_all(wickets.map(|wicket| self.clone().watch_one(wicket.clone()))).await;
    }

    /// Lists `wicket`'s apps and fixes the difference at startup, whenever its revision moves and
    /// when it answers again, then tries its pending removals and purges (after a hold too: a
    /// drained route's end moves no revision); while it is unreachable, every share placed on it
    /// is `STARTING`, and it is tried again with a growing pause.
    async fn watch_one(self: Arc<Self>, wicket: Arc<Wicket>) {
        let mut after: Option<u64> = None;
        let mut pause = Duration::from_secs(1);
        loop {
            let listing = match wicket.list(after).await {
                Ok(listing) => listing,
                Err(e) => {
                    self.unreachable(&wicket, &e);
                    // back, it is looked at whatever its revision: a wicket restarted
                    // may have come to the one it had
                    after = None;
                    (self.did_not_start.lock().expect("no panic under the lock"))
                        .retain(|(at, _), _| *at != wicket.name);
                    tokio::time::sleep(pause).await;
                    pause = (pause * 2).min(Duration::from_secs(30));
                    continue;
                }
            };
            pause = Duration::from_secs(1);
            self.note(&wicket, &listing);
            let moved = after != Some(listing.revision);
            after = Some(listing.revision);
            let pending = self.store.read(|state| {
                (state.purges.iter()).any(|purge| !purge.purged.contains(&wicket.name))
                    || (state.removals.iter()).any(|removal| removal.wicket == wicket.name)
            });
            if !moved && !pending {
                continue;
            }
            if moved {
                self.fix(&wicket, &listing.apps);
            }
            self.remove_pending(&wicket).await;
            self.purge_on(&wicket).await;
        }
    }

    /// Every share placed on `wicket` is `STARTING` until it answers.
    fn unreachable(&self, wicket: &Wicket, e: &Failure) {
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
            self.record(wicket, &guid, Outcome::Unreachable, &line);
        }
    }
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
/// listen, and connects to the destinations of `egress`.
fn vm_sandbox(
    memory_mib: u64,
    instances: u64,
    timeout: u64,
    env: &BTreeMap<String, String>,
    egress: &[String],
    lifetime: &Lifetime,
) -> VmSandbox {
    let mode = lifetime.mode.as_deref().unwrap_or("pool");
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
        egress: egress.to_vec(),
        env: env.clone(),
    }
}

/// The droplet `sha256` is neither where it was kept nor to be made again.
fn gone(sha256: &str, path: &std::path::Path, e: std::io::Error) -> Failure {
    Failure::Refused {
        status: 409,
        error: format!("the droplet {sha256} is gone from {}: {e}", path.display()),
        host: None,
        missing: Vec::new(),
        did_not_start: false,
    }
}

/// Whether the wicket serves `desired` as listed: the capabilities and the egress in any order;
/// the env, which the wicket lists by its names, by the tag; the sandbox as its wicket lists it
/// ([`same_wasm`], [`same_vm`]).
fn same(listed: &AppBody, desired: &AppBody) -> bool {
    sorted(&listed.hosts) == desired.hosts
        && listed.component == desired.component
        && listed.files == desired.files
        && listed.tag == desired.tag
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
    sorted(&listed.egress) == sorted(&desired.egress)
        && listed.mode == desired.mode
        && listed.memory_mib == desired.memory_mib
        && instances(listed) == instances(desired)
        && warm(listed) == warm(desired)
        && keep_ms(listed) == keep_ms(desired)
        && concurrency(listed) == concurrency(desired)
        && listed.uses == desired.uses
        && listed.lifetime_ms == desired.lifetime_ms
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

    /// The share of `app` on the one wicket `w`.
    fn on_w(state: &State, app: &App) -> Option<AppBody> {
        let mut shares = Controller::desired(state, app, "cf");
        let share = shares.remove("w");
        assert!(shares.is_empty(), "on another wicket: {shares:?}");
        share
    }

    #[test]
    fn an_app_is_desired_started_with_a_droplet_and_a_route() {
        let mut state = State::new("o", "s", &["localhost".into()]);
        state.follow_wickets(&["w".into()]);
        let space = state.spaces[0].guid.clone();
        let guid = state
            .create_app("hello", crate::store::STACK, &space)
            .guid
            .clone();
        let domain = state.domains[0].guid.clone();
        let app = state.app(&guid).unwrap().clone();
        assert_eq!(on_w(&state, &app), None);
        let running = Running {
            droplet: "d".into(),
            sha256: "ab".repeat(32),
            file: "hello.wasm".into(),
            files: Some("cd".repeat(32)),
            memory_mib: 64,
            budget_ms: Some(5000),
            lifetime: Lifetime::default(),
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
            let sandbox = vm_sandbox(64, instances, 60, &BTreeMap::new(), &[], &annotated);
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
        assert!(!same(&body(told), &body(asked)));
        // booted, one VM unlisted, more listed
        let booted = |instances| {
            let lifetime = Lifetime {
                mode: Some("boot".into()),
                ..Lifetime::default()
            };
            vm_sandbox(64, instances, 60, &BTreeMap::new(), &[], &lifetime)
        };
        let mut told = booted(1);
        told.instances = None;
        assert!(same(&body(told.clone()), &body(booted(1))));
        assert!(!same(&body(told), &body(booted(2))));
        assert!(same(&body(booted(2)), &body(booted(2))));
        // a removal the wicket did not take is tried again, but for another owner's app
        let refused = |status| Failure::Refused {
            status,
            error: String::new(),
            host: None,
            missing: Vec::new(),
            did_not_start: false,
        };
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
}
