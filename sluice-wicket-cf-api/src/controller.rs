//! The wicket serves what the state wants: an app started with a droplet and
//! a route is an app of the wicket, named by its guid (its routes' hosts,
//! the snapshot's component and sandbox -- its env, which the wicket lists
//! by the names only, told by the snapshot's tag -- and the process's
//! instances as the pool, or a droplet's rootfs as a Firecracker wicket's
//! VMs: [`vm_sandbox`]); stopped, or without a route, it is none. The
//! desired state is the cf-api's: at startup and whenever the wicket's
//! revision moves, its apps of the owner are listed and the difference put /
//! deleted. The wicket serves on while the cf-api is down.
//!
//! An app of the owner the state does not know is left where it is, and logged: a state
//! that never knew an app did not put it (a lost data directory, another cf-api on the
//! wicket), and its absence says nothing of what is wanted. What the state deletes it
//! removes itself, at once or, the wicket not taking it, from `State::removals`.
//!
//! The data of a deleted keyvalue instance is deleted once no app maps its
//! namespace, the wicket's draining routes included: tried at the deletion
//! and again after each listing.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::logs::Logs;
use crate::services::Offer;
use crate::store::{App, Outcome, Purge, State, Store};
use crate::wicket::{AppBody, ComponentRef, Failure, ListedApp, Runtime, Sandbox, Wicket};

pub struct Controller {
    pub store: Arc<Store>,
    pub wicket: Arc<Wicket>,
    pub logs: Arc<Logs>,
    /// One edit at a time: a trial against the wicket and its commit are
    /// not interleaved with another's.
    pub edits: tokio::sync::Mutex<()>,
    /// What the wicket offers, as it last listed it.
    pub offer: Mutex<Option<Offer>>,
    /// What the wicket runs apps as, as it last listed it.
    pub runtime: Mutex<Option<Runtime>>,
    /// The apps of the owner the state does not know, as logged: once each.
    pub strangers: Mutex<BTreeSet<String>>,
}

impl Controller {
    /// The wicket's app of `app` as `state` wants it; None when the app is
    /// stopped, was never started with a droplet, or has no route.
    pub fn desired(state: &State, app: &App, owner: &str) -> Option<AppBody> {
        let running = app.running.as_ref().filter(|_| app.started)?;
        let mut hosts: Vec<String> = state
            .routes_of(&app.guid)
            .map(|route| state.url(route))
            .collect();
        if hosts.is_empty() {
            return None;
        }
        hosts.sort();
        Some(AppBody {
            owner: owner.to_string(),
            hosts,
            component: ComponentRef {
                sha256: running.sha256.clone(),
                name: running.file.clone(),
            },
            sandbox: match running.file.ends_with(crate::staging::ROOTFS) {
                true => vm_sandbox(running.memory_mib, app.process.instances, &running.env),
                false => Sandbox {
                    wasi: running.wasi.clone(),
                    budget_ms: running.budget_ms,
                    memory_mib: Some(running.memory_mib),
                    pool: Some(app.process.instances),
                    instances: running.instances,
                    keyvalue: running.keyvalue.clone(),
                    env: running.env.clone(),
                    ..Sandbox::default()
                },
            },
            tag: running.tag.clone(),
        })
    }

    /// What the wicket offers: as last listed, else listed now.
    pub async fn offer(&self) -> Result<Offer, Failure> {
        if let Some(offer) = self.offer.lock().expect("no panic under the lock").clone() {
            return Ok(offer);
        }
        let listing = self.wicket.list(None).await?;
        Ok(self.note_offer(&listing))
    }

    /// What the wicket runs apps as: as last listed, else listed now.
    pub async fn runtime(&self) -> Result<Runtime, Failure> {
        if let Some(runtime) = *self.runtime.lock().expect("no panic under the lock") {
            return Ok(runtime);
        }
        let listing = self.wicket.list(None).await?;
        self.note_offer(&listing);
        Ok(listing.runtime())
    }

    fn note_offer(&self, listing: &crate::wicket::Listing) -> Offer {
        let offer = Offer::new(&listing.capabilities, &listing.keyvalue_stores);
        *self.offer.lock().expect("no panic under the lock") = Some(offer.clone());
        *self.runtime.lock().expect("no panic under the lock") = Some(listing.runtime());
        offer
    }

    /// Deletes the data of the deleted instances no started app's snapshot
    /// maps any more, as far as the wicket lets it: one a route still maps
    /// (409) waits, as does an unreachable wicket; one it cannot delete (a
    /// store gone: 404) is dropped. Not under the edit lock: the caller
    /// holds it.
    pub async fn purge_pending(&self) {
        let due: Vec<Purge> = self.store.read(|state| {
            state
                .purges
                .iter()
                .filter(|purge| {
                    let place = format!("{}/{}", purge.store, purge.namespace);
                    !state.apps.iter().any(|app| {
                        app.started
                            && app.running.as_ref().is_some_and(|running| {
                                running.keyvalue.values().any(|kept| *kept == place)
                            })
                    })
                })
                .cloned()
                .collect()
        });
        for purge in due {
            let named = format!(
                "the data of service instance {} ({}/{})",
                purge.instance, purge.store, purge.namespace
            );
            match self.wicket.purge(&purge.store, &purge.namespace).await {
                Ok(keys) => println!("SERVICES: deleted {named}: {keys} key(s)"),
                Err(Failure::Refused {
                    status: status @ (400 | 404),
                    error,
                    ..
                }) => {
                    println!("SERVICES: dropped {named}: the wicket refused it ({status}): {error}")
                }
                Err(_) => continue,
            }
            self.store
                .update(|state| state.purges.retain(|kept| *kept != purge));
        }
    }

    /// Removes from the wicket the apps deleted while it was unreachable, as far as it
    /// answers. Not under the edit lock: the caller holds it.
    async fn remove_pending(&self) {
        for guid in self.store.read(|state| state.removals.clone()) {
            match self.wicket.delete(&guid).await {
                Ok(_) => println!("[{guid}] CELL: removed from the wicket: the app was deleted"),
                Err(e) if e.retried() => {
                    println!("[{guid}] CELL: not removed from the wicket yet: {e}");
                    continue;
                }
                Err(e) => println!("[{guid}] CELL: not removed from the wicket: {e}"),
            }
            self.store
                .update(|state| state.removals.retain(|kept| *kept != guid));
        }
    }

    /// The desired app of `guid` in the state now.
    pub fn desired_of(&self, state: &State, guid: &str) -> Option<AppBody> {
        state
            .app(guid)
            .and_then(|app| Controller::desired(state, app, &self.wicket.owner))
    }

    /// Puts the app `guid` as `desired` (or removes it) and records the
    /// outcome: what the process stats and the log show.
    pub async fn apply(&self, guid: &str, desired: Option<&AppBody>) -> Result<(), Failure> {
        let Some(app) = desired else {
            return match self.wicket.delete(guid).await {
                Ok(edited) => {
                    let was = self.store.note(|state| state.wicket.remove(guid));
                    if let Some(edited) = edited.filter(|_| was.is_some()) {
                        self.logs.add(
                            guid,
                            "CELL",
                            format!("Removed from the wicket: {}", edited.message),
                        );
                    }
                    Ok(())
                }
                Err(e) => {
                    self.logs
                        .add(guid, "CELL", format!("Not removed from the wicket: {e}"));
                    Err(e)
                }
            };
        };
        let mut app = app.clone();
        let mut put = self.wicket.put(guid, &app).await;
        if let Err(Failure::Refused { missing, .. }) = &put
            && !missing.is_empty()
        {
            // the droplet is not uploaded (or no more): upload it and put again
            for sha256 in missing.clone() {
                if let Err(e) = self.provide(&sha256, &mut app).await {
                    put = Err(e);
                    break;
                }
            }
            if put.as_ref().is_err_and(
                |e| matches!(e, Failure::Refused { missing, .. } if !missing.is_empty()),
            ) {
                put = self.wicket.put(guid, &app).await;
            }
        }
        let app = &app;
        let (outcome, line) = match &put {
            Ok(edited) => (
                Outcome::Running,
                format!(
                    "Deployed {} (sha256 {}) to the wicket as {}: {}",
                    app.component.name,
                    &app.component.sha256[..12],
                    app.hosts.join(", "),
                    edited.message
                ),
            ),
            Err(Failure::Refused { error, .. }) => (
                Outcome::Refused(error.clone()),
                format!("The wicket refused the app: {error}"),
            ),
            Err(Failure::Unreachable(e)) => (
                Outcome::Unreachable,
                format!("The wicket is unreachable, the app is put when it is back: {e}"),
            ),
        };
        self.record(guid, outcome, &line);
        put.map(|_| ())
    }

    /// Has the wicket hold the droplet `sha256` of `app`: uploaded when the cf-api has its
    /// bytes, else made again of the image it was pinned from (a wicket's own rootfs). A
    /// wicket of another guest makes another digest: the droplet and the running app are
    /// that one's from here on, a rebuild of the same image and guest the same.
    async fn provide(&self, sha256: &str, app: &mut AppBody) -> Result<(), Failure> {
        let path = self.store.droplet_path(sha256);
        match std::fs::read(&path) {
            Ok(component) => return self.wicket.upload(sha256, component).await,
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
                return Err(gone(sha256, &path, e));
            }
            Err(_) => {}
        }
        let image = self.store.read(|state| {
            let droplet = state.droplets.iter().find(|kept| kept.sha256 == sha256)?;
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
        let made = self.wicket.make_image(&image, credentials).await?;
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

    /// Records the outcome, logging it when it changed.
    fn record(&self, guid: &str, outcome: Outcome, line: &str) {
        let changed = self.store.note(|state| {
            if state.app(guid).is_none() {
                return false;
            }
            state.wicket.insert(guid.to_string(), outcome.clone()) != Some(outcome)
        });
        if changed {
            self.logs.add(guid, "CELL", line);
        }
    }

    /// The desired app of `guid` now, applied.
    pub async fn sync(&self, guid: &str) -> Result<(), Failure> {
        let desired = self.store.read(|state| self.desired_of(state, guid));
        self.apply(guid, desired.as_ref()).await
    }

    /// Puts / deletes the difference between the apps the wicket listed and the state, and
    /// logs the apps of the owner the state does not know. The listing is taken under the
    /// edit lock, which the caller holds: what an edit did while it waited is in it.
    async fn fix(&self, listed: &[ListedApp]) {
        let owner = &self.wicket.owner;
        let wanted: Vec<(String, Option<AppBody>)> = self.store.read(|state| {
            state
                .apps
                .iter()
                .map(|app| (app.guid.clone(), Controller::desired(state, app, owner)))
                .collect()
        });
        for (guid, desired) in &wanted {
            let listed = listed
                .iter()
                .find(|app| app.name == *guid && app.app.owner == *owner);
            match (desired, listed) {
                (Some(desired), Some(listed)) if same(&listed.app, desired) => {
                    self.record(guid, Outcome::Running, "Found in the wicket as wanted");
                }
                (Some(_), _) | (None, Some(_)) => {
                    let _ = self.apply(guid, desired.as_ref()).await;
                }
                (None, None) => {}
            }
        }
        let removing = self.store.read(|state| state.removals.clone());
        let strangers: BTreeSet<&str> = listed
            .iter()
            .filter(|app| app.app.owner == *owner)
            .map(|app| app.name.as_str())
            .filter(|name| !wanted.iter().any(|(guid, _)| guid == name))
            .filter(|name| !removing.iter().any(|guid| guid == name))
            .collect();
        let mut logged = self.strangers.lock().expect("no panic under the lock");
        logged.retain(|name| strangers.contains(name.as_str()));
        for name in strangers {
            if logged.insert(name.to_string()) {
                println!(
                    "[{name}] CELL: left in the wicket: this state did not put it (a lost \
                     --data-dir, another cf-api?); DELETE /api/apps/{name}?owner={owner} of the \
                     wicket removes it"
                );
            }
        }
    }

    /// Lists the wicket's apps and fixes the difference at startup, whenever the wicket's
    /// revision moves and when it answers again, then tries the pending removals and
    /// purges (after a hold too: a drained route's end moves no revision); while the wicket
    /// is unreachable, every started app is `STARTING`, and the wicket is tried again with
    /// a growing pause.
    pub async fn watch(self: Arc<Self>) {
        let mut after: Option<u64> = None;
        let mut pause = Duration::from_secs(1);
        loop {
            let listing = match self.wicket.list(after).await {
                Ok(listing) => listing,
                Err(e) => {
                    self.unreachable(&e);
                    // back, it is looked at whatever its revision: a wicket restarted
                    // may have come to the one it had
                    after = None;
                    tokio::time::sleep(pause).await;
                    pause = (pause * 2).min(Duration::from_secs(30));
                    continue;
                }
            };
            pause = Duration::from_secs(1);
            self.note_offer(&listing);
            let moved = after != Some(listing.revision);
            after = Some(listing.revision);
            let pending = self
                .store
                .read(|state| !state.purges.is_empty() || !state.removals.is_empty());
            if !moved && !pending {
                continue;
            }
            let _edit = self.edits.lock().await;
            if moved {
                match self.wicket.list(None).await {
                    Ok(current) => self.fix(&current.apps).await,
                    Err(_) => after = None,
                }
            }
            self.remove_pending().await;
            self.purge_pending().await;
        }
    }

    /// Every app the wicket should serve is `STARTING` until it answers.
    fn unreachable(&self, e: &Failure) {
        let guids: Vec<String> = self.store.read(|state| {
            state
                .apps
                .iter()
                .filter(|app| Controller::desired(state, app, &self.wicket.owner).is_some())
                .map(|app| app.guid.clone())
                .collect()
        });
        for guid in guids {
            self.record(
                &guid,
                Outcome::Unreachable,
                &format!("The wicket is unreachable, the app is put when it is back: {e}"),
            );
        }
    }
}

/// A rootfs's sandbox on a Firecracker wicket: `instances` VMs, as a process's are. One
/// is a shared VM, booted at once; more, a pool cloned from a snapshot; none still
/// serves, as on a wasm wicket: a VM of its own per connection.
fn vm_sandbox(memory_mib: u64, instances: u64, env: &BTreeMap<String, String>) -> Sandbox {
    let (mode, vms) = match instances {
        0 => ("per-connection", None),
        1 => ("shared", None),
        n => ("pool", Some(n)),
    };
    Sandbox {
        mode: Some(mode.into()),
        mem: Some(memory_mib),
        vms,
        env: env.clone(),
        ..Sandbox::default()
    }
}

/// The droplet `sha256` is neither where it was kept nor to be made again.
fn gone(sha256: &str, path: &std::path::Path, e: std::io::Error) -> Failure {
    Failure::Refused {
        status: 409,
        error: format!("the droplet {sha256} is gone from {}: {e}", path.display()),
        host: None,
        missing: Vec::new(),
    }
}

/// Whether the wicket serves `desired` as listed: a `budget_ms` left to
/// the wicket's default matches whatever it chose; the capabilities in any
/// order; the env, which the wicket lists by its names, by the tag.
fn same(listed: &AppBody, desired: &AppBody) -> bool {
    let sorted = |list: &[String]| {
        let mut list = list.to_vec();
        list.sort();
        list
    };
    sorted(&listed.hosts) == desired.hosts
        && listed.component == desired.component
        && sorted(&listed.sandbox.wasi) == sorted(&desired.sandbox.wasi)
        && listed.sandbox.pool == desired.sandbox.pool
        && listed.sandbox.instances == desired.sandbox.instances
        && listed.sandbox.memory_mib == desired.sandbox.memory_mib
        && desired
            .sandbox
            .budget_ms
            .is_none_or(|budget| listed.sandbox.budget_ms == Some(budget))
        && listed.sandbox.keyvalue == desired.sandbox.keyvalue
        && listed.sandbox.mode == desired.sandbox.mode
        && listed.sandbox.mem == desired.sandbox.mem
        && listed.sandbox.vms == desired.sandbox.vms
        && listed.tag == desired.tag
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::Running;

    #[test]
    fn an_app_is_desired_started_with_a_droplet_and_a_route() {
        let mut state = State::new("o", "s", &["localhost".into()]);
        let guid = state.create_app("hello").guid.clone();
        let domain = state.domains[0].guid.clone();
        let app = state.app(&guid).unwrap().clone();
        assert_eq!(Controller::desired(&state, &app, "cf"), None);
        let running = Running {
            droplet: "d".into(),
            sha256: "ab".repeat(32),
            file: "hello.wasm".into(),
            memory_mib: 64,
            budget_ms: Some(5000),
            instances: None,
            wasi: vec!["keyvalue".into(), "cli".into()],
            keyvalue: [(String::new(), "local/cf/i".to_string())].into(),
            env: [("GREETING".to_string(), "hello".to_string())].into(),
            tag: Some("t1".into()),
            since: 0,
        };
        state.app_mut(&guid).unwrap().running = Some(running);
        state.app_mut(&guid).unwrap().started = true;
        state.app_mut(&guid).unwrap().process.instances = 2;
        let app = state.app(&guid).unwrap().clone();
        assert_eq!(Controller::desired(&state, &app, "cf"), None, "no route");
        let route = state.create_route("hello", &domain).guid.clone();
        state.map(&route, &guid);
        let desired = Controller::desired(&state, state.app(&guid).unwrap(), "cf").unwrap();
        assert_eq!(desired.hosts, ["hello.localhost"]);
        assert_eq!(desired.sandbox.pool, Some(2));
        assert_eq!(desired.sandbox.memory_mib, Some(64));
        assert_eq!(desired.sandbox.keyvalue[""], "local/cf/i");
        // the env put, listed by its names: told by the tag
        let mut body = serde_json::to_value(&desired).unwrap();
        assert_eq!(
            (&body["sandbox"]["env"]["GREETING"], &body["tag"]),
            (&"hello".into(), &"t1".into())
        );
        body["sandbox"]["env"] = serde_json::json!(["GREETING"]);
        let mut listed: AppBody = serde_json::from_value(body).unwrap();
        assert!(same(&listed, &desired));
        listed.tag = Some("t0".into());
        assert!(!same(&listed, &desired), "another snapshot");
        listed.tag = desired.tag.clone();
        listed.sandbox.budget_ms = Some(5000);
        // the wicket lists the capabilities in its order
        listed.sandbox.wasi = vec!["cli".into(), "keyvalue".into()];
        assert!(same(&listed, &desired));
        listed.sandbox.pool = Some(3);
        assert!(!same(&listed, &desired));
        let mut unset = desired.clone();
        unset.sandbox.budget_ms = None;
        listed.sandbox.pool = Some(2);
        listed.sandbox.budget_ms = Some(10_000);
        assert!(same(&listed, &unset));

        // a rootfs is a Firecracker wicket's VMs, its env the same, the wasm options none
        let running = state.app_mut(&guid).unwrap().running.as_mut().unwrap();
        running.file = "hello.ext4".into();
        let desired = Controller::desired(&state, state.app(&guid).unwrap(), "cf").unwrap();
        let body = serde_json::to_value(&desired).unwrap();
        assert_eq!(
            body["sandbox"],
            serde_json::json!({"mode": "pool", "mem": 64, "vms": 2, "env": {"GREETING": "hello"}})
        );
        // as the wicket lists it: its vCPUs too, the env by its names
        let mut listed = body;
        listed["sandbox"]["vcpus"] = 1.into();
        listed["sandbox"]["env"] = serde_json::json!(["GREETING"]);
        let mut listed: AppBody = serde_json::from_value(listed).unwrap();
        assert!(same(&listed, &desired));
        listed.sandbox.vms = Some(3);
        assert!(!same(&listed, &desired));
        for (instances, mode, vms) in [
            (0, "per-connection", None),
            (1, "shared", None),
            (3, "pool", Some(3)),
        ] {
            let sandbox = vm_sandbox(64, instances, &BTreeMap::new());
            assert_eq!((sandbox.mode.as_deref(), sandbox.vms), (Some(mode), vms));
        }
        // a removal the wicket did not take is tried again, but for another owner's app
        let refused = |status| Failure::Refused {
            status,
            error: String::new(),
            host: None,
            missing: Vec::new(),
        };
        assert!(refused(422).retried());
        assert!(Failure::Unreachable(String::new()).retried());
        assert!(!refused(409).retried());

        state.app_mut(&guid).unwrap().started = false;
        assert_eq!(
            Controller::desired(&state, state.app(&guid).unwrap(), "cf"),
            None
        );
    }
}
